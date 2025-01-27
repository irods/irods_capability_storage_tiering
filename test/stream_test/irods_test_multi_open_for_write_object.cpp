#include <irods/connection_pool.hpp>
#include <irods/dataObjWrite.h>
#include <irods/fully_qualified_username.hpp>
#include <irods/irods_at_scope_exit.hpp>
#include <irods/replica_close.h>
#include <irods/replica_open.h>
#include <irods/rodsClient.h>

#include <fmt/format.h>
#include <nlohmann/json.hpp>

#include <boost/program_options.hpp>

#include <cstdlib>
#include <cstring>
#include <exception>
#include <iostream>
#include <string>

// This program opens the specified existing iRODS data object a number of times, simultaneously, and then closes it.
//
// Assumes an authenticated iRODS client environment is available in the executing user's ~/.irods directory.
//
// NOTE: This program is only intended for testing. Do not use in production.

namespace
{
    struct open_input
    {
        // Logical path of the iRODS data object being opened.
        std::string path;

        // Root resource to target for open.
        std::string resource;

        // Replica token to use if the object has already been opened.
        std::string replica_token;

        // Replica number to target for open.
        int replica_number = -1;
    };

    auto open_replica(RcComm* _comm, const open_input& _inp, char** _out) -> int
    {
        DataObjInp open_inp{};
        const auto free_input_and_output = irods::at_scope_exit{[&open_inp] { clearKeyVal(&open_inp.condInput); }};

        std::strncpy(open_inp.objPath, _inp.path.c_str(), sizeof(open_inp.objPath));

        // For this test application, we are only interested in opening for write. This does not create data objects.
        // This test application also does not actually write anything to the opened replica, so there is no seeking
        // or truncating.
        open_inp.openFlags = O_WRONLY;

        if (!_inp.resource.empty()) {
            addKeyVal(&open_inp.condInput, RESC_NAME_KW, _inp.resource.c_str());
        }

        if (!_inp.replica_token.empty()) {
            addKeyVal(&open_inp.condInput, REPLICA_TOKEN_KW, _inp.replica_token.c_str());
        }

        // We allow resource and replica number to be specified at the same time in this application because the
        // open API will return an error if these are used together.
        if (_inp.replica_number > 0) {
            addKeyVal(&open_inp.condInput, REPL_NUM_KW, std::to_string(_inp.replica_number).c_str());
        }

        const auto fd = rc_replica_open(_comm, &open_inp, _out);
        if (fd < 0) {
            fmt::print(stderr, "Failed to open data object [{}]. ec=[{}]\n", _inp.path, fd);
        }
        return fd;
    } // open_replica

    auto close_replica(RcComm* _comm, int fd, bool finalize = false) -> int
    {
        // Close data object.
        auto inp = nlohmann::json{{"fd", fd}};
        if (!finalize) {
            inp["update_status"] = false;
            inp["update_size"] = false;
            inp["send_notifications"] = false;
        }
        if (const auto ec = rc_replica_close(_comm, inp.dump().c_str()); ec < 0) {
            return ec;
        }
        return 0;
    } // close_replica
} // anonymous namespace

int main(int argc, char** argv)
{
    try {
        // Parse command line options to get the path and number of bytes to read.
        namespace po = boost::program_options;

        constexpr const char* object_path_option_name = "logical-path";
        constexpr const char* open_count_option_name = "open-count";
        constexpr const char* target_resource_option_name = "resource";
        // This value has no special meaning.
        constexpr int default_open_count = 1;

        std::string path;
        std::string target_resource;
        int open_count;

        po::options_description od("options");
        // clang-format off
        od.add_options()("help", "produce help message")
            (object_path_option_name, po::value<std::string>(&path), "logical path to iRODS data object to read")
            (target_resource_option_name, po::value<std::string>(&target_resource), "resource for target replica")
            (open_count_option_name, po::value<int>(&open_count)->default_value(default_open_count), "number of times to open the data object");
        // clang-format on

        po::positional_options_description pd;
        pd.add(object_path_option_name, 1);

        po::variables_map vm;
        po::store(po::command_line_parser(argc, argv).options(od).positional(pd).run(), vm);
        po::notify(vm);

        const auto print_usage = [&od] {
            std::cout << "NOTE: This program is only intended for testing. Do not use in production.\n";
            std::cout << "Usage: irods_test_multi_open_for_write_object [OPTION] ... [DATA_OBJECT_PATH]\n";
            std::cout << od << "\n";
        };

        // --help option prints the usage text...
        if (vm.count("help")) {
            print_usage();
            return 0;
        }

        // Path to the data object is required. Hard stop if it's not there.
        if (0 == vm.count(object_path_option_name)) {
            print_usage();
            return 1;
        }

        if (open_count < 1) {
            fmt::print("{} must be a positive integer.", open_count_option_name);
            return 1;
        }

        rodsEnv env;
        _getRodsEnv(env);

        // Load client-side API plugins so that we can authenticate.
        load_client_api_plugins();

        // Create a connection using the local client environment. If none exists, an error will occur.
        auto conn_pool =
            irods::connection_pool{open_count,
                                   env.rodsHost,
                                   env.rodsPort,
                                   irods::experimental::fully_qualified_username{env.rodsUserName, env.rodsZone}};

        // Make a map of RcComm pointers to file descriptors. This allows us to keep track of the file descriptor for
        // each RcComm. The file descriptors here will all refer to secondary opens.
        std::map<RcComm*, int> comms_to_file_descriptors;
        const auto disconnect = irods::at_scope_exit{[&comms_to_file_descriptors] {
            for (auto& [comm, fd] : comms_to_file_descriptors) {
                if (comm) {
                    rcDisconnect(comm);
                }
            }
        }};

        // Keep the file descriptor info from the opened replica for the replica number and replica token.
        char* open_output = nullptr;
        const auto free_open_output = irods::at_scope_exit{[&open_output] { std::free(open_output); }};

        // Open data object for the first time. It is called the "primary" file descriptor and RcComm because this
        // file descriptor will be the last one to be closed and is responsible for finalizing the data object.
        auto primary_conn = conn_pool.get_connection();
        auto* primary_comm = static_cast<RcComm*>(primary_conn);
        const auto primary_fd = open_replica(primary_comm, open_input{path, target_resource}, &open_output);
        if (primary_fd < 0) {
            return 1;
        }

        const auto fd_info = nlohmann::json::parse(open_output);

        // Get replica token for the open file descriptor. This may or may not be needed.
        std::string replica_token;
        if (const auto replica_token_iter = fd_info.find("replica_token"); fd_info.end() != replica_token_iter) {
            replica_token = replica_token_iter->get<std::string>();
        }

        // Get replica number so that we are targeting the same replica.
        const auto replica_number = fd_info.at("data_object_info").at("replica_number").get<int>();

        // open_count - 1 accounts for the connection which was already used above.
        bool open_object_failed = false;
        for (int i = 0; i < open_count - 1; ++i) {
            auto conn = conn_pool.get_connection();
            auto* comm = conn.release();

            char* out = nullptr;
            const auto free_out = irods::at_scope_exit{[&out] { std::free(out); }};
            const auto fd = open_replica(
                comm, open_input{.path = path, .replica_token = replica_token, .replica_number = replica_number}, &out);
            if (fd < 0) {
                open_object_failed = true;
                // Disconnect here because it was never inserted into the map despite being released above.
                if (comm) {
                    rcDisconnect(comm);
                }
                break;
            }

            // Release each connection so that the file descriptor map owns them. The connections will be disconnected
            // by an irods::at_scope_exit defined above.
            comms_to_file_descriptors[comm] = fd;
        }

        // Close the secondary file descriptors.
        bool close_object_failed = false;
        for (auto& [comm, fd] : comms_to_file_descriptors) {
            if (const auto ec = close_replica(comm, fd); ec < 0) {
                fmt::print(stderr, "Failed to close file descriptor for [{}]. ec=[{}]\n", path, ec);
                close_object_failed = true;
            }
        }

        // Close the primary file descriptor.
        if (const auto ec = close_replica(primary_comm, primary_fd, true); ec < 0) {
            fmt::print(stderr, "Failed to close primary file descriptor for [{}]. ec=[{}]\n", path, ec);
            return 1;
        }

        // If opening or closing any of the secondary file descriptors failed, exit with an error code.
        if (open_object_failed || close_object_failed) {
            fmt::print(stderr, "A call to open or close the replica failed.\n");
            return 1;
        }
    }
    catch (const std::exception& e) {
        fmt::print(stderr, "Caught exception: {}\n", e.what());
        return 1;
    }

    return 0;
} // main
