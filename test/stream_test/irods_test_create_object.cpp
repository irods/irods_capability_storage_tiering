#include <irods/client_connection.hpp>
#include <irods/dataObjClose.h>
#include <irods/dataObjCreate.h>
#include <irods/dataObjOpen.h>
#include <irods/irods_at_scope_exit.hpp>
#include <irods/rcMisc.h>
#include <irods/rodsClient.h>

#include <fmt/format.h>

#include <boost/program_options.hpp>

#include <cstdlib>
#include <cstring>
#include <exception>
#include <iostream>
#include <string>

// This program creates an empty iRODS data object at the specified logical path.
//
// Assumes an authenticated iRODS client environment is available in the executing user's ~/.irods directory.
//
// NOTE: This program is only intended for testing. Do not use in production.

int main(int argc, char** argv)
{
    try {
        // Parse command line options to get the path and number of bytes to read.
        namespace po = boost::program_options;

        constexpr const char* object_path_option_name = "logical-path";
        constexpr const char* use_create_api_option_name = "use-create-api";
        constexpr const char* target_resource_option_name = "resource";

        std::string path;
        std::string target_resource;
        bool use_create_api = false;

        po::options_description od("options");
        // clang-format off
        od.add_options()("help", "produce help message")
            (object_path_option_name, po::value<std::string>(&path), "logical path to iRODS data object to create")
            (target_resource_option_name, po::value<std::string>(&target_resource), "target resource for new replica")
            (use_create_api_option_name, po::value<bool>(&use_create_api), "use create API instead of open API");
        // clang-format on

        po::positional_options_description pd;
        pd.add(object_path_option_name, 1);

        po::variables_map vm;
        po::store(po::command_line_parser(argc, argv).options(od).positional(pd).run(), vm);
        po::notify(vm);

        const auto print_usage = [&od] {
            std::cout << "NOTE: This program is only intended for testing. Do not use in production.\n";
            std::cout << "Usage: irods_test_create_object [OPTION] ... [DATA_OBJECT_PATH]\n";
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

        // Load client-side API plugins so that we can authenticate.
        load_client_api_plugins();

        // Create a connection using the local client environment. If none exists, an error will occur.
        irods::experimental::client_connection conn;
        RcComm& comm = static_cast<RcComm&>(conn);

        // Create the data object using the specified API endpoint.
        DataObjInp open_inp{};
        const auto free_open_inp_cond_input = irods::at_scope_exit{[&open_inp] { clearKeyVal(&open_inp.condInput); }};
        std::strncpy(open_inp.objPath, path.c_str(), sizeof(open_inp.objPath));
        open_inp.openFlags = O_CREAT | O_TRUNC | O_WRONLY;
        if (!target_resource.empty()) {
            addKeyVal(&open_inp.condInput, RESC_NAME_KW, target_resource.c_str());
        }
        const auto fd = use_create_api ? rcDataObjCreate(&comm, &open_inp) : rcDataObjOpen(&comm, &open_inp);
        if (fd < 0) {
            fmt::print(stderr, "Failed to open data object [{}]. ec=[{}]\n", path, fd);
            return 1;
        }

        // Close data object.
        OpenedDataObjInp close_inp{};
        close_inp.l1descInx = fd;
        if (const auto ec = rcDataObjClose(&comm, &close_inp); ec < 0) {
            fmt::print(stderr, "Failed to close data object [{}]. ec=[{}]\n", path, ec);
            return 1;
        }
    }
    catch (const std::exception& e) {
        fmt::print(stderr, "Caught exception: {}\n", e.what());
        return 1;
    }

    return 0;
} // main
