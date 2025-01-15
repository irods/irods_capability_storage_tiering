#include <irods/irods_at_scope_exit.hpp>
#include <irods/client_connection.hpp>
#include <irods/dataObjClose.h>
#include <irods/dataObjOpen.h>
#include <irods/dataObjRead.h>
#include <irods/rodsClient.h>

#include <fmt/format.h>

#include <boost/program_options.hpp>

#include <cstdlib>
#include <cstring>
#include <exception>
#include <iostream>
#include <string>

// This program opens the specified iRODS data object, reads the specified number of bytes, and prints it to stdout.
//
// Assumes an authenticated iRODS client environment is available in the executing user's ~/.irods directory.
// The program prints the contents to stdout. Would not recommend using this on anything other than text files.
//
// NOTE: This program is only intended for testing. Do not use in production.

int main(int argc, char** argv)
{
    try {
        // Parse command line options to get the path and number of bytes to read.
        namespace po = boost::program_options;

        constexpr const char* object_path_option_name = "logical-path";
        constexpr const char* read_len_option_name = "read-len";
        // This value has no special meaning.
        constexpr int default_read_len = 100;

        std::string path;
        int read_len;

        po::options_description od("options");
        // clang-format off
        od.add_options()("help", "produce help message")
            (object_path_option_name, po::value<std::string>(&path), "logical path to iRODS data object to read")
            (read_len_option_name, po::value<int>(&read_len)->default_value(default_read_len), "number of bytes to read");
        // clang-format on

        po::positional_options_description pd;
        pd.add(object_path_option_name, 1);

        po::variables_map vm;
        po::store(po::command_line_parser(argc, argv).options(od).positional(pd).run(), vm);
        po::notify(vm);

        const auto print_usage = [&od] {
            std::cout << "NOTE: This program is only intended for testing. Do not use in production.\n";
            std::cout << "Usage: irods_test_read_object [OPTION] ... [DATA_OBJECT_PATH]\n";
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

        // Open data object.
        DataObjInp open_inp{};
        std::strncpy(open_inp.objPath, path.c_str(), sizeof(open_inp.objPath));
        open_inp.openFlags = O_RDONLY;
        const auto fd = rcDataObjOpen(&comm, &open_inp);
        if (fd < 0) {
            fmt::print(stderr, "Failed to open data object [{}]. ec=[{}]\n", path, fd);
            return 1;
        }

        // Read data object.
        OpenedDataObjInp read_inp{};
        read_inp.l1descInx = fd;
        read_inp.len = read_len;
        // The program is going to print the contents as a string, so +1 for null terminator.
        const auto buf_len = read_inp.len + 1;
        bytesBuf_t read_bbuf{};
        const auto free_buf = irods::at_scope_exit{[&read_bbuf] { std::free(read_bbuf.buf); }};
        read_bbuf.len = buf_len;
        read_bbuf.buf = std::malloc(buf_len);
        std::memset(read_bbuf.buf, 0, buf_len);
        if (const auto ec = rcDataObjRead(&comm, &read_inp, &read_bbuf); ec < 0) {
            fmt::print(stderr, "Failed to read data object [{}]. ec=[{}]\n", path, ec);
            return 1;
        }

        // Close data object.
        OpenedDataObjInp close_inp{};
        close_inp.l1descInx = fd;
        if (const auto ec = rcDataObjClose(&comm, &close_inp); ec < 0) {
            fmt::print(stderr, "Failed to close data object [{}]. ec=[{}]\n", path, ec);
            return 1;
        }

        // Print the contents.
        const auto object_contents = std::string{static_cast<char*>(read_bbuf.buf)};
        fmt::print("{}\n", object_contents);
    }
    catch (const std::exception& e) {
        fmt::print(stderr, "Caught exception: {}\n", e.what());
        return 1;
    }

    return 0;
} // main
