import argparse
import glob
import multiprocessing
import os
import tempfile

import irods_python_ci_utilities

def install_building_dependencies(externals_directory):
    externals_list = [
        'irods-externals-boost1.81.0-2',
        'irods-externals-clang16.0.6-0',
        'irods-externals-fmt8.1.1-2',
        'irods-externals-json3.10.4-0'
    ]
    if externals_directory == 'None' or externals_directory is None:
        irods_python_ci_utilities.install_irods_core_dev_repository()
        irods_python_ci_utilities.install_os_packages(externals_list)
    else:
        package_suffix = irods_python_ci_utilities.get_package_suffix()
        os_specific_directory = irods_python_ci_utilities.append_os_specific_directory(externals_directory)
        externals = []
        for irods_externals in externals_list:
            externals.append(glob.glob(os.path.join(os_specific_directory, irods_externals + '*.{0}'.format(package_suffix)))[0])
        irods_python_ci_utilities.install_os_packages_from_files(externals)
    install_os_specific_dependencies()

def install_os_specific_dependencies_apt():
    irods_python_ci_utilities.install_os_packages(['libcurl4-gnutls-dev', 'cmake', 'make', 'libssl-dev', 'gcc'])

def install_os_specific_dependencies_yum():
    irods_python_ci_utilities.install_os_packages(['cmake', 'make', 'curl-devel', 'openssl-devel'])

def install_os_specific_dependencies():
    dispatch_map = {
        'Almalinux': install_os_specific_dependencies_yum,
        'Centos linux': install_os_specific_dependencies_yum,
        'Centos': install_os_specific_dependencies_yum,
        'Debian gnu_linux': install_os_specific_dependencies_apt,
        'Opensuse ': install_os_specific_dependencies_yum,
        'Rocky linux': install_os_specific_dependencies_yum,
        'Ubuntu': install_os_specific_dependencies_apt
    }
    try:
        return dispatch_map[irods_python_ci_utilities.get_distribution()]()
    except KeyError:
        irods_python_ci_utilities.raise_not_implemented_for_distribution()

def copy_output_packages(build_directory, output_root_directory):
    irods_python_ci_utilities.gather_files_satisfying_predicate(
        build_directory,
        irods_python_ci_utilities.append_os_specific_directory(output_root_directory),
        lambda s:s.endswith(irods_python_ci_utilities.get_package_suffix()))

def main(build_directory, output_root_directory, irods_packages_root_directory, externals_directory, debug_build=False, enable_asan=False, build_test_executables=True):
    install_building_dependencies(externals_directory)
    if irods_packages_root_directory:
        irods_python_ci_utilities.install_irods_dev_and_runtime_packages(irods_packages_root_directory)
    build_directory = os.path.abspath(build_directory or tempfile.mkdtemp(prefix='irods_storage_tiering_plugin_build_directory'))
    build_type = "Debug" if debug_build else "Release"
    cmake_options = [f"-DCMAKE_BUILD_TYPE={build_type}"]
    if enable_asan:
        cmake_options.append("-DIRODS_ENABLE_ADDRESS_SANITIZER=YES")
    cmake_options.append("-DIRODS_TEST_EXECUTABLES_BUILD={}".format("YES" if build_test_executables else "NO"))
    cmake_command = ['cmake', os.path.dirname(os.path.realpath(__file__))] + cmake_options
    irods_python_ci_utilities.subprocess_get_output(cmake_command, check_rc=True, cwd=build_directory)
    irods_python_ci_utilities.subprocess_get_output(['make', '-j', str(multiprocessing.cpu_count()), 'package'], check_rc=True, cwd=build_directory)
    if output_root_directory:
        copy_output_packages(build_directory, output_root_directory)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Build unified storage tiering plugin.')
    parser.add_argument('--build_directory')
    parser.add_argument('--output_root_directory')
    parser.add_argument('--irods_packages_root_directory')
    parser.add_argument('--externals_packages_directory')
    parser.add_argument('--debug_build', action='store_true')
    parser.add_argument("--enable_address_sanitizer", dest="enable_asan", action="store_true")
    parser.add_argument("--exclude_test_executables", dest="build_test_executables", action="store_false")
    args = parser.parse_args()

    main(args.build_directory,
         args.output_root_directory,
         args.irods_packages_root_directory,
         args.externals_packages_directory,
         args.debug_build,
         args.enable_asan,
         args.build_test_executables)
