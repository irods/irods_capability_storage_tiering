from __future__ import print_function

import multiprocessing
import optparse
import os
import shutil
import glob
import time
import tempfile
import re
import irods_python_ci_utilities

def install_cmake_and_add_to_front_of_path():
    irods_python_ci_utilities.install_os_packages(['irods-externals-cmake3.5.2-0'])
    os.environ['PATH'] = '/opt/irods-externals/cmake3.5.2-0/bin' + os.pathsep + os.environ['PATH']

def get_build_prerequisites_all():
    return ['irods-externals-clang3.8-0',
            'irods-externals-cppzmq4.1-0',
            'irods-externals-libarchive3.1.2-0',
            'irods-externals-avro1.7.7-0',
            'irods-externals-clang-runtime3.8-0',
            'irods-externals-boost1.60.0-0',
            'irods-externals-jansson2.7-0',
            'irods-externals-zeromq4-14.1.3-0']

def get_build_prerequisites_apt():
    return ['libstdc++6', 'libcurl4-gnutls-dev', 'make', 'libssl-dev', 'gcc', 'g++', 'libfuse-dev'] + get_build_prerequisites_all()

def get_build_prerequisites_yum():
    return ['curl-devel', 'openssl-devel', 'gcc-g++', 'fuse', 'fuse-devel'] + get_build_prerequisites_all()

def get_build_prerequisites():
    dispatch_map = {
        'Ubuntu': get_build_prerequisites_apt,
        'Centos': get_build_prerequisites_yum,
        'Centos linux': get_build_prerequisites_yum,
        'Opensuse ': get_build_prerequisites_yum,
    }
    try:
        return dispatch_map[irods_python_ci_utilities.get_distribution()]()
    except KeyError:
        irods_python_ci_utilities.raise_not_implemented_for_distribution()

def install_build_prerequisites_apt():
    if irods_python_ci_utilities.get_distribution() == 'Ubuntu': # cmake from externals requires newer libstdc++ on ub12
        if irods_python_ci_utilities.get_distribution_version_major() == '12':
            irods_python_ci_utilities.install_os_packages(['python-software-properties'])
            irods_python_ci_utilities.subprocess_get_output(['sudo', 'add-apt-repository', '-y', 'ppa:ubuntu-toolchain-r/test'], check_rc=True)
            irods_python_ci_utilities.install_os_packages(['libstdc++6'])
    irods_python_ci_utilities.install_os_packages(get_build_prerequisites())

def install_build_prerequisites_yum():
    irods_python_ci_utilities.install_os_packages(get_build_prerequisites())

def install_build_prerequisites():
    dispatch_map = {
        'Ubuntu': install_build_prerequisites_apt,
        'Centos': install_build_prerequisites_yum,
        'Centos linux': install_build_prerequisites_yum,
        'Opensuse ': install_build_prerequisites_yum,
    }
    try:
        return dispatch_map[irods_python_ci_utilities.get_distribution()]()
    except KeyError:
        irods_python_ci_utilities.raise_not_implemented_for_distribution()

def main():
    parser = optparse.OptionParser()
    parser.add_option('--output_root_directory')
    parser.add_option('--built_packages_root_directory')
    parser.add_option('--mungefs_packages_dir')

    options, _ = parser.parse_args()

    output_root_directory = options.output_root_directory
    built_packages_root_directory = options.built_packages_root_directory
    package_suffix = irods_python_ci_utilities.get_package_suffix()
    os_specific_directory = irods_python_ci_utilities.append_os_specific_directory(built_packages_root_directory)
    irods_python_ci_utilities.install_os_packages_from_files(glob.glob(os.path.join(os_specific_directory, 'irods-rule-engine-plugin-tiered-storage*.{0}'.format(package_suffix))))

    irods_python_ci_utilities.install_irods_core_dev_repository()
    install_cmake_and_add_to_front_of_path()
    install_build_prerequisites()

    time.sleep(10)
    irods_python_ci_utilities.subprocess_get_output(['sudo', 'chmod', 'g+rwx', '/dev/fuse'], check_rc=True)

    time.sleep(10)

    try:
        test_output_file = 'log/test_output.log'
        irods_python_ci_utilities.subprocess_get_output(['sudo', 'su', '-', 'irods', '-c', 'python2 scripts/run_tests.py --xml_output --use_mungefs --run_s test_plugin_tiered_storage 2>&1 | tee {0}; exit $PIPESTATUS'.format(test_output_file)], check_rc=True)
    finally:
        if output_root_directory:
            irods_python_ci_utilities.gather_files_satisfying_predicate('/var/lib/irods/log', output_root_directory, lambda x: True)
            shutil.copy('/var/lib/irods/log/test_output.log', output_root_directory)
            #shutil.copytree('/var/lib/irods/test-reports', os.path.join(output_root_directory, 'test-reports'))


if __name__ == '__main__':
    main()
