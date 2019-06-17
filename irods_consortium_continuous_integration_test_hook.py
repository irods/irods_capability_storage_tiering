from __future__ import print_function

import optparse
import os
import shutil
import glob
import time
import irods_python_ci_utilities

def main():
    parser = optparse.OptionParser()
    parser.add_option('--output_root_directory')
    parser.add_option('--built_packages_root_directory')
    parser.add_option('--mungefs_packages_directory')

    options, _ = parser.parse_args()

    output_root_directory = options.output_root_directory
    built_packages_root_directory = options.built_packages_root_directory
    package_suffix = irods_python_ci_utilities.get_package_suffix()
    os_specific_directory = irods_python_ci_utilities.append_os_specific_directory(built_packages_root_directory)
    irods_python_ci_utilities.install_os_packages_from_files(glob.glob(os.path.join(os_specific_directory, 'irods-rule-engine-plugin-apply-access-time*.{0}'.format(package_suffix))))
    irods_python_ci_utilities.install_os_packages_from_files(glob.glob(os.path.join(os_specific_directory, 'irods-rule-engine-plugin-data-movement*.{0}'.format(package_suffix))))
    irods_python_ci_utilities.install_os_packages_from_files(glob.glob(os.path.join(os_specific_directory, 'irods-rule-engine-plugin-data-replication*.{0}'.format(package_suffix))))
    irods_python_ci_utilities.install_os_packages_from_files(glob.glob(os.path.join(os_specific_directory, 'irods-rule-engine-plugin-data-verification*.{0}'.format(package_suffix))))
    irods_python_ci_utilities.install_os_packages_from_files(glob.glob(os.path.join(os_specific_directory, 'irods-rule-engine-plugin-storage-tiering*.{0}'.format(package_suffix))))

    time.sleep(10)
    irods_python_ci_utilities.subprocess_get_output(['sudo', 'chmod', 'g+rwx', '/dev/fuse'], check_rc=True)

    time.sleep(10)

    try:
        test_output_file = 'log/test_output.log'
        irods_python_ci_utilities.subprocess_get_output(['sudo', 'su', '-', 'irods', '-c', 'python2 scripts/run_tests.py --xml_output --run_s test_plugin_storage_tiering 2>&1 | tee {0}; exit $PIPESTATUS'.format(test_output_file)], check_rc=True)
    finally:
        if output_root_directory:
            irods_python_ci_utilities.gather_files_satisfying_predicate('/var/lib/irods/log', output_root_directory, lambda x: True)
            shutil.copy('/var/lib/irods/log/test_output.log', output_root_directory)
            #shutil.copytree('/var/lib/irods/test-reports', os.path.join(output_root_directory, 'test-reports'))


if __name__ == '__main__':
    main()
