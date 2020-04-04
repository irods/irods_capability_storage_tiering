from __future__ import print_function

import argparse
import os
import shutil
import glob
import time
import irods_python_ci_utilities

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output_root_directory', type=str, required=True)
    parser.add_argument('--built_packages_root_directory', type=str, required=True)
    parser.add_argument('--munge_path', type=str, default=None, help='munge externals path')
    parser.add_argument('--test_unified_storage_tiering', type=str, default=None, help='should be either True or False')

    args = parser.parse_args()

    output_root_directory = args.output_root_directory
    built_packages_root_directory = args.built_packages_root_directory
    package_suffix = irods_python_ci_utilities.get_package_suffix()
    os_specific_directory = irods_python_ci_utilities.append_os_specific_directory(built_packages_root_directory)
    irods_python_ci_utilities.subprocess_get_output(['sudo', '-EH', 'pip', 'install', 'unittest-xml-reporting==1.14.0'])
    irods_python_ci_utilities.install_os_packages_from_files(glob.glob(os.path.join(os_specific_directory, 'irods-rule-engine-plugin-unified-storage-tiering*')))
    
    test_name = 'test_plugin_unified_storage_tiering'

    time.sleep(10)
    irods_python_ci_utilities.subprocess_get_output(['sudo', 'chmod', 'g+rwx', '/dev/fuse'], check_rc=True)

    time.sleep(10)

    try:
        test_output_file = '/var/lib/irods/log/test_output.log'

        if args.munge_path is not None or args.munge_path != '':
            irods_python_ci_utilities.subprocess_get_output(['sudo', 'su', '-', 'irods', '-c', 'cd scripts; {0}; python2 run_tests.py --xml_output --run_s {1} 2>&1 | tee {2}; exit $PIPESTATUS'.format(args.munge_path, test_name, test_output_file)], check_rc=True)
        else:
            irods_python_ci_utilities.subprocess_get_output(['sudo', 'su', '-', 'irods', '-c', 'python2 scripts/run_tests.py --xml_output --run_s {0} 2>&1 | tee {1}; exit $PIPESTATUS'.format(test_name, test_output_file)], check_rc=True)
    finally:
        if output_root_directory:
            irods_python_ci_utilities.gather_files_satisfying_predicate('/var/lib/irods/log', output_root_directory, lambda x: True)
            shutil.copy('/var/lib/irods/log/test_output.log', output_root_directory)
            shutil.copytree('/var/lib/irods/test-reports', os.path.join(output_root_directory, 'test-reports'))


if __name__ == '__main__':
    main()
