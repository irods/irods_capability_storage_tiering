from __future__ import print_function

import argparse
import os
import shutil
import glob
import irods_python_ci_utilities

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output_root_directory', type=str)
    parser.add_argument('--built_packages_root_directory', type=str, required=True)
    parser.add_argument('--test', metavar='dotted name', type=str)
    parser.add_argument('--skip-setup', action='store_false', dest='do_setup', default=True)

    args = parser.parse_args()

    built_packages_root_directory = args.built_packages_root_directory
    package_suffix = irods_python_ci_utilities.get_package_suffix()
    os_specific_directory = irods_python_ci_utilities.append_os_specific_directory(built_packages_root_directory)

    if args.do_setup:
        irods_python_ci_utilities.subprocess_get_output(['sudo', '-EH', 'pip', 'install', 'unittest-xml-reporting==1.14.0'])

        irods_python_ci_utilities.install_os_packages_from_files(
            glob.glob(os.path.join(os_specific_directory,
                                   'irods-rule-engine-plugin-unified-storage-tiering*.{}'.format(package_suffix))
            )
        )

    test = args.test or 'test_plugin_unified_storage_tiering'

    try:
        test_output_file = '/var/lib/irods/log/test_output.log'

        irods_python_ci_utilities.subprocess_get_output(['sudo', 'su', '-', 'irods', '-c',
            'python2 scripts/run_tests.py --xml_output --run_s {} 2>&1 | tee {}; exit $PIPESTATUS'.format(test, test_output_file)],
            check_rc=True)
    finally:
        output_root_directory = args.output_root_directory
        if output_root_directory:
            irods_python_ci_utilities.gather_files_satisfying_predicate('/var/lib/irods/log', output_root_directory, lambda x: True)
            shutil.copy('/var/lib/irods/log/test_output.log', output_root_directory)


if __name__ == '__main__':
    main()
