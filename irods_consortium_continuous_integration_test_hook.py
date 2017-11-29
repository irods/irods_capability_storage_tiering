from __future__ import print_function

import optparse
import os
import shutil
import glob
import time

import irods_python_ci_utilities


def get_build_prerequisites_all():
    return[]


def get_build_prerequisites_apt():
    if irods_python_ci_utilities.get_distribution_version_major() == '12':
        return['openjdk-7-jre']+get_build_prerequisites_all()

    else:
        return['default-jre']+get_build_prerequisites_all()


def get_build_prerequisites_yum():
    return['java-1.7.0-openjdk-devel']+get_build_prerequisites_all()


def get_build_prerequisites_zypper():
    return['java-1.7.0-openjdk-devel']+get_build_prerequisites_all()


def get_build_prerequisites():
    dispatch_map = {
        'Ubuntu': get_build_prerequisites_apt,
        'Centos': get_build_prerequisites_yum,
        'Centos linux': get_build_prerequisites_yum,
        'Opensuse': get_build_prerequisites_zypper,
    }
    try:
        return dispatch_map[irods_python_ci_utilities.get_distribution()]()
    except KeyError:
        irods_python_ci_utilities.raise_not_implemented_for_distribution()


def install_build_prerequisites():
    irods_python_ci_utilities.subprocess_get_output(['sudo', '-EH', 'pip', 'install', 'stomp.py==4.1.17'])
    irods_python_ci_utilities.subprocess_get_output(['sudo', '-EH', 'pip', 'install', 'unittest-xml-reporting==1.14.0'])

    if irods_python_ci_utilities.get_distribution() == 'Ubuntu': # cmake from externals requires newer libstdc++ on ub12
        if irods_python_ci_utilities.get_distribution_version_major() == '12':
            irods_python_ci_utilities.install_os_packages(['python-software-properties'])
            irods_python_ci_utilities.subprocess_get_output(['sudo', 'add-apt-repository', '-y', 'ppa:ubuntu-toolchain-r/test'], check_rc=True)
            irods_python_ci_utilities.subprocess_get_output(['sudo', 'update-java-alternatives', '--set', 'java-1.7.0-openjdk-amd64'])
            irods_python_ci_utilities.install_os_packages(['libstdc++6'])

    irods_python_ci_utilities.install_os_packages(get_build_prerequisites())


def install_messaging_package(message_broker):
    if 'apache-activemq-' in message_broker:
        version_number = message_broker.split('-')[2]
        tarfile = message_broker + '-bin.tar.gz'
        url = 'http://archive.apache.org/dist/activemq/' + version_number + '/' + tarfile
        activemq_dir = message_broker + '/bin/activemq'

        irods_python_ci_utilities.subprocess_get_output(['wget', url])
        irods_python_ci_utilities.subprocess_get_output(['tar', 'xvfz', tarfile])
        irods_python_ci_utilities.subprocess_get_output([activemq_dir, 'start'])



def main():
    parser = optparse.OptionParser()
    parser.add_option('--output_root_directory')
    parser.add_option('--built_packages_root_directory')
    parser.add_option('--message_broker', default='apache-activemq-5.14.1', help='MQ server package name that needs to be tested')
    options, _ = parser.parse_args()

    output_root_directory = options.output_root_directory
    built_packages_root_directory = options.built_packages_root_directory
    package_suffix = irods_python_ci_utilities.get_package_suffix()
    os_specific_directory = irods_python_ci_utilities.append_os_specific_directory(built_packages_root_directory)

    irods_python_ci_utilities.install_os_packages_from_files(glob.glob(os.path.join(os_specific_directory, 'irods-rule-engine-plugin-tiered_storage*.{0}'.format(package_suffix))))

    install_build_prerequisites()
    install_messaging_package(options.message_broker)

    time.sleep(10)

    try:
        test_output_file = 'log/test_output.log'
        irods_python_ci_utilities.subprocess_get_output(['sudo', 'su', '-', 'irods', '-c', 'python2 scripts/run_tests.py --xml_output --run_s=test_plugin_tiered_storage 2>&1 | tee {0}; exit $PIPESTATUS'.format(test_output_file)], check_rc=True)
    finally:
        if output_root_directory:
            irods_python_ci_utilities.gather_files_satisfying_predicate('/var/lib/irods/log', output_root_directory, lambda x: True)
            shutil.copy('/var/lib/irods/log/test_output.log', output_root_directory)
            shutil.copytree('/var/lib/irods/test-reports', os.path.join(output_root_directory, 'test-reports'))


if __name__ == '__main__':
    main()
