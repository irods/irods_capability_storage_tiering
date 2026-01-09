import os
import sys
import shutil
import contextlib
import os.path
import unittest

import time

from ..controller import IrodsController
from ..configuration import IrodsConfig
from .resource_suite import ResourceBase
from . import session
from .. import test
from .. import paths
from .. import lib

@contextlib.contextmanager
def storage_tiering_configured_custom(arg=None, sleep_time=1):
    filename = paths.server_config_path()
    with lib.file_backed_up(filename):
        irods_config = IrodsConfig()
        irods_config.server_config['advanced_settings']['delay_server_sleep_time_in_seconds'] = sleep_time

        irods_config.server_config['plugin_configuration']['rule_engines'].insert(0,
            {
                "instance_name" : "irods_rule_engine_plugin-unified_storage_tiering-instance",
                "plugin_name" : "irods_rule_engine_plugin-unified_storage_tiering",
                "plugin_specific_configuration" : {
                    "access_time_attribute" : "irods::custom_access_time",
                    "group_attribute" : "irods::custom_storage_tiering::group",
                    "time_attribute" : "irods::custom_storage_tiering::time",
                    "query_attribute" : "irods::custom_storage_tiering::query",
                    "verification_attribute" : "irods::custom_storage_tiering::verification",
                    "restage_delay_attribute" : "irods::custom_storage_tiering::restage_delay",

                    "default_restage_delay_parameters" : "<PLUSET>1s</PLUSET>",
                    "time_check_string" : "TIME_CHECK_STRING"
                }
            }
        )

        irods_config.commit(irods_config.server_config, irods_config.server_config_path)
        try:
            yield
        finally:
            pass

@contextlib.contextmanager
def storage_tiering_configured(arg=None, sleep_time=1):
    filename = paths.server_config_path()
    with lib.file_backed_up(filename):
        irods_config = IrodsConfig()
        irods_config.server_config['advanced_settings']['delay_server_sleep_time_in_seconds'] = sleep_time

        irods_config.server_config['plugin_configuration']['rule_engines'].insert(0,
            {
                "instance_name": "irods_rule_engine_plugin-unified_storage_tiering-instance",
                "plugin_name": "irods_rule_engine_plugin-unified_storage_tiering",
                "plugin_specific_configuration": {
                }
            }
        )

        irods_config.commit(irods_config.server_config, irods_config.server_config_path)
        try:
            yield
        finally:
            pass

@contextlib.contextmanager
def storage_tiering_configured_with_log(arg=None, sleep_time=1):
    filename = paths.server_config_path()
    with lib.file_backed_up(filename):
        irods_config = IrodsConfig()
        irods_config.server_config['advanced_settings']['delay_server_sleep_time_in_seconds'] = sleep_time

        irods_config.server_config['plugin_configuration']['rule_engines'].insert(0,
            {
                "instance_name": "irods_rule_engine_plugin-unified_storage_tiering-instance",
                "plugin_name": "irods_rule_engine_plugin-unified_storage_tiering",
                "plugin_specific_configuration": {
                    "data_transfer_log_level" : "LOG_NOTICE"
                }
            }
        )

        irods_config.commit(irods_config.server_config, irods_config.server_config_path)

        try:
            yield
        finally:
            pass

@contextlib.contextmanager
def storage_tiering_configured_without_replication(arg=None, sleep_time=1):
    filename = paths.server_config_path()
    with lib.file_backed_up(filename):
        irods_config = IrodsConfig()
        irods_config.server_config['advanced_settings']['delay_server_sleep_time_in_seconds'] = sleep_time

        irods_config.server_config['plugin_configuration']['rule_engines'].insert(0,
            {
                "instance_name": "irods_rule_engine_plugin-unified_storage_tiering-instance",
                "plugin_name": "irods_rule_engine_plugin-unified_storage_tiering",
                "plugin_specific_configuration": {
                }
            }
        )

        irods_config.commit(irods_config.server_config, irods_config.server_config_path)
        try:
            yield
        finally:
            pass

@contextlib.contextmanager
def storage_tiering_configured_without_verification(arg=None, sleep_time=1):
    filename = paths.server_config_path()
    with lib.file_backed_up(filename):
        irods_config = IrodsConfig()
        irods_config.server_config['advanced_settings']['delay_server_sleep_time_in_seconds'] = sleep_time

        irods_config.server_config['plugin_configuration']['rule_engines'].insert(0,
            {
                "instance_name": "irods_rule_engine_plugin-unified_storage_tiering-instance",
                "plugin_name": "irods_rule_engine_plugin-unified_storage_tiering",
                "plugin_specific_configuration": {
                }
            }
        )

        irods_config.commit(irods_config.server_config, irods_config.server_config_path)
        try:
            yield
        finally:
            pass

@contextlib.contextmanager
def storage_tiering_configured_without_access_time(arg=None, sleep_time=1):
    filename = paths.server_config_path()
    with lib.file_backed_up(filename):
        irods_config = IrodsConfig()
        irods_config.server_config['advanced_settings']['delay_server_sleep_time_in_seconds'] = sleep_time

        irods_config.server_config['plugin_configuration']['rule_engines'].insert(0,
            {
                "instance_name": "irods_rule_engine_plugin-unified_storage_tiering-instance",
                "plugin_name": "irods_rule_engine_plugin-unified_storage_tiering",
                "plugin_specific_configuration": {
                }
            }
        )

        irods_config.commit(irods_config.server_config, irods_config.server_config_path)
        try:
            yield
        finally:
            pass

def wait_for_empty_queue(function, timeout_function=None, timeout_in_seconds=600):
    """Wait for empty delay queue and then run the provided function.

    Keyword arguments:
    timeout_function -- Function to run when a timeout occurs. None means TimeoutError is raised. Default: None
    timeout_in_seconds -- Time in seconds to wait for delay queue to empty. <=0 means no timeout. Default: 600
    """
    done = False
    start_time = time.time()
    while done == False:
        out, err, rc = lib.execute_command_permissive(['iqstat'])
        if -1 != out.find('No delayed rules pending'):
            function()
            done = True
        else:
            if timeout_in_seconds > 0 and time.time() - start_time > timeout_in_seconds:
                if timeout_function is None:
                    raise TimeoutError(f"Timed out after [{timeout_in_seconds}] seconds waiting for queue to empty.")
                else:
                    timeout_function()
                    return
            print(out)
            time.sleep(1)

def delay_assert(function):
    max_iter = 100
    counter = 0
    done = False
    while done == False:
        try:
            out, err, rc = lib.execute_command_permissive(['iqstat'])
            print(out)
            out, err, rc = function()
            print(out)
            print(err)
            print(rc)
        except:
            counter = counter + 1
            if(counter > max_iter):
                assert(False)
            time.sleep(1)
            continue
        else:
            done = True

def delay_assert_icommand(session, *args, **kwargs):
    max_iter = 100
    counter = 0
    done = False
    while done == False:
        try:
            session.assert_icommand(*args, **kwargs)
        except:
            counter = counter + 1
            if(counter > max_iter):
                assert(False)
            time.sleep(1)
            continue
        else:
            done = True

def invoke_storage_tiering_rule(sess=None):
    rep_instance = 'irods_rule_engine_plugin-unified_storage_tiering-instance'
    rule_file_path = '/var/lib/irods/example_unified_tiering_invocation.r'
    rule_command = ['irule', '-r', rep_instance, '-F', rule_file_path]
    if sess is None:
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand(rule_command)
    else:
        sess.assert_icommand(rule_command)

def get_tracked_replica(session, logical_path, group_attribute_name=None):
    coll_name = os.path.dirname(logical_path)
    data_name = os.path.basename(logical_path)

    tracked_replica_query = \
        "select META_DATA_ATTR_UNITS where DATA_NAME = '{}' and COLL_NAME = '{}' and META_DATA_ATTR_NAME = '{}'".format(
        data_name, coll_name, group_attribute_name or 'irods::storage_tiering::group')

    return session.run_icommand(['iquest', '%s', tracked_replica_query])[0].strip()

def get_access_time(session, data_object_path):
    """Return value of AVU with attribute irods::access_time annotated on provided data_object_path.

    If the provided data object path does not exist or does not have an irods::access_time AVU, the output will contain
    CAT_NO_ROWS_FOUND.

    Arguments:
    session - iRODSSession which will run the query
    data_object_path - Full iRODS logical path to a data object
    """
    coll_name = os.path.dirname(data_object_path)
    data_name = os.path.basename(data_object_path)

    query = "select META_DATA_ATTR_VALUE where " \
            f"COLL_NAME = '{coll_name}' and DATA_NAME = '{data_name}' and " \
            "META_DATA_ATTR_NAME = 'irods::access_time'"

    return session.assert_icommand(['iquest', '%s', query], 'STDOUT')[1].strip()


class TestStorageTieringPlugin(ResourceBase, unittest.TestCase):
    def setUp(self):
        super(TestStorageTieringPlugin, self).setUp()
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand('iqdel -a')
            admin_session.assert_icommand('iadmin mkresc ufs0 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs0', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs1 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs1', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs2 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs2', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs3 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs3', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs4 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs4', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs5 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs5', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc rnd0 random', 'STDOUT_SINGLELINE', 'random')
            admin_session.assert_icommand('iadmin mkresc rnd1 random', 'STDOUT_SINGLELINE', 'random')
            admin_session.assert_icommand('iadmin mkresc rnd2 random', 'STDOUT_SINGLELINE', 'random')
            admin_session.assert_icommand('iadmin addchildtoresc rnd0 ufs0')
            admin_session.assert_icommand('iadmin addchildtoresc rnd0 ufs1')
            admin_session.assert_icommand('iadmin addchildtoresc rnd1 ufs2')
            admin_session.assert_icommand('iadmin addchildtoresc rnd1 ufs3')
            admin_session.assert_icommand('iadmin addchildtoresc rnd2 ufs4')
            admin_session.assert_icommand('iadmin addchildtoresc rnd2 ufs5')

            admin_session.assert_icommand('imeta add -R rnd0 irods::storage_tiering::group example_group 0')
            admin_session.assert_icommand('imeta add -R rnd1 irods::storage_tiering::group example_group 1')
            admin_session.assert_icommand('imeta add -R rnd2 irods::storage_tiering::group example_group 2')
            admin_session.assert_icommand('imeta add -R rnd0 irods::storage_tiering::time 5')
            admin_session.assert_icommand('imeta add -R rnd1 irods::storage_tiering::time 15')
            admin_session.assert_icommand('''imeta set -R rnd1 irods::storage_tiering::query "SELECT DATA_NAME, COLL_NAME, USER_NAME, USER_ZONE, DATA_REPL_NUM where RESC_NAME = 'ufs2' || = 'ufs3' and META_DATA_ATTR_NAME = 'irods::access_time' and META_DATA_ATTR_VALUE < 'TIME_CHECK_STRING'"''')
            admin_session.assert_icommand('imeta add -R rnd0 irods::storage_tiering::minimum_delay_time_in_seconds 1')
            admin_session.assert_icommand('imeta add -R rnd0 irods::storage_tiering::maximum_delay_time_in_seconds 2')
            admin_session.assert_icommand('imeta add -R rnd1 irods::storage_tiering::minimum_delay_time_in_seconds 1')
            admin_session.assert_icommand('imeta add -R rnd1 irods::storage_tiering::maximum_delay_time_in_seconds 2')

            admin_session.assert_icommand('ilsresc -l', 'STDOUT_SINGLELINE', 'random')

    def tearDown(self):
        super(TestStorageTieringPlugin, self).tearDown()
        with session.make_session_for_existing_admin() as admin_session:

            admin_session.assert_icommand('iadmin rmchildfromresc rnd0 ufs0')
            admin_session.assert_icommand('iadmin rmchildfromresc rnd0 ufs1')
            admin_session.assert_icommand('iadmin rmchildfromresc rnd1 ufs2')
            admin_session.assert_icommand('iadmin rmchildfromresc rnd1 ufs3')
            admin_session.assert_icommand('iadmin rmchildfromresc rnd2 ufs4')
            admin_session.assert_icommand('iadmin rmchildfromresc rnd2 ufs5')

            admin_session.assert_icommand('iadmin rmresc rnd0')
            admin_session.assert_icommand('iadmin rmresc rnd1')
            admin_session.assert_icommand('iadmin rmresc rnd2')
            admin_session.assert_icommand('iadmin rmresc ufs0')
            admin_session.assert_icommand('iadmin rmresc ufs1')
            admin_session.assert_icommand('iadmin rmresc ufs2')
            admin_session.assert_icommand('iadmin rmresc ufs3')
            admin_session.assert_icommand('iadmin rmresc ufs4')
            admin_session.assert_icommand('iadmin rmresc ufs5')
            admin_session.assert_icommand('iadmin rum')

    def test_put_and_get(self):
        with storage_tiering_configured():
            IrodsController().restart(test_mode=True)
            zone_name = IrodsConfig().client_environment['irods_zone_name']
            with session.make_session_for_existing_admin() as admin_session:
                with session.make_session_for_existing_user('alice', 'apass', lib.get_hostname(), zone_name) as alice_session:
                    filename = "test_put_file"

                    try:
                        lib.create_local_testfile(filename)
                        alice_session.assert_icommand('iput -R rnd0 ' + filename)
                        alice_session.assert_icommand('imeta ls -d ' + filename, 'STDOUT_SINGLELINE', filename)
                        alice_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', filename)
                        time.sleep(5)

                        # test stage to tier 1
                        invoke_storage_tiering_rule()
                        delay_assert_icommand(alice_session, 'ils -L ' + filename, 'STDOUT_SINGLELINE', 'rnd1')

                        # test stage to tier 2
                        time.sleep(15)
                        invoke_storage_tiering_rule()
                        delay_assert_icommand(alice_session, 'ils -L ' + filename, 'STDOUT_SINGLELINE', 'rnd2')

                        # test restage to tier 0
                        alice_session.assert_icommand('iget ' + filename + ' - ', 'STDOUT_SINGLELINE', 'TESTFILE')
                        delay_assert_icommand(alice_session, 'ils -L ' + filename, 'STDOUT_SINGLELINE', 'rnd0')

                    finally:
                        alice_session.assert_icommand('irm -f ' + filename)

    def test_put_and_get_with_preserve_replica__92(self):
        with storage_tiering_configured():
            IrodsController().restart(test_mode=True)
            zone_name = IrodsConfig().client_environment['irods_zone_name']
            with session.make_session_for_existing_admin() as admin_session:
                with session.make_session_for_existing_user('alice', 'apass', lib.get_hostname(), zone_name) as alice_session:
                    filename = "test_put_file"

                    try:
                        admin_session.assert_icommand('imeta add -R rnd0 irods::storage_tiering::preserve_replicas true')

                        lib.create_local_testfile(filename)
                        alice_session.assert_icommand('iput -R rnd0 ' + filename)
                        alice_session.assert_icommand('imeta ls -d ' + filename, 'STDOUT_SINGLELINE', filename)
                        alice_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', filename)

                        # wait for object to age out of tier 0
                        time.sleep(5)

                        # test stage to tier 1
                        invoke_storage_tiering_rule()
                        delay_assert_icommand(alice_session, 'ils -L ' + filename, 'STDOUT_SINGLELINE', 'rnd0')
                        delay_assert_icommand(alice_session, 'ils -L ' + filename, 'STDOUT_SINGLELINE', 'rnd1')

                        # wait for object to age back out of tier 0
                        time.sleep(5)

                        invoke_storage_tiering_rule()

                        # wait for rule to execute
                        time.sleep(1)

                        # check for objects to have been queued from tier 0
                        for i in range(50):
                            stdout, _, _ = admin_session.run_icommand('iqstat')
                            self.assertEqual(-1, stdout.find('rnd0'))

                    finally:
                        alice_session.assert_icommand('irm -f ' + filename)
                        admin_session.assert_icommand('imeta rm -R rnd0 irods::storage_tiering::preserve_replicas true')

    def test_put_and_get_with_preserve_replica_restage__125(self):
        with storage_tiering_configured():
            IrodsController().restart(test_mode=True)
            zone_name = IrodsConfig().client_environment['irods_zone_name']
            with session.make_session_for_existing_admin() as admin_session:
                admin_session.assert_icommand('imeta add -R rnd0 irods::storage_tiering::preserve_replicas true')
                with session.make_session_for_existing_user('alice', 'apass', lib.get_hostname(), zone_name) as alice_session:
                    filename = "test_put_file"

                    try:
                        lib.create_local_testfile(filename)
                        alice_session.assert_icommand('iput -R rnd0 ' + filename)
                        alice_session.assert_icommand('imeta ls -d ' + filename, 'STDOUT_SINGLELINE', filename)
                        alice_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', filename)
                        time.sleep(5)

                        # test stage to tier 1
                        invoke_storage_tiering_rule()
                        delay_assert_icommand(alice_session, 'ils -L ' + filename, 'STDOUT_SINGLELINE', 'rnd1')

                        # test stage to tier 2
                        time.sleep(15)
                        invoke_storage_tiering_rule()
                        delay_assert_icommand(alice_session, 'ils -L ' + filename, 'STDOUT_SINGLELINE', 'rnd2')

                        # test restage to tier 0
                        alice_session.assert_icommand('iget ' + filename + ' - ', 'STDOUT_SINGLELINE', 'TESTFILE')
                        delay_assert_icommand(alice_session, 'ils -L ' + filename, 'STDOUT_SINGLELINE', 'rnd0')
                        delay_assert_icommand(alice_session, 'ils -L ' + filename, 'STDOUT_SINGLELINE', 'rnd2')

                    finally:
                        alice_session.assert_icommand('irm -f ' + filename)

    def test_single_quote_data_name__127(self):
        with storage_tiering_configured():
            IrodsController().restart(test_mode=True)
            zone_name = IrodsConfig().client_environment['irods_zone_name']
            with session.make_session_for_existing_admin() as admin_session:
                with session.make_session_for_existing_user('alice', 'apass', lib.get_hostname(), zone_name) as alice_session:
                    filename = "test_put_file_with_\'quotes\'"
                    cmd_filename = '\"'+filename+'\"'

                    try:
                        lib.create_local_testfile(filename)
                        alice_session.assert_icommand('iput -R rnd0 ' + cmd_filename)
                        alice_session.assert_icommand('imeta ls -d ' + cmd_filename, 'STDOUT_SINGLELINE', filename)
                        alice_session.assert_icommand('ils -L ' + cmd_filename, 'STDOUT_SINGLELINE', filename)
                        time.sleep(5)

                        # test stage to tier 1
                        invoke_storage_tiering_rule()
                        delay_assert_icommand(alice_session, 'ils -L ' + cmd_filename, 'STDOUT_SINGLELINE', 'rnd1')

                        # test stage to tier 2
                        time.sleep(15)
                        invoke_storage_tiering_rule()
                        delay_assert_icommand(alice_session, 'ils -L ' + cmd_filename, 'STDOUT_SINGLELINE', 'rnd2')

                        # test restage to tier 0
                        alice_session.assert_icommand('iget ' + cmd_filename + ' - ', 'STDOUT_SINGLELINE', 'TESTFILE')
                        delay_assert_icommand(alice_session, 'ils -L ' + cmd_filename, 'STDOUT_SINGLELINE', 'rnd0')

                    finally:
                        alice_session.assert_icommand('irm -f ' + cmd_filename)

    def test_storage_tiering_sets_admin_keyword_when_updating_access_time_as_rodsadmin__222(self):
        with storage_tiering_configured():
            IrodsController().restart(test_mode=True)

            with session.make_session_for_existing_admin() as admin_session:
                zone_name = IrodsConfig().client_environment['irods_zone_name']

                with session.make_session_for_existing_user('alice', 'apass', lib.get_hostname(), zone_name) as alice_session:
                    resc_name = 'storage_tiering_ufs_222'
                    filename = 'test_file_issue_222'

                    try:
                        lib.create_local_testfile(filename)
                        alice_session.assert_icommand(f'iput -R rnd0 {filename}')
                        alice_session.assert_icommand(f'imeta ls -d {filename}', 'STDOUT_SINGLELINE', filename)
                        alice_session.assert_icommand(f'ils -L {filename}', 'STDOUT_SINGLELINE', filename)
                        time.sleep(5)

                        # test stage to tier 1.
                        invoke_storage_tiering_rule()
                        delay_assert_icommand(alice_session, f'ils -L {filename}', 'STDOUT_SINGLELINE', 'rnd1')

                        # test stage to tier 2.
                        time.sleep(15)
                        invoke_storage_tiering_rule()
                        delay_assert_icommand(alice_session, f'ils -L {filename}', 'STDOUT_SINGLELINE', 'rnd2')

                        # capture the access time.
                        _, out, _ = admin_session.assert_icommand(
                            ['iquest', '%s', f"select META_DATA_ATTR_VALUE where DATA_NAME = '{filename}' and META_DATA_ATTR_NAME = 'irods::access_time'"], 'STDOUT')
                        access_time = out.strip()
                        self.assertGreater(len(access_time), 0)

                        # sleeping guarantees the access time will be different following the call to irepl.
                        time.sleep(2)

                        # show the access time is updated correctly.
                        lib.create_ufs_resource(admin_session, resc_name)
                        admin_session.assert_icommand(f'irepl -M -R {resc_name} {alice_session.home_collection}/{filename}')

                        _, out, _ = admin_session.assert_icommand(
                            ['iquest', '%s', f"select META_DATA_ATTR_VALUE where DATA_NAME = '{filename}' and META_DATA_ATTR_NAME = 'irods::access_time'"], 'STDOUT')
                        new_access_time = out.strip()
                        self.assertGreater(len(new_access_time), 0)

                        # this assertion is the primary focus of the test.
                        self.assertGreater(int(new_access_time), int(access_time))

                    finally:
                        alice_session.assert_icommand(f'irm -f {filename}')
                        admin_session.assert_icommand(f'iadmin rmresc {resc_name}')

    def test_checksum_verification_with_regular_user_data_object__issue_354(self):
        """This tests the fix for the issue where admin-initiated tiering with checksum
        verification would fail with permission denied when trying to compute/update
        the checksum on a data object owned by a regular user.
        """
        with storage_tiering_configured():
            zone_name = IrodsConfig().client_environment['irods_zone_name']
            with session.make_session_for_existing_admin() as admin_session:
                with session.make_session_for_existing_user('alice', 'apass', lib.get_hostname(), zone_name) as alice_session:
                    filename = 'test_file_checksum_verification'

                    try:
                        # Configure tier 1 to use checksum verification
                        admin_session.assert_icommand('imeta add -R rnd1 irods::storage_tiering::verification checksum')

                        # Create a file as a regular user
                        contents = 'The checksum knows things.'
                        alice_session.assert_icommand(['istream', '-R', 'rnd0', 'write', filename], input=contents)

                        # Wait for object to age out of tier 0
                        time.sleep(5)

                        # Trigger tiering to move to tier 1 with checksum verification
                        invoke_storage_tiering_rule()
                        delay_assert_icommand(alice_session, f'ils -L {filename}', 'STDOUT_SINGLELINE', 'rnd1')

                        # Verify checksum was computed and stored
                        coll_name = alice_session.home_collection
                        stdout, err, rc = admin_session.run_icommand(
                            ['iquest', '%s', f"select DATA_CHECKSUM where DATA_NAME = '{filename}' and COLL_NAME = '{coll_name}' and DATA_RESC_HIER like 'rnd1;%'"])
                        # The checksum should exist now
                        self.assertEqual('sha2:gkAGWzFOSdRYKUCqHcR7lCX80mYbPYjaBkqqJYZovAI=\n', stdout)
                        self.assertEqual('', err)
                        self.assertEqual(0, rc)

                    finally:
                        alice_session.assert_icommand(f'irm -f {filename}')
                        admin_session.assert_icommand('imeta rm -R rnd1 irods::storage_tiering::verification checksum')


class TestStorageTieringPluginMultiGroup(ResourceBase, unittest.TestCase):
    def setUp(self):
        super(TestStorageTieringPluginMultiGroup, self).setUp()
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand('iqdel -a')
            admin_session.assert_icommand('iadmin mkresc ufs0 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs0', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs1 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs1', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs2 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs2', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs3 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs3', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs4 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs4', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs5 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs5', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc rnd0 random', 'STDOUT_SINGLELINE', 'random')
            admin_session.assert_icommand('iadmin mkresc rnd1 random', 'STDOUT_SINGLELINE', 'random')
            admin_session.assert_icommand('iadmin mkresc rnd2 random', 'STDOUT_SINGLELINE', 'random')
            admin_session.assert_icommand('iadmin addchildtoresc rnd0 ufs0')
            admin_session.assert_icommand('iadmin addchildtoresc rnd0 ufs1')
            admin_session.assert_icommand('iadmin addchildtoresc rnd1 ufs2')
            admin_session.assert_icommand('iadmin addchildtoresc rnd1 ufs3')
            admin_session.assert_icommand('iadmin addchildtoresc rnd2 ufs4')
            admin_session.assert_icommand('iadmin addchildtoresc rnd2 ufs5')

            admin_session.assert_icommand('imeta add -R rnd0 irods::storage_tiering::group example_group 0')
            admin_session.assert_icommand('imeta add -R rnd1 irods::storage_tiering::group example_group 1')
            admin_session.assert_icommand('imeta add -R rnd2 irods::storage_tiering::group example_group 2')
            admin_session.assert_icommand('imeta add -R rnd0 irods::storage_tiering::time 5')
            admin_session.assert_icommand('imeta add -R rnd1 irods::storage_tiering::time 15')
            admin_session.assert_icommand('''imeta set -R rnd1 irods::storage_tiering::query "SELECT DATA_NAME, COLL_NAME, USER_NAME, USER_ZONE, DATA_REPL_NUM  where RESC_NAME = 'ufs2' || = 'ufs3' and META_DATA_ATTR_NAME = 'irods::access_time' and META_DATA_ATTR_VALUE < 'TIME_CHECK_STRING'"''')

            admin_session.assert_icommand('iadmin mkresc ufs0g2 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs0g2', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs1g2 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs1g2', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs2g2 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs2g2', 'STDOUT_SINGLELINE', 'unixfilesystem')

            admin_session.assert_icommand('imeta add -R ufs0g2 irods::storage_tiering::group example_group_g2 0')
            admin_session.assert_icommand('imeta add -R ufs1g2 irods::storage_tiering::group example_group_g2 1')
            admin_session.assert_icommand('imeta add -R ufs2g2 irods::storage_tiering::group example_group_g2 2')

            admin_session.assert_icommand('imeta add -R ufs0g2 irods::storage_tiering::time 5')
            admin_session.assert_icommand('imeta add -R ufs1g2 irods::storage_tiering::time 15')

            admin_session.assert_icommand('''imeta set -R ufs1g2 irods::storage_tiering::query "SELECT DATA_NAME, COLL_NAME, USER_NAME, USER_ZONE, DATA_REPL_NUM where RESC_NAME = 'ufs1g2' and META_DATA_ATTR_NAME = 'irods::access_time' and META_DATA_ATTR_VALUE < 'TIME_CHECK_STRING'"''')
            admin_session.assert_icommand('imeta add -R rnd0 irods::storage_tiering::minimum_delay_time_in_seconds 1')
            admin_session.assert_icommand('imeta add -R rnd0 irods::storage_tiering::maximum_delay_time_in_seconds 2')
            admin_session.assert_icommand('imeta add -R rnd1 irods::storage_tiering::minimum_delay_time_in_seconds 1')
            admin_session.assert_icommand('imeta add -R rnd1 irods::storage_tiering::maximum_delay_time_in_seconds 2')

    def tearDown(self):
        super(TestStorageTieringPluginMultiGroup, self).tearDown()
        with session.make_session_for_existing_admin() as admin_session:

            admin_session.assert_icommand('iadmin rmchildfromresc rnd0 ufs0')
            admin_session.assert_icommand('iadmin rmchildfromresc rnd0 ufs1')
            admin_session.assert_icommand('iadmin rmchildfromresc rnd1 ufs2')
            admin_session.assert_icommand('iadmin rmchildfromresc rnd1 ufs3')
            admin_session.assert_icommand('iadmin rmchildfromresc rnd2 ufs4')
            admin_session.assert_icommand('iadmin rmchildfromresc rnd2 ufs5')

            admin_session.assert_icommand('iadmin rmresc rnd0')
            admin_session.assert_icommand('iadmin rmresc rnd1')
            admin_session.assert_icommand('iadmin rmresc rnd2')
            admin_session.assert_icommand('iadmin rmresc ufs0')
            admin_session.assert_icommand('iadmin rmresc ufs1')
            admin_session.assert_icommand('iadmin rmresc ufs2')
            admin_session.assert_icommand('iadmin rmresc ufs3')
            admin_session.assert_icommand('iadmin rmresc ufs4')
            admin_session.assert_icommand('iadmin rmresc ufs5')

            admin_session.assert_icommand('iadmin rmresc ufs0g2')
            admin_session.assert_icommand('iadmin rmresc ufs1g2')
            admin_session.assert_icommand('iadmin rmresc ufs2g2')

            admin_session.assert_icommand('iadmin rum')

    def test_put_and_get(self):
        with storage_tiering_configured():
            IrodsController().restart(test_mode=True)
            with session.make_session_for_existing_admin() as admin_session:
                print("yep")
                admin_session.assert_icommand('ils -L ', 'STDOUT_SINGLELINE', 'rods')

                filename = 'test_put_file'

                filenameg2 = 'test_put_fileg2'

                try:
                    lib.create_local_testfile(filename)
                    lib.create_local_testfile(filenameg2)

                    admin_session.assert_icommand('iput -R rnd0 ' + filename)
                    admin_session.assert_icommand('imeta ls -d ' + filename, 'STDOUT_SINGLELINE', filename)
                    admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', filename)

                    admin_session.assert_icommand('iput -R ufs0g2 ' + filenameg2)
                    admin_session.assert_icommand('imeta ls -d ' + filenameg2, 'STDOUT_SINGLELINE', filenameg2)
                    admin_session.assert_icommand('ils -L ' + filenameg2, 'STDOUT_SINGLELINE', filenameg2)

                    # test stage to tier 1
                    time.sleep(5)
                    invoke_storage_tiering_rule()
                    delay_assert_icommand(admin_session, 'ils -L ' + filename, 'STDOUT_SINGLELINE', 'rnd1')
                    delay_assert_icommand(admin_session, 'ils -L ' + filenameg2, 'STDOUT_SINGLELINE', 'ufs1g2')

                    # test stage to tier 2
                    time.sleep(15)
                    invoke_storage_tiering_rule()
                    delay_assert_icommand(admin_session, 'ils -L ' + filename, 'STDOUT_SINGLELINE', 'rnd2')
                    delay_assert_icommand(admin_session, 'ils -L ' + filenameg2, 'STDOUT_SINGLELINE', 'ufs2g2')

                    # test restage to tier 0
                    admin_session.assert_icommand('iget ' + filename + ' - ', 'STDOUT_SINGLELINE', 'TESTFILE')
                    admin_session.assert_icommand('iget ' + filenameg2 + ' - ', 'STDOUT_SINGLELINE', 'TESTFILE')
                    delay_assert_icommand(admin_session, 'ils -L ' + filename, 'STDOUT_SINGLELINE', 'rnd0')
                    delay_assert_icommand(admin_session, 'ils -L ' + filenameg2, 'STDOUT_SINGLELINE', 'ufs0g2')

                finally:
                    admin_session.assert_icommand('irm -f ' + filename)
                    admin_session.assert_icommand('irm -f ' + filenameg2)

class TestStorageTieringPluginCustomMetadata(ResourceBase, unittest.TestCase):
    def setUp(self):
        super(TestStorageTieringPluginCustomMetadata, self).setUp()
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand('iqdel -a')
            admin_session.assert_icommand('iadmin mkresc ufs0 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs0', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs1 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs1', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs2 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs2', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs3 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs3', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs4 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs4', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs5 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs5', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc rnd0 random', 'STDOUT_SINGLELINE', 'random')
            admin_session.assert_icommand('iadmin mkresc rnd1 random', 'STDOUT_SINGLELINE', 'random')
            admin_session.assert_icommand('iadmin mkresc rnd2 random', 'STDOUT_SINGLELINE', 'random')
            admin_session.assert_icommand('iadmin addchildtoresc rnd0 ufs0')
            admin_session.assert_icommand('iadmin addchildtoresc rnd0 ufs1')
            admin_session.assert_icommand('iadmin addchildtoresc rnd1 ufs2')
            admin_session.assert_icommand('iadmin addchildtoresc rnd1 ufs3')
            admin_session.assert_icommand('iadmin addchildtoresc rnd2 ufs4')
            admin_session.assert_icommand('iadmin addchildtoresc rnd2 ufs5')

            admin_session.assert_icommand('imeta add -R rnd0 irods::custom_storage_tiering::group example_group 0')
            admin_session.assert_icommand('imeta add -R rnd1 irods::custom_storage_tiering::group example_group 1')
            admin_session.assert_icommand('imeta add -R rnd2 irods::custom_storage_tiering::group example_group 2')
            admin_session.assert_icommand('imeta add -R rnd0 irods::custom_storage_tiering::time 5')
            admin_session.assert_icommand('imeta add -R rnd1 irods::custom_storage_tiering::time 15')
            admin_session.assert_icommand('''imeta set -R rnd1 irods::custom_storage_tiering::query "SELECT DATA_NAME, COLL_NAME, USER_NAME, USER_ZONE, DATA_REPL_NUM where RESC_NAME = 'ufs2' || = 'ufs3' and META_DATA_ATTR_NAME = 'irods::custom_access_time' and META_DATA_ATTR_VALUE < 'TIME_CHECK_STRING'"''')
            admin_session.assert_icommand('imeta add -R rnd0 irods::storage_tiering::minimum_delay_time_in_seconds 1')
            admin_session.assert_icommand('imeta add -R rnd0 irods::storage_tiering::maximum_delay_time_in_seconds 2')
            admin_session.assert_icommand('imeta add -R rnd1 irods::storage_tiering::minimum_delay_time_in_seconds 1')
            admin_session.assert_icommand('imeta add -R rnd1 irods::storage_tiering::maximum_delay_time_in_seconds 2')

    def tearDown(self):
        super(TestStorageTieringPluginCustomMetadata, self).tearDown()
        with session.make_session_for_existing_admin() as admin_session:

            admin_session.assert_icommand('iadmin rmchildfromresc rnd0 ufs0')
            admin_session.assert_icommand('iadmin rmchildfromresc rnd0 ufs1')
            admin_session.assert_icommand('iadmin rmchildfromresc rnd1 ufs2')
            admin_session.assert_icommand('iadmin rmchildfromresc rnd1 ufs3')
            admin_session.assert_icommand('iadmin rmchildfromresc rnd2 ufs4')
            admin_session.assert_icommand('iadmin rmchildfromresc rnd2 ufs5')

            admin_session.assert_icommand('iadmin rmresc rnd0')
            admin_session.assert_icommand('iadmin rmresc rnd1')
            admin_session.assert_icommand('iadmin rmresc rnd2')
            admin_session.assert_icommand('iadmin rmresc ufs0')
            admin_session.assert_icommand('iadmin rmresc ufs1')
            admin_session.assert_icommand('iadmin rmresc ufs2')
            admin_session.assert_icommand('iadmin rmresc ufs3')
            admin_session.assert_icommand('iadmin rmresc ufs4')
            admin_session.assert_icommand('iadmin rmresc ufs5')
            admin_session.assert_icommand('iadmin rum')


    def test_put_and_get(self):
        with storage_tiering_configured_custom():
            IrodsController().restart(test_mode=True)
            with session.make_session_for_existing_admin() as admin_session:
                filename = 'test_put_file'
                logical_path = "/".join([admin_session.home_collection, filename])

                try:
                    lib.create_local_testfile(filename)
                    admin_session.assert_icommand('iput -R rnd0 ' + filename)
                    admin_session.assert_icommand('imeta ls -d ' + filename, 'STDOUT_SINGLELINE', filename)
                    admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', filename)

                    # test stage to tier 1
                    time.sleep(5)
                    invoke_storage_tiering_rule()
                    # Wait until the object migrates to the next tier.
                    lib.delayAssert(lambda: lib.replica_exists(admin_session, logical_path, 0) == False)
                    lib.replica_exists(admin_session, logical_path, 1)
                    admin_session.assert_icommand(["ils", "-L", filename], "STDOUT", "rnd1")

                    # test stage to tier 2
                    time.sleep(15)
                    invoke_storage_tiering_rule()
                    # Wait until the object migrates to the next tier.
                    lib.delayAssert(lambda: lib.replica_exists(admin_session, logical_path, 1) == False)
                    lib.replica_exists(admin_session, logical_path, 2)
                    admin_session.assert_icommand(["ils", "-L", filename], "STDOUT", "rnd2")

                    # test restage to tier 0
                    admin_session.assert_icommand('iget ' + filename + ' - ', 'STDOUT_SINGLELINE', 'TESTFILE')
                    # Wait until the object migrates to the next tier.
                    lib.delayAssert(lambda: lib.replica_exists(admin_session, logical_path, 2) == False)
                    lib.replica_exists(admin_session, logical_path, 3)
                    admin_session.assert_icommand(["ils", "-L", filename], "STDOUT", "rnd0")

                finally:
                    admin_session.assert_icommand('irm -f ' + filename)

class TestStorageTieringPluginMinimumRestage(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        with session.make_session_for_existing_admin() as admin_session:
            self.user1 = session.mkuser_and_return_session("rodsuser", "tolstoy", "tpass", lib.get_hostname())

            self.tier0 = "ufs0"
            self.tier1 = "ufs1"
            self.tier2 = "ufs2"

            admin_session.assert_icommand('iqdel -a')
            admin_session.assert_icommand('iadmin mkresc ufs0 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs0', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs1 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs1', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs2 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs2', 'STDOUT_SINGLELINE', 'unixfilesystem')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::group example_group 0')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::group example_group 1')
            admin_session.assert_icommand('imeta add -R ufs2 irods::storage_tiering::group example_group 2')
            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::time 5')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::time 15')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::minimum_restage_tier true')
            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::minimum_delay_time_in_seconds 1')
            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::maximum_delay_time_in_seconds 2')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::minimum_delay_time_in_seconds 1')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::maximum_delay_time_in_seconds 2')

    @classmethod
    def tearDownClass(self):
        self.user1.__exit__()
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand(['iadmin', 'rmuser', self.user1.username])
            admin_session.assert_icommand('iadmin rmresc ufs0')
            admin_session.assert_icommand('iadmin rmresc ufs1')
            admin_session.assert_icommand('iadmin rmresc ufs2')
            admin_session.assert_icommand('iadmin rum')

    def test_put_and_get(self):
        with storage_tiering_configured():
            IrodsController().restart(test_mode=True)
            with session.make_session_for_existing_admin() as admin_session:
                filename = 'test_put_file'

                try:
                    lib.create_local_testfile(filename)
                    admin_session.assert_icommand('iput -R ufs0 ' + filename)
                    admin_session.assert_icommand('ils -L ', 'STDOUT_SINGLELINE', 'rods')
                    time.sleep(5)
                    invoke_storage_tiering_rule()
                    delay_assert_icommand(admin_session, 'ils -L ' + filename, 'STDOUT_SINGLELINE', 'ufs1')

                    time.sleep(15)
                    invoke_storage_tiering_rule()
                    delay_assert_icommand(admin_session, 'ils -L ' + filename, 'STDOUT_SINGLELINE', 'ufs2')

                    # test restage to tier 1
                    admin_session.assert_icommand('iget ' + filename + ' - ', 'STDOUT_SINGLELINE', 'TESTFILE')
                    delay_assert_icommand(admin_session, 'ils -L ' + filename, 'STDOUT_SINGLELINE', 'ufs1')

                finally:
                    admin_session.assert_icommand('irm -f ' + filename)

    def accessing_existing_data_object_triggers_restage(
        self, logical_path, expected_destination_resource, access_command, *access_command_args, **access_command_kwargs
    ):
        """Implementation of a very basic test to demonstrate that objects restage when accessed from a higher tier.

        Arguments:
        self - Instance of this class
        logical_path - Logical path to iRODS object being created
        expected_destination_resource - Resource on which the replica is asserted to reside after restaging
        access_command - A callable which will execute some sort of access operation on the object
        access_command_args - *args to pass to the access_command callable
        access_command_kwargs - **kwargs to pass to the access_command callable
        """
        with storage_tiering_configured():
            IrodsController().restart(test_mode=True)
            filename = "test_open_read_close_triggers_restage__issue_303"
            try:
                with session.make_session_for_existing_admin() as admin_session:
                    lib.create_local_testfile(filename)
                    self.user1.assert_icommand(["iput", "-R", self.tier0, filename, logical_path])

                    # Tier out the object...
                    self.assertTrue(lib.replica_exists_on_resource(self.user1, logical_path, self.tier0))
                    time.sleep(5)
                    invoke_storage_tiering_rule()
                    # Wait for the object to be trimmed before checking destination to avoid timing issues.
                    lib.delayAssert(
                        lambda: lib.replica_exists_on_resource(self.user1, logical_path, self.tier0) == False)
                    self.assertTrue(lib.replica_exists_on_resource(self.user1, logical_path, self.tier1))

                    # And then again...
                    time.sleep(15)
                    invoke_storage_tiering_rule()
                    # Wait for the object to be trimmed before checking destination to avoid timing issues.
                    lib.delayAssert(
                        lambda: lib.replica_exists_on_resource(self.user1, logical_path, self.tier1) == False)
                    self.assertTrue(lib.replica_exists_on_resource(self.user1, logical_path, self.tier2))

                    # Ensure that the delay queue is empty before creating the data object. This is to prevent false
                    # negatives in a later assertion of the emptiness of the delay queue.
                    admin_session.assert_icommand(["iqstat", "-a"], "STDOUT", "No delayed rules pending")

                    # Now see if the object is restaged appropriately on access.
                    access_command(*access_command_args, **access_command_kwargs)

                    # Ensure that the restage is scheduled exactly once.
                    scheduled_delay_rule_count = admin_session.run_icommand(
                        ["iquest", "%s", "select COUNT(RULE_EXEC_ID)"])[0].strip()
                    self.assertEqual("1", scheduled_delay_rule_count)

                    # Wait for the object to be trimmed before checking destination to avoid timing issues.
                    lib.delayAssert(
                        lambda: lib.replica_exists_on_resource(self.user1, logical_path, self.tier2) == False)
                    self.assertTrue(
                        lib.replica_exists_on_resource(self.user1, logical_path, expected_destination_resource))

                    # Ensure that nothing is scheduled in the delay queue. The restage should have completed. If any
                    # failures occurred, it is likely that the delayed rule will be retried so we are making sure that
                    # is not happening here.
                    admin_session.assert_icommand(["iqstat", "-a"], "STDOUT", "No delayed rules pending")

            finally:
                # Run ils to show the state of the world for debugging purposes.
                self.user1.assert_icommand(["ils", "-L", logical_path], "STDOUT")
                if os.path.exists(filename):
                    os.unlink(filename)
                self.user1.assert_icommand(["irm", "-f", logical_path])

    def test_open_read_close_triggers_restage__issue_303(self):
        filename = "test_open_read_close_triggers_restage__issue_303"
        logical_path = "/".join([self.user1.session_collection, filename])
        self.accessing_existing_data_object_triggers_restage(
            logical_path, self.tier1, self.user1.assert_icommand, ["irods_test_read_object", logical_path], "STDOUT")

    def test_replica_open_read_replica_close_triggers_restage__issue_316(self):
        filename = "test_replica_open_read_replica_close_triggers_restage__issue_316"
        logical_path = "/".join([self.user1.session_collection, filename])
        self.accessing_existing_data_object_triggers_restage(
            logical_path, self.tier1, self.user1.assert_icommand, ["istream", "read", logical_path], "STDOUT")

    def test_replica_open_write_replica_close_triggers_restage__issue_316(self):
        filename = "test_replica_open_read_replica_close_triggers_restage__issue_316"
        logical_path = "/".join([self.user1.session_collection, filename])
        self.accessing_existing_data_object_triggers_restage(
            logical_path,
            self.tier1,
            self.user1.assert_icommand,
            # Target tier2 because that is where the data object tiers out in the test implementation.
            ["istream", "-R", self.tier2, "write", logical_path],
            "STDOUT",
            input="restage this data")

    def test_multiple_replica_opens_and_replica_closes_triggers_restage__issue_316(self):
        filename = "test_multiple_replica_opens_and_replica_closes_triggers_restage__issue_316"
        logical_path = "/".join([self.user1.session_collection, filename])
        self.accessing_existing_data_object_triggers_restage(
            logical_path,
            self.tier1,
            self.user1.assert_icommand,
            # Target tier2 because that is where the data object tiers out in the test implementation.
            ["irods_test_multi_open_for_write_object", "--resource", self.tier2, "--open-count", "10", logical_path],
            "STDOUT")

    @unittest.skip("TODO(#326): Cannot deterministically implement restage for touch.")
    def test_touch_existing_object_triggers_restage__issue_326(self):
        filename = "test_touch_existing_object_triggers_restage__issue_266"
        logical_path = "/".join([self.user1.session_collection, filename])
        # Target tier2 because that is where the data object tiers out in the test implementation.
        self.accessing_existing_data_object_triggers_restage(
            logical_path, self.tier1, self.user1.assert_icommand, ["itouch", "-R", self.tier2, logical_path], "STDOUT")

    def creating_data_object_does_not_trigger_restage_test_impl(
        self, logical_path, expected_destination_resource, create_command, *create_command_args, **create_command_kwargs
    ):
        """Implementation of a very basic test to demonstrate that objects do not restage for some operations.

        Arguments:
        self - Instance of this class
        logical_path - Logical path to iRODS object being created
        expected_destination_resource - Resource on which the replica is asserted to reside after not restaging
        create_command - A callable which will execute some sort of create operation on the object
        create_command_args - *args to pass to the create_command callable
        create_command_kwargs - **kwargs to pass to the create_command callable
        """
        with storage_tiering_configured():
            IrodsController().restart(test_mode=True)
            try:
                with session.make_session_for_existing_admin() as admin_session:
                    # Ensure that the delay queue is empty before creating the data object. This is to prevent false
                    # negatives in a later assertion of the emptiness of the delay queue.
                    admin_session.assert_icommand(["iqstat", "-a"], "STDOUT", "No delayed rules pending")

                    create_command(*create_command_args, **create_command_kwargs)

                    # Immediately check to see whether a restage was scheduled (it should not have been). We do this
                    # first to ensure that the delay rule was not scheduled and processed by the time we check.
                    admin_session.assert_icommand(["iqstat", "-a"], "STDOUT", "No delayed rules pending")

                    # Then, make sure that the object is in the correct tier and has an access_time.
                    self.assertTrue(
                        lib.replica_exists_on_resource(admin_session, logical_path, expected_destination_resource)
                    )
                    access_time = get_access_time(admin_session, logical_path)
                    self.assertNotIn("CAT_NO_ROWS_FOUND", access_time)

            finally:
                self.user1.assert_icommand(["irm", "-f", logical_path])

    def test_rcDataObjCreate_does_not_trigger_restage__issue_303(self):
        # TODO(#280): This test will fail once tiering group metadata is annotated to objects on create. This test
        # should continue to pass as-is.
        filename = "test_rcDataObjCreate_does_not_trigger_restage__issue_303"
        logical_path = "/".join([self.user1.session_collection, filename])
        self.creating_data_object_does_not_trigger_restage_test_impl(
            logical_path,
            self.tier2,
            self.user1.assert_icommand,
            ["irods_test_create_object", "--resource", self.tier2, "--use-create-api", "1", logical_path],
            "STDOUT")

    def test_rcDataObjOpen_with_O_CREAT_does_not_trigger_restage__issue_303(self):
        # TODO(#280): This test will fail once tiering group metadata is annotated to objects on create. This test
        # should continue to pass as-is.
        filename = "test_rcDataObjOpen_with_O_CREAT_does_not_trigger_restage__issue_303"
        logical_path = "/".join([self.user1.session_collection, filename])
        self.creating_data_object_does_not_trigger_restage_test_impl(
            logical_path,
            self.tier2,
            self.user1.assert_icommand,
            ["irods_test_create_object", "--resource", self.tier2, logical_path],
            "STDOUT")

    def test_iput_new_object_does_not_trigger_restage__issue_303(self):
        # TODO(#280): This test will fail once tiering group metadata is annotated to objects on create. This test
        # should continue to pass as-is.
        filename = "test_iput_new_object_does_not_trigger_restage__issue_303"
        logical_path = "/".join([self.user1.session_collection, filename])

        try:
            lib.create_local_testfile(filename)
            self.creating_data_object_does_not_trigger_restage_test_impl(
                logical_path,
                self.tier2,
                self.user1.assert_icommand,
                ["iput", "-R", self.tier2, filename, logical_path],
                "STDOUT")

        finally:
            if os.path.exists(filename):
                os.unlink(filename)

    def test_replica_open_replica_close_does_not_trigger_restage__issue_316(self):
        # TODO(#280): This test will fail once tiering group metadata is annotated to objects on create. This test
        # should continue to pass as-is.
        filename = "test_replica_open_replica_close_does_not_trigger_restage__issue_316"
        logical_path = "/".join([self.user1.session_collection, filename])
        self.creating_data_object_does_not_trigger_restage_test_impl(
            logical_path,
            self.tier2,
            self.user1.assert_icommand,
            ["istream", "-R", self.tier2, "write", logical_path],
            "STDOUT",
            input="do not restage this")

    def test_touch_created_object_does_not_trigger_restage__issue_266(self):
        # TODO(#280): This test will fail once tiering group metadata is annotated to objects on create. This test
        # should continue to pass as-is.
        filename = "test_touch_created_object_does_not_trigger_restage__issue_266"
        logical_path = "/".join([self.user1.session_collection, filename])
        self.creating_data_object_does_not_trigger_restage_test_impl(
            logical_path, self.tier2, self.user1.assert_icommand, ["itouch", "-R", self.tier2, logical_path], "STDOUT")

    def test_touch_collection_does_not_trigger_restage__issue_266(self):
        filename = "test_touch_collection_does_not_trigger_restage__issue_266"
        collection_path = self.user1.session_collection
        logical_path = "/".join([collection_path, filename])
        with storage_tiering_configured():
            IrodsController().restart(test_mode=True)
            try:
                with session.make_session_for_existing_admin() as admin_session:
                    self.user1.assert_icommand(["istream", "-R", self.tier0, "write", logical_path], input=filename)

                    # Tier out the object...
                    self.assertTrue(lib.replica_exists_on_resource(self.user1, logical_path, self.tier0))
                    time.sleep(5)
                    invoke_storage_tiering_rule()
                    # Wait for the object to be trimmed before checking destination to avoid timing issues.
                    lib.delayAssert(
                        lambda: lib.replica_exists_on_resource(self.user1, logical_path, self.tier0) == False)
                    self.assertTrue(lib.replica_exists_on_resource(self.user1, logical_path, self.tier1))

                    # And then again...
                    time.sleep(15)
                    invoke_storage_tiering_rule()
                    # Wait for the object to be trimmed before checking destination to avoid timing issues.
                    lib.delayAssert(
                        lambda: lib.replica_exists_on_resource(self.user1, logical_path, self.tier1) == False)
                    self.assertTrue(lib.replica_exists_on_resource(self.user1, logical_path, self.tier2))

                    # Ensure that the delay queue is empty before touching the collection. This is to prevent false
                    # negatives in a later assertion of the emptiness of the delay queue.
                    admin_session.assert_icommand(["iqstat", "-a"], "STDOUT", "No delayed rules pending")

                    # Touch the collection and ensure that no restage occurs...
                    self.user1.assert_icommand(["itouch", collection_path])

                    # Ensure that no restage is scheduled.
                    scheduled_delay_rule_count = admin_session.run_icommand(
                        ["iquest", "%s", "select COUNT(RULE_EXEC_ID)"])[0].strip()
                    self.assertEqual("0", scheduled_delay_rule_count)

                    # Ensure that the replica has not moved.
                    self.assertTrue(lib.replica_exists_on_resource(self.user1, logical_path, self.tier2))

            finally:
                # Run ils to show the state of the world for debugging purposes.
                self.user1.assert_icommand(["ils", "-L", logical_path], "STDOUT")
                self.user1.assert_icommand(["irm", "-f", logical_path])


class TestStorageTieringPluginPreserveReplica(ResourceBase, unittest.TestCase):
    def setUp(self):
        super(TestStorageTieringPluginPreserveReplica, self).setUp()
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand('iqdel -a')
            admin_session.assert_icommand('iadmin mkresc ufs0 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs0', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs1 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs1', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs2 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs2', 'STDOUT_SINGLELINE', 'unixfilesystem')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::group example_group 0')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::group example_group 1')
            admin_session.assert_icommand('imeta add -R ufs2 irods::storage_tiering::group example_group 2')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::time 5')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::time 15')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::minimum_restage_tier true')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::preserve_replicas true')
            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::minimum_delay_time_in_seconds 1')
            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::maximum_delay_time_in_seconds 2')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::minimum_delay_time_in_seconds 1')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::maximum_delay_time_in_seconds 2')

    def tearDown(self):
        super(TestStorageTieringPluginPreserveReplica, self).tearDown()
        with session.make_session_for_existing_admin() as admin_session:

            admin_session.assert_icommand('iadmin rmresc ufs0')
            admin_session.assert_icommand('iadmin rmresc ufs1')
            admin_session.assert_icommand('iadmin rmresc ufs2')
            admin_session.assert_icommand('iadmin rum')

    def test_put(self):
        with storage_tiering_configured_with_log():
            IrodsController().restart(test_mode=True)
            with session.make_session_for_existing_admin() as admin_session:
                filename = 'test_put_file'

                try:
                    lib.create_local_testfile(filename)
                    admin_session.assert_icommand('iput -R ufs0 ' + filename)
                    admin_session.assert_icommand('ils -L ', 'STDOUT_SINGLELINE', 'rods')

                    # stage to tier 1, look for both replicas
                    time.sleep(6)
                    invoke_storage_tiering_rule()
                    admin_session.assert_icommand('iqstat', 'STDOUT_SINGLELINE', 'irods_policy_storage_tiering')

                    delay_assert_icommand(admin_session, 'ils -L ' + filename, 'STDOUT_SINGLELINE', 'ufs0')
                    delay_assert_icommand(admin_session, 'ils -L ' + filename, 'STDOUT_SINGLELINE', 'ufs1')

                    time.sleep(15)
                    # stage to tier 2, look for replica in tier 0 and tier 2
                    invoke_storage_tiering_rule()
                    admin_session.assert_icommand('iqstat', 'STDOUT_SINGLELINE', 'irods_policy_storage_tiering')

                    delay_assert_icommand(admin_session, 'ils -L ' + filename, 'STDOUT_SINGLELINE', 'ufs0')
                    delay_assert_icommand(admin_session, 'ils -L ' + filename, 'STDOUT_SINGLELINE', 'ufs2')

                finally:
                    admin_session.assert_icommand('irm -f ' + filename)

    def test_preserve_replicas_works_with_restage_when_replicas_exist_in_multiple_tiers__issue_232(self):
        with storage_tiering_configured_with_log():
            IrodsController().restart(test_mode=True)
            with session.make_session_for_existing_admin() as admin_session:
                filename = 'test_put_file'
                logical_path = '/'.join([admin_session.home_collection, filename])

                try:
                    # Replicas should be preserved on ufs1 and ufs2. ufs1 should not be the minimum restage tier to
                    # ensure that a restage to a lower tier can occur.
                    admin_session.assert_icommand('imeta rm -R ufs1 irods::storage_tiering::minimum_restage_tier true')
                    admin_session.assert_icommand('imeta rm -R ufs0 irods::storage_tiering::preserve_replicas true')
                    admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::preserve_replicas true')
                    admin_session.assert_icommand('imeta add -R ufs2 irods::storage_tiering::preserve_replicas true')

                    # Create test file and upload it.
                    lib.create_local_testfile(filename)
                    admin_session.assert_icommand('iput -R ufs0 ' + filename)
                    admin_session.assert_icommand('ils -L ', 'STDOUT_SINGLELINE', 'rods')

                    # Stage to tier 1 and ensure that the replica on ufs0 was trimmed.
                    time.sleep(6)
                    invoke_storage_tiering_rule()
                    admin_session.assert_icommand('iqstat', 'STDOUT_SINGLELINE', 'irods_policy_storage_tiering')

                    delay_assert_icommand(admin_session, 'ils -L ' + filename, 'STDOUT_SINGLELINE', '1 ufs1')
                    self.assertFalse(lib.replica_exists_on_resource(admin_session, logical_path, 'ufs0'))

                    # Stage to tier 2, look for replica in tier 1 and tier 2.
                    time.sleep(15)
                    invoke_storage_tiering_rule()
                    admin_session.assert_icommand('iqstat', 'STDOUT_SINGLELINE', 'irods_policy_storage_tiering')

                    delay_assert_icommand(admin_session, 'ils -L ' + filename, 'STDOUT_SINGLELINE', '1 ufs1')
                    delay_assert_icommand(admin_session, 'ils -L ' + filename, 'STDOUT_SINGLELINE', '2 ufs2')

                    # Test restage to tier 0 using replica from tier 1 to ensure that a replica that is not the
                    # "tracked" replica (the tracked replica is in ufs2) gets restaged. Look for replica in all tiers
                    # afterwards.
                    admin_session.assert_icommand('iget -R ufs1 ' + filename + ' - ', 'STDOUT_SINGLELINE', 'TESTFILE')

                    time.sleep(15)
                    delay_assert_icommand(admin_session, 'ils -L ' + filename, 'STDOUT_SINGLELINE', '3 ufs0')
                    delay_assert_icommand(admin_session, 'ils -L ' + filename, 'STDOUT_SINGLELINE', '1 ufs1')
                    delay_assert_icommand(admin_session, 'ils -L ' + filename, 'STDOUT_SINGLELINE', '2 ufs2')


                finally:
                    admin_session.assert_icommand('imeta rm -R ufs1 irods::storage_tiering::preserve_replicas true')
                    admin_session.assert_icommand('imeta rm -R ufs2 irods::storage_tiering::preserve_replicas true')
                    admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::preserve_replicas true')
                    admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::minimum_restage_tier true')

                    admin_session.assert_icommand('irm -f ' + filename)

    def test_preserve_replicas_works_with_restage_when_replica_only_exists_in_last_tier__issue_233(self):
        with storage_tiering_configured_with_log():
            IrodsController().restart(test_mode=True)
            with session.make_session_for_existing_admin() as admin_session:
                filename = 'test_put_file'

                try:
                    # make sure replicas stored on tier 1 are preserved and the minimum_restage_tier is not set
                    admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::preserve_replicas true')
                    admin_session.assert_icommand('imeta rm  -R ufs1 irods::storage_tiering::minimum_restage_tier true')

                    # make sure tier 2 does not get involved
                    admin_session.assert_icommand('imeta rm -R ufs2 irods::storage_tiering::group example_group 2')

                    # create test file and upload it
                    lib.create_local_testfile(filename)
                    admin_session.assert_icommand('iput -R ufs0 ' + filename)
                    admin_session.assert_icommand('ils -L ', 'STDOUT_SINGLELINE', 'rods')

                    # stage to tier 1, look for both replicas
                    time.sleep(6)
                    invoke_storage_tiering_rule()
                    admin_session.assert_icommand('iqstat', 'STDOUT_SINGLELINE', 'irods_policy_storage_tiering')

                    delay_assert_icommand(admin_session, 'ils -L ' + filename, 'STDOUT_SINGLELINE', '0 ufs0')
                    delay_assert_icommand(admin_session, 'ils -L ' + filename, 'STDOUT_SINGLELINE', '1 ufs1')

                    # remove the object in tier 0
                    admin_session.assert_icommand('itrim -N1 -n0 ' + filename, 'STDOUT_SINGLELINE', 'Number of files trimmed = 1' )

                    # test restage to tier 0, look for replica in tier 0 and tier 1
                    admin_session.assert_icommand('iget ' + filename + ' - ', 'STDOUT_SINGLELINE', 'TESTFILE')

                    time.sleep(15)
                    delay_assert_icommand(admin_session, 'ils -L ' + filename, 'STDOUT_SINGLELINE', '2 ufs0')
                    delay_assert_icommand(admin_session, 'ils -L ' + filename, 'STDOUT_SINGLELINE', '1 ufs1')


                finally:
                    admin_session.assert_icommand('irm -f ' + filename)

    def test_tiering_out_with_existing_replica_in_higher_tier__issue_235(self):
        with storage_tiering_configured_with_log():
            IrodsController().restart(test_mode=True)
            with session.make_session_for_existing_admin() as admin_session:
                filename = 'test_put_file'
                logical_path = '/'.join([admin_session.home_collection, filename])

                try:
                    # Preserve replicas on tier 1 and tier 2, not tier 0. Make tier 1 NOT the minimum restage tier.
                    admin_session.assert_icommand('imeta rm -R ufs0 irods::storage_tiering::preserve_replicas true')
                    admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::preserve_replicas true')
                    admin_session.assert_icommand('imeta rm -R ufs1 irods::storage_tiering::minimum_restage_tier true')

                    lib.create_local_testfile(filename)
                    admin_session.assert_icommand('iput -R ufs0 ' + filename)
                    self.assertTrue(lib.replica_exists_on_resource(admin_session, logical_path, 'ufs0'))

                    # Tier out replica from ufs0 to ufs1.
                    time.sleep(6)
                    invoke_storage_tiering_rule()
                    admin_session.assert_icommand('iqstat', 'STDOUT', 'irods_policy_storage_tiering')

                    # Ensure that the replica is trimmed from ufs0 and tiered out to ufs1. Check for the trim first
                    # because once that happens, we know the replication should have happened already.
                    lib.delayAssert(
                        lambda: lib.replica_exists_on_resource(admin_session, logical_path, 'ufs0') == False)
                    self.assertTrue(lib.replica_exists_on_resource(admin_session, logical_path, 'ufs1'))
                    self.assertFalse(lib.replica_exists_on_resource(admin_session, logical_path, 'ufs2'))

                    # Ensure that the "tracked" replica updates to replica 1, which is the tiered-out replica on ufs1.
                    self.assertEqual('1', get_tracked_replica(admin_session, logical_path))

                    # Tier out replica from ufs1 to ufs2.
                    time.sleep(15)
                    invoke_storage_tiering_rule()
                    admin_session.assert_icommand('iqstat', 'STDOUT', 'irods_policy_storage_tiering')

                    # Ensure that the replica was tiered out to ufs2 and preserved on ufs1, and no replica exists on
                    # ufs0.
                    lib.delayAssert(lambda: lib.replica_exists_on_resource(admin_session, logical_path, 'ufs2'))
                    self.assertTrue(lib.replica_exists_on_resource(admin_session, logical_path, 'ufs1'))
                    self.assertFalse(lib.replica_exists_on_resource(admin_session, logical_path, 'ufs0'))

                    # Ensure that the "tracked" replica updates to replica 2, which is the tiered-out replica on ufs2.
                    self.assertEqual('2', get_tracked_replica(admin_session, logical_path))

                    # Get the data object in order to trigger a restage, targeting replica 2 to ensure it is the replica
                    # that gets restaged.
                    admin_session.assert_icommand(['iget', '-n', '2', filename, '-'], 'STDOUT', 'TESTFILE')

                    # Ensure that the restage has been scheduled.
                    admin_session.assert_icommand('iqstat', 'STDOUT', 'irods_policy_data_movement')

                    # Ensure that the replica is restaged to the minimum restage tier (ufs0) and the replica on ufs2
                    # is trimmed. Also make sure ufs1 still has a replica. The restage is signified by the
                    # trimming of the higher tier replica and an updated group AVU.
                    lib.delayAssert(lambda: lib.replica_exists_on_resource(admin_session, logical_path, 'ufs2') == False)
                    self.assertTrue(lib.replica_exists_on_resource(admin_session, logical_path, 'ufs0'))
                    self.assertTrue(lib.replica_exists_on_resource(admin_session, logical_path, 'ufs1'))

                    # Ensure that the "tracked" replica updates to replica 3, which is the replica restaged to ufs0.
                    self.assertEqual('3', get_tracked_replica(admin_session, logical_path))

                    # Now for the actual test. Tier out replica from ufs0 to ufs1.
                    time.sleep(6)
                    invoke_storage_tiering_rule()
                    admin_session.assert_icommand('iqstat', 'STDOUT', 'irods_policy_storage_tiering')

                    # Ensure that the replica was tiered out to ufs1 and not preserved on ufs0. We test for the absence
                    # of a replica on ufs0 because the restage is signified by the trimming of the lower tier replica
                    # and an updated group AVU.
                    lib.delayAssert(lambda: lib.replica_exists_on_resource(admin_session, logical_path, 'ufs0') == False)
                    self.assertTrue(lib.replica_exists_on_resource(admin_session, logical_path, 'ufs1'))
                    self.assertFalse(lib.replica_exists_on_resource(admin_session, logical_path, 'ufs2'))

                    # Ensure that the "tracked" replica updates to replica 1, which is the replica on ufs1 that has not
                    # moved since it was created.
                    self.assertEqual('1', get_tracked_replica(admin_session, logical_path))

                finally:
                    # Ensure that the preservation policy for each resource is set back to what it was before.
                    admin_session.run_icommand('imeta add -R ufs0 irods::storage_tiering::preserve_replicas true')
                    admin_session.run_icommand('imeta rm -R ufs1 irods::storage_tiering::preserve_replicas true')
                    admin_session.run_icommand('imeta add -R ufs1 irods::storage_tiering::minimum_restage_tier true')

                    admin_session.assert_icommand('irm -f ' + filename)

    def test_restaging_with_existing_replica_in_lower_tier__issue_235(self):
        with storage_tiering_configured_with_log():
            IrodsController().restart(test_mode=True)
            with session.make_session_for_existing_admin() as admin_session:
                filename = 'test_put_file'
                logical_path = '/'.join([admin_session.home_collection, filename])

                try:
                    # Preserve replicas on tier 1, not tier 0 or tier 2.
                    admin_session.assert_icommand('imeta rm -R ufs0 irods::storage_tiering::preserve_replicas true')
                    admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::preserve_replicas true')

                    lib.create_local_testfile(filename)
                    admin_session.assert_icommand('iput -R ufs0 ' + filename)
                    self.assertTrue(lib.replica_exists_on_resource(admin_session, logical_path, 'ufs0'))

                    # Tier out replica from ufs0 to ufs1.
                    time.sleep(6)
                    invoke_storage_tiering_rule()
                    admin_session.assert_icommand('iqstat', 'STDOUT', 'irods_policy_storage_tiering')

                    # Ensure that the replica was tiered out to ufs1 and not preserved on ufs0.
                    lib.delayAssert(lambda: lib.replica_exists_on_resource(admin_session, logical_path, 'ufs0') == False)
                    self.assertTrue(lib.replica_exists_on_resource(admin_session, logical_path, 'ufs1'))
                    self.assertFalse(lib.replica_exists_on_resource(admin_session, logical_path, 'ufs2'))

                    # Ensure that the "tracked" replica updates to replica 1, which is the tiered-out replica on ufs1.
                    self.assertEqual('1', get_tracked_replica(admin_session, logical_path))

                    # Tier out replica from ufs1 to ufs2.
                    time.sleep(15)
                    invoke_storage_tiering_rule()
                    admin_session.assert_icommand('iqstat', 'STDOUT', 'irods_policy_storage_tiering')

                    # Ensure that the replica was tiered out to ufs2 and preserved on ufs1, and no replica exists on
                    # ufs0.
                    lib.delayAssert(lambda: lib.replica_exists_on_resource(admin_session, logical_path, 'ufs2'))
                    self.assertTrue(lib.replica_exists_on_resource(admin_session, logical_path, 'ufs1'))
                    self.assertFalse(lib.replica_exists_on_resource(admin_session, logical_path, 'ufs0'))

                    # Ensure that the "tracked" replica updates to replica 2, which is the tiered-out replica on ufs2.
                    self.assertEqual('2', get_tracked_replica(admin_session, logical_path))

                    # Now for the actual test. Get the data object in order to trigger a restage, targeting replica 2
                    # to ensure it is the replica that gets restaged.
                    admin_session.assert_icommand(['iget', '-n', '2', filename, '-'], 'STDOUT', 'TESTFILE')

                    # Ensure that the restage has been scheduled.
                    admin_session.assert_icommand('iqstat', 'STDOUT', 'irods_policy_data_movement')

                    # Ensure that the replica is restaged to the minimum restage tier (ufs1) and the replica on ufs2
                    # is trimmed. Also make sure ufs0 still does not have a replica. We test for the absence of a
                    # replica on ufs2 because the restage is signified by the trimming of the higher tier replica and an
                    # updated group AVU.
                    lib.delayAssert(lambda: lib.replica_exists_on_resource(admin_session, logical_path, 'ufs2') == False)
                    self.assertFalse(lib.replica_exists_on_resource(admin_session, logical_path, 'ufs0'))
                    self.assertTrue(lib.replica_exists_on_resource(admin_session, logical_path, 'ufs1'))

                    # Ensure that the "tracked" replica updates to replica 1, which is the replica on ufs1 that has not
                    # moved since it was created.
                    self.assertEqual('1', get_tracked_replica(admin_session, logical_path))

                finally:
                    # Ensure that the preservation policy for each resource is set back to what it was before.
                    admin_session.run_icommand('imeta add -R ufs0 irods::storage_tiering::preserve_replicas true')
                    admin_session.run_icommand('imeta rm -R ufs1 irods::storage_tiering::preserve_replicas true')

                    admin_session.assert_icommand('irm -f ' + filename)

    def test_replicas_cannot_be_restaged_to_a_higher_tier__issue_239(self):
        with storage_tiering_configured_with_log():
            IrodsController().restart(test_mode=True)
            with session.make_session_for_existing_admin() as admin_session:
                filename = 'test_put_file'
                logical_path = '/'.join([admin_session.home_collection, filename])

                try:
                    lib.create_local_testfile(filename)
                    admin_session.assert_icommand('iput -R ufs0 ' + filename)
                    self.assertTrue(lib.replica_exists_on_resource(admin_session, logical_path, 'ufs0'))

                    # Tier out replica from ufs0 to ufs1.
                    time.sleep(6)
                    invoke_storage_tiering_rule()
                    admin_session.assert_icommand('iqstat', 'STDOUT', 'irods_policy_storage_tiering')

                    # Ensure that the replica was tiered out to ufs1 and preserved on ufs0.
                    lib.delayAssert(lambda: lib.replica_exists_on_resource(admin_session, logical_path, 'ufs1'))
                    self.assertTrue(lib.replica_exists_on_resource(admin_session, logical_path, 'ufs0'))
                    self.assertFalse(lib.replica_exists_on_resource(admin_session, logical_path, 'ufs2'))

                    # Ensure that the "tracked" replica updates to replica 1, which is the tiered-out replica on ufs1.
                    self.assertEqual('1', get_tracked_replica(admin_session, logical_path))

                    # Tier out replica from ufs1 to ufs2.
                    time.sleep(15)
                    invoke_storage_tiering_rule()
                    admin_session.assert_icommand('iqstat', 'STDOUT', 'irods_policy_storage_tiering')

                    # Ensure that the replica was tiered out to ufs2 and not preserved on ufs1.
                    lib.delayAssert(lambda: lib.replica_exists_on_resource(admin_session, logical_path, 'ufs2'))
                    self.assertFalse(lib.replica_exists_on_resource(admin_session, logical_path, 'ufs1'))
                    self.assertTrue(lib.replica_exists_on_resource(admin_session, logical_path, 'ufs0'))

                    # Ensure that the "tracked" replica updates to replica 2, which is the tiered-out replica on ufs2.
                    self.assertEqual('2', get_tracked_replica(admin_session, logical_path))

                    # Now for the actual test. Get the data object in order to trigger a restage, targeting replica 0
                    # to ensure it is the replica that gets restaged.
                    admin_session.assert_icommand(['iget', '-n', '0', filename, '-'], 'STDOUT', 'TESTFILE')

                    # Wait for a bit to ensure that no data movement is happening. Assertions about whether anything
                    # happened are made below. The sleep time is 1 second, so 15 seconds of sleep is plenty of time to
                    # let any data migrations (or lack thereof) finish up. This test expects no movement to occur.
                    time.sleep(15)
                    admin_session.assert_icommand_fail('iqstat', 'STDOUT', 'irods_policy_data_movement')

                    # Ensure that the replica is NOT restaged to the minimum restage tier (ufs1) and the replica on ufs0
                    # is NOT trimmed. This is because the restage is happening from a tier lower than the minimum
                    # restage tier. Ensure that the replica on ufs2 hasn't moved either because it is not the replica
                    # which was scheduled for restage.
                    self.assertFalse(lib.replica_exists_on_resource(admin_session, logical_path, 'ufs1'))
                    self.assertTrue(lib.replica_exists_on_resource(admin_session, logical_path, 'ufs0'))
                    self.assertTrue(lib.replica_exists_on_resource(admin_session, logical_path, 'ufs2'))

                    # Ensure that the "tracked" replica does not update. We assert this because the restage is not
                    # supposed to happen.
                    self.assertEqual('2', get_tracked_replica(admin_session, logical_path))

                finally:
                    admin_session.assert_icommand('irm -f ' + filename)


class TestStorageTieringPluginObjectLimit(ResourceBase, unittest.TestCase):
    def setUp(self):
        super(TestStorageTieringPluginObjectLimit, self).setUp()
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand('iqdel -a')
            admin_session.assert_icommand('iadmin mkresc ufs0 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs0', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs1 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs1', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs2 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs2', 'STDOUT_SINGLELINE', 'unixfilesystem')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::group example_group 0')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::group example_group 1')
            admin_session.assert_icommand('imeta add -R ufs2 irods::storage_tiering::group example_group 2')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::time 5')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::time 15')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::minimum_restage_tier true')
            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::minimum_delay_time_in_seconds 1')
            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::maximum_delay_time_in_seconds 2')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::minimum_delay_time_in_seconds 1')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::maximum_delay_time_in_seconds 2')

            self.filename  = 'test_put_file'
            self.filename2 = 'test_put_file2'

    def tearDown(self):
        super(TestStorageTieringPluginObjectLimit, self).tearDown()
        with session.make_session_for_existing_admin() as admin_session:

            admin_session.assert_icommand('iadmin rmresc ufs0')
            admin_session.assert_icommand('iadmin rmresc ufs1')
            admin_session.assert_icommand('iadmin rmresc ufs2')
            admin_session.assert_icommand('iadmin rum')

    def test_put_and_get_limit_1(self):
        with storage_tiering_configured():
            IrodsController().restart(test_mode=True)
            with session.make_session_for_existing_admin() as admin_session:
                try:
                    admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::object_limit 1')

                    lib.create_local_testfile(self.filename)

                    admin_session.assert_icommand('iput -R ufs0 ' + self.filename)
                    admin_session.assert_icommand('iput -R ufs0 ' + self.filename + " " + self.filename2)
                    admin_session.assert_icommand('ils -L ', 'STDOUT_SINGLELINE', 'rods')

                    # stage to tier 1, look for both replicas (only one should move)
                    time.sleep(5)
                    invoke_storage_tiering_rule()
                    admin_session.assert_icommand('iqstat', 'STDOUT_SINGLELINE', 'irods_policy_storage_tiering')
                    delay_assert_icommand(admin_session, 'ils -L ' + self.filename2, 'STDOUT_SINGLELINE', 'ufs0')
                    delay_assert_icommand(admin_session, 'ils -L ' + self.filename, 'STDOUT_SINGLELINE', 'ufs1')

                finally:
                    admin_session.assert_icommand('irm -f ' + self.filename)
                    admin_session.assert_icommand('irm -f ' + self.filename2)

    def test_put_and_get_no_limit_zero(self):
        with storage_tiering_configured():
            IrodsController().restart(test_mode=True)
            with session.make_session_for_existing_admin() as admin_session:
                try:
                    admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::object_limit 0')

                    lib.create_local_testfile(self.filename)

                    admin_session.assert_icommand('iput -R ufs0 ' + self.filename)
                    admin_session.assert_icommand('iput -R ufs0 ' + self.filename + " " + self.filename2)
                    admin_session.assert_icommand('ils -L ', 'STDOUT_SINGLELINE', 'rods')

                    # stage to tier 1, everything should move
                    time.sleep(5)
                    invoke_storage_tiering_rule()
                    admin_session.assert_icommand('iqstat', 'STDOUT_SINGLELINE', 'irods_policy_storage_tiering')
                    delay_assert_icommand(admin_session, 'ils -L ' + self.filename, 'STDOUT_SINGLELINE', 'ufs1')
                    delay_assert_icommand(admin_session, 'ils -L ' + self.filename2, 'STDOUT_SINGLELINE', 'ufs1')

                finally:
                    admin_session.assert_icommand('irm -f ' + self.filename)
                    admin_session.assert_icommand('irm -f ' + self.filename2)

    def test_put_and_get_no_limit_default(self):
        with storage_tiering_configured():
            IrodsController().restart(test_mode=True)
            with session.make_session_for_existing_admin() as admin_session:
                try:
                    lib.create_local_testfile(self.filename)
                    admin_session.assert_icommand('iput -R ufs0 ' + self.filename)
                    admin_session.assert_icommand('iput -R ufs0 ' + self.filename + " " + self.filename2)
                    admin_session.assert_icommand('ils -L ', 'STDOUT_SINGLELINE', 'rods')

                    # stage to tier 1, everything should move
                    time.sleep(5)
                    invoke_storage_tiering_rule()
                    admin_session.assert_icommand('iqstat', 'STDOUT_SINGLELINE', 'irods_policy_storage_tiering')
                    delay_assert_icommand(admin_session, 'ils -L ' + self.filename, 'STDOUT_SINGLELINE', 'ufs1')
                    delay_assert_icommand(admin_session, 'ils -L ' + self.filename2, 'STDOUT_SINGLELINE', 'ufs1')

                finally:
                    admin_session.assert_icommand('irm -f ' + self.filename)
                    admin_session.assert_icommand('irm -f ' + self.filename2)


class TestStorageTieringMultipleQueries(ResourceBase, unittest.TestCase):
    def setUp(self):
        super(TestStorageTieringMultipleQueries, self).setUp()
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand('iqdel -a')
            admin_session.assert_icommand('''iadmin asq "select distinct R_DATA_MAIN.data_name, R_COLL_MAIN.coll_name, R_USER_MAIN.user_name, R_USER_MAIN.zone_name, R_DATA_MAIN.data_repl_num from R_DATA_MAIN inner join R_COLL_MAIN on R_DATA_MAIN.coll_id = R_COLL_MAIN.coll_id inner join R_RESC_MAIN on R_DATA_MAIN.resc_id = R_RESC_MAIN.resc_id inner join R_OBJT_ACCESS r_data_access on R_DATA_MAIN.data_id = r_data_access.object_id inner join R_OBJT_METAMAP r_data_metamap on R_DATA_MAIN.data_id = r_data_metamap.object_id inner join R_META_MAIN r_data_meta_main on r_data_metamap.meta_id = r_data_meta_main.meta_id inner join R_USER_MAIN on r_data_access.user_id = R_USER_MAIN.user_id WHERE R_RESC_MAIN.resc_name = 'ufs0' and r_data_meta_main.meta_attr_name = 'archive_object' and r_data_meta_main.meta_attr_value = 'yes' order by R_COLL_MAIN.coll_name, R_DATA_MAIN.data_name, R_DATA_MAIN.data_repl_num" archive_query''')

            admin_session.assert_icommand('iadmin mkresc ufs0 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs0', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs1 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs1', 'STDOUT_SINGLELINE', 'unixfilesystem')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::group example_group 0')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::group example_group 1')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::time 15')

            admin_session.assert_icommand('''imeta add -R ufs0 irods::storage_tiering::query "SELECT DATA_NAME, COLL_NAME, USER_NAME, USER_ZONE, DATA_REPL_NUM where RESC_NAME = 'ufs0' and META_DATA_ATTR_NAME = 'irods::access_time' and META_DATA_ATTR_VALUE < 'TIME_CHECK_STRING'"''')
            admin_session.assert_icommand('''imeta add -R ufs0 irods::storage_tiering::query archive_query specific''')
            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::minimum_delay_time_in_seconds 1')
            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::maximum_delay_time_in_seconds 2')


    def tearDown(self):
        super(TestStorageTieringMultipleQueries, self).tearDown()
        with session.make_session_for_existing_admin() as admin_session:

            admin_session.assert_icommand('iadmin rmresc ufs0')
            admin_session.assert_icommand('iadmin rmresc ufs1')
            admin_session.assert_icommand('iadmin rum')
            admin_session.assert_icommand('iadmin rsq archive_query')

    def test_put_and_get(self):
        with storage_tiering_configured():
            IrodsController().restart(test_mode=True)
            with session.make_session_for_existing_admin() as admin_session:

                filename  = 'test_put_file'
                filename2 = 'test_put_file2'
                filepath  = lib.create_local_testfile(filename)

                try:
                    admin_session.assert_icommand('iput -R ufs0 ' + filename)
                    admin_session.assert_icommand('imeta add -d ' + filename + ' archive_object yes')
                    admin_session.assert_icommand('imeta ls -d ' + filename, 'STDOUT_SINGLELINE', 'irods::access_time')

                    admin_session.assert_icommand('iput -R ufs0 ' + filename + ' ' + filename2)
                    admin_session.assert_icommand('imeta ls -d ' + filename2, 'STDOUT_SINGLELINE', 'irods::access_time')

                    # test stage to tier 1
                    invoke_storage_tiering_rule()
                    delay_assert_icommand(admin_session, 'ils -L ' + filename, 'STDOUT_SINGLELINE', 'ufs1')
                    delay_assert_icommand(admin_session, 'ils -L ' + filename2, 'STDOUT_SINGLELINE', 'ufs0')

                    time.sleep(15)
                    invoke_storage_tiering_rule()
                    delay_assert_icommand(admin_session, 'ils -L ' + filename2, 'STDOUT_SINGLELINE', 'ufs1')

                finally:
                    admin_session.assert_icommand('irm -f ' + filename)
                    admin_session.assert_icommand('irm -f ' + filename2)

class TestStorageTieringPluginRegistration(ResourceBase, unittest.TestCase):
    def setUp(self):
        super(TestStorageTieringPluginRegistration, self).setUp()
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand('iqdel -a')
            admin_session.assert_icommand('iadmin mkresc ufs0 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs0', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs1 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs1', 'STDOUT_SINGLELINE', 'unixfilesystem')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::group example_group 0')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::group example_group 1')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::time 5')
            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::minimum_delay_time_in_seconds 1')
            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::maximum_delay_time_in_seconds 2')


    def tearDown(self):
        super(TestStorageTieringPluginRegistration, self).tearDown()
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand('iadmin rmresc ufs0')
            admin_session.assert_icommand('iadmin rmresc ufs1')
            admin_session.assert_icommand('iadmin rum')

    def test_file_registration(self):
        with storage_tiering_configured():
            IrodsController().restart(test_mode=True)
            with session.make_session_for_existing_admin() as admin_session:
                filename  = 'test_put_file'
                filepath  = lib.create_local_testfile(filename)
                ipwd, _, _ = admin_session.run_icommand('ipwd')
                ipwd = ipwd.rstrip()
                dest_path = ipwd + '/' + filename

                try:
                    admin_session.assert_icommand('ipwd', 'STDOUT_SINGLELINE', 'rods')
                    admin_session.assert_icommand('ireg -R ufs0 ' + filepath + ' ' + dest_path)
                    admin_session.assert_icommand('imeta ls -d ' + filename, 'STDOUT_SINGLELINE', filename)
                    admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', filename)

                    # test stage to tier 1
                    time.sleep(5)
                    invoke_storage_tiering_rule()
                    delay_assert_icommand(admin_session, 'ils -L ' + filename, 'STDOUT_SINGLELINE', 'ufs1')

                finally:
                    admin_session.assert_icommand('irm -f ' + filename)

    def test_directory_registration(self):
        with storage_tiering_configured():
            IrodsController().restart(test_mode=True)
            with session.make_session_for_existing_admin() as admin_session:
                local_dir_name = '/tmp/test_directory_registration_dir'
                shutil.rmtree(local_dir_name, ignore_errors=True)
                local_tree = lib.make_deep_local_tmp_dir(local_dir_name, 3, 10, 5)

                dest_path = '/tempZone/home/rods/reg_coll'

                try:
                    admin_session.assert_icommand('ireg -r -R ufs0 ' + local_dir_name + ' ' + dest_path)
                    admin_session.assert_icommand('ils -rL ' + dest_path, 'STDOUT_SINGLELINE', dest_path)

                    # test stage to tier 1
                    time.sleep(5)
                    invoke_storage_tiering_rule()
                    # Wait until the object migrates to the next tier.
                    lib.delayAssert(lambda: lib.replica_exists_on_resource(admin_session, dest_path, "ufs0") == False)
                    lib.replica_exists_on_resource(admin_session, dest_path, "ufs1")

                finally:
                    delay_assert_icommand(admin_session, 'iqdel -a')

                    admin_session.assert_icommand('irm -rf ' + dest_path)


class TestStorageTieringContinueInxMigration(ResourceBase, unittest.TestCase):
    def setUp(self):
        super(TestStorageTieringContinueInxMigration, self).setUp()
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand('iqdel -a')
            admin_session.assert_icommand('iadmin mkresc ufs0 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs0', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs1 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs1', 'STDOUT_SINGLELINE', 'unixfilesystem')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::group example_group 0')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::group example_group 1')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::time 5')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::minimum_delay_time_in_seconds 1')
            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::maximum_delay_time_in_seconds 2')

            self.max_sql_rows = 256

    def tearDown(self):
        super(TestStorageTieringContinueInxMigration, self).tearDown()
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand('iadmin rmresc ufs0')
            admin_session.assert_icommand('iadmin rmresc ufs1')
            admin_session.assert_icommand('iadmin rum')

    def test_put_gt_max_sql_rows(self):
        with storage_tiering_configured():
            IrodsController().restart(test_mode=True)
            with session.make_session_for_existing_admin() as admin_session:
                # Put enough objects to force continueInx when iterating over violating objects (above MAX_SQL_ROWS)
                file_count = self.max_sql_rows + 1
                dirname = 'test_put_gt_max_sql_rows'

                try:
                    shutil.rmtree(dirname, ignore_errors=True)
                    lib.make_large_local_tmp_dir(dirname, file_count, 1)
                    admin_session.assert_icommand(['iput', '-R', 'ufs0', '-r', dirname])

                    # stage to tier 1, everything should have been tiered out
                    time.sleep(5)
                    invoke_storage_tiering_rule()
                    time.sleep(5)
                    delay_assert_icommand(admin_session, ['ils', '-l', dirname], 'STDOUT_SINGLELINE', 'ufs1')
                    delay_assert_icommand(admin_session, ['ils', '-l', dirname], 'STDOUT_SINGLELINE', 'ufs0')
                    # Wait for the queue to be emptied and ensure that everything has tiered out from ufs0
                    wait_for_empty_queue(
                        lambda: admin_session.assert_icommand_fail(['ils', '-l', dirname], 'STDOUT', 'ufs0'),
                        timeout_function=lambda: self.fail("Timed out waiting on queue to empty."))

                finally:
                    delay_assert_icommand(admin_session, 'iqdel -a')

                    # cleanup
                    admin_session.assert_icommand(['irm', '-f', '-r', dirname])
                    shutil.rmtree(dirname, ignore_errors=True)

    def test_put_max_sql_rows(self):
        with storage_tiering_configured():
            IrodsController().restart(test_mode=True)
            with session.make_session_for_existing_admin() as admin_session:
                # Put exactly MAX_SQL_ROWS objects (boundary test)
                file_count = self.max_sql_rows
                dirname = 'test_put_max_sql_rows'

                try:
                    shutil.rmtree(dirname, ignore_errors=True)
                    lib.make_large_local_tmp_dir(dirname, file_count, 1)
                    admin_session.assert_icommand(['iput', '-R', 'ufs0', '-r', dirname])

                    # stage to tier 1, everything should have been tiered out
                    time.sleep(5)
                    invoke_storage_tiering_rule()
                    time.sleep(5)
                    delay_assert_icommand(admin_session, ['ils', '-l', dirname], 'STDOUT_SINGLELINE', 'ufs1')
                    delay_assert_icommand(admin_session, ['ils', '-l', dirname], 'STDOUT_SINGLELINE', 'ufs0')
                    # Wait for the queue to be emptied and ensure that everything has tiered out from ufs0
                    wait_for_empty_queue(
                        lambda: admin_session.assert_icommand_fail(['ils', '-l', dirname], 'STDOUT', 'ufs0'),
                        timeout_function=lambda: self.fail("Timed out waiting on queue to empty."))

                finally:
                    delay_assert_icommand(admin_session, 'iqdel -a')

                    # cleanup
                    admin_session.assert_icommand(['irm', '-f', '-r', dirname])
                    shutil.rmtree(dirname, ignore_errors=True)

    def test_put_object_limit_lt(self):
        with storage_tiering_configured():
            IrodsController().restart(test_mode=True)
            with session.make_session_for_existing_admin() as admin_session:
                # Put enough objects to force continueInx and set object_limit to one less than that (above MAX_SQL_ROWS)
                file_count = self.max_sql_rows + 2
                admin_session.assert_icommand(['imeta', 'add', '-R', 'ufs0', 'irods::storage_tiering::object_limit', str(file_count - 1)])
                dirname = 'test_put_object_limit_lt'

                try:
                    shutil.rmtree(dirname, ignore_errors=True)
                    last_item_path = os.path.join(dirname, 'junk0' + str(file_count - 1))
                    next_to_last_item_path = os.path.join(dirname, 'junk0' + str(file_count - 2))
                    lib.make_large_local_tmp_dir(dirname, file_count, 1)
                    admin_session.assert_icommand(['iput', '-R', 'ufs0', '-r', dirname])

                    # stage to tier 1, only the last item should not have been tiered out
                    time.sleep(5)
                    invoke_storage_tiering_rule()
                    time.sleep(5)
                    delay_assert_icommand(admin_session, ['ils', '-l', dirname], 'STDOUT_SINGLELINE', 'ufs0')
                    delay_assert_icommand(admin_session, ['ils', '-l', dirname], 'STDOUT_SINGLELINE', 'ufs1')
                    # Wait for the queue to be emptied and ensure that everything has tiered out from ufs0 except for 1
                    wait_for_empty_queue(
                        lambda: admin_session.assert_icommand(['ils', '-l', dirname], 'STDOUT', 'ufs0'),
                        timeout_function=lambda: self.fail("Timed out waiting on queue to empty."))
                    # Ensure that exactly 1 item did not tier out
                    _, out, _ = admin_session.assert_icommand(['ils', '-l', dirname], 'STDOUT_SINGLELINE', 'ufs0')
                    self.assertEqual(file_count - 1, out.count('ufs1'))
                    self.assertEqual(1, out.count('ufs0'))

                finally:
                    delay_assert_icommand(admin_session, 'iqdel -a')

                    # cleanup
                    admin_session.assert_icommand(['irm', '-f', '-r', dirname])
                    shutil.rmtree(dirname, ignore_errors=True)

    def test_put_multi_fetch_page(self):
        with storage_tiering_configured():
            IrodsController().restart(test_mode=True)
            with session.make_session_for_existing_admin() as admin_session:
                # Put enough objects to force results paging more than once
                file_count = (self.max_sql_rows * 2) + 1
                dirname = 'test_put_multi_fetch_page'

                try:
                    shutil.rmtree(dirname, ignore_errors=True)
                    lib.make_large_local_tmp_dir(dirname, file_count, 1)
                    admin_session.assert_icommand(['iput', '-R', 'ufs0', '-r', dirname])

                    # stage to tier 1, everything should have been tiered out
                    time.sleep(5)
                    invoke_storage_tiering_rule()
                    delay_assert_icommand(admin_session, ['ils', '-l', dirname], 'STDOUT_SINGLELINE', 'ufs1')
                    delay_assert(lambda: admin_session.assert_icommand_fail(['ils', '-l', dirname], 'STDOUT_SINGLELINE', 'ufs0'))

                finally:
                    delay_assert_icommand(admin_session, 'iqdel -a')

                    # cleanup
                    admin_session.assert_icommand(["irm", "-rf", dirname])
                    shutil.rmtree(dirname, ignore_errors=True)

class TestStorageTieringPluginMultiGroupRestage(ResourceBase, unittest.TestCase):
    def setUp(self):
        super(TestStorageTieringPluginMultiGroupRestage, self).setUp()
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand('iqdel -a')
            admin_session.assert_icommand('iadmin mkresc ufs0 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs0', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs1 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs1', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs2 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs2', 'STDOUT_SINGLELINE', 'unixfilesystem')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::group example_group 0')
            admin_session.assert_icommand('imeta add -R ufs2 irods::storage_tiering::group example_group 1')

            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::group example_group2 0')
            admin_session.assert_icommand('imeta add -R ufs2 irods::storage_tiering::group example_group2 1')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::time 5')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::time 5')
            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::minimum_delay_time_in_seconds 1')
            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::maximum_delay_time_in_seconds 2')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::minimum_delay_time_in_seconds 1')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::maximum_delay_time_in_seconds 2')


    def tearDown(self):
        super(TestStorageTieringPluginMultiGroupRestage, self).tearDown()
        with session.make_session_for_existing_admin() as admin_session:

            admin_session.assert_icommand('iadmin rmresc ufs0')
            admin_session.assert_icommand('iadmin rmresc ufs1')
            admin_session.assert_icommand('iadmin rmresc ufs2')
            admin_session.assert_icommand('iadmin rum')

    def test_put_and_get(self):
        with storage_tiering_configured():
            IrodsController().restart(test_mode=True)
            with session.make_session_for_existing_admin() as admin_session:

                try:
                    filename = 'test_put_file'
                    logical_path = "/".join([admin_session.home_collection, filename])
                    filepath  = lib.create_local_testfile(filename)
                    admin_session.assert_icommand('iput -R ufs0 ' + filename)
                    admin_session.assert_icommand('ils -L ', 'STDOUT_SINGLELINE', 'rods')
                    time.sleep(5)
                    invoke_storage_tiering_rule()
                    # Wait until the object migrates to the next tier.
                    lib.delayAssert(
                        lambda: lib.replica_exists_on_resource(admin_session, logical_path, "ufs0") == False)
                    lib.replica_exists_on_resource(admin_session, logical_path, "ufs2")
                    admin_session.assert_icommand('imeta ls -d '+filename, 'STDOUT_SINGLELINE', '--')

                    # test restage to tier 0
                    admin_session.assert_icommand('iget ' + filename + ' - ', 'STDOUT_SINGLELINE', 'TESTFILE')
                    delay_assert_icommand(admin_session, 'ils -L ' + filename, 'STDOUT_SINGLELINE', 'ufs0')
                finally:
                    admin_session.assert_icommand('irm -f ' + filename)


class test_incorrect_custom_violating_queries(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.user = session.mkuser_and_return_session('rodsuser', 'alice', 'apass', lib.get_hostname())

        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand(['iqdel', '-a'])

            lib.create_ufs_resource(admin_session, 'ufs0', test.settings.HOSTNAME_2)
            admin_session.assert_icommand(
                ['imeta', 'add', '-R', 'ufs0', 'irods::storage_tiering::group', 'example_group', '0'])
            admin_session.assert_icommand(['imeta', 'add', '-R', 'ufs0', 'irods::storage_tiering::time', '5'])
            admin_session.assert_icommand(
                ['imeta', 'ls', '-R', 'ufs0'], 'STDOUT_MULTILINE', ['value: 5', 'value: example_group'])

            lib.create_ufs_resource(admin_session, 'ufs1', test.settings.HOSTNAME_3)
            admin_session.assert_icommand(
                ['imeta', 'add', '-R', 'ufs1', 'irods::storage_tiering::group', 'example_group', '1'])
            admin_session.assert_icommand(['imeta', 'ls', '-R', 'ufs1'], 'STDOUT', 'value: example_group')

    @classmethod
    def tearDownClass(self):
        self.user.__exit__()

        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand(['iadmin', 'rmuser', self.user.username])

            admin_session.assert_icommand(['iadmin', 'rmresc', 'ufs0'])
            admin_session.assert_icommand(['iadmin', 'rmresc', 'ufs1'])
            admin_session.assert_icommand(['iadmin', 'rum'])

    def do_incorrect_violating_query_test(self, columns_to_select):
        with storage_tiering_configured_with_log():
            IrodsController().restart(test_mode=True)

            with session.make_session_for_existing_admin() as admin_session:
                zone_name = IrodsConfig().client_environment['irods_zone_name']

                filename = 'test_incorrect_violating_query.txt'
                logical_path = '/'.join([self.user.session_collection, filename])
                resource = 'ufs0'
                other_resource = 'ufs1'
                query_attribute_name = 'irods::storage_tiering::query'
                custom_violating_query = '''"SELECT {} where RESC_NAME = '{}' ''' \
                                         '''and META_DATA_ATTR_NAME = 'irods::access_time' ''' \
                                         '''and META_DATA_ATTR_VALUE < 'TIME_CHECK_STRING'"'''.format(
                                         columns_to_select, resource)

                try:
                    lib.create_local_testfile(filename)

                    admin_session.assert_icommand(
                        ['imeta', 'add', '-R', resource, query_attribute_name, custom_violating_query])

                    self.user.assert_icommand(['iput', '-R', resource, filename])
                    delay_assert_icommand(self.user, f'ils -l {filename}', 'STDOUT', resource)

                    # Attempt tiering out to tier 1. This should fail because the custom violating query is
                    # incorrect. Wait for the delay queue to empty and ensure that nothing has moved. There should
                    # be an error message in the log, but we don't assert that in the test here due to log
                    # reliability issues.
                    time.sleep(6)
                    invoke_storage_tiering_rule()
                    admin_session.assert_icommand(['iqstat', '-a'], 'STDOUT', 'irods_policy_storage_tiering')
                    lib.delayAssert(lambda:
                        'No delayed rules pending' in admin_session.run_icommand(['iqstat', '-a'])[0].strip())
                    self.user.assert_icommand_fail(['ils', '-l', logical_path], 'STDOUT', other_resource)
                    self.user.assert_icommand(['ils', '-l', logical_path], 'STDOUT', resource)

                finally:
                    self.user.run_icommand(['irm', '-f', logical_path])

                    admin_session.run_icommand(
                        ['imeta', 'rm', '-R', resource, query_attribute_name, custom_violating_query])

    def test_no_tiering_occurs_when_custom_violating_query_selects_fewer_columns_than_necessary__issue_231(self):
        # Missing USER_ZONE in query.
        columns = 'DATA_NAME, COLL_NAME, USER_NAME, DATA_REPL_NUM'
        self.do_incorrect_violating_query_test(columns)

    def test_no_tiering_occurs_when_custom_violating_query_selects_more_columns_than_necessary__issue_231(self):
        # Should not be selecting DATA_ID.
        columns = 'DATA_NAME, COLL_NAME, USER_NAME, USER_ZONE, DATA_REPL_NUM, DATA_ID'
        self.do_incorrect_violating_query_test(columns)

    def test_no_tiering_occurs_when_custom_violating_query_selects_wrong_columns__issue_231(self):
        # DATA_ID is supposed to be DATA_NAME.
        columns = 'DATA_ID, COLL_NAME, USER_NAME, USER_ZONE, DATA_REPL_NUM'
        self.do_incorrect_violating_query_test(columns)

    def test_no_tiering_occurs_when_custom_violating_query_selects_correct_columns_in_the_wrong_order__issue_231(self):
        # The ordering of DATA_NAME and COLL_NAME is flipped.
        columns = 'COLL_NAME, DATA_NAME, USER_NAME, USER_ZONE, DATA_REPL_NUM'
        self.do_incorrect_violating_query_test(columns)


class test_executing_rules_as_rodsuser__issue_293(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.user1 = session.mkuser_and_return_session("rodsuser", "tolstoy", "tpass", lib.get_hostname())

        self.tier0 = "ufs0"
        self.tier1 = "ufs1"
        self.tier0_time_in_seconds = 5

        self.filename = "test_executing_rules_as_rodsuser__issue_293"
        if not os.path.exists(self.filename):
            lib.create_local_testfile(self.filename)

        self.collection_path = self.user1.home_collection
        self.object_path = "/".join([self.collection_path, self.filename])

        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand(['iqdel', '-a'])

            # Using HOSTNAME_2 here so that topology testing takes effect should we ever employ it in plugin testing.
            lib.create_ufs_resource(admin_session, self.tier0, test.settings.HOSTNAME_2)
            admin_session.assert_icommand(
                ['imeta', 'add', '-R', self.tier0, 'irods::storage_tiering::group', 'example_group', '0'])
            admin_session.assert_icommand(
                ['imeta', 'add', '-R', self.tier0, 'irods::storage_tiering::time', str(self.tier0_time_in_seconds)])
            admin_session.assert_icommand(
                ['imeta', 'ls', '-R', self.tier0], 'STDOUT', [f'value: {self.tier0_time_in_seconds}', 'value: example_group'])

            # Using HOSTNAME_3 here so that topology testing takes effect should we ever employ it in plugin testing.
            lib.create_ufs_resource(admin_session, self.tier1, test.settings.HOSTNAME_3)
            admin_session.assert_icommand(
                ['imeta', 'add', '-R', self.tier1, 'irods::storage_tiering::group', 'example_group', '1'])
            admin_session.assert_icommand(['imeta', 'ls', '-R', self.tier1], 'STDOUT', 'value: example_group')

    @classmethod
    def tearDownClass(self):
        if os.path.exists(self.filename):
            os.unlink(self.filename)

        self.user1.__exit__()

        with session.make_session_for_existing_admin() as admin_session:
            admin_session.run_icommand(['iadmin', 'rmuser', self.user1.username])

            admin_session.run_icommand(['iqdel', '-a'])

            admin_session.run_icommand(['iadmin', 'rmresc', self.tier0])
            admin_session.run_icommand(['iadmin', 'rmresc', self.tier1])
            admin_session.run_icommand(['iadmin', 'rum'])

    def tearDown(self):
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.run_icommand(["ichmod", "-M", "own", admin_session.username, self.object_path])
            admin_session.run_icommand(["irm", "-f", self.object_path])

    def test_rule_invoked_by_rodsuser_directly_via_irule_fails(self):
        with storage_tiering_configured():
            IrodsController().restart(test_mode=True)

            # TODO(#200): Replace with itouch or istream. Have to use put API due to missing PEP support.
            self.user1.assert_icommand(["iput", "-R", self.tier0, self.filename, self.object_path])

            # Sleep long enough to ensure that the object needs to be tiered out.
            time.sleep(self.tier0_time_in_seconds)

            # Ensure that the delay queue is empty before attempting to schedule the tiering rule. This is to
            # prevent false negatives in a later assertion of the emptiness of the delay queue.
            self.user1.assert_icommand(["iqstat"], "STDOUT", "No delayed rules pending")

            # This implementation is derived from invoke_storage_tiering_rule().
            rep_instance = "irods_rule_engine_plugin-unified_storage_tiering-instance"
            rule_file_path = "/var/lib/irods/example_unified_tiering_invocation.r"
            self.user1.assert_icommand(
                ["irule", "-r", rep_instance, "-F", rule_file_path], "STDERR", "CAT_INSUFFICIENT_PRIVILEGE_LEVEL")

            # Ensure that nothing is scheduled in the delay queue. The tiering rule should have failed.
            self.user1.assert_icommand(["iqstat"], "STDOUT", "No delayed rules pending")

            # Ensure that the object did not migrate to the next tier because the tiering rule should have failed.
            self.user1.assert_icommand(["ils", "-l", self.object_path], "STDOUT", self.tier0)

    @unittest.skip("TODO(#294): Stub for now.")
    def test_rule_invoked_by_rodsuser_with_remote_via_irule_fails(self):
        pass

    @unittest.skip("TODO(#294): Stub for now.")
    def test_rule_invoked_by_rodsuser_with_delay_via_irule_fails(self):
        pass


class test_tiering_out_one_object_with_various_owners(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.user1 = session.mkuser_and_return_session("rodsuser", "tolstoy", "tpass", lib.get_hostname())
        self.admin1 = session.mkuser_and_return_session("rodsadmin", "dostoevsky", "dpass", lib.get_hostname())
        self.group1_name = "sillybillies"
        self.emptygroup_name = "anothergroup"

        self.tier0 = "ufs0"
        self.tier1 = "ufs1"
        self.tier0_time_in_seconds = 5

        self.filename = "test_tiering_out_one_object_with_various_owners"
        if not os.path.exists(self.filename):
            lib.create_local_testfile(self.filename)

        self.collection_path = "/".join(["/" + self.user1.zone_name, "public_collection"])
        self.object_path = "/".join([self.collection_path, self.filename])

        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand(['iqdel', '-a'])

            # Make a place for public group to put stuff.
            admin_session.assert_icommand(["imkdir", "-p", self.collection_path])
            admin_session.assert_icommand(["ichmod", "-r", "own", "public", self.collection_path])

            session.mkgroup_and_add_users(self.group1_name, [self.user1.username, self.admin1.username])
            # Empty group should be empty.
            session.mkgroup_and_add_users(self.emptygroup_name, [])

            # Using HOSTNAME_2 here so that topology testing takes effect should we ever employ it in plugin testing.
            lib.create_ufs_resource(admin_session, self.tier0, test.settings.HOSTNAME_2)
            admin_session.assert_icommand(
                ['imeta', 'add', '-R', self.tier0, 'irods::storage_tiering::group', 'example_group', '0'])
            admin_session.assert_icommand(
                ['imeta', 'add', '-R', self.tier0, 'irods::storage_tiering::time', str(self.tier0_time_in_seconds)])
            admin_session.assert_icommand(
                ['imeta', 'ls', '-R', self.tier0], 'STDOUT', [f'value: {self.tier0_time_in_seconds}', 'value: example_group'])

            # Using HOSTNAME_3 here so that topology testing takes effect should we ever employ it in plugin testing.
            lib.create_ufs_resource(admin_session, self.tier1, test.settings.HOSTNAME_3)
            admin_session.assert_icommand(
                ['imeta', 'add', '-R', self.tier1, 'irods::storage_tiering::group', 'example_group', '1'])
            admin_session.assert_icommand(['imeta', 'ls', '-R', self.tier1], 'STDOUT', 'value: example_group')

    @classmethod
    def tearDownClass(self):
        if os.path.exists(self.filename):
            os.unlink(self.filename)

        self.user1.__exit__()
        self.admin1.__exit__()

        with session.make_session_for_existing_admin() as admin_session:
            admin_session.run_icommand(['iadmin', 'rmuser', self.user1.username])
            admin_session.run_icommand(['iadmin', 'rmuser', self.admin1.username])
            admin_session.run_icommand(['iadmin', 'rmgroup', self.group1_name])
            admin_session.run_icommand(['iadmin', 'rmgroup', self.emptygroup_name])

            admin_session.run_icommand(['iqdel', '-a'])

            admin_session.run_icommand(['iadmin', 'rmresc', self.tier0])
            admin_session.run_icommand(['iadmin', 'rmresc', self.tier1])
            admin_session.run_icommand(['iadmin', 'rum'])

    def tearDown(self):
        # Make sure the test object is cleaned up after each test runs.
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.run_icommand(["ichmod", "-M", "own", admin_session.username, self.object_path])
            admin_session.run_icommand(["irm", "-f", self.object_path])

    def tier_out_object_with_specified_permissions_test_impl(
        self, username_permission_pairs, original_owner_session=None
    ):
        """Implementation of a very basic test to demonstrate that objects tier out regardless of existing permissions.

        Passing an empty list for username_permission_pairs means no users will have any permissions on the object.

        Passing None for original_owner_session (this is the default value) will use the "existing admin" session.

        Arguments:
        self - Instance of this class
        username_permission_pairs - List of tuples of the username and the permission to set for that user on the object
        original_owner_session - iRODSSession creating the data object (Default: None)
        """
        with storage_tiering_configured():
            IrodsController().restart(test_mode=True)

            with session.make_session_for_existing_admin() as admin_session:
                if original_owner_session is None:
                    original_owner_session = admin_session

                # TODO(#200): Replace with itouch or istream. Have to use put API due to missing PEP support.
                original_owner_session.assert_icommand(["iput", "-R", self.tier0, self.filename, self.object_path])

                # Give permissions exclusively to a rodsuser, thereby removing permissions for the original owner.
                admin_session.assert_icommand(["ichmod", "-M", "null", original_owner_session.username, self.object_path])
                for username, permission in username_permission_pairs:
                    admin_session.assert_icommand(["ichmod", "-M", permission, username, self.object_path])

                # Ensure that the delay queue is empty before attempting to schedule the tiering rule. This is to
                # prevent false negatives in a later assertion of the emptiness of the delay queue.
                admin_session.assert_icommand(["iqstat", "-a"], "STDOUT", "No delayed rules pending")

                time.sleep(self.tier0_time_in_seconds)
                invoke_storage_tiering_rule(admin_session)

                # Wait until the object migrates to the next tier.
                lib.delayAssert(
                    lambda: lib.replica_exists_on_resource(self.admin1, self.object_path, self.tier0) == False)
                lib.delayAssert(lambda: lib.replica_exists_on_resource(self.admin1, self.object_path, self.tier1))

                # Ensure that nothing is scheduled in the delay queue. The tiering rule should have completed. If
                # any failures occurred, it is likely that the delayed rule will be retried so we are making sure
                # that is not happening here.
                admin_session.assert_icommand(["iqstat", "-a"], "STDOUT", "No delayed rules pending")

    def test_one_rodsadmin_with_own_permission_succeeds(self):
        # The common function with the test implementation uses the "existing admin" session to run the tiering rule.
        with session.make_session_for_existing_admin() as admin_session:
            self.tier_out_object_with_specified_permissions_test_impl([(admin_session.username, "own")])

    def test_one_rodsadmin_with_own_permission_and_rule_invoked_by_different_rodsadmin_succeeds(self):
        self.tier_out_object_with_specified_permissions_test_impl([(self.admin1.username, "own")])

    def test_one_rodsuser_with_read_permission_succeeds__issue_164_189(self):
        self.tier_out_object_with_specified_permissions_test_impl([(self.user1.username, "read_object")])

    def test_one_rodsuser_with_write_permission_succeeds(self):
        self.tier_out_object_with_specified_permissions_test_impl([(self.user1.username, "modify_object")])

    def test_one_rodsuser_with_own_permission_succeeds(self):
        self.tier_out_object_with_specified_permissions_test_impl([(self.user1.username, "own")])

    def test_one_empty_rodsgroup_with_read_permission_succeeds__issue_164_189_273(self):
        self.tier_out_object_with_specified_permissions_test_impl([(self.emptygroup_name, "read_object")])

    def test_one_empty_rodsgroup_with_write_permission_succeeds__issue_273(self):
        self.tier_out_object_with_specified_permissions_test_impl([(self.emptygroup_name, "modify_object")])

    def test_one_empty_rodsgroup_with_own_permission_succeeds__issue_273(self):
        self.tier_out_object_with_specified_permissions_test_impl([(self.emptygroup_name, "own")])

    def test_one_rodsgroup_with_read_permission_succeeds__issue_164_189_273(self):
        self.tier_out_object_with_specified_permissions_test_impl([(self.group1_name, "read_object")])

    def test_one_rodsgroup_with_write_permission_succeeds__issue_273(self):
        self.tier_out_object_with_specified_permissions_test_impl([(self.group1_name, "modify_object")])

    def test_one_rodsgroup_with_own_permission_succeeds__issue_273(self):
        self.tier_out_object_with_specified_permissions_test_impl([(self.group1_name, "own")])

    def test_two_rodsgroups_with_own_permission_succeeds__issue_273(self):
        self.tier_out_object_with_specified_permissions_test_impl(
            [(self.emptygroup_name, "own"), (self.group1_name, "own")]
        )

    def test_one_rodsuser_and_one_rodsadmin_with_own_permission_succeeds__issue_273(self):
        self.tier_out_object_with_specified_permissions_test_impl(
            [(self.user1.username, "own"), (self.admin1.username, "own")]
        )


class test_accessing_read_only_object_updates_access_time(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.user1 = session.mkuser_and_return_session("rodsuser", "tolstoy", "tpass", lib.get_hostname())

        self.filename = "test_accessing_read_only_object_updates_access_time"
        if not os.path.exists(self.filename):
            lib.create_local_testfile(self.filename)

        self.collection_path = "/".join(["/" + self.user1.zone_name, "public_collection"])
        self.object_path = "/".join([self.collection_path, self.filename])

        with session.make_session_for_existing_admin() as admin_session:
            # Make a place for public group to put stuff.
            admin_session.assert_icommand(["imkdir", "-p", self.collection_path])
            admin_session.assert_icommand(["ichmod", "-r", "own", "public", self.collection_path])

            with storage_tiering_configured():
                IrodsController().restart(test_mode=True)
                # TODO(#200): Replace with itouch or istream. Have to use put API due to missing PEP support.
                # For this test, we don't actually care about tiering or restaging objects. We just want to test
                # updating the access_time metadata. So, it doesn't matter into what resource the object's replica goes
                # This is why no tier groups are being configured in this test.
                admin_session.assert_icommand(["iput", self.filename, self.object_path])

            # Give permissions exclusively to a rodsuser (removing permissions for original owner).
            admin_session.assert_icommand(["ichmod", "read", self.user1.username, self.object_path])
            admin_session.assert_icommand(["ichmod", "null", admin_session.username, self.object_path])

        if os.path.exists(self.filename):
            os.unlink(self.filename)

    @classmethod
    def tearDownClass(self):
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.run_icommand(["ichmod", "-M", "own", admin_session.username, self.object_path])
            admin_session.run_icommand(["irm", "-f", self.object_path])

            self.user1.__exit__()

            admin_session.run_icommand(['iadmin', 'rmuser', self.user1.username])
            admin_session.run_icommand(['iadmin', 'rum'])

    def read_object_updates_access_time_test_impl(self, read_command, *read_command_args):
        """A basic test implementation to show that access_time metadata is updated for reads accessing data.

        Arguments:
        self - Instance of this class
        read_command - A callable which will execute some sort of read operation on the object
        read_command_args - *args to pass to the read_command callable
        """
        with storage_tiering_configured():
            IrodsController().restart(test_mode=True)

            # Capture the original access time so we have something against which to compare.
            access_time = get_access_time(self.user1, self.object_path)
            self.assertNotIn("CAT_NO_ROWS_FOUND", access_time)

            # Sleeping guarantees the access time will be different following the access.
            time.sleep(2)

            # Access the data...
            read_command(*read_command_args)

            # Ensure the access_time was updated as a result of the access.
            new_access_time = get_access_time(self.user1, self.object_path)
            self.assertNotIn("CAT_NO_ROWS_FOUND", new_access_time)
            self.assertGreater(new_access_time, access_time)

    def test_dataobj_open_read_close_updates_access_time__issue_175_203(self):
        # This basic test shows that access_time metadata is updated when dataObjOpen/Read/Close APIs access the data.
        self.read_object_updates_access_time_test_impl(
            self.user1.assert_icommand, ["irods_test_read_object", self.object_path], "STDOUT")

    def test_replica_open_close_updates_access_time(self):
        # This basic test shows that access_time metadata is updated when replica_open/close APIs access the data.
        self.read_object_updates_access_time_test_impl(
            self.user1.assert_icommand, ["istream", "read", self.object_path], "STDOUT")

    def test_get_updates_access_time(self):
        # This basic test shows that access_time metadata is updated when get API accesses the data.
        self.read_object_updates_access_time_test_impl(
            self.user1.assert_icommand, ["iget", self.object_path, "-"], "STDOUT")


class test_basic_tier_out_after_creating_single_data_object(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.user1 = session.mkuser_and_return_session("rodsuser", "tolstoy", "tpass", lib.get_hostname())

        self.other_resc = "other_resc"
        self.tier0 = "ufs0"
        self.tier1 = "ufs1"
        self.tier0_time_in_seconds = 5

        self.filename = "test_basic_tier_out_after_creating_single_data_object"
        if not os.path.exists(self.filename):
            lib.create_local_testfile(self.filename)

        self.collection_path = "/".join(["/" + self.user1.zone_name, "public_collection"])
        self.object_path = "/".join([self.collection_path, self.filename])

        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand(['iqdel', '-a'])

            # Make a place for public group to put stuff.
            admin_session.assert_icommand(["imkdir", "-p", self.collection_path])
            admin_session.assert_icommand(["ichmod", "-r", "own", "public", self.collection_path])

            # Create a separate resource so we can create replicas with replicating APIs.
            lib.create_ufs_resource(admin_session, self.other_resc, test.settings.HOSTNAME_2)

            # Using HOSTNAME_2 here so that topology testing takes effect should we ever employ it in plugin testing.
            lib.create_ufs_resource(admin_session, self.tier0, test.settings.HOSTNAME_2)
            admin_session.assert_icommand(
                ['imeta', 'add', '-R', self.tier0, 'irods::storage_tiering::group', 'example_group', '0'])
            admin_session.assert_icommand(
                ['imeta', 'add', '-R', self.tier0, 'irods::storage_tiering::time', str(self.tier0_time_in_seconds)])
            admin_session.assert_icommand(
                ['imeta', 'ls', '-R', self.tier0], 'STDOUT', [f'value: {self.tier0_time_in_seconds}', 'value: example_group'])

            # Using HOSTNAME_3 here so that topology testing takes effect should we ever employ it in plugin testing.
            lib.create_ufs_resource(admin_session, self.tier1, test.settings.HOSTNAME_3)
            admin_session.assert_icommand(
                ['imeta', 'add', '-R', self.tier1, 'irods::storage_tiering::group', 'example_group', '1'])
            admin_session.assert_icommand(['imeta', 'ls', '-R', self.tier1], 'STDOUT', 'value: example_group')

    @classmethod
    def tearDownClass(self):
        if os.path.exists(self.filename):
            os.unlink(self.filename)

        self.user1.__exit__()

        with session.make_session_for_existing_admin() as admin_session:
            admin_session.run_icommand(['iadmin', 'rmuser', self.user1.username])

            admin_session.run_icommand(['iqdel', '-a'])

            admin_session.run_icommand(['iadmin', 'rmresc', self.other_resc])
            admin_session.run_icommand(['iadmin', 'rmresc', self.tier0])
            admin_session.run_icommand(['iadmin', 'rmresc', self.tier1])
            admin_session.run_icommand(['iadmin', 'rum'])

    def tearDown(self):
        # Make sure the test object is cleaned up after each test runs.
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.run_icommand(["ichmod", "-M", "own", admin_session.username, self.object_path])
            admin_session.run_icommand(["irm", "-f", self.object_path])

    def object_created_on_tiering_resource_tiers_out_test_impl(
        self, logical_path, create_command, *create_command_args, **create_command_kwargs
    ):
        """A basic test implementation to show that replicas created by any means are tiered out from the lowest tier.

        Arguments:
        self - Instance of this class
        logical_path - Logical path to iRODS object being created
        create_command - A callable which will execute some sort of create operation for an object
        create_command_args - *args to pass to the create_command callable
        create_command_kwargs - **kwargs to pass to the create_command callable
        """
        with storage_tiering_configured():
            IrodsController().restart(test_mode=True)

            with session.make_session_for_existing_admin() as admin_session:
                # Create the data...
                create_command(*create_command_args, **create_command_kwargs)
                self.assertTrue(lib.replica_exists_on_resource(admin_session, logical_path, self.tier0))

                # Ensure that the delay queue is empty before attempting to schedule the tiering rule. This is to
                # prevent false negatives in a later assertion of the emptiness of the delay queue.
                admin_session.assert_icommand(["iqstat", "-a"], "STDOUT", "No delayed rules pending")

                time.sleep(self.tier0_time_in_seconds)
                invoke_storage_tiering_rule(admin_session)

                # Wait until the object migrates to the next tier.
                lib.delayAssert(
                    lambda: lib.replica_exists_on_resource(admin_session, logical_path, self.tier0) == False)
                lib.delayAssert(lambda: lib.replica_exists_on_resource(admin_session, logical_path, self.tier1))

                # Ensure that nothing is scheduled in the delay queue. The tiering rule should have completed. If
                # any failures occurred, it is likely that the delayed rule will be retried so we are making sure
                # that is not happening here.
                admin_session.assert_icommand(["iqstat", "-a"], "STDOUT", "No delayed rules pending")

    def test_DataObjPut_created_data_tiers_out(self):
        # This basic test shows that objects created with DataObjPut API tier out.
        self.object_created_on_tiering_resource_tiers_out_test_impl(
            self.object_path,
            self.user1.assert_icommand,
            ["iput", "-R", self.tier0, self.filename, self.object_path],
            "STDOUT")

    def test_replica_open_created_data_tiers_out__issue_316(self):
        # This basic test shows that objects created with replica_open API tier out.
        self.object_created_on_tiering_resource_tiers_out_test_impl(
            self.object_path,
            self.user1.assert_icommand,
            ["istream", "-R", self.tier0, "write", self.object_path],
            "STDOUT",
            input="tier this data")

    def test_DataObjOpen_created_data_tiers_out__issue_303(self):
        # This basic test shows that objects created with DataObjOpen API tier out.
        self.object_created_on_tiering_resource_tiers_out_test_impl(
            self.object_path,
            self.user1.assert_icommand,
            ["irods_test_create_object", "--resource", self.tier0, self.object_path],
            "STDOUT")

    def test_DataObjCreate_created_data_tiers_out__issue_303(self):
        # This basic test shows that objects created with DataObjCreate API tier out.
        self.object_created_on_tiering_resource_tiers_out_test_impl(
            self.object_path,
            self.user1.assert_icommand,
            ["irods_test_create_object", "--use-create-api", "1", "--resource", self.tier0, self.object_path],
            "STDOUT")

    def test_DataObjRepl_created_data_tiers_out(self):
        # This basic test shows that replicas created with DataObjRepl API tier out.
        self.user1.assert_icommand(["iput", "-R", self.other_resc, self.filename, self.object_path], "STDOUT")
        self.object_created_on_tiering_resource_tiers_out_test_impl(
            self.object_path, self.user1.assert_icommand, ["irepl", "-R", self.tier0, self.object_path], "STDOUT")

    @unittest.skipIf(test.settings.RUN_IN_TOPOLOGY, "Registers local file to a resource.")
    def test_PhyPathReg_created_data_tiers_out(self):
        # This basic test shows that objects created with PhyPathReg API tier out.
        phypath = os.path.abspath(self.filename)
        # Need to execute this as an administrator because rodsusers cannot register physical paths by default.
        with session.make_session_for_existing_admin() as admin_session:
            self.object_created_on_tiering_resource_tiers_out_test_impl(
                self.object_path,
                admin_session.assert_icommand,
                ["ireg", "-R", self.tier0, phypath, self.object_path],
                "STDOUT")

    @unittest.skipIf(test.settings.RUN_IN_TOPOLOGY, "Registers local file to a resource.")
    def test_PhyPathReg_as_replica_created_data_tiers_out(self):
        # This basic test shows that replicas created with PhyPathReg API tier out.
        phypath = os.path.abspath(self.filename)
        # Need to execute this as an administrator because rodsusers cannot register physical paths by default.
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand(["iput", "-R", self.other_resc, self.filename, self.object_path], "STDOUT")
            self.object_created_on_tiering_resource_tiers_out_test_impl(
                self.object_path,
                admin_session.assert_icommand,
                ["ireg", "--repl", "-R", self.tier0, phypath, self.object_path],
                "STDOUT")

    def test_touch_created_data_tiers_out__issue_266(self):
        # This basic test shows that objects created with touch API tier out.
        self.object_created_on_tiering_resource_tiers_out_test_impl(
            self.object_path, self.user1.assert_icommand, ["itouch", "-R", self.tier0, self.object_path], "STDOUT")

    def test_data_object_with_select_in_name_tiers_out__issue_281(self):
        data_name = "select"
        logical_path = "/".join([self.user1.session_collection, data_name])
        try:
            self.object_created_on_tiering_resource_tiers_out_test_impl(
                logical_path,
                self.user1.assert_icommand,
                ["istream", "-R", self.tier0, "write", data_name],
                "STDOUT",
                input="tier this data")

        finally:
            self.user1.assert_icommand(["ils", "-L", logical_path], "STDOUT")
            self.user1.run_icommand(["irm", "-f", logical_path])


class test_accessing_object_for_write_updates_access_time(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.user1 = session.mkuser_and_return_session("rodsuser", "tolstoy", "tpass", lib.get_hostname())

        self.filename = "test_accessing_object_for_write_updates_access_time"

        self.collection_path = "/".join(["/" + self.user1.zone_name, "public_collection"])
        self.object_path = "/".join([self.collection_path, self.filename])

        with session.make_session_for_existing_admin() as admin_session:
            # Make a place for public group to put stuff.
            admin_session.assert_icommand(["imkdir", "-p", self.collection_path])
            admin_session.assert_icommand(["ichmod", "-r", "own", "public", self.collection_path])

            with storage_tiering_configured():
                IrodsController().restart(test_mode=True)
                # For this test, we don't actually care about tiering or restaging objects. We just want to test
                # updating the access_time metadata. So, it doesn't matter into what resource the object's replica goes
                # This is why no tier groups are being configured in this test.
                admin_session.assert_icommand(["istream", "write", self.object_path], input=self.filename)

            # Give permissions exclusively to a rodsuser (removing permissions for original owner).
            admin_session.assert_icommand(["ichmod", "own", self.user1.username, self.object_path])
            admin_session.assert_icommand(["ichmod", "null", admin_session.username, self.object_path])

        if os.path.exists(self.filename):
            os.unlink(self.filename)

    @classmethod
    def tearDownClass(self):
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.run_icommand(["ichmod", "-r", "-M", "own", admin_session.username, self.collection_path])
            admin_session.run_icommand(["irm", "-rf", self.collection_path])

            self.user1.__exit__()

            admin_session.run_icommand(['iadmin', 'rmuser', self.user1.username])
            admin_session.run_icommand(['iadmin', 'rum'])

    def access_object_for_write_updates_access_time_test_impl(self, open_command, *open_command_args):
        """A basic test implementation to show that access_time metadata is updated for writes accessing data.

        Arguments:
        self - Instance of this class
        open_command - A callable which will execute some sort of open operation on the object
        open_command_args - *args to pass to the open_command callable
        """
        with storage_tiering_configured():
            IrodsController().restart(test_mode=True)

            # Capture the original access time so we have something against which to compare.
            access_time = get_access_time(self.user1, self.object_path)
            self.assertNotIn("CAT_NO_ROWS_FOUND", access_time)

            # Sleeping guarantees the access time will be different following the access.
            time.sleep(2)

            # Access the data...
            open_command(*open_command_args)

            # Ensure the access_time was updated as a result of the access.
            new_access_time = get_access_time(self.user1, self.object_path)
            self.assertNotIn("CAT_NO_ROWS_FOUND", new_access_time)
            self.assertGreater(new_access_time, access_time)

    def test_multiple_replica_opens_and_replica_closes_updates_access_time__issue_316(self):
        # This test shows that access_time is updated if replica_open accesses a replica multiple times simultaneously.
        self.access_object_for_write_updates_access_time_test_impl(
            self.user1.assert_icommand,
            ["irods_test_multi_open_for_write_object", "--open-count", "10", self.object_path],
            "STDOUT")

    def test_touch_updates_access_time__issue_266(self):
        # This is a basic test to show that access_time metadata is updated when touch API accesses the data.
        self.access_object_for_write_updates_access_time_test_impl(
            self.user1.assert_icommand, ["itouch", self.object_path], "STDOUT")

    def test_touch_collection_does_not_update_access_time__issue_266(self):
        with storage_tiering_configured():
            IrodsController().restart(test_mode=True)

            # Capture the original access time so we have something against which to compare.
            access_time = get_access_time(self.user1, self.object_path)
            self.assertNotIn("CAT_NO_ROWS_FOUND", access_time)

            # Sleeping guarantees the access time could be different following the access.
            time.sleep(2)

            # Touch the collection (note: this is NOT accessing any data).
            self.user1.assert_icommand(["itouch", self.collection_path], "STDOUT")

            # Ensure the access_time was NOT updated as a result of the access.
            new_access_time = get_access_time(self.user1, self.object_path)
            self.assertNotIn("CAT_NO_ROWS_FOUND", new_access_time)
            self.assertEqual(new_access_time, access_time)
