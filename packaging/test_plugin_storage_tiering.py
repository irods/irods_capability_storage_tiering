import sys
import shutil
import contextlib
import tempfile

from time import sleep

if sys.version_info >= (2, 7):
    import unittest
else:
    import unittest2 as unittest

from ..configuration import IrodsConfig
from .resource_suite import ResourceBase
from ..test.command import assert_command
from . import session
from .. import test
from .. import paths
from .. import lib

@contextlib.contextmanager
def storage_tiering_configured_custom(arg=None):
    filename = paths.server_config_path()
    with lib.file_backed_up(filename):
        irods_config = IrodsConfig()
        irods_config.server_config['advanced_settings']['rule_engine_server_sleep_time_in_seconds'] = 1

        irods_config.server_config['plugin_configuration']['rule_engines'].insert(0,
            {
                "instance_name" : "irods_rule_engine_plugin-storage_tiering-instance",
                "plugin_name" : "irods_rule_engine_plugin-storage_tiering",
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
def storage_tiering_configured(arg=None):
    filename = paths.server_config_path()
    with lib.file_backed_up(filename):
        irods_config = IrodsConfig()
        irods_config.server_config['advanced_settings']['rule_engine_server_sleep_time_in_seconds'] = 1

        irods_config.server_config['plugin_configuration']['rule_engines'].insert(0,
            {
                "instance_name": "irods_rule_engine_plugin-storage_tiering-instance",
                "plugin_name": "irods_rule_engine_plugin-storage_tiering",
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
def storage_tiering_configured_with_log(arg=None):
    filename = paths.server_config_path()
    with lib.file_backed_up(filename):
        irods_config = IrodsConfig()
        irods_config.server_config['advanced_settings']['rule_engine_server_sleep_time_in_seconds'] = 1

        irods_config.server_config['plugin_configuration']['rule_engines'].insert(0,
            {
                "instance_name": "irods_rule_engine_plugin-storage_tiering-instance",
                "plugin_name": "irods_rule_engine_plugin-storage_tiering",
                "plugin_specific_configuration": {
                    "data_transfer_log_level": "LOG_NOTICE"
                }
            }
        )
        irods_config.commit(irods_config.server_config, irods_config.server_config_path)

        try:
            yield
        finally:
            pass

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
            admin_session.assert_icommand('imeta add -R rnd1 irods::storage_tiering::time 65')
            admin_session.assert_icommand('''imeta set -R rnd1 irods::storage_tiering::query "select DATA_NAME, COLL_NAME, DATA_RESC_ID where RESC_NAME = 'ufs2' || = 'ufs3' and META_DATA_ATTR_NAME = 'irods::access_time' and META_DATA_ATTR_VALUE < 'TIME_CHECK_STRING'"''')

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
            zone_name = IrodsConfig().client_environment['irods_zone_name']
            with session.make_session_for_existing_admin() as admin_session:
                with session.make_session_for_existing_user('alice', 'apass', lib.get_hostname(), zone_name) as alice_session:
                    filename = 'test_put_file'
                    lib.create_local_testfile(filename)
                    alice_session.assert_icommand('iput -R rnd0 ' + filename)
                    alice_session.assert_icommand('imeta ls -d ' + filename, 'STDOUT_SINGLELINE', filename)
                    alice_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', filename)

                    # test stage to tier 1
                    sleep(5)
                    admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
                    sleep(60)
                    alice_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'rnd1')

                    # test stage to tier 2
                    sleep(10)
                    admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
                    sleep(60)
                    alice_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'rnd2')

                    # test restage to tier 0
                    alice_session.assert_icommand('iget ' + filename + ' - ', 'STDOUT_SINGLELINE', 'TESTFILE')
                    sleep(40)
                    alice_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'rnd0')

                    alice_session.assert_icommand('irm -f ' + filename)

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
            admin_session.assert_icommand('imeta add -R rnd1 irods::storage_tiering::time 65')
            admin_session.assert_icommand('''imeta set -R rnd1 irods::storage_tiering::query "select DATA_NAME, COLL_NAME, DATA_RESC_ID where RESC_NAME = 'ufs2' || = 'ufs3' and META_DATA_ATTR_NAME = 'irods::access_time' and META_DATA_ATTR_VALUE < 'TIME_CHECK_STRING'"''')

            admin_session.assert_icommand('iadmin mkresc ufs0g2 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs0g2', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs1g2 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs1g2', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs2g2 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs2g2', 'STDOUT_SINGLELINE', 'unixfilesystem')

            admin_session.assert_icommand('imeta add -R ufs0g2 irods::storage_tiering::group example_group_g2 0')
            admin_session.assert_icommand('imeta add -R ufs1g2 irods::storage_tiering::group example_group_g2 1')
            admin_session.assert_icommand('imeta add -R ufs2g2 irods::storage_tiering::group example_group_g2 2')

            admin_session.assert_icommand('imeta add -R ufs0g2 irods::storage_tiering::time 5')
            admin_session.assert_icommand('imeta add -R ufs1g2 irods::storage_tiering::time 65')

            admin_session.assert_icommand('''imeta set -R ufs1g2 irods::storage_tiering::query "select DATA_NAME, COLL_NAME, DATA_RESC_ID where RESC_NAME = 'ufs1g2' and META_DATA_ATTR_NAME = 'irods::access_time' and META_DATA_ATTR_VALUE < 'TIME_CHECK_STRING'"''')

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
            with session.make_session_for_existing_admin() as admin_session:
                print("yep")
                admin_session.assert_icommand('ils -L ', 'STDOUT_SINGLELINE', 'rods')

                filename = 'test_put_file'

                filenameg2 = 'test_put_fileg2'
                lib.create_local_testfile(filenameg2)

                admin_session.assert_icommand('iput -R rnd0 ' + filename)
                admin_session.assert_icommand('imeta ls -d ' + filename, 'STDOUT_SINGLELINE', filename)
                admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', filename)

                admin_session.assert_icommand('iput -R ufs0g2 ' + filenameg2)
                admin_session.assert_icommand('imeta ls -d ' + filenameg2, 'STDOUT_SINGLELINE', filenameg2)
                admin_session.assert_icommand('ils -L ' + filenameg2, 'STDOUT_SINGLELINE', filenameg2)

                # test stage to tier 1
                sleep(5)
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
                sleep(60)
                admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'rnd1')
                admin_session.assert_icommand('ils -L ' + filenameg2, 'STDOUT_SINGLELINE', 'ufs1g2')

                # test stage to tier 2
                sleep(10)
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
                sleep(60)
                admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'rnd2')
                admin_session.assert_icommand('ils -L ' + filenameg2, 'STDOUT_SINGLELINE', 'ufs2g2')

                # test restage to tier 0
                admin_session.assert_icommand('iget ' + filename + ' - ', 'STDOUT_SINGLELINE', 'TESTFILE')
                admin_session.assert_icommand('iget ' + filenameg2 + ' - ', 'STDOUT_SINGLELINE', 'TESTFILE')

                sleep(40)
                admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'rnd0')
                admin_session.assert_icommand('ils -L ' + filenameg2, 'STDOUT_SINGLELINE', 'ufs0g2')

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
            admin_session.assert_icommand('imeta add -R rnd1 irods::custom_storage_tiering::time 65')
            admin_session.assert_icommand('''imeta set -R rnd1 irods::custom_storage_tiering::query "select DATA_NAME, COLL_NAME, DATA_RESC_ID where RESC_NAME = 'ufs2' || = 'ufs3' and META_DATA_ATTR_NAME = 'irods::custom_access_time' and META_DATA_ATTR_VALUE < 'TIME_CHECK_STRING'"''')

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
            with session.make_session_for_existing_admin() as admin_session:
                filename = 'test_put_file'
                admin_session.assert_icommand('iput -R rnd0 ' + filename)
                admin_session.assert_icommand('imeta ls -d ' + filename, 'STDOUT_SINGLELINE', filename)
                admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', filename)

                # test stage to tier 1
                sleep(5)
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
                admin_session.assert_icommand('iqstat', 'STDOUT_SINGLELINE', 'apply')
                sleep(40)
                admin_session.assert_icommand('iqstat', 'STDOUT_SINGLELINE', 'No')
                admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'rnd1')

                # test stage to tier 2
                sleep(25)
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
                admin_session.assert_icommand('iqstat', 'STDOUT_SINGLELINE', 'apply')
                sleep(60)
                admin_session.assert_icommand('iqstat', 'STDOUT_SINGLELINE', 'No')
                admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'rnd2')

                # test restage to tier 0
                admin_session.assert_icommand('iget ' + filename + ' - ', 'STDOUT_SINGLELINE', 'TESTFILE')
                sleep(40)
                admin_session.assert_icommand('iqstat', 'STDOUT_SINGLELINE', 'No')
                admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'rnd0')

                admin_session.assert_icommand('irm -f ' + filename)

class TestStorageTieringPluginWithMungefs(ResourceBase, unittest.TestCase):
    def setUp(self):
        super(TestStorageTieringPluginWithMungefs, self).setUp()
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand('iqdel -a')
            # configure mungefs
            self.munge_mount=tempfile.mkdtemp(prefix='munge_mount_')
            self.munge_target=tempfile.mkdtemp(prefix='munge_target_')

            assert_command('mungefs ' + self.munge_mount + ' -omodules=subdir,subdir=' + self.munge_target)

            admin_session.assert_icommand('iadmin mkresc ufs0 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs0', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs1 unixfilesystem '+test.settings.HOSTNAME_1 +':'+self.munge_mount, 'STDOUT_SINGLELINE', 'unixfilesystem')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::group example_group 0')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::group example_group 1')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::time 5')

    def tearDown(self):
        super(TestStorageTieringPluginWithMungefs, self).tearDown()
        with session.make_session_for_existing_admin() as admin_session:
            assert_command('fusermount -u '+self.munge_mount)
            shutil.rmtree(self.munge_mount, ignore_errors=True)
            shutil.rmtree(self.munge_target, ignore_errors=True)

            admin_session.assert_icommand('iadmin rmresc ufs0')
            admin_session.assert_icommand('iadmin rmresc ufs1')
            admin_session.assert_icommand('iadmin rum')

    @classmethod
    def test_put_verify_filesystem(self):
        with storage_tiering_configured():
            with session.make_session_for_existing_admin() as admin_session:
                # configure mungefs to report an invalid file size
                assert_command('mungefsctl --operations "getattr" --corrupt_size')

                # set filesystem verification
                admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::verification filesystem')

                # debug
                admin_session.assert_icommand('ils -L ', 'STDOUT_SINGLELINE', 'rods')

                filename = 'test_put_file'
                filepath = lib.create_local_testfile(filename)
                admin_session.assert_icommand('iput -R ufs0 ' + filename)
                admin_session.assert_icommand('imeta ls -d ' + filename, 'STDOUT_SINGLELINE', filename)
                admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', filename)

                # test stage to tier 1
                sleep(5)
                initial_size_of_server_log = lib.get_file_size_by_path(paths.server_log_path())
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
                sleep(60)
                log_cnt = lib.count_occurrences_of_string_in_log(
                        paths.server_log_path(),
                        'failed to migrate [/tempZone/home/rods/test_put_file] to [ufs1]',
                        start_index=initial_size_of_server_log)

                if not 1 == log_cnt:
                    print('log_cnt: '+str(log_cnt))
                    raise AssertionError()

                admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'ufs0')

                # clean up
                assert_command('mungefsctl --operations "getattr"')
                admin_session.assert_icommand('irm -f ' + filename)

    @classmethod
    def test_put_verify_checksum(self):
        with storage_tiering_configured():
            with session.make_session_for_existing_admin() as admin_session:
                # configure mungefs to report an invalid file size
                assert_command('mungefsctl --operations "read" --corrupt_data')

                # set checksum verification
                admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::verification checksum')

                # debug
                admin_session.assert_icommand('ils -L ', 'STDOUT_SINGLELINE', 'rods')

                filename = 'test_put_file'
                filepath = lib.create_local_testfile(filename)
                admin_session.assert_icommand('iput -R ufs0 ' + filename)
                admin_session.assert_icommand('imeta ls -d ' + filename, 'STDOUT_SINGLELINE', filename)
                admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', filename)

                # test stage to tier 1
                sleep(5)
                initial_size_of_server_log = lib.get_file_size_by_path(paths.server_log_path())
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
                sleep(60)
                admin_session.assert_icommand('ils -L ', 'STDOUT_SINGLELINE', 'rods')

                log_cnt = lib.count_occurrences_of_string_in_log(
                        paths.server_log_path(),
                        'failed to migrate [/tempZone/home/rods/test_put_file] to [ufs1]',
                        start_index=initial_size_of_server_log)

                if not 1 == log_cnt:
                    print('log_cnt: '+str(log_cnt))
                    raise AssertionError()


                admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'ufs0')

                # clean up
                assert_command('mungefsctl --operations "read"')
                admin_session.assert_icommand('irm -f ' + filename)

class TestStorageTieringPluginMinimumRestage(ResourceBase, unittest.TestCase):
    def setUp(self):
        super(TestStorageTieringPluginMinimumRestage, self).setUp()
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand('iqdel -a')
            admin_session.assert_icommand('iadmin mkresc ufs0 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs0', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs1 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs1', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs2 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs2', 'STDOUT_SINGLELINE', 'unixfilesystem')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::group example_group 0')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::group example_group 1')
            admin_session.assert_icommand('imeta add -R ufs2 irods::storage_tiering::group example_group 2')
            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::time 5')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::time 65')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::minimum_restage_tier true')

    def tearDown(self):
        super(TestStorageTieringPluginMinimumRestage, self).tearDown()
        with session.make_session_for_existing_admin() as admin_session:

            admin_session.assert_icommand('iadmin rmresc ufs0')
            admin_session.assert_icommand('iadmin rmresc ufs1')
            admin_session.assert_icommand('iadmin rmresc ufs2')
            admin_session.assert_icommand('iadmin rum')

    def test_put_and_get(self):
        with storage_tiering_configured():
            with session.make_session_for_existing_admin() as admin_session:
                filename = 'test_put_file'
                filepath = lib.create_local_testfile(filename)
                admin_session.assert_icommand('iput -R ufs2 ' + filename)
                admin_session.assert_icommand('ils -L ', 'STDOUT_SINGLELINE', 'rods')

                # test restage to tier 1
                admin_session.assert_icommand('iget ' + filename + ' - ', 'STDOUT_SINGLELINE', 'TESTFILE')
                sleep(40)
                admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'ufs1')

                admin_session.assert_icommand('irm -f ' + filename)

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
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::time 65')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::minimum_restage_tier true')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::preserve_replicas true')

    def tearDown(self):
        super(TestStorageTieringPluginPreserveReplica, self).tearDown()
        with session.make_session_for_existing_admin() as admin_session:

            admin_session.assert_icommand('iadmin rmresc ufs0')
            admin_session.assert_icommand('iadmin rmresc ufs1')
            admin_session.assert_icommand('iadmin rmresc ufs2')
            admin_session.assert_icommand('iadmin rum')

    def test_put_and_get(self):
        with storage_tiering_configured():
            with session.make_session_for_existing_admin() as admin_session:
                filename = 'test_put_file'
                filepath = lib.create_local_testfile(filename)
                admin_session.assert_icommand('iput -R ufs0 ' + filename)
                admin_session.assert_icommand('ils -L ', 'STDOUT_SINGLELINE', 'rods')

                # stage to tier 1, look for both replicas
                sleep(5)
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
                admin_session.assert_icommand('iqstat', 'STDOUT_SINGLELINE', 'apply')
                sleep(40)
                admin_session.assert_icommand('iqstat', 'STDOUT_SINGLELINE', 'No')
                admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'ufs0')
                admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'ufs1')

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
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::time 65')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::minimum_restage_tier true')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::object_limit 1')

    def tearDown(self):
        super(TestStorageTieringPluginObjectLimit, self).tearDown()
        with session.make_session_for_existing_admin() as admin_session:

            admin_session.assert_icommand('iadmin rmresc ufs0')
            admin_session.assert_icommand('iadmin rmresc ufs1')
            admin_session.assert_icommand('iadmin rmresc ufs2')
            admin_session.assert_icommand('iadmin rum')

    def test_put_and_get(self):
        with storage_tiering_configured():
            with session.make_session_for_existing_admin() as admin_session:
                filename  = 'test_put_file'
                filename2 = 'test_put_file2'
                filepath  = lib.create_local_testfile(filename)
                admin_session.assert_icommand('iput -R ufs0 ' + filename)
                admin_session.assert_icommand('iput -R ufs0 ' + filename + " " + filename2)
                admin_session.assert_icommand('ils -L ', 'STDOUT_SINGLELINE', 'rods')

                # stage to tier 1, look for both replicas
                sleep(5)
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
                admin_session.assert_icommand('iqstat', 'STDOUT_SINGLELINE', 'apply')
                sleep(40)
                admin_session.assert_icommand('iqstat', 'STDOUT_SINGLELINE', 'No')
                admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'ufs1')
                admin_session.assert_icommand_fail('ils -L ' + filename2, 'STDOUT_SINGLELINE', 'ufs1')

                admin_session.assert_icommand('irm -f ' + filename)
                admin_session.assert_icommand('irm -f ' + filename2)

class TestStorageTieringPluginLogMigration(ResourceBase, unittest.TestCase):
    def setUp(self):
        super(TestStorageTieringPluginLogMigration, self).setUp()
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand('iqdel -a')
            admin_session.assert_icommand('iadmin mkresc ufs0 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs0', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs1 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs1', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs2 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs2', 'STDOUT_SINGLELINE', 'unixfilesystem')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::group example_group 0')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::group example_group 1')
            admin_session.assert_icommand('imeta add -R ufs2 irods::storage_tiering::group example_group 2')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::time 5')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::time 65')

    def tearDown(self):
        super(TestStorageTieringPluginLogMigration, self).tearDown()
        with session.make_session_for_existing_admin() as admin_session:

            admin_session.assert_icommand('iadmin rmresc ufs0')
            admin_session.assert_icommand('iadmin rmresc ufs1')
            admin_session.assert_icommand('iadmin rmresc ufs2')
            admin_session.assert_icommand('iadmin rum')

    def test_put_and_get(self):
        with storage_tiering_configured_with_log():
            with session.make_session_for_existing_admin() as admin_session:
                initial_log_size = lib.get_file_size_by_path(paths.server_log_path())

                filename  = 'test_put_file'
                filepath  = lib.create_local_testfile(filename)
                admin_session.assert_icommand('iput -R ufs0 ' + filename)
                admin_session.assert_icommand('imeta ls -d ' + filename, 'STDOUT_SINGLELINE', filename)
                admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', filename)

                # test stage to tier 1
                sleep(5)
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
                admin_session.assert_icommand('iqstat', 'STDOUT_SINGLELINE', 'apply')
                sleep(40)
                admin_session.assert_icommand('iqstat', 'STDOUT_SINGLELINE', 'No')
                admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'ufs1')

                # test stage to tier 2
                sleep(25)
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
                admin_session.assert_icommand('iqstat', 'STDOUT_SINGLELINE', 'apply')
                sleep(60)
                admin_session.assert_icommand('iqstat', 'STDOUT_SINGLELINE', 'No')
                admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'ufs2')

                # test restage to tier 0
                admin_session.assert_icommand('iget ' + filename + ' - ', 'STDOUT_SINGLELINE', 'TESTFILE')
                sleep(40)
                admin_session.assert_icommand('iqstat', 'STDOUT_SINGLELINE', 'No')
                admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'ufs0')

                admin_session.assert_icommand('irm -f ' + filename)

                log_count = lib.count_occurrences_of_string_in_log(paths.server_log_path(), 'storage_tiering::migrating', start_index=initial_log_size)
                print("LOG COUNT: "+str(log_count))
                self.assertTrue(3 == log_count)

class TestStorageTieringMultipleQueries(ResourceBase, unittest.TestCase):
    def setUp(self):
        super(TestStorageTieringMultipleQueries, self).setUp()
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand('iqdel -a')
            admin_session.assert_icommand('''iadmin asq "select distinct R_DATA_MAIN.data_name, R_COLL_MAIN.coll_name, R_DATA_MAIN.resc_id from R_DATA_MAIN, R_COLL_MAIN, R_RESC_MAIN, R_OBJT_METAMAP r_data_metamap, R_META_MAIN r_data_meta_main where R_RESC_MAIN.resc_name = 'ufs0' AND r_data_meta_main.meta_attr_name = 'archive_object' AND r_data_meta_main.meta_attr_value = 'yes' AND R_COLL_MAIN.coll_id = R_DATA_MAIN.coll_id AND R_RESC_MAIN.resc_id = R_DATA_MAIN.resc_id AND R_DATA_MAIN.data_id = r_data_metamap.object_id AND r_data_metamap.meta_id = r_data_meta_main.meta_id order by R_COLL_MAIN.coll_name, R_DATA_MAIN.data_name" archive_query''')

            admin_session.assert_icommand('iadmin mkresc ufs0 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs0', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs1 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs1', 'STDOUT_SINGLELINE', 'unixfilesystem')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::group example_group 0')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::group example_group 1')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::time 80')

            admin_session.assert_icommand('''imeta add -R ufs0 irods::storage_tiering::query "select DATA_NAME, COLL_NAME, DATA_RESC_ID where RESC_NAME = 'ufs0' and META_DATA_ATTR_NAME = 'irods::access_time' and META_DATA_ATTR_VALUE < 'TIME_CHECK_STRING'"''')
            admin_session.assert_icommand('''imeta add -R ufs0 irods::storage_tiering::query archive_query specific''')

    def tearDown(self):
        super(TestStorageTieringMultipleQueries, self).tearDown()
        with session.make_session_for_existing_admin() as admin_session:

            admin_session.assert_icommand('iadmin rmresc ufs0')
            admin_session.assert_icommand('iadmin rmresc ufs1')
            admin_session.assert_icommand('iadmin rum')
            admin_session.assert_icommand('iadmin rsq archive_query')

    def test_put_and_get(self):
        with storage_tiering_configured():
            with session.make_session_for_existing_admin() as admin_session:

                filename  = 'test_put_file'
                filename2 = 'test_put_file2'
                filepath  = lib.create_local_testfile(filename)
                admin_session.assert_icommand('iput -R ufs0 ' + filename)
                admin_session.assert_icommand('imeta add -d ' + filename + ' archive_object yes')
                admin_session.assert_icommand('imeta ls -d ' + filename, 'STDOUT_SINGLELINE', 'irods::access_time')

                admin_session.assert_icommand('iput -R ufs0 ' + filename + ' ' + filename2)
                admin_session.assert_icommand('imeta ls -d ' + filename2, 'STDOUT_SINGLELINE', 'irods::access_time')

                # test stage to tier 1
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
                admin_session.assert_icommand('iqstat', 'STDOUT_SINGLELINE', 'apply')
                sleep(40)
                admin_session.assert_icommand('iqstat', 'STDOUT_SINGLELINE', 'No')
                admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'ufs1')
                admin_session.assert_icommand('ils -L ' + filename2, 'STDOUT_SINGLELINE', 'ufs0')

                sleep(40)

                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
                admin_session.assert_icommand('iqstat', 'STDOUT_SINGLELINE', 'apply')
                sleep(40)
                admin_session.assert_icommand('ils -L ' + filename2, 'STDOUT_SINGLELINE', 'ufs1')

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

    def tearDown(self):
        super(TestStorageTieringPluginRegistration, self).tearDown()
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand('iadmin rmresc ufs0')
            admin_session.assert_icommand('iadmin rmresc ufs1')
            admin_session.assert_icommand('iadmin rum')

    def test_file_registration(self):
        with storage_tiering_configured():
            with session.make_session_for_existing_admin() as admin_session:

                filename  = 'test_put_file'
                filepath  = lib.create_local_testfile(filename)
                ipwd, _, _ = admin_session.run_icommand('ipwd')
                ipwd = ipwd.rstrip()
                dest_path = ipwd + '/' + filename

                admin_session.assert_icommand('ipwd', 'STDOUT_SINGLELINE', 'rods')
                admin_session.assert_icommand('ireg -R ufs0 ' + filepath + ' ' + dest_path)
                admin_session.assert_icommand('imeta ls -d ' + filename, 'STDOUT_SINGLELINE', filename)
                admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', filename)

                # test stage to tier 1
                sleep(5)
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
                admin_session.assert_icommand('iqstat', 'STDOUT_SINGLELINE', 'apply')
                sleep(40)
                admin_session.assert_icommand('iqstat', 'STDOUT_SINGLELINE', 'No')
                admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'ufs1')

                admin_session.assert_icommand('irm -f ' + filename)

    def test_directory_registration(self):
        with storage_tiering_configured():
            with session.make_session_for_existing_admin() as admin_session:
                local_dir_name = '/tmp/test_directory_registration_dir'
                shutil.rmtree(local_dir_name, ignore_errors=True)
                local_tree = lib.make_deep_local_tmp_dir(local_dir_name, 3, 10, 5)

                dest_path = '/tempZone/home/rods/reg_coll'
                admin_session.assert_icommand('ireg -CR ufs0 ' + local_dir_name + ' ' + dest_path)
                admin_session.assert_icommand('ils -rL ' + dest_path, 'STDOUT_SINGLELINE', dest_path)

                # test stage to tier 1
                sleep(5)
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
                admin_session.assert_icommand('iqstat', 'STDOUT_SINGLELINE', 'apply')
                sleep(40)
                admin_session.assert_icommand('iqstat', 'STDOUT_SINGLELINE', 'No')
                admin_session.assert_icommand('ils -rL ' + dest_path, 'STDOUT_SINGLELINE', 'ufs1')

                admin_session.assert_icommand('irm -rf ' + dest_path)




