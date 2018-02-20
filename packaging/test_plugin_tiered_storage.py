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
def tiered_storage_configured_custom(arg=None):
    filename = paths.server_config_path()
    with lib.file_backed_up(filename):
        irods_config = IrodsConfig()
        irods_config.server_config['advanced_settings']['rule_engine_server_sleep_time_in_seconds'] = 1

        irods_config.server_config['plugin_configuration']['rule_engines'].insert(0,
            {
                "instance_name" : "irods_rule_engine_plugin-tiered_storage-instance",
                "plugin_name" : "irods_rule_engine_plugin-tiered_storage",
                "plugin_specific_configuration" : {
                    "access_time_attribute" : "irods::custom_access_time",
                    "storage_tiering_group_attribute" : "irods::custom_storage_tier_group",
                    "storage_tiering_time_attribute" : "irods::custom_storage_tier_time",
                    "storage_tiering_query_attribute" : "irods::custom_storage_tier_query",
                    "storage_tiering_verification_attribute" : "irods::custom_storage_tier_verification",
                    "storage_tiering_restage_delay_attribute" : "irods::custom_storage_tier_restage_delay",

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
def tiered_storage_configured(arg=None):
    filename = paths.server_config_path()
    with lib.file_backed_up(filename):
        irods_config = IrodsConfig()
        irods_config.server_config['advanced_settings']['rule_engine_server_sleep_time_in_seconds'] = 1

        irods_config.server_config['plugin_configuration']['rule_engines'].insert(0,
            {
                "instance_name": "irods_rule_engine_plugin-tiered_storage-instance",
                "plugin_name": "irods_rule_engine_plugin-tiered_storage",
                "plugin_specific_configuration": {
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
        with session.make_session_for_existing_admin() as admin_session:
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

            admin_session.assert_icommand('imeta add -R rnd0 irods::storage_tier_group example_group 0')
            admin_session.assert_icommand('imeta add -R rnd1 irods::storage_tier_group example_group 1')
            admin_session.assert_icommand('imeta add -R rnd2 irods::storage_tier_group example_group 2')
            admin_session.assert_icommand('imeta add -R rnd0 irods::storage_tier_time 5')
            admin_session.assert_icommand('imeta add -R rnd1 irods::storage_tier_time 65')
            admin_session.assert_icommand('''imeta set -R rnd1 irods::storage_tier_query "select DATA_NAME, COLL_NAME where RESC_NAME = 'ufs2' || = 'ufs3' and META_DATA_ATTR_NAME = 'irods::access_time' and META_DATA_ATTR_VALUE < 'TIME_CHECK_STRING'"''')

    def tearDown(self):
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
        with tiered_storage_configured():
            with session.make_session_for_existing_admin() as admin_session:
                shutil.copy('/etc/irods/server_config.json', '/var/lib/irods/server_config.copy')

                admin_session.assert_icommand('ils -L ', 'STDOUT_SINGLELINE', 'rods')

                filename = 'test_put_file'
                filepath = lib.create_local_testfile(filename)
                admin_session.assert_icommand('iput -R rnd0 ' + filename)
                admin_session.assert_icommand('imeta ls -d ' + filename, 'STDOUT_SINGLELINE', filename)
                admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', filename)

                # test stage to tier 1
                sleep(5)
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-tiered_storage-instance -F /var/lib/irods/example_tiering_invocation.r')
                sleep(60)
                admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'rnd1')

                # test stage to tier 2
                sleep(10)
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-tiered_storage-instance -F /var/lib/irods/example_tiering_invocation.r')
                sleep(60)
                admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'rnd2')

                # test restage to tier 0
                admin_session.assert_icommand('iget ' + filename + ' - ', 'STDOUT_SINGLELINE', 'TESTFILE')
                sleep(40)
                admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'rnd0')

                admin_session.assert_icommand('irm -f ' + filename)

class TestStorageTieringPluginMultiGroup(ResourceBase, unittest.TestCase):
    def setUp(self):
        with session.make_session_for_existing_admin() as admin_session:
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

            admin_session.assert_icommand('imeta add -R rnd0 irods::storage_tier_group example_group 0')
            admin_session.assert_icommand('imeta add -R rnd1 irods::storage_tier_group example_group 1')
            admin_session.assert_icommand('imeta add -R rnd2 irods::storage_tier_group example_group 2')
            admin_session.assert_icommand('imeta add -R rnd0 irods::storage_tier_time 5')
            admin_session.assert_icommand('imeta add -R rnd1 irods::storage_tier_time 65')
            admin_session.assert_icommand('''imeta set -R rnd1 irods::storage_tier_query "select DATA_NAME, COLL_NAME where RESC_NAME = 'ufs2' || = 'ufs3' and META_DATA_ATTR_NAME = 'irods::access_time' and META_DATA_ATTR_VALUE < 'TIME_CHECK_STRING'"''')

            admin_session.assert_icommand('iadmin mkresc ufs0g2 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs0g2', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs1g2 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs1g2', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs2g2 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs2g2', 'STDOUT_SINGLELINE', 'unixfilesystem')

            admin_session.assert_icommand('imeta add -R ufs0g2 irods::storage_tier_group example_group_g2 0')
            admin_session.assert_icommand('imeta add -R ufs1g2 irods::storage_tier_group example_group_g2 1')
            admin_session.assert_icommand('imeta add -R ufs2g2 irods::storage_tier_group example_group_g2 2')

            admin_session.assert_icommand('imeta add -R ufs0g2 irods::storage_tier_time 5')
            admin_session.assert_icommand('imeta add -R ufs1g2 irods::storage_tier_time 65')

            admin_session.assert_icommand('''imeta set -R ufs1g2 irods::storage_tier_query "select DATA_NAME, COLL_NAME where RESC_NAME = 'ufs1g2' and META_DATA_ATTR_NAME = 'irods::access_time' and META_DATA_ATTR_VALUE < 'TIME_CHECK_STRING'"''')

    def tearDown(self):
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
        with tiered_storage_configured():
            with session.make_session_for_existing_admin() as admin_session:
                print("yep")
                admin_session.assert_icommand('ils -L ', 'STDOUT_SINGLELINE', 'rods')

                filename = 'test_put_file'

                filenameg2 = 'test_put_fileg2'
                filepath = lib.create_local_testfile(filenameg2)

                admin_session.assert_icommand('iput -R rnd0 ' + filename)
                admin_session.assert_icommand('imeta ls -d ' + filename, 'STDOUT_SINGLELINE', filename)
                admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', filename)

                admin_session.assert_icommand('iput -R ufs0g2 ' + filenameg2)
                admin_session.assert_icommand('imeta ls -d ' + filenameg2, 'STDOUT_SINGLELINE', filenameg2)
                admin_session.assert_icommand('ils -L ' + filenameg2, 'STDOUT_SINGLELINE', filenameg2)

                # test stage to tier 1
                sleep(5)
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-tiered_storage-instance -F /var/lib/irods/example_tiering_invocation.r')
                sleep(60)
                admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'rnd1')
                admin_session.assert_icommand('ils -L ' + filenameg2, 'STDOUT_SINGLELINE', 'ufs1g2')

                # test stage to tier 2
                sleep(10)
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-tiered_storage-instance -F /var/lib/irods/example_tiering_invocation.r')
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
        with session.make_session_for_existing_admin() as admin_session:
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

            admin_session.assert_icommand('imeta add -R rnd0 irods::custom_storage_tier_group example_group 0')
            admin_session.assert_icommand('imeta add -R rnd1 irods::custom_storage_tier_group example_group 1')
            admin_session.assert_icommand('imeta add -R rnd2 irods::custom_storage_tier_group example_group 2')
            admin_session.assert_icommand('imeta add -R rnd0 irods::custom_storage_tier_time 5')
            admin_session.assert_icommand('imeta add -R rnd1 irods::custom_storage_tier_time 65')
            admin_session.assert_icommand('''imeta set -R rnd1 irods::custom_storage_tier_query "select DATA_NAME, COLL_NAME where RESC_NAME = 'ufs2' || = 'ufs3' and META_DATA_ATTR_NAME = 'irods::custom_access_time' and META_DATA_ATTR_VALUE < 'TIME_CHECK_STRING'"''')

    def tearDown(self):
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
        with tiered_storage_configured_custom():
            with session.make_session_for_existing_admin() as admin_session:
                shutil.copy('/etc/irods/server_config.json', '/var/lib/irods/server_config.copy')

                admin_session.assert_icommand('ils -L ', 'STDOUT_SINGLELINE', 'rods')

                filename = 'test_put_file'
                filepath = lib.create_local_testfile(filename)
                admin_session.assert_icommand('iput -R rnd0 ' + filename)
                admin_session.assert_icommand('imeta ls -d ' + filename, 'STDOUT_SINGLELINE', filename)
                admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', filename)

                # test stage to tier 1
                sleep(5)
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-tiered_storage-instance -F /var/lib/irods/example_tiering_invocation.r')
                admin_session.assert_icommand('iqstat', 'STDOUT_SINGLELINE', 'apply')
                sleep(40)
                admin_session.assert_icommand('iqstat', 'STDOUT_SINGLELINE', 'No')
                admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'rnd1')

                # test stage to tier 2
                sleep(25)
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-tiered_storage-instance -F /var/lib/irods/example_tiering_invocation.r')
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
        with session.make_session_for_existing_admin() as admin_session:
            # configure mungefs
            self.munge_mount=tempfile.mkdtemp(prefix='munge_mount_')
            self.munge_target=tempfile.mkdtemp(prefix='munge_target_')

            assert_command('mungefs ' + self.munge_mount + ' -omodules=subdir,subdir=' + self.munge_target)

            admin_session.assert_icommand('iadmin mkresc ufs0 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs0', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs1 unixfilesystem '+test.settings.HOSTNAME_1 +':'+self.munge_mount, 'STDOUT_SINGLELINE', 'unixfilesystem')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tier_group example_group 0')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tier_group example_group 1')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tier_time 5')

    def tearDown(self):
        with session.make_session_for_existing_admin() as admin_session:
            assert_command('fusermount -u '+self.munge_mount)
            shutil.rmtree(self.munge_mount, ignore_errors=True)
            shutil.rmtree(self.munge_target, ignore_errors=True)

            admin_session.assert_icommand('iadmin rmresc ufs0')
            admin_session.assert_icommand('iadmin rmresc ufs1')
            admin_session.assert_icommand('iadmin rum')

    @classmethod
    def test_put_verify_filesystem(self):
        with tiered_storage_configured():
            with session.make_session_for_existing_admin() as admin_session:
                shutil.copy('/etc/irods/server_config.json', '/var/lib/irods/server_config.copy')

                # configure mungefs to report an invalid file size
                assert_command('mungefsctl --operations "getattr" --corrupt_size')

                # set filesystem verification
                admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tier_verification filesystem')

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
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-tiered_storage-instance -F /var/lib/irods/example_tiering_invocation.r')
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
        with tiered_storage_configured():
            with session.make_session_for_existing_admin() as admin_session:
                shutil.copy('/etc/irods/server_config.json', '/var/lib/irods/server_config.copy')

                # configure mungefs to report an invalid file size
                assert_command('mungefsctl --operations "read" --corrupt_data')

                # set checksum verification
                admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tier_verification checksum')

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
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-tiered_storage-instance -F /var/lib/irods/example_tiering_invocation.r')
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







