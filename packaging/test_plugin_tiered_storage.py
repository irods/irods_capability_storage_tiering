import time, sys, re
import json
import unittest
import subprocess
import stomp


class DoNothingListener(stomp.ConnectionListener):
    def on_error(self, headers, message):
        pass

    def on_message(self, headers, message):
        pass


class TestListener(stomp.ConnectionListener):
    def __init__(self):

        # the following is used for a count of each rule being executed
        self.rule_counts = {}

        # the following is used for any summation of values associated with rules
        self.rule_summation ={}

    def on_error(self, headers, message):
        print('received an error "%s"' % message)

    def on_message(self, headers, message):
        if "__BEGIN_JSON__" in message:
            message = message.split("__BEGIN_JSON__")[1]
            if "__END_JSON__" in message:
                message = message.split("__END_JSON__")[0]
        rule_name = json.loads(message)['rule_name']
        # print("%s" % rule_name)

        # count the number of entries for this pep
        if rule_name in self.rule_counts.keys():
            self.rule_counts[rule_name] = self.rule_counts[rule_name] + 1
        else:
            self.rule_counts[rule_name] = 1

        if rule_name == 'audit_pep_resource_write_post' or rule_name == 'audit_pep_resource_read_post':
            if rule_name in self.rule_summation.keys():
                self.rule_summation[rule_name] = self.rule_summation[rule_name] + int(json.loads(message)['int'])
            else:
                self.rule_summation[rule_name] = int(json.loads(message)['int'])


class TestAuditPlugin(unittest.TestCase):
    def setUp(self):

        subprocess.Popen(['cp', '/var/lib/irods/scripts/irods/test/test_plugin_tiered_storage_server_config.json', '/etc/irods/server_config.json'])
        time.sleep(3);

        # create a test file
        subprocess.call(['dd', 'if=/dev/zero', 'of=testfile.dat', 'bs=1M', 'count=1'])

    def purge_queue(self):
        conn = stomp.Connection()
        conn.set_listener('', DoNothingListener())
        conn.start()
        conn.connect('', '', wait=True)
        conn.subscribe(destination='/queue/audit_messages', id=1, ack='auto')
        time.sleep(2)
        conn.disconnect()

    def tearDown(self):
        subprocess.call(['rm', 'testfile.dat']) 

    def test_put(self):
        self.purge_queue()
        listener = TestListener()
        conn = stomp.Connection()
        conn.set_listener('', listener)
        conn.start()
        conn.connect('', '', wait=True)
        conn.subscribe(destination='/queue/audit_messages', id=1, ack='auto')
        subprocess.call(['iput', '-f', 'testfile.dat'])
        time.sleep(2)
        conn.disconnect()

        self.assertTrue('audit_pep_resource_write_post' in listener.rule_counts)
        write_pep_count = listener.rule_counts['audit_pep_resource_write_post']
        self.assertTrue(write_pep_count >= 1)
        self.assertTrue('audit_pep_auth_agent_auth_request_pre' in listener.rule_counts)
        self.assertTrue(listener.rule_counts['audit_pep_auth_agent_auth_request_pre'] >= 1)
        self.assertTrue('audit_pep_resource_write_post' in listener.rule_summation)
        self.assertEqual(listener.rule_summation['audit_pep_resource_write_post'] / write_pep_count, 1048576)

    def test_get(self):
        subprocess.call(['iput', '-f', 'testfile.dat'])
        self.purge_queue()
        listener = TestListener()
        conn = stomp.Connection()
        conn.set_listener('', listener)
        conn.start()
        conn.connect('', '', wait=True)
        conn.subscribe(destination='/queue/audit_messages', id=1, ack='auto')
        subprocess.call(['iget', '-f', 'testfile.dat'])
        time.sleep(2)
        conn.disconnect()

        self.assertTrue('audit_pep_resource_read_post' in listener.rule_counts)
        read_pep_count = listener.rule_counts['audit_pep_resource_read_post']
        self.assertTrue(read_pep_count >= 1)
        self.assertTrue('audit_pep_auth_agent_auth_request_pre' in listener.rule_counts)
        self.assertTrue(listener.rule_counts['audit_pep_auth_agent_auth_request_pre'] >= 1)
        self.assertTrue('audit_pep_resource_read_post' in listener.rule_summation)
        self.assertEqual(listener.rule_summation['audit_pep_resource_read_post'] / read_pep_count, 1048576)

