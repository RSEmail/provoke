
import os
import unittest
import resource
from ConfigParser import NoOptionError

from mock import patch, MagicMock

from provoke.common.config import Configuration, read_configuration_dir
from provoke.common.app import WorkerApplication
from provoke.common.amqp import AmqpConnection
from provoke.common.mysql import MySQLConnection


class TestConfiguration(unittest.TestCase):

    def test_from_config_str(self):
        cfgparser = MagicMock()
        cfg = Configuration(cfgparser)
        cfgparser.get.return_value = 'three'
        ret = {}
        cfg._from_config(ret, 'sec', 'onetest', dict_key='one')
        cfgparser.get.assert_called_with('sec', 'onetest')
        self.assertEqual('three', ret['one'])

    def test_from_config_missing(self):
        cfgparser = MagicMock()
        cfg = Configuration(cfgparser)
        cfgparser.get.side_effect = NoOptionError('sec', 'one')
        ret = {}
        cfg._from_config(ret, 'sec', 'one')
        cfgparser.get.assert_called_with('sec', 'one')
        self.assertFalse('one' in ret)

    def test_from_config_int(self):
        cfgparser = MagicMock()
        cfg = Configuration(cfgparser)
        cfgparser.getint.return_value = 13
        ret = {}
        cfg._from_config(ret, 'sec', 'one', opt_type='int')
        cfgparser.getint.assert_called_with('sec', 'one')
        self.assertEqual(13, ret['one'])

    def test_from_config_float(self):
        cfgparser = MagicMock()
        cfg = Configuration(cfgparser)
        cfgparser.getfloat.return_value = 3.14
        ret = {}
        cfg._from_config(ret, 'sec', 'one', opt_type='float')
        cfgparser.getfloat.assert_called_with('sec', 'one')
        self.assertEqual(3.14, ret['one'])

    def test_from_config_bool(self):
        cfgparser = MagicMock()
        cfg = Configuration(cfgparser)
        cfgparser.getboolean.return_value = True
        ret = {}
        cfg._from_config(ret, 'sec', 'one', opt_type='bool')
        cfgparser.getboolean.assert_called_with('sec', 'one')
        self.assertEqual(True, ret['one'])

    def test_from_config_list(self):
        cfgparser = MagicMock()
        cfg = Configuration(cfgparser)
        cfgparser.get.return_value = 'one, two,\tthree'
        ret = {}
        cfg._from_config(ret, 'sec', 'one', opt_type='list')
        cfgparser.get.assert_called_with('sec', 'one')
        self.assertEqual(['one', 'two', 'three'], ret['one'])

    @patch.object(MySQLConnection, 'set_connection_params')
    def test_configure_databases(self, set_mock):
        cfgparser = MagicMock()
        cfg = Configuration(cfgparser)
        cfgparser.sections.return_value = ['one', 'mysql:test']
        cfgparser.get.side_effect = ['testuser',
            'testpass',
            'testhost',
            'testdb',
            'testcharset',
            NoOptionError('mysql:test', 'unix_socket')]
        cfgparser.getint.side_effect = [3306, 10]
        cfg._configure_mysql()
        set_mock.assert_called_with('test',
            user='testuser',
            passwd='testpass',
            host='testhost',
            port=3306,
            db='testdb',
            charset='testcharset',
            connect_timeout=10)

    @patch.object(AmqpConnection, 'set_connection_params')
    def test_configure_amqp(self, set_mock):
        cfgparser = MagicMock()
        cfg = Configuration(cfgparser)
        cfgparser.sections.return_value = ['one', 'amqp']
        cfgparser.get.side_effect = ['testhost',
            'testuser',
            'testpass',
            NoOptionError('amqp', 'virtual_host')]
        cfgparser.getint.return_value = 5672
        cfgparser.getfloat.side_effect = [30.0, 10.0]
        cfg._configure_amqp()
        set_mock.assert_called_with(userid='testuser',
            password='testpass',
            host='testhost',
            port=5672,
            heartbeat=30.0,
            connect_timeout=10.0)

    @patch.object(WorkerApplication, 'declare_taskgroup')
    def test_configure_taskgroups(self, declare_mock):
        cfgparser = MagicMock()
        cfg = Configuration(cfgparser)
        cfgparser.sections.return_value = ['one', 'taskgroup:testgroup']
        cfgparser.get.side_effect = [NoOptionError(None, 'queue'),
                                     'testkey',
                                     'testexchange']
        cfg._configure_taskgroups()
        declare_mock.assert_called_with('testgroup', exchange='testexchange',
                                        routing_key='testkey')

    def test_get_workers(self):
        cfgparser = MagicMock()
        cfg = Configuration(cfgparser)
        cfgparser.sections.return_value = ['other', 'worker:test']
        cfgparser.get.return_value = 'one'
        cfgparser.getint.side_effect = [2, 20]
        cfgparser.getboolean.return_value = True
        workers = cfg.get_workers()
        self.assertEqual([(['one'], {'num_processes': 2, 'exclusive': True,
                                     'task_limit': 20})],
                         workers)

    def test_get_rlimits(self):
        cfgparser = MagicMock()
        cfg = Configuration(cfgparser)
        cfgparser.getint.side_effect = [65535,
                                        NoOptionError('daemon', 'max-fd')]
        self.assertEqual([(resource.RLIMIT_NOFILE, (65535, 65535))],
                         list(cfg.get_rlimits()))
        cfgparser.getint.assert_called_with('daemon', 'max-fd')
        self.assertEqual([], list(cfg.get_rlimits()))
        cfgparser.getint.assert_called_with('daemon', 'max-fd')

    def test_get_pidfile(self):
        cfgparser = MagicMock()
        cfg = Configuration(cfgparser)
        cfgparser.get.side_effect = ['/var/run/test',
                                     NoOptionError('daemon', 'pidfile')]
        self.assertEqual('/var/run/test', cfg.get_pidfile())
        cfgparser.get.assert_called_with('daemon', 'pidfile')
        self.assertEqual(None, cfg.get_pidfile())
        cfgparser.get.assert_called_with('daemon', 'pidfile')

    def test_get_stdio_redirects(self):
        cfgparser = MagicMock()
        cfg = Configuration(cfgparser)
        cfgparser.get.side_effect = ['test_stdout',
                                     'test_stderr',
                                     'test_stdin']
        self.assertEqual(('test_stdout', 'test_stderr', 'test_stdin'),
                         cfg.get_stdio_redirects())
        self.assertEqual(3, cfgparser.get.call_count)
        cfgparser.get.assert_any_call('daemon', 'stdout')
        cfgparser.get.assert_any_call('daemon', 'stderr')
        cfgparser.get.assert_any_call('daemon', 'stdin')

    def test_get_worker_privileges(self):
        cfgparser = MagicMock()
        cfg = Configuration(cfgparser)
        cfgparser.get.side_effect = ['test_user',
                                     'test_group',
                                     '1234']
        self.assertEqual(('test_user', 'test_group', 1234),
                         cfg.get_worker_privileges())
        self.assertEqual(3, cfgparser.get.call_count)
        cfgparser.get.assert_any_call('daemon', 'user')
        cfgparser.get.assert_any_call('daemon', 'group')
        cfgparser.get.assert_any_call('daemon', 'umask')

    @patch.object(os, 'listdir')
    def test_read_configuration_dir(self, listdir_mock):
        listdir_mock.return_value = ['one', 'two.conf', 'three.conf']
        configparser = MagicMock()
        ret = read_configuration_dir('testdir', configparser=configparser)
        configparser.read.assert_called_with(['testdir/two.conf',
                                              'testdir/three.conf'])
        self.assertEqual(configparser, ret)


# vim:et:fdm=marker:sts=4:sw=4:ts=4
