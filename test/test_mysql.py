
import unittest

try:
    from mock import patch, MagicMock
except ImportError:
    from unittest.mock import patch, MagicMock

try:
    import MySQLdb as mysql_mod
    connect_func = 'connect'
except ImportError:
    import pymysql as mysql_mod
    connect_func = 'Connect'

from provoke.mysql import MySQLConnection, _MySQLContext
from provoke.connectionpool import ConnectionPool


@patch.object(ConnectionPool, 'enabled', new=False)
class TestMySQLContext(unittest.TestCase):

    @patch.object(mysql_mod, connect_func)
    def test_check(self, conn_func_mock):
        conn_func_mock.return_value = conn = MagicMock()
        ctx = _MySQLContext(one=1)
        self.assertTrue(ctx.check())
        conn_func_mock.assert_called_with(one=1)
        conn.ping.assert_called_with()

    @patch.object(mysql_mod, connect_func)
    def test_check_fail(self, conn_func_mock):
        conn_func_mock.return_value = conn = MagicMock()
        conn.ping.side_effect = mysql_mod.OperationalError
        ctx = _MySQLContext(one=1)
        self.assertFalse(ctx.check())
        conn_func_mock.assert_called_with(one=1)
        conn.ping.assert_called_with()

    @patch.object(mysql_mod, connect_func)
    def test_close(self, conn_func_mock):
        conn_func_mock.return_value = conn = MagicMock()
        ctx = _MySQLContext(one=1)
        ctx.close()
        conn_func_mock.assert_called_with(one=1)
        conn.close.assert_called_with()


@patch.object(ConnectionPool, 'enabled', new=False)
class TestMySQLConnection(unittest.TestCase):

    def test_set_connection_params(self):
        old = MySQLConnection._connection_params.copy()
        expected = {'host': 'testhost',
                    'user': 'testuser',
                    'password': 'testpass'}
        MySQLConnection.set_connection_params('test', **expected)
        received = MySQLConnection._connection_params.get('test')
        self.assertEqual(expected, received)
        MySQLConnection._connection_params = old

    @patch.object(mysql_mod, connect_func)
    def test_successful(self, conn_func_mock):
        conn_func_mock.return_value = conn_mock = MagicMock()
        with MySQLConnection('test') as testdb:
            self.assertEqual(conn_mock, testdb.conn)
        conn_func_mock.assert_called_with()
        conn_mock.close.assert_called_with()

    @patch.object(mysql_mod, connect_func)
    def test_exception_raised(self, conn_func_mock):
        conn_func_mock.return_value = conn_mock = MagicMock()
        try:
            with MySQLConnection('test') as testdb:
                self.assertEqual(conn_mock, testdb.conn)
                raise ValueError('failure')
        except ValueError as exc:
            self.assertEqual('failure', str(exc))
        else:
            self.fail('ValueError was suppressed or not raised')
        conn_func_mock.assert_called_with()
        conn_mock.close.assert_called_with()

    @patch.object(mysql_mod, connect_func)
    def test_close_exception(self, conn_func_mock):
        conn_func_mock.return_value = conn_mock = MagicMock()
        conn_mock.close.side_effect = ValueError('close failure')
        try:
            with MySQLConnection('test') as testdb:
                self.assertEqual(conn_mock, testdb.conn)
        except ValueError as exc:
            self.assertEqual('close failure', str(exc))
        else:
            self.fail('ValueError was suppressed or not raised')
        conn_func_mock.assert_called_with()
        conn_mock.close.assert_called_with()
