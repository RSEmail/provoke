
import unittest

try:
    from mock import patch, MagicMock
except ImportError:
    from unittest.mock import patch, MagicMock
import amqp

from provoke.amqp import AmqpConnection, _PoolableAmqp
from provoke.connectionpool import ConnectionPool


@patch.object(ConnectionPool, 'enabled', new=False)
class TestPoolableAmqp(unittest.TestCase):

    @patch.object(amqp, 'Connection')
    def test_check(self, conn_class_mock):
        conn_class_mock.return_value = conn = MagicMock()
        ctx = _PoolableAmqp(one=1)
        self.assertTrue(ctx.check())
        conn_class_mock.assert_called_with(one=1)
        conn.send_heartbeat.assert_called_with()

    @patch.object(amqp, 'Connection')
    def test_check_fail(self, conn_class_mock):
        conn_class_mock.return_value = conn = MagicMock()
        conn.send_heartbeat.side_effect = ValueError
        ctx = _PoolableAmqp(one=1)
        self.assertFalse(ctx.check())
        conn_class_mock.assert_called_with(one=1)
        conn.send_heartbeat.assert_called_with()

    @patch.object(amqp, 'Connection')
    def test_close(self, conn_class_mock):
        conn_class_mock.return_value = conn = MagicMock()
        ctx = _PoolableAmqp(one=1)
        ctx.close()
        conn_class_mock.assert_called_with(one=1)
        conn.close.assert_called_with()

    @patch.object(amqp, 'Connection')
    def test_close_fd(self, conn_class_mock):
        conn_class_mock.return_value = conn = MagicMock()
        conn.close.side_effect = ValueError
        ctx = _PoolableAmqp(one=1)
        self.assertRaises(ValueError, ctx.close)
        conn_class_mock.assert_called_with(one=1)
        conn.sock.close.assert_called_with()


@patch.object(ConnectionPool, 'enabled', new=False)
class TestAmqpConnection(unittest.TestCase):

    @patch.object(amqp, 'Connection')
    def test_set_connection_params(self, conn_class_mock):
        old = AmqpConnection._connection_params
        AmqpConnection.set_connection_params(host='testhost',
                                             username='testuser',
                                             password='testpass')
        self.assertEqual({'host': 'testhost',
                          'username': 'testuser',
                          'password': 'testpass'},
                         AmqpConnection._connection_params)
        conn_class_mock.return_value = MagicMock()
        AmqpConnection._connection_params = old

    @patch.object(amqp, 'Connection')
    def test_successful(self, conn_class_mock):
        conn_class_mock.return_value = conn_mock = MagicMock()
        conn_mock.channel.return_value = chan_mock = MagicMock()
        with AmqpConnection() as channel:
            self.assertEqual(chan_mock, channel)
        conn_class_mock.assert_called_with()
        conn_mock.channel.assert_called_with()
        chan_mock.close.assert_called_with()
        conn_mock.close.assert_called_with()

    @patch.object(amqp, 'Connection')
    def test_exception_raised(self, conn_class_mock):
        conn_class_mock.return_value = conn_mock = MagicMock()
        conn_mock.channel.return_value = chan_mock = MagicMock()
        try:
            with AmqpConnection() as channel:
                self.assertEqual(chan_mock, channel)
                raise ValueError('failure')
        except ValueError as exc:
            self.assertEqual('failure', str(exc))
        else:
            self.fail('ValueError was suppressed or not raised')
        conn_class_mock.assert_called_with()
        conn_mock.channel.assert_called_with()
        chan_mock.close.assert_called_with()
        conn_mock.close.assert_called_with()

    @patch.object(amqp, 'Connection')
    def test_close_exception(self, conn_class_mock):
        conn_class_mock.return_value = conn_mock = MagicMock()
        conn_mock.channel.return_value = chan_mock = MagicMock()
        conn_mock.close.side_effect = ValueError('close failure')
        try:
            with AmqpConnection() as channel:
                self.assertEqual(chan_mock, channel)
        except ValueError as exc:
            self.assertEqual('close failure', str(exc))
        else:
            self.fail('ValueError was suppressed or not raised')
        conn_class_mock.assert_called_with()
        conn_mock.channel.assert_called_with()
        chan_mock.close.assert_called_with()
        conn_mock.close.assert_called_with()

    @patch.object(amqp, 'Connection')
    def test_disconnect_exception(self, conn_class_mock):
        conn_class_mock.return_value = conn_mock = MagicMock()
        conn_mock.channel.return_value = chan_mock = MagicMock()
        conn_mock.disconnect.side_effect = ValueError('disconnect failure')
        with AmqpConnection() as channel:
            self.assertEqual(chan_mock, channel)
        conn_class_mock.assert_called_with()
        conn_mock.channel.assert_called_with()
        chan_mock.close.assert_called_with()
        conn_mock.close.assert_called_with()
