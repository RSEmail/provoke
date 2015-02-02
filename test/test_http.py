
import unittest

try:
    from mock import patch, MagicMock
except ImportError:
    from unittest.mock import patch, MagicMock
from six.moves import http_client

from provoke.http import HttpConnection


class TestHttpConnection(unittest.TestCase):

    def setUp(self):
        HttpConnection._connection_params = {}

    def test_reset_connection_params(self):
        HttpConnection.set_connection_params('test', host='testhost')
        HttpConnection.reset_connection_params()
        self.assertEqual({}, HttpConnection._connection_params)

    def test_set_connection_params(self):
        HttpConnection.set_connection_params('test',
                                             host='testhost',
                                             user='testuser',
                                             password='testpass',
                                             ssl=True)
        self.assertEqual({'test': {'host': 'testhost',
                                   'user': 'testuser',
                                   'password': 'testpass',
                                   'ssl': True}},
                         HttpConnection._connection_params)

    @patch.object(http_client, 'HTTPConnection')
    def test_successful(self, conn_class_mock):
        conn_class_mock.return_value = conn_mock = MagicMock()
        with HttpConnection('test') as (conn, headers):
            self.assertEqual(conn_mock, conn)
        conn_class_mock.assert_called_with('localhost')
        conn_mock.close.assert_called_with()

    @patch.object(http_client, 'HTTPSConnection')
    def test_successful_ssl(self, conn_class_mock):
        HttpConnection.set_connection_params('test', ssl=True)
        conn_class_mock.return_value = conn_mock = MagicMock()
        with HttpConnection('test') as (conn, headers):
            self.assertEqual(conn_mock, conn)
        conn_class_mock.assert_called_with('localhost')
        conn_mock.close.assert_called_with()

    @patch.object(http_client, 'HTTPConnection')
    def test_successful_auth(self, conn_class_mock):
        HttpConnection.set_connection_params('test',
                                             user='testuser',
                                             password='testpass')
        conn_class_mock.return_value = conn_mock = MagicMock()
        with HttpConnection('test') as (conn, headers):
            self.assertEqual(conn_mock, conn)
            expected = {'Authorization': b'Basic dGVzdHVzZXI6dGVzdHBhc3M='}
            self.assertEqual(expected, headers)
        conn_class_mock.assert_called_with('localhost')
        conn_mock.close.assert_called_with()

    @patch.object(http_client, 'HTTPConnection')
    def test_exception_raised(self, conn_class_mock):
        conn_class_mock.return_value = conn_mock = MagicMock()
        try:
            with HttpConnection('test') as (conn, headers):
                self.assertEqual(conn_mock, conn)
                raise ValueError('failure')
        except ValueError as exc:
            self.assertEqual('failure', str(exc))
        else:
            self.fail('ValueError was suppressed or not raised')
        conn_class_mock.assert_called_with('localhost')
        conn_mock.close.assert_called_with()

    @patch.object(http_client, 'HTTPConnection')
    def test_close_exception(self, conn_class_mock):
        conn_class_mock.return_value = conn_mock = MagicMock()
        conn_mock.close.side_effect = ValueError('close failure')
        try:
            with HttpConnection('test') as (conn, headers):
                self.assertEqual(conn_mock, conn)
        except ValueError as exc:
            self.assertEqual('close failure', str(exc))
        else:
            self.fail('ValueError was suppressed or not raised')
        conn_class_mock.assert_called_with('localhost')
        conn_mock.close.assert_called_with()

    @patch.object(http_client, 'HTTPConnection')
    def test_disconnect_exception(self, conn_class_mock):
        conn_class_mock.return_value = conn_mock = MagicMock()
        conn_mock.disconnect.side_effect = ValueError('disconnect failure')
        with HttpConnection('test') as (conn, headers):
            self.assertEqual(conn_mock, conn)
        conn_class_mock.assert_called_with('localhost')
        conn_mock.close.assert_called_with()
