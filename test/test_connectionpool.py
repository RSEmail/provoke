
import unittest

try:
    from mock import patch, MagicMock
except ImportError:
    from unittest.mock import patch, MagicMock

from provoke.connectionpool import ConnectionPool


class TestConnectionPool(unittest.TestCase):

    def test_get_new(self):
        conn_factory = MagicMock()
        conn_factory.return_value = conn = MagicMock()
        pool = ConnectionPool(conn_factory, (1, 2), {'three': 4})
        self.assertEqual(conn, pool.get())
        conn_factory.assert_called_with(1, 2, three=4)

    def test_get_existing(self):
        conn = MagicMock()
        conn.check.return_value = True
        pool = ConnectionPool(None)
        pool._get_pool().put_nowait(conn)
        self.assertEqual(conn, pool.get())
        conn.check.assert_called_with()

    def test_get_existing_broken(self):
        conn_factory = MagicMock()
        conn_factory.return_value = new_conn = MagicMock()
        old_conn = MagicMock()
        old_conn.check.return_value = False
        pool = ConnectionPool(conn_factory)
        pool._get_pool().put_nowait(old_conn)
        self.assertEqual(new_conn, pool.get())
        conn_factory.assert_called_with()
        old_conn.check.assert_called_with()

    def test_release(self):
        conn = MagicMock()
        pool = ConnectionPool(None)
        pool.release(conn)
        self.assertEqual(1, pool._get_pool().qsize())
        self.assertFalse(conn.close.called)

    def test_release_full(self):
        conn1 = MagicMock()
        conn2 = MagicMock()
        pool = ConnectionPool(None, max_size=1)
        pool.release(conn1)
        pool.release(conn2)
        self.assertEqual(1, pool._get_pool().qsize())
        self.assertFalse(conn1.close.called)
        conn2.close.assert_called_with()

    @patch.object(ConnectionPool, 'enabled', new=False)
    def test_release_disabled(self):
        conn = MagicMock()
        pool = ConnectionPool(None)
        pool.release(conn)
        conn.close.assert_called_with()
