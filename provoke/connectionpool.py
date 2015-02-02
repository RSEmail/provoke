# The MIT License (MIT)
#
# Copyright (c) 2014 Ian Good
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

"""Manages pools of connections that may be reused. These pools may be shared
between threads. If is shared to a separate process, it is reset to empty.

This module should only be needed for developing new context managers, like
:class:`~provoke.mysql.MySQLConnection`, that can utilize a pool of connections
in a thread-safe manner.

"""

from __future__ import absolute_import

import os
from six.moves.queue import Queue, Empty, Full

__all__ = ['ConnectionPool']


class ConnectionPool(object):
    """Creates a connection pool for the given type of connection. The
    connection type is expected to implement ``.check()``, which returns True
    if the connection is still available and False otherwise, and ``.close()``
    which will close the connection, gracefully if possible.

    :param conn_factory: A callable object (usually a class) for creating new
                         connections when necessary.
    :type conn_factory: collections.Callable
    :param conn_args: Arguments passed to ``conn_factory``.
    :type conn_args: positional arguments
    :param conn_kwargs: Arguments passed to ``conn_factory``.
    :type conn_kwargs: keyword arguments
    :param max_size: The maximum size of the pool. Additional connections will
                     be immediately closed after use, rather than put in the
                     pool.
    :type max_size: int

    """

    #: With this static flag you can globally disable connection pooling and
    #: instead close connections immediately after use.
    enabled = True

    def __init__(self, conn_factory, conn_args=None, conn_kwargs=None,
                 max_size=10):
        super(ConnectionPool, self).__init__()
        self.conn_factory = conn_factory
        self.conn_args = conn_args or ()
        self.conn_kwargs = conn_kwargs or {}
        self.max_size = max_size
        self._pid = None
        self._get_pool()

    def __del__(self):
        pool = self._get_pool()
        while True:
            try:
                conn = pool.get_nowait()
            except Empty:
                break
            try:
                conn.close()
            except Exception:
                pass

    def _get_pool(self):
        pid = os.getpid()
        if self._pid != pid:
            self._pid = pid
            self._pool = Queue(self.max_size)
        return self._pool

    def get(self):
        """Acquires a connection. If an existing connection is available and
        ready, it is used, otherwise a new connection is established.

        :returns: A new connection object.

        """
        pool = self._get_pool()
        try:
            conn = pool.get_nowait()
            if not conn.check():
                try:
                    conn.close()
                except Exception:
                    pass
                return self.get()
            return conn
        except Empty:
            return self.conn_factory(*self.conn_args, **self.conn_kwargs)

    def release(self, conn):
        """Releases the connection back to the pool. If the pool is full, the
        connection is closed instead.

        :param conn: The connection to release.

        """
        if not self.enabled:
            conn.close()
        else:
            pool = self._get_pool()
            try:
                pool.put_nowait(conn)
            except Full:
                conn.close()
