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

"""Common database module. Provides a central point where databases may be
configured, and a context manager to ensure connections are closed after use.

.. py:class:: _MySQLContext

   This is the object returned by the :class:`MySQLConnection` context manager.

   .. py:attribute:: conn

      Database connection object, conforming to the Python DB API 2.0. This
      connection is already open and will be closed when the context manager
      exits.

   .. py:attribute:: params

      Contains a dictionary of the parameters used to create the database
      connection.

   .. py:attribute:: module

      A reference to the module containing the DB API 2.0 interface that the
      connection was made with. This is useful for accessing exception classes.

"""

from __future__ import absolute_import

import logging

import MySQLdb

from ..connectionpool import ConnectionPool

__all__ = ['MySQLConnection']


logger = logging.getLogger('provoke.mysql')


class _MySQLContext(object):

    def __init__(self, **params):
        super(_MySQLContext, self).__init__()
        if 'password' in params:
            params['passwd'] = params.pop('password')
        if 'database' in params:
            params['db'] = params.pop('database')
        self.conn = MySQLdb.connect(**params)
        self.module = MySQLdb
        self._set_session_vars()
        host = params.get('host', 'localhost')
        db = params.get('db')
        logger.debug('Connection established: host=%s, db=%s', host, db)

    def _set_session_vars(self):
        cur = self.conn.cursor()
        try:
            cur.execute("""SET SESSION `time_zone` = '+00:00'""")
            self.conn.commit()
        finally:
            cur.close()

    def check(self):
        try:
            self.conn.ping()
        except MySQLdb.OperationalError:
            return False
        return True

    def rollback(self):
        try:
            self.conn.rollback()
        except Exception:
            pass

    def close(self):
        self.conn.close()


class MySQLConnection(object):
    """Context manager that allows easy connection to databases by name using
    configured connection parameters. For example::

        MySQLConnection.set_connection_params('admin', host='...')

        with MySQLConnection('admin') as admin:
            try:
                cur = admin.conn.cursor()
                cur.execute("SELECT * FROM `tbl`")
                for row in cur:
                    print list(row)
            finally:
                cur.close()

    The connection is automatically closed when the context manager ends, but
    any cursors you created should be closed manually.

    :param db_names: The pre-configured database names to create connections
                     for. Upon context manager entry, one
                     :class:`_MySQLContext` object will be returned for each
                     entry in ``db_names``.
    :type db_names: str

    """

    _connection_params = {}
    _pools = None

    def __init__(self, *db_names):
        super(MySQLConnection, self).__init__()
        self.db_names = db_names
        self.db_conns = [None]*len(db_names)

    @classmethod
    def reset_connection_params(cls):
        """Removes all existing connection parameter information."""
        cls._connection_params = {}

    @classmethod
    def set_connection_params(cls, db_name, **params):
        """Configures future uses of the given name with a set of connection
        parameters.

        :param db_name: A unique, memorable name to reference the database by.
        :type db_name: str
        :param params: The database connection parameters, as you would pass in
                       to the ``.connect()`` function of the database.
        :type params: Keyword arguments

        """
        cls._connection_params[db_name] = params

    @classmethod
    def _get_pools(cls, db_names):
        if not cls._pools:
            cls._pools = {}
        for db_name in db_names:
            if db_name not in cls._pools:
                params = cls._connection_params.get(db_name, {})
                cls._pools[db_name] = ConnectionPool(_MySQLContext,
                                                     conn_kwargs=params)
        return cls._pools

    def __enter__(self):
        pools = self._get_pools(self.db_names)
        for i, db_name in enumerate(self.db_names):
            pool = pools[db_name]
            self.db_conns[i] = pool.get()
        if len(self.db_conns) > 1:
            return self.db_conns
        elif len(self.db_conns) == 1:
            return self.db_conns[0]

    def __exit__(self, exc_type, exc_val, exc_tb):
        pools = self._get_pools(self.db_names)
        try:
            for i, db_name in enumerate(self.db_names):
                self.db_conns[i].rollback()
                if exc_type:
                    self.db_conns[i].close()
                else:
                    pool = pools[db_name]
                    pool.release(self.db_conns[i])
        except Exception:
            if not exc_type:
                raise
