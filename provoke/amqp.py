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

"""Common AMQP module. Provides a simple context manager interface for
connecting to an AMQP broker and returning a usable channel, closing both when
the context manager exits.

"""

from __future__ import absolute_import

import amqp

from .connectionpool import ConnectionPool
from .logging import log_debug

__all__ = ['AmqpConnection']


class _PoolableAmqp(object):

    def __init__(self, **kwargs):
        super(_PoolableAmqp, self).__init__()
        self.conn = amqp.Connection(**kwargs)
        host = kwargs.get('host', 'localhost')
        user = kwargs.get('userid', 'guest')
        vhost = kwargs.get('virtual_host', '/')
        log_debug('Connection established', logger='amqp',
                  host=host, user=user, vhost=vhost)

    def check(self):
        try:
            self.conn.send_heartbeat()
        except Exception:
            return False
        return True

    def close(self):
        try:
            self.conn.close()
        except Exception:
            try:
                self.conn.sock.close()
            except Exception:
                pass
            raise


class AmqpConnection(object):
    """Defines the context manager for connecting to the AMQP broker and
    cleaning up afterwards. For example::

        with AmqpConnection() as channel:
            channel.basic_publish(exchange='',
                                  routing_key='testqueue',
                                  properties=my_properties,
                                  body='Test message')

    If custom connection parameters are needed (e.g. RabbitMQ is not running
    locally), they may be configured beforehand and all future context managers
    will use them.

    """

    _connection_params = {}
    _pool = None

    def __init__(self):
        super(AmqpConnection, self).__init__()

    @classmethod
    def set_connection_params(cls, **params):
        """Configures connection parameters that future usages of the context
        manager will use to connect to the AMQP broker.

        :param params: The AMQP connection parameters, as you would pass in to
                       the amqp's
                       :class:`~amqp.connection.Connection` class.
        :type params: Keyword arguments

        """
        cls._connection_params = params

    @classmethod
    def _get_pool(cls):
        if not cls._pool:
            cls._pool = ConnectionPool(_PoolableAmqp,
                                       conn_kwargs=cls._connection_params)
        return cls._pool

    def __enter__(self):
        pool = self._get_pool()
        self.conn = pool.get()
        self.channel = self.conn.conn.channel()
        return self.channel

    def __exit__(self, exc_type, exc_val, exc_tb):
        pool = self._get_pool()
        try:
            self.channel.close()
        except Exception:
            pass
        try:
            if exc_type:
                self.conn.close()
            else:
                pool.release(self.conn)
        except Exception:
            if not exc_type:
                raise
