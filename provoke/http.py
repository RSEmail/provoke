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

"""Common HTTP module. Provides a central point where HTTP endpoints may be
configured, and a context manager to ensure connections are closed after use.

"""

from __future__ import absolute_import

from six.moves import http_client
import base64

__all__ = ['HttpConnection']


class HttpConnection(object):
    """Defines the context manager for connecting to an HTTP endpoint and
    cleaning up afterwards. The resulting connection is a :py:mod:`http_client`
    object. For example::

        with HttpConnection('example-endpoint') as conn, headers:
            conn.request('GET', '/', headers=headers)
            res = conn.getresponse()
            assert res.status == 200

    :param name: The pre-configured endpoint name.
    :type name: str

    """

    _connection_params = {}

    def __init__(self, name):
        super(HttpConnection, self).__init__()
        self.name = name
        self.conn = None

    @classmethod
    def reset_connection_params(cls):
        """Removes all existing connection parameter information."""
        cls._connection_params = {}

    @classmethod
    def set_connection_params(cls, name, **params):
        """Configures connection parameters that future usages of the context
        manager will use to connect to the HTTP endpoint.

        :param name: A unique, memorable name to reference the endpoint by.
        :type name: str
        :param params: The HTTP endpoint parameters. The boolean keyword
                       ``ssl`` determines whether the rest of the keywords will
                       be passed into :py:class:`http_client.HTTPConnection` or
                       :py:class:`http_client.HTTPSConnection`. The keywords
                       ``user`` and ``password`` will be used to construct a
                       Basic *Authorization* header.
        :type params: Keyword arguments

        """
        cls._connection_params[name] = params

    def _create_conn(self, params):
        user = params.pop('user', None)
        password = params.pop('password', None)
        host = params.pop('host', 'localhost')
        if params.pop('ssl', False):
            conn = http_client.HTTPSConnection(host, **params)
        else:
            conn = http_client.HTTPConnection(host, **params)
        headers = {}
        if user is not None:
            auth = '{0}:{1}'.format(user, password).encode('utf-8')
            auth_raw = base64.b64encode(auth)
            headers['Authorization'] = b'Basic ' + auth_raw
        return conn, headers

    def __enter__(self):
        params = self._connection_params.get(self.name, {}).copy()
        self.conn, headers = self._create_conn(params)
        return self.conn, headers

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.conn.close()
        except Exception:
            if not exc_type:
                raise
