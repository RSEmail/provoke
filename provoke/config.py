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

"""This module defines a set of routines for gathering needed information from
a config file.

"""

from __future__ import absolute_import

import re
import resource
from ast import literal_eval
from six.moves.configparser import NoOptionError, NoSectionError

__all__ = ['Configuration']


_COMMA_DELIMITER = re.compile(r',\s*')


class Configuration(object):
    """This class streamlines the process of reading data from config files.

    :param config: The config parser object.
    :type config: :py:class:`~ConfigParser.SafeConfigParser`

    """

    def __init__(self, config):
        super(Configuration, self).__init__()
        self._config = config

    def _from_config(self, in_dict, section, name, dict_key=None,
                     opt_type='str'):
        config = self._config
        if dict_key is None:
            dict_key = name
        try:
            if opt_type == 'int':
                in_dict[dict_key] = config.getint(section, name)
            elif opt_type == 'float':
                in_dict[dict_key] = config.getfloat(section, name)
            elif opt_type == 'bool':
                in_dict[dict_key] = config.getboolean(section, name)
            elif opt_type == 'list':
                values = config.get(section, name)
                in_dict[dict_key] = re.split(_COMMA_DELIMITER, values)
            else:
                in_dict[dict_key] = config.get(section, name)
        except (NoOptionError, NoSectionError):
            pass

    def configure_mysql(self):
        """Searches for config sections prefixed with ``mysql:`` and loads them
        into the global dictionary of MySQL databases.

        The section name after the prefix will be the name of the database for
        later use.

        """
        try:
            from .mysql import MySQLConnection
        except ImportError:
            return
        MySQLConnection.reset_connection_params()
        section_prefix = 'mysql:'
        for section in self._config.sections():
            if section.startswith(section_prefix):
                db_name = section[len(section_prefix):]
                params = {}
                self._from_config(params, section, 'user')
                self._from_config(params, section, 'password')
                self._from_config(params, section, 'host')
                self._from_config(params, section, 'port', opt_type='int')
                self._from_config(params, section, 'database')
                self._from_config(params, section, 'charset')
                self._from_config(params, section, 'unix_socket')
                self._from_config(params, section, 'connect_timeout',
                                  opt_type='int')
                MySQLConnection.set_connection_params(db_name, **params)

    def configure_http(self):
        """Searches for config sections prefixed with ``http:`` and loads them
        into the global dictionary of HTTP endpoints.

        The section name after the prefix will be the name of the endpoint for
        later use.

        """
        try:
            from .http import HttpConnection
        except ImportError:
            return
        HttpConnection.reset_connection_params()
        section_prefix = 'http:'
        for section in self._config.sections():
            if section.startswith(section_prefix):
                name = section[len(section_prefix):]
                params = {}
                self._from_config(params, section, 'host')
                self._from_config(params, section, 'port', opt_type='int')
                self._from_config(params, section, 'user')
                self._from_config(params, section, 'password')
                self._from_config(params, section, 'timeout', opt_type='int')
                self._from_config(params, section, 'ssl', opt_type='bool')
                self._from_config(params, section, 'key_file')
                self._from_config(params, section, 'cert_file')
                HttpConnection.set_connection_params(name, **params)

    def configure_amqp(self):
        """Loads AMQP connection information from the config section ``amqp``.
        This connection information will be available globally.

        If this section does not exist, or if this method is not called, the
        default connection parameters will look for a server running on
        localhost.

        """
        try:
            from .amqp import AmqpConnection
        except ImportError:
            return
        AmqpConnection.set_connection_params()
        section = 'amqp'
        if self._config.has_section(section):
            params = {}
            self._from_config(params, section, 'host')
            self._from_config(params, section, 'port', opt_type='int')
            self._from_config(params, section, 'user', dict_key='userid')
            self._from_config(params, section, 'password')
            self._from_config(params, section, 'virtual_host')
            self._from_config(params, section, 'heartbeat', opt_type='float')
            self._from_config(params, section, 'connect_timeout',
                              opt_type='float')
            AmqpConnection.set_connection_params(**params)

    def configure_taskgroups(self):
        """Searches for sections prefixed with ``taskgroup:`` and loads them
        into the global dictionary of application taskgroups.

        """
        try:
            from .app import WorkerApplication
        except ImportError:
            return
        WorkerApplication.reset_taskgroups()
        section_prefix = 'taskgroup:'
        for section in self._config.sections():
            if section.startswith(section_prefix):
                tg_name = section[len(section_prefix):]
                params = {'exchange': '',
                          'routing_key': None}
                self._from_config(params, section, 'queue')
                self._from_config(params, section, 'routing_key')
                self._from_config(params, section, 'exchange')
                if 'queue' in params:
                    params['exchange'] = ''
                    params['routing_key'] = params.pop('queue')
                WorkerApplication.declare_taskgroup(tg_name, **params)

    def get_worker_master(self, section='daemon'):
        try:
            return self._config.get(section, 'master')
        except (NoOptionError, NoSectionError):
            pass

    def get_rlimits(self, section='daemon'):
        try:
            max_fd = self._config.getint(section, 'max-fd')
            yield resource.RLIMIT_NOFILE, (max_fd, max_fd)
        except (NoOptionError, NoSectionError):
            pass

    def get_pidfile(self, section='daemon'):
        try:
            return self._config.get(section, 'pidfile')
        except (NoOptionError, NoSectionError):
            pass

    def get_stdio_redirects(self, section='daemon'):
        info = {}
        self._from_config(info, section, 'stdout')
        self._from_config(info, section, 'stderr')
        self._from_config(info, section, 'stdin')
        return info.get('stdout'), info.get('stderr'), info.get('stdin')

    def get_worker_privileges(self, section='daemon'):
        info = {}
        self._from_config(info, section, 'user')
        self._from_config(info, section, 'group')
        self._from_config(info, section, 'umask')
        if 'umask' in info:
            info['umask'] = literal_eval(info['umask'])
        return info.get('user'), info.get('group'), info.get('umask')
