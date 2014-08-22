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

from __future__ import absolute_import

import re
import os
import os.path
import resource
from ast import literal_eval
from collections import defaultdict
from ConfigParser import SafeConfigParser, NoOptionError, NoSectionError

__all__ = ['load_configuration', 'read_configuration_dir']


_COMMA_DELIMITER = re.compile(r',\s*')


class Configuration(object):

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

    def _configure_mysql(self):
        try:
            from .mysql import MySQLConnection
        except ImportError:
            return
        section_prefix = 'mysql:'
        for section in self._config.sections():
            if section.startswith(section_prefix):
                db_name = section[len(section_prefix):]
                params = {}
                self._from_config(params, section, 'user')
                self._from_config(params, section, 'password',
                                  dict_key='passwd')
                self._from_config(params, section, 'host')
                self._from_config(params, section, 'port', opt_type='int')
                self._from_config(params, section, 'database', dict_key='db')
                self._from_config(params, section, 'charset')
                self._from_config(params, section, 'unix_socket')
                self._from_config(params, section, 'connect_timeout',
                                  opt_type='int')
                MySQLConnection.set_connection_params(db_name, **params)

    def _configure_amqp(self):
        try:
            from .amqp import AmqpConnection
        except ImportError:
            return
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

    def _configure_taskgroups(self):
        try:
            from .app import WorkerApplication
        except ImportError:
            return
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

    def get_workers(self):
        workers = []
        section_prefix = 'worker:'
        for section in self._config.sections():
            if section.startswith(section_prefix):
                params = {}
                self._from_config(params, section, 'queues', opt_type='list')
                self._from_config(params, section, 'processes', opt_type='int',
                                  dict_key='num_processes')
                self._from_config(params, section, 'task_limit',
                                  opt_type='int')
                self._from_config(params, section, 'exclusive', 
                                  opt_type='bool')
                queues = params.pop('queues', [])
                workers.append((queues, params))
        return workers

    def get_rlimits(self):
        try:
            max_fd = self._config.getint('daemon', 'max-fd')
            yield resource.RLIMIT_NOFILE, (max_fd, max_fd)
        except (NoOptionError, NoSectionError):
            pass

    def get_pidfile(self):
        try:
            return self._config.get('daemon', 'pidfile')
        except (NoOptionError, NoSectionError):
            pass

    def get_stdio_redirects(self):
        section = 'daemon'
        info = {}
        self._from_config(info, section, 'stdout')
        self._from_config(info, section, 'stderr')
        self._from_config(info, section, 'stdin')
        return info.get('stdout'), info.get('stderr'), info.get('stdin')

    def get_worker_privileges(self):
        section = 'daemon'
        info = {}
        self._from_config(info, section, 'user')
        self._from_config(info, section, 'group')
        self._from_config(info, section, 'umask')
        if 'umask' in info:
            info['umask'] = literal_eval(info['umask'])
        return info.get('user'), info.get('group'), info.get('umask')


def load_configuration(configparser):
    config = Configuration(configparser)
    config._configure_amqp()
    config._configure_mysql()
    config._configure_taskgroups()
    return config


def read_configuration_dir(directory, configparser=None, suffix='.conf'):
    configparser = configparser or SafeConfigParser()
    files = [os.path.join(directory, filename) for filename
             in os.listdir(directory) if filename.endswith(suffix)]
    configparser.read(files)
    return configparser
