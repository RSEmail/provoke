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

"""Common logging functions.

"""

from __future__ import absolute_import

import sys
import json
import logging
import logging.handlers

__all__ = ['setup_logging', 'log_debug', 'log_info', 'log_warning',
           'log_error', 'log_exception']


def setup_logging(debug=True, syslog_address='/dev/log',
                  syslog_facility='user'):
    """Sets up the :py:mod:`logging` system with full debug logging enabled to
    standard output and more fine-grained logging to syslog.

    :param debug: Use debug-level logging for all handlers.
    :type debug: bool
    :param syslog_address: The socket address for syslog messages.
    :type syslog_address: Unix socket string or ``(host, port)`` tuple.
    :param syslog_facility: The facility to send syslog messages to.
    :type syslog_facility: str

    """
    root = logging.getLogger('')
    root.setLevel(logging.DEBUG)
    _setup_stdout_logging(debug)
    _setup_syslog_logging(debug, syslog_address, syslog_facility)


def _setup_stdout_logging(debug):
    format = '%(asctime)-26s %(levelname)-8s %(name)-14s %(message)s'
    log = logging.getLogger('')
    level = logging.DEBUG if debug else logging.WARNING
    stdout = logging.StreamHandler(sys.stdout)
    stdout.setLevel(level)
    stdout.setFormatter(logging.Formatter(format))
    log.addHandler(stdout)


def _setup_syslog_logging(debug, address, facility):
    format = '%(asctime)s %(levelname)s %(process)d %(name)s %(message)s'
    log = logging.getLogger('')
    level = logging.DEBUG if debug else logging.INFO
    syslog = logging.handlers.SysLogHandler(address=address, facility=facility)
    syslog.setLevel(level)
    syslog.setFormatter(logging.Formatter(format))
    log.addHandler(syslog)


def _log_generic(logger, msg, info, level):
    logger = 'provoke.'+logger if logger else 'provoke'
    log = logging.getLogger(logger)
    info_str = json.dumps(info)
    log_message = '{0}: {1}'.format(msg, info_str)
    getattr(log, level)(log_message)


def log_debug(msg, logger='', **info):
    """Logs a debug message using a standard format.

    :param msg: A simple message describing the log event.
    :type msg: str
    :param logger: The name of the logger used for the log event.
    :type logger: str
    :param info: Arbitrary metadata associated with the log event.
    :type info: keyword arguments

    """
    _log_generic(logger, msg, info, 'debug')


def log_info(msg, logger='', **info):
    """Logs an info message using a standard format.

    :param msg: A simple message describing the log event.
    :type msg: str
    :param logger: The name of the logger used for the log event.
    :type logger: str
    :param info: Arbitrary metadata associated with the log event.
    :type info: keyword arguments

    """
    _log_generic(logger, msg, info, 'info')


def log_warning(msg, logger='', **info):
    """Logs a warning message using a standard format.

    :param msg: A simple message describing the log event.
    :type msg: str
    :param logger: The name of the logger used for the log event.
    :type logger: str
    :param info: Arbitrary metadata associated with the log event.
    :type info: keyword arguments

    """
    _log_generic(logger, msg, info, 'warning')


def log_error(msg, logger='', **info):
    """Logs an error message using a standard format.

    :param msg: A simple message describing the log event.
    :type msg: str
    :param logger: The name of the logger used for the log event.
    :type logger: str
    :param info: Arbitrary metadata associated with the log event.
    :type info: keyword arguments

    """
    _log_generic(logger, msg, info, 'error')


def log_exception(msg='Unhandled exception occurred'):
    """Logs an exception with full traceback information to the root logger,
    along with an error log in the standard format to syslog.

    :param msg: Message associated with the exception.
    :type msg: str

    """
    type, value, traceback = sys.exc_info()
    log_error(msg, type=type.__name__, value=str(value))
