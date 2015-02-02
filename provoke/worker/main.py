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

"""Implements a possible entrypoint for instantiating a worker service that
reads necessary information from configuration.

"""

from __future__ import absolute_import

import sys
import signal
import resource
from optparse import OptionParser
from six.moves.configparser import SafeConfigParser

from ..config import Configuration
from .. import import_attr
from . import system


class ReloadSignal(Exception):
    pass


def handle_signals():
    def exit_sig(signum, frame):
        sys.exit(0)

    def reload_sig(signum, frame):
        raise ReloadSignal()
    signal.signal(signal.SIGTERM, exit_sig)
    signal.signal(signal.SIGHUP, reload_sig)


def start_master():
    parser = OptionParser()
    parser.add_option('--config', action='append',
                      help='Configuration file')
    parser.add_option('--daemon', action='store_true', default=False,
                      help='Daemonize the master process.')
    parser.add_option('--worker-master', metavar='WHERE',
                      help='Attempt to load the worker master from WHERE, '
                      'e.g. someapp.worker:master')

    options, extra = parser.parse_args()

    cfgparser = SafeConfigParser()
    try:
        if not cfgparser.read(options.config):
            raise TypeError
    except TypeError:
        pass

    cfg = Configuration(cfgparser)
    cfg.configure_taskgroups()
    cfg.configure_amqp()
    cfg.configure_mysql()

    try:
        what = options.worker_master or cfg.get_worker_master()
        master = import_attr(what, 'master')
    except (ImportError, AttributeError, ValueError):
        parser.error('Could not find importable worker master.')

    pidfile = None
    user, group, umask = None, None, None

    def process_init():
        system.drop_privileges(user, group, umask)

    master._internal_process_callback = process_init

    for res, limits in cfg.get_rlimits():
        resource.setrlimit(res, limits)

    if options.daemon:
        pidfile = cfg.get_pidfile()
        stdout, stderr, stdin = cfg.get_stdio_redirects()
        user, group, umask = cfg.get_worker_privileges()
        if stdout or stderr or stdin:
            system.redirect_stdio(stdout, stderr, stdin)
        system.daemonize()
    with system.PidFile(pidfile):
        handle_signals()
        try:
            master.run()
        finally:
            master.wait()


def main():
    while True:
        try:
            start_master()
        except ReloadSignal:
            pass
        except (SystemExit, KeyboardInterrupt):
            break
