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

"""Loads plugins defining worker processes and task functions. Starts and
manages the pool of worker processes to execute tasks as the come in.

"""

from __future__ import absolute_import, print_function

import sys
import time
import logging
import signal
import resource
from optparse import OptionParser

from pkg_resources import iter_entry_points
from six.moves.configparser import SafeConfigParser

from ..config import Configuration
from ..app import WorkerApplication
from . import system, WorkerMaster


logger = logging.getLogger('provoke.main')


class BadPlugin(Exception):
    pass


class ReloadSignal(Exception):
    pass


def handle_signals():
    def exit_sig(signum, frame):
        sys.exit(0)

    def reload_sig(signum, frame):
        raise ReloadSignal()
    signal.signal(signal.SIGTERM, exit_sig)
    signal.signal(signal.SIGHUP, reload_sig)


def start_master(options, plugins):
    cfgparser = SafeConfigParser()
    cfgparser.read(options.config)
    cfg = Configuration(cfgparser)

    pidfile = None
    user, group, umask = None, None, None

    def process_init():
        system.drop_privileges(user, group, umask)
        time.sleep(0.1)

    app = WorkerApplication()
    master = WorkerMaster(app, process_callback=process_init)

    cfg.configure_logging()
    cfg.configure_taskgroups()
    cfg.configure_amqp()
    cfg.configure_mysql()

    for plugin in plugins:
        for entry_point in iter_entry_points('provoke.workers', plugin):
            logger.debug('Loading plugin: name=%s', entry_point.name)
            register = entry_point.load()
            register(app, master, cfg)
            break
        else:
            raise BadPlugin(plugin)

    for res, limits in cfg.get_rlimits():
        resource.setrlimit(res, limits)

    if options.daemon:
        logger.debug('Daemonizing master process')
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
    usage = 'Usage: %prog [options] <plugins>'
    parser = OptionParser(description=__doc__, usage=usage)
    parser.add_option('--config', action='append', metavar='PATH', default=[],
                      help='Configuration file')
    parser.add_option('--daemon', action='store_true', default=False,
                      help='Daemonize the master process')
    parser.add_option('-l', '--list-plugins', action='store_true',
                      default=False, help='List all known plugins')

    options, plugins = parser.parse_args()

    if options.list_plugins:
        for entry_point in iter_entry_points('provoke.workers'):
            print(entry_point.name)
        return
    elif not plugins:
        parser.error('One or more plugins required')

    while True:
        try:
            start_master(options, plugins)
        except BadPlugin as exc:
            parser.error('Unrecognized plugin name: ' + str(exc))
        except ReloadSignal:
            pass
        except (SystemExit, KeyboardInterrupt):
            break
