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

import os
import sys
import time
import signal
import resource
from optparse import OptionParser, OptionGroup

from provoke.common.app import WorkerApplication
from provoke.common.config import load_configuration, read_configuration_dir
from provoke.common.logging import setup_logging
from provoke.worker.master import WorkerMaster
from provoke.worker import system


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
    default_config_dir = os.getenv('PROVOKE_CONFIG_DIR', '/etc/provoke')

    parser = OptionParser()
    parser.add_option('--debug', action='store_true',
                      help='Debug-level logging')
    parser.add_option('--config-dir', default=default_config_dir,
                      help='Configuration directory')
    parser.add_option('--limit', type='int', help='Worker task limit')
    parser.add_option('--daemon', action='store_true', default=False,
                      help='Daemonize the master process.')

    options, extra = parser.parse_args()

    configparser = read_configuration_dir(options.config_dir)
    config = load_configuration(configparser, options)

    setup_logging(debug=options.debug, syslog_facility='local6')

    app = WorkerApplication()

    pidfile = None
    user, group, umask = None, None, None

    def worker_enter(app, queues):
        system.drop_privileges(user, group, umask)
        time.sleep(1.0)

    master = WorkerMaster(worker_limit=options.limit)
    for worker in config.get_workers():
        exclusive = worker.get('exclusive', False)
        master.add_worker(app, worker['queues'], worker['processes'],
                          worker_enter, exclusive=exclusive)

    for res, limits in config.get_rlimits():
        resource.setrlimit(res, limits)

    if options.daemon:
        pidfile = config.get_pidfile()
        stdout, stderr, stdin = config.get_stdio_redirects()
        user, group, umask = config.get_worker_privileges()
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
