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

import os
import sys
import time
import signal
import resource
from optparse import OptionParser

from ..common.app import WorkerApplication
from ..common.config import load_configuration, read_configuration_dir
from ..common.logging import setup_logging
from .master import WorkerMaster
from . import system

__all__ = ['WorkerMain']


class _ReloadSignal(Exception):
    pass


class WorkerMain(object):
    """This class controls the setup and daemonization of a task worker
    service.

    :param app: The application backend that knows how to enqueue and execute
                tasks.
    :type app: :class:`~provoke.common.app.WorkerApplication`
    :param config_dir: The directory containing configuration files to load.
    :type config_dir: str
    :param syslog_facility: The syslog facility use for worker logs.
    :type syslog_facility: str
    :param debug: Enable debug-level logging.
    :type debug: bool

    """

    def __init__(self, app, config_dir='/etc/provoke',
                 syslog_facility='local6', debug=False):
        super(WorkerMain, self).__init__()
        self.app = app
        self.config_dir = config_dir
        self.syslog_facility = syslog_facility
        self.debug = debug

        #: If set, this function will be called from the master process
        #: immediately after forking a child (worker) process, passing in a
        #: list of queues the worker will consume, and the PID of the worker
        #: process.
        self.start_callback = None

        #: If set, this function will be called from the master process
        #: immediately after a child (worker) process exits, passing in the
        #: list of queues the worker will consume, the PID of the worker
        #: process, and the process exit code.
        self.exit_callback = None

        #: If set, this function will be called from the worker process
        #: immediately after the process started, passing in the list of queues
        #: the worker will consume.
        self.worker_callback = None

        #: If set, this function will be called from the worker process
        #: immediately before each task is run, passing in the name of
        #: the task, the positional and the keyword arguments.
        self.task_callback = None

        #: If set, this function will be called from the worker process
        #: immediately after each task returns, passing in the name of the
        #: task, and the return value (or ``None``). If the task threw an
        #: exception, this callback will be called from inside its handler.
        self.task_return_callback = None

    def _handle_signals(self):
        def exit_sig(signum, frame):
            sys.exit(0)

        def reload_sig(signum, frame):
            raise _ReloadSignal()
        signal.signal(signal.SIGTERM, exit_sig)
        signal.signal(signal.SIGHUP, reload_sig)

    def _start_master(self, daemonize):
        configparser = read_configuration_dir(self.config_dir)
        config = load_configuration(configparser, options)

        setup_logging(debug=self.debug, syslog_facility='local6')

        pidfile = None
        user, group, umask = None, None, None

        def _worker_callback(*args, **kwargs):
            system.drop_privileges(user, group, umask)
            time.sleep(1.0)
            self.worker_callback(*arg, **kwargs)

        master = WorkerMaster(start_callback=self.start_callback,
                              exit_callback=self.exit_callback)
        for worker_queues, worker_params in config.get_workers():
            master.add_worker(self.app, worker_queues,
                              start_callback=_worker_callback,
                              task_callback=self.task_callback,
                              return_callback=self.task_return_callback,
                              **worker_params)

        for res, limits in config.get_rlimits():
            resource.setrlimit(res, limits)

        if daemonize:
            pidfile = config.get_pidfile()
            stdout, stderr, stdin = config.get_stdio_redirects()
            user, group, umask = config.get_worker_privileges()
            if stdout or stderr or stdin:
                system.redirect_stdio(stdout, stderr, stdin)
            system.daemonize()
        with system.PidFile(pidfile):
            self._handle_signals()
            try:
                master.run()
            finally:
                master.wait()

    def run(self, daemonize=False):
        """The main event loop. This method will only return gracefully if
        ``SystemExit`` or ``KeyboardInterrupt`` are raised.

        """
        while True:
            try:
                self._start_master(daemonize)
            except _ReloadSignal:
                pass
            except (SystemExit, KeyboardInterrupt):
                break
