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

"""Worker master module. Provides a system that maintains a set of worker
processes that consume from task queues. If a worker process dies for any
reason, a new one is created.

"""

from __future__ import absolute_import

import os
import time
import json
import logging
import errno
import signal
import traceback
import threading
from functools import partial

from six.moves import cPickle
import amqp
from amqp.exceptions import AccessRefused

from ..amqp import AmqpConnection

__all__ = ['WorkerMaster', 'DiscardTask', 'RequeueTask',
           'get_worker_data', 'get_worker_app']


_current_worker_data = None
_current_worker_app = None

logger = logging.getLogger('provoke.worker')


def get_worker_data(key, default=None):
    """This function, when called from within tasks running in worker
    processes, will get values from the ``worker_data`` dictionary passed in to
    the :class:`WorkerMaster` constructor.

    :param key: The key to retrieve from the ``worker_data`` dictionary.
    :param default: This value is returned when ``key`` cannot be found in
                    ``worker_data``.
    :returns: The value stored for ``key`` in the ``worker_data`` dictionary.
    :raises: RuntimeError

    """
    if _current_worker_data:
        return _current_worker_data.get(key, default)
    else:
        raise RuntimeError('Must be called from worker process')


def get_worker_app():
    """This function, when called from within tasks running in worker
    processes, will get the :class:`~provoke.app.WorkerApplication` object that
    is executing the task.

    :rtype: :class:`provoke.app.WorkerApplication`
    :raises: RuntimeError

    """
    if _current_worker_app:
        return _current_worker_app
    else:
        raise RuntimeError('Must be called from worker process')


class DiscardTask(Exception):
    """May be raised from the callback that runs before each task to indicate
    that the task should be discarded without execution. The task is
    acknowledged to the AMQP server as if it were successfully processed.

    The ``return_callback`` that runs after task execution will not be called
    if the task is discarded by raising this exception.

    """
    pass


class RequeueTask(Exception):
    """May be called from inside task functions to indicate that the AMQP
    message should be rejected but requeued immediately. This should
    effectively initiate a retry of the task.

    To prevent an infinite loop of requeuing, tasks should use a guard against
    requeuing messages that have already been requeued::

        if not get_worker_data('redelivered'):
            raise RequeueTask()

    """
    pass


class _WorkerProcess(object):

    def __init__(self, app, queues, limit=None, process_callback=None,
                 task_callback=None, return_callback=None, exclusive=False):
        super(_WorkerProcess, self).__init__()
        self.app = app
        self.queues = queues
        self.limit = limit
        self.process_callback = process_callback
        self.task_callback = task_callback
        self.return_callback = return_callback
        self.exclusive = exclusive
        self.active_connection = None
        self.pid = None

    def _send_result(self, channel, reply_to, body):
        result_raw = json.dumps(body)
        msg = amqp.Message(result_raw, content_type='application/json')
        channel.basic_publish(msg, exchange='', routing_key=reply_to)

    def _handle_message(self, channel, msg):
        body = json.loads(msg.body)
        task_name = body['task']
        task_args = body.get('args', [])
        task_kwargs = body.get('kwargs')
        task_id = getattr(msg, 'correlation_id', None)
        reply_to = getattr(msg, 'reply_to', None)
        call = getattr(self.app.tasks, task_name)
        skip = False
        _current_worker_data['correlation_id'] = task_id
        _current_worker_data['redelivered'] = \
            msg.delivery_info.get('redelivered')
        if self.task_callback:
            try:
                self.task_callback(task_name, task_args, task_kwargs)
            except DiscardTask:
                skip = True
        if not skip:
            try:
                ret = call.apply(task_args, task_kwargs, task_id)
            except Exception as exc:
                if reply_to:
                    body['exception'] = {'value': cPickle.dumps(exc),
                                         'traceback': traceback.format_exc()}
                    self._send_result(channel, reply_to, body)
                if self.return_callback:
                    self.return_callback(task_name, None)
                raise
            else:
                if reply_to:
                    body['return'] = ret
                    self._send_result(channel, reply_to, body)
                if self.return_callback:
                    self.return_callback(task_name, ret)

    def _on_message(self, channel, msg):
        try:
            self._handle_message(channel, msg)
        except (SystemExit, KeyboardInterrupt):
            channel.basic_reject(msg.delivery_tag, requeue=True)
            raise
        except RequeueTask:
            channel.basic_reject(msg.delivery_tag, requeue=True)
        except Exception:
            channel.basic_reject(msg.delivery_tag, requeue=False)
            raise
        else:
            channel.basic_ack(msg.delivery_tag)
        finally:
            self.counter += 1
            if self.limit and self.counter >= self.limit:
                self._cancel_consumers(channel)

    def _cancel_consumers(self, channel):
        for consumer_tag in list(channel.callbacks.keys()):
            channel.basic_cancel(consumer_tag)

    def _consume(self):
        self.counter = 0

        with AmqpConnection() as channel:
            self.active_connection = channel.connection
            callback = partial(self._on_message, channel)
            channel.basic_qos(0, 1, False)
            for queue_name in self.queues:
                channel.basic_consume(queue=queue_name,
                                      consumer_tag=queue_name,
                                      callback=callback,
                                      exclusive=self.exclusive)
            logger.info('Accepting jobs: queues=%s', repr(self.queues))
            while channel.callbacks:
                channel.connection.drain_events()

    def _try_consuming(self):
        while True:
            try:
                self._consume()
                return
            except AccessRefused:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.exception('Queue access refused')
                time.sleep(5.0)

    def _send_heartbeats(self):
        while True:
            try:
                interval = self.active_connection.heartbeat / 2.0
                assert interval > 0.0
                time.sleep(interval)
                self.active_connection.send_heartbeat()
            except Exception:
                time.sleep(1.0)

    def _start_heartbeat_thread(self):
        thread = threading.Thread(target=self._send_heartbeats)
        thread.daemon = True
        thread.start()

    def _run(self):
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        signal.signal(signal.SIGHUP, signal.SIG_DFL)
        self._start_heartbeat_thread()
        try:
            self._try_consuming()
        except (SystemExit, KeyboardInterrupt):
            pass
        except Exception:
            logger.exception('Unhandled exception in worker process')
            traceback.print_exc()
            raise


class WorkerMaster(object):
    """Manages child processes that execute application workers. These workers
    may be listening on one or many queues.

    :param start_callback: This function is called in the master process every
                           time a new worker process is started. This callback
                           is given three parameters, the
                           :class:`~provoke.app.WorkerApplication`, a list of
                           queues consumed by the new process, and the PID of
                           the new process.
    :type start_callback: collections.Callable
    :param exit_callback: This function is called in the master process every
                          time a worker process exits for any reason. It is
                          passed in the same arguments as ``start_callback``
                          plus the exit status integer.
    :type exit_callback: collections.Callable
    :param process_callback: This function is called with no arguments in new
                             child processes before any tasks are executed.
    :type process_callback: collections.Callable
    :param worker_data: Arbitrary data may be made available to workers with
                        this dictionary. Tasks running in worker processes may
                        use
                        :func:`~provoke.worker.master.get_worker_data` to
                        access copies of this dictionary.
    :type worker_data: dict

    """

    def __init__(self, start_callback=None, exit_callback=None,
                 process_callback=None, worker_data=None):
        super(WorkerMaster, self).__init__()
        self._start_callback = start_callback
        self._exit_callback = exit_callback
        self._process_callback = process_callback
        self._worker_data = worker_data or {}
        self.workers = []

    def start_callback(self, worker):
        logger.info('Process started: pid=%s, queues=%s',
                    worker.pid, repr(worker.queues))
        if self._start_callback:
            try:
                self._start_callback(worker.queues, worker.pid)
            except Exception:
                pass

    def exit_callback(self, worker, status):
        logger.info('Process exited: pid=%s, status=%s, queues=%s',
                    worker.pid, status, repr(worker.queues))
        if self._exit_callback:
            try:
                self._exit_callback(worker.queues, worker.pid, status)
            except Exception:
                pass

    def process_callbacks(self, worker):
        if self._process_callback:
            self._process_callback()
        if worker.process_callback:
            worker.process_callback()

    def add_worker(self, app, queues, num_processes=1, task_limit=10,
                   process_callback=None, task_callback=None,
                   return_callback=None, exclusive=False):
        """Adds a new worker process to be managed by the :meth:`.run` method.

        :param app: The application backend that knows how to enqueue and
                    execute tasks.
        :type app: :class:`~provoke.app.WorkerApplication`
        :param queues: List of queue names to consume task execution messages
                       from.
        :type queues: list
        :param num_processes: The number of processes to maintain for this
                              worker.
        :type num_processes: int
        :param task_limit: The number of tasks a worker process may execute
                           before it exits and is replaced by a new worker
                           process.
        :type task_limit: int
        :param process_callback: This function is called with no arguments in
                                 new child processes before any tasks are
                                 executed.
        :type process_callback: collections.Callable
        :param task_callback: This function is called inside the child process
                              every time a task is ready for execution. This
                              function is given four arguments, the
                              :class:`~provoke.app.WorkerApplication`, the task
                              name, and the positional and keyword arguments of
                              the task.
        :type task_callback: collections.Callable
        :param return_callback: Like ``task_callback`` but called when the
                                task is completed. This function is given three
                                arguments, the
                                :class:`~provoke.app.WorkerApplication`, the
                                task name, and the return value. If an
                                exception was raised during execution, it will
                                be available in :func:`sys.exc_info`.
        :type return_callback: collections.Callable
        :param exclusive: If True, the worker process will request
                          exclusive access to consume the queues. Additional
                          consume requests on the queue by other workers will
                          raise errors. Only makes sense to use one process!
        :type exclusive: bool

        """
        for i in range(num_processes):
            worker = _WorkerProcess(app, queues, task_limit, process_callback,
                                    task_callback, return_callback, exclusive)
            self.workers += [worker]

    def _check_workers(self):
        try:
            pid, status = os.waitpid(0, 0)
        except OSError as exc:
            if exc.errno == errno.ECHILD:
                for worker in self.workers:
                    self.exit_callback(worker, None)
                    worker.pid = None
                return False
            raise
        for worker in self.workers:
            exit_status = os.WEXITSTATUS(status)
            if pid == worker.pid:
                self.exit_callback(worker, exit_status)
                worker.pid = None
                return True
        raise Exception('Received exit status for unknown process: '+pid)

    def _start_worker(self, worker):
        pid = os.fork()
        if pid == 0:
            global _current_worker_data, _current_worker_app
            _current_worker_data = self._worker_data.copy()
            _current_worker_app = worker.app
            self.process_callbacks(worker)
            try:
                worker._run()
            except Exception:
                os._exit(1)
            finally:
                os._exit(0)
        else:
            return pid

    def _restart_workers(self):
        for worker in self.workers:
            if worker.pid is None:
                worker.pid = self._start_worker(worker)
                self.start_callback(worker)

    def _stop_workers(self):
        for worker in self.workers:
            if worker.pid is not None:
                try:
                    os.kill(worker.pid, signal.SIGTERM)
                except OSError as exc:
                    if exc.errno != errno.ESRCH:
                        raise

    def wait(self):
        """This method may be used after :meth:`.run` to wait for killed worker
        processes to finish and return an exit status. This is necessary for
        ensuring ``exit_callback`` is called appropriately::

            try:
                master.run()
            finally:
                master.wait()

        """
        for worker in self.workers:
            if worker.pid is not None:
                try:
                    pid, status = os.waitpid(worker.pid, 0)
                except OSError as exc:
                    if exc.errno in (errno.ECHILD, errno.ESRCH):
                        status = None
                    else:
                        raise
                self.exit_callback(worker, status)
                worker.pid = None

    def run(self):
        """Starts all worker processes, and as they exit they are restarted.
        When this function exits for any reason (e.g. ``KeyboardInterrupt``),
        ``SIGTERM`` is sent to each worker process.

        """
        try:
            while True:
                self._restart_workers()
                self._check_workers()
        finally:
            self._stop_workers()
