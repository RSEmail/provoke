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
    if not _current_worker_data:
        raise RuntimeError('Must be called from worker process')
    return _current_worker_data.get(key, default)


def get_worker_app():
    """This function, when called from within tasks running in worker
    processes, will get the :class:`~provoke.app.WorkerApplication` object that
    is executing the task.

    :rtype: :class:`provoke.app.WorkerApplication`
    :raises: RuntimeError

    """
    if not _current_worker_app:
        raise RuntimeError('Must be called from worker process')
    return _current_worker_app


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


def _run_cb(obj, name, *args, **kwargs):
    try:
        callback = getattr(obj, name)
    except AttributeError:
        return
    try:
        callback(*args, **kwargs)
    except (DiscardTask, AssertionError, SystemExit, KeyboardInterrupt):
        raise
    except Exception:
        logger.exception('Unhandled exception in callback')


class WorkerProcess(object):
    """The default class for managing worker processes executed by the
    :class:`WorkerMaster`. Override this class to implement callbacks and pass
    the new class as the ``worker_class`` parameter to
    :meth:`~WorkerMaster.add_worker`.

    .. method:: start_callback()

       Called within the child process when it is first created, before any
       tasks are executed.

    .. method:: exit_callback()

       Called within the child process immediately before the process exits for
       any reason. You can determine if the process died from an exception by
       calling :func:`sys.exc_info`.

    .. method:: task_callback(name, args, kwargs)

       Called within the child process before executing each task.

       :param str name: Name of the task to be executed.
       :param list args: List of positional arguments for the task.
       :param dict kwargs: Dictionary of keyword arguments for the task.

    .. method:: return_callback(name, ret)

       Called within the child process after executing a task, whether success
       or failure.

       :param str name: Name of the task that was executed.
       :param ret: Return value of the task function. If this is ``None``,
                   check :func:`sys.exc_info` to see if the task function
                   raised an exception.

    """

    def __init__(self, app, queues, limit=None, exclusive=False):
        super(WorkerProcess, self).__init__()
        self.app = app
        self.queues = queues
        self.limit = limit
        self.exclusive = exclusive
        self.active_connection = None
        self.pid = None

    def _send_result(self, channel, task_id, reply_to, body):
        result_raw = json.dumps(body)
        msg = amqp.Message(result_raw, content_type='application/json',
                           correlation_id=task_id)
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
        try:
            _run_cb(self, 'task_callback', task_name, task_args,
                    task_kwargs)
        except DiscardTask:
            skip = True
        if not skip:
            try:
                ret = call.apply(task_args, task_kwargs, task_id)
            except Exception as exc:
                if reply_to:
                    body['exception'] = {'value': cPickle.dumps(exc),
                                         'traceback': traceback.format_exc()}
                    self._send_result(channel, task_id, reply_to, body)
                _run_cb(self, 'return_callback', task_name, None)
                raise
            else:
                if reply_to:
                    body['return'] = ret
                    self._send_result(channel, task_id, reply_to, body)
                _run_cb(self, 'return_callback', task_name, ret)

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


class _LocalProcess(object):

    def __init__(self, func):
        super(_LocalProcess, self).__init__()
        self._func = func
        self.queues = None
        self.app = None
        self.pid = None

    def _run(self):
        try:
            return self._func()
        except (SystemExit, KeyboardInterrupt):
            pass
        except Exception:
            logger.exception('Unhandled exception in worker process')
            traceback.print_exc()
            raise


class WorkerMaster(object):
    """Manages child processes that execute application workers. These workers
    may be listening on one or many queues.

    Override this class to implement its callback methods.

    :param worker_data: Arbitrary data may be made available to workers with
                        this dictionary. Tasks running in worker processes may
                        use
                        :func:`~provoke.worker.master.get_worker_data` to
                        access copies of this dictionary.
    :type worker_data: dict

    .. method:: start_callback(app, queues, pid)

       Called in the master process every time a new worker process is started.

       :param app: The application the worker is running tasks for.
       :type app: :class:`~provoke.app.WorkerApplication`
       :param list queues: List of queues consumed by the new process.
       :param int pid: The PID of the new process.

    .. method:: exit_callback(app, queues, pid, status)

       Called in the master process every time a worker process exits for any
       reason.

       :param app: The application the worker was running tasks for.
       :type app: :class:`~provoke.app.WorkerApplication`
       :param list queues: List of queues consumed by the dead process.
       :param int pid: The former PID of the dead process.
       :param int status: The exit status of the dead process.

    .. method:: process_callback()

       Called inside the new worker process, before it executes any tasks.

    """

    def __init__(self, worker_data=None):
        super(WorkerMaster, self).__init__()
        self._worker_data = worker_data or {}
        self.workers = []

    def _start_callback(self, worker):
        logger.info('Process started: pid=%s, queues=%s',
                    worker.pid, repr(worker.queues))
        _run_cb(self, 'start_callback', worker.queues, worker.pid)

    def _exit_callback(self, worker, status):
        logger.info('Process exited: pid=%s, status=%s, queues=%s',
                    worker.pid, status, repr(worker.queues))
        _run_cb(self, 'exit_callback', worker.queues, worker.pid, status)

    def add_worker(self, app, queues, num_processes=1, task_limit=10,
                   exclusive=False, worker_class=WorkerProcess):
        """Adds a new worker to be managed by the :meth:`.run` method.

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
        :param exclusive: If True, the worker process will request
                          exclusive access to consume the queues. Additional
                          consume requests on the queue by other workers will
                          raise errors. Only makes sense to use one process!
        :type exclusive: bool
        :param worker_class: Class to use when managing worker processes. See
                             :class:`WorkerProcess` for details.

        """
        for i in range(num_processes):
            worker = worker_class(app, queues, task_limit, exclusive)
            self.workers += [worker]

    def add_local_worker(self, func, num_processes=1, args=None, kwargs=None):
        """Adds a new worker to be managed by the :meth:`.run` method. A local
        worker can be used to perform any additional processing that should be
        managed and restarted.

        :param func: The function to execute inside the worker.
        :type func: collections.Callable
        :param num_processes: The number of processes to maintain for this
                              worker.
        :type num_processes: int
        :param args: Positional arguments to the function.
        :type args: tuple
        :param kwargs: Keyword arguments to the function.
        :type kwargs: dict

        """
        func_partial = partial(func, *(args or ()), **(kwargs or {}))
        for i in range(num_processes):
            worker = _LocalProcess(func_partial)
            self.workers += [worker]

    def _check_workers(self):
        try:
            pid, status = os.waitpid(0, 0)
        except OSError as exc:
            if exc.errno == errno.ECHILD:
                for worker in self.workers:
                    self._exit_callback(worker, None)
                    worker.pid = None
                return False
            raise
        for worker in self.workers:
            exit_status = os.WEXITSTATUS(status)
            if pid == worker.pid:
                self._exit_callback(worker, exit_status)
                worker.pid = None
                return True
        msg = 'Received exit status for unknown process: {0!s}'.format(pid)
        raise Exception(msg)

    def _start_worker(self, worker):
        pid = os.fork()
        if pid == 0:
            global _current_worker_data, _current_worker_app
            _current_worker_data = self._worker_data.copy()
            _current_worker_app = worker.app
            _run_cb(self, 'process_callback')
            _run_cb(self, 'start_callback')
            _run_cb(worker, 'start_callback')
            try:
                worker._run()
            except Exception:
                _run_cb(worker, 'exit_callback')
                os._exit(1)
            else:
                _run_cb(worker, 'exit_callback')
                os._exit(0)
        else:
            return pid

    def _restart_workers(self):
        for worker in self.workers:
            if worker.pid is None:
                worker.pid = self._start_worker(worker)
                self._start_callback(worker)

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
                self._exit_callback(worker, status)
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
