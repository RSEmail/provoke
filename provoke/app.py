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

"""Common worker application module. Provides the interface with which
producers and executors see the task system.

.. class:: _TaskCaller

   This object represents a task that may be called synchronously in the
   current process, or asynchronously by a separate worker process.

   .. method:: delay(*args, **kwargs)

      Convenience method for calling :meth:`.apply_async` in a more
      traditional fashion.

      :param args: Positional arguments passed in to the task function.
      :type args: positional arguments
      :param kwargs: Keyword arguments passed in to the task function.
      :type kwargs: keyword arguments
      :returns: A way to retrieve the result of the task's execution when it
                is ready.
      :rtype: :class:`AsyncResult`

   .. method:: apply_async(args, kwargs=None, correlation_id=None,
                           routing_key=None, send_result=False,
                           result_queue=None)

      Triggers the asynchronous execution of a task by a separate worker
      process. Tasks will be executed in the order they are triggered, so a
      task may not be executed immediately.

      :param args: Positional arguments to be passed in to the task function.
      :type args: list
      :param kwargs: Keyword arguments to be passed in to the task function.
      :type kwargs: dict
      :param correlation_id: Force the message to use a specific correlation
                             identifier string.
      :type correlation_id: str
      :param routing_key: Override the default AMQP routing key for the task
                          with the given string.
      :type routing_key: str
      :param send_result: Request that the worker publish the task's result to
                          a result queue.
      :type send_result: bool
      :param result_queue: If ``send_result`` is True, use the given string as
                           the result queue name. By default a queue is created
                           based on the correlation ID string.
      :type result_queue: str
      :returns: If ``send_result`` is True, used retrieve the result of the
                task's execution when it is ready. Otherwise, returns ``None``.
      :rtype: :class:`AsyncResult`

   .. method:: apply(args, kwargs=None, correlation_id=None)

      Calls the task function synchronously in the current process, returning
      the result. This is equivalent to a normal function call, except
      start and finish events are logged.

      :param args: Arguments passed directly into the function.
      :type args: list
      :param kwargs: Arguments passed directly into the function.
      :type kwargs: dict
      :param correlation_id: The task being executed had this string as its
                             correlation identifier.
      :type correlation_id: str
      :returns: The return value of the function call.

   .. method:: __call__(*args, **kwargs)

      Equivalent to :meth:`.apply` passing in ``args`` and ``kwargs``.

      :param args: Arguments passed directly into the function.
      :type args: positional arguments
      :param kwargs: Arguments passed directly into the function.
      :type kwargs: keyword arguments
      :returns: The return value of the function call.

"""

from __future__ import absolute_import

import time
import json
import errno
import logging
from collections import deque
from uuid import uuid4
from socket import timeout as socket_timeout, error as socket_error
from multiprocessing import TimeoutError

from six.moves import cPickle
import amqp

from .amqp import AmqpConnection

__all__ = ['AsyncResult', 'WorkerApplication', 'routing_info']


logger = logging.getLogger('provoke.tasks')


class AsyncResult(object):
    """Used to query for the results of an asynchronous operation. Compatible
    with the builtin :py:class:`multiprocessing.pool.AsyncResult` class
    interface.

    :param result_queue: The AMQP queue name where task results are posted.
    :type result_queue: str

    """

    def __init__(self, result_queue):
        super(AsyncResult, self).__init__()
        self.result_queue = result_queue

    @property
    def correlation_id(self):
        """If the result is available, this will be the correlation identifier
        string of the original task.

        """
        if self.ready():
            return self._correlation_id
        raise AttributeError

    @property
    def args(self):
        """If the result is available, these will be a tuple of the original
        positional and keyword arguments sent with the request.

        """
        if self.ready():
            return (self._result['args'], self._result['kwargs'])
        raise AttributeError

    @property
    def name(self):
        """If the result is available, this will be the string name of the
        completed task.

        """
        if self.ready():
            return self._result['task']
        raise AttributeError

    @property
    def returned(self):
        """If the result is available and was successful, this will be the
        returned value of the completed task.

        """
        if self.ready() and hasattr(self, '_return'):
            return self._return
        raise AttributeError

    @property
    def exception(self):
        """If the result is available and was not successful, this will be the
        exception object raised by the completed task.

        """
        if self.ready() and hasattr(self, '_exc'):
            return self._exc
        raise AttributeError

    @property
    def traceback(self):
        """If the result is available and was not successful, this will be the
        exception traceback from the completed task.

        """
        if self.ready() and hasattr(self, '_exc'):
            return self._result['exception']['traceback']
        raise AttributeError

    def _get_cached_result(self):
        if hasattr(self, '_return'):
            return self._return
        elif hasattr(self, '_exc'):
            raise self._exc

    @classmethod
    def _handle_message(cls, msg, on):
        on._correlation_id = msg.correlation_id
        on._result = res = json.loads(msg.body)
        if 'return' in res:
            on._return = res['return']
        elif 'exception' in res:
            exception_raw = res['exception']['value'].encode('ascii')
            on._exc = cPickle.loads(exception_raw)

    def _check(self):
        with AmqpConnection() as channel:
            try:
                return channel.basic_get(queue=self.result_queue, no_ack=True)
            except amqp.exceptions.NotFound:
                raise KeyError(self.result_queue)
        raise TimeoutError(0.0)

    def _wait(self, timeout):
        start_time = time.time()
        msgs = deque()

        def msg_callback(msg):
            msgs.append(msg)

        with AmqpConnection() as channel:
            channel.basic_qos(0, 1, False)
            try:
                tag = channel.basic_consume(queue=self.result_queue,
                                            callback=msg_callback)
            except amqp.exceptions.NotFound:
                raise KeyError(self.result_queue)
            while channel.callbacks:
                elapsed = time.time() - start_time
                cur_timeout = 10.0
                if timeout is not None:
                    remaining = timeout - elapsed
                    cur_timeout = max(min(remaining, 10.0), 0.0)
                try:
                    channel.connection.drain_events(timeout=cur_timeout)
                except socket_timeout:
                    channel.connection.send_heartbeat()
                except socket_error as exc:
                    if exc.errno != errno.EAGAIN:
                        raise
                while len(msgs):
                    msg = msgs.popleft()
                    try:
                        yield msg
                    except GeneratorExit:
                        channel.basic_cancel(tag)
                        raise
                    finally:
                        channel.basic_ack(msg.delivery_tag)
                if timeout is not None and remaining <= 0.0:
                    break
        raise TimeoutError(timeout)

    def get(self, timeout=None):
        """Returns the task result when it becomes available. If the result was
        an exception, the exception is re-raised by this method.

        :param timeout: If this many seconds elapse before the result is ready,
                        this method will stop and return ``None``. The default
                        is to wait indefinitely.
        :type timeout: float
        :raises: :py:exc:`~multiprocessing.TimeoutError`

        """
        if hasattr(self, '_result'):
            return self._get_cached_result()
        if timeout == 0.0:
            msg = self._check()
        else:
            for first in self._wait(timeout):
                msg = first
                break
        if msg:
            self._handle_message(msg, self)
            return self._get_cached_result()

    def wait(self, timeout=None):
        """Waits for the task result to become available. It does not return
        the result or re-raise any exceptions.

        :param timeout: Wait at most this many seconds.
        :type timeout: float

        """
        if hasattr(self, '_result'):
            return
        try:
            self.get(timeout)
        except Exception:
            pass

    def ready(self):
        """Checks if the result is immediately available. If so, the
        :meth:`.get` and :meth:`.wait` methods will return immediately.

        :rtype: bool

        """
        if hasattr(self, '_result'):
            return True
        self.wait(0.0)
        return hasattr(self, '_result')

    def successful(self):
        """Checks if the result is both available and the task result will not
        re-raise an exception.

        :rtype: bool

        """
        if self.ready():
            return hasattr(self, '_return')
        return False

    def gather(self, timeout=None):
        """Returns a generator object that waits for task results to be
        available on the result queue, yielding a new :class:`AsyncResult`
        object for each one.

        This is useful if you route a single task to many
        worker queues and want to gather the results of each worker's
        execution.

        :param timeout: Wait at most this many seconds.
        :type timeout: float

        """
        try:
            for msg in self._wait(timeout):
                next_result = AsyncResult(self.result_queue)
                self._handle_message(msg, next_result)
                yield next_result
        except TimeoutError:
            pass

    def delete(self):
        """Delete the task result queue, if it still exists."""
        with AmqpConnection() as channel:
            try:
                channel.queue_delete(self.result_queue)
            except amqp.exceptions.NotFound:
                pass


class _TaskCaller(object):

    def __init__(self, func, name, app, exchange, routing_key):
        super(_TaskCaller, self).__init__()
        self.func = func
        self.name = name
        self.app = app
        self.exchange = exchange or ''
        if routing_key is None:
            self.routing_key = name
        else:
            self.routing_key = routing_key

    def delay(self, *args, **kwargs):
        return self.apply_async(args, kwargs)

    def apply_async(self, args, kwargs=None, correlation_id=None,
                    routing_key=None, send_result=False, result_queue=None):
        if correlation_id is None:
            correlation_id = str(uuid4())
        job = {'task': self.name,
               'args': args,
               'kwargs': kwargs}
        job_raw = json.dumps(job)
        reply_to = result_queue or 'result_{0}'.format(correlation_id)
        result = AsyncResult(reply_to)
        msg = amqp.Message(job_raw,
                           content_type='application/json',
                           reply_to=reply_to,
                           correlation_id=correlation_id)
        if routing_key is None:
            routing_key = self.routing_key
        with AmqpConnection() as channel:
            if send_result and result_queue is None:
                channel.queue_declare(queue=reply_to, auto_delete=False)
            channel.basic_publish(msg, exchange=self.exchange,
                                  routing_key=routing_key)
        logger.info('Task queued: name=%s, id=%s', self.name, correlation_id)
        logger.debug('Task details: args=%s, kwargs=%s',
                     repr(args), repr(kwargs))
        if send_result:
            return result

    def apply(self, args, kwargs=None, correlation_id=None):
        logger.info('Task starting: name=%s, id=%s', self.name, correlation_id)
        kwargs = kwargs or {}
        start_time = time.time()
        try:
            return self(*args, **kwargs)
        finally:
            elapsed = time.time() - start_time
            logger.info('Task finished: name=%s, id=%s, elapsed=%s',
                        self.name, correlation_id, elapsed)

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)


class _TaskSet(object):

    def __init__(self, app):
        super(_TaskSet, self).__init__()
        self._app = app
        self._tasks = {}

    def _set(self, func, name, exchange, routing_key):
        call = _TaskCaller(func, name, self._app, exchange, routing_key)
        self._tasks[name] = call

    def _declare(self, name, exchange, routing_key):
        call = _TaskCaller(None, name, self._app, exchange, routing_key)
        self._tasks.setdefault(name, call)

    def __len__(self):
        return len(self._tasks)

    def __contains__(self, name):
        return name in self._tasks

    def __getattr__(self, name):
        call = self._tasks.get(name)
        if call:
            return call
        raise AttributeError(name)

    def __repr__(self):
        task_names = self._tasks.keys()
        return '<registered task set {0!r}>'.format(task_names)


def routing_info(exchange, routing_key=None):
    """Convenience decorator for specifying the AMQP routing information that
    task messages will publish with.

    :param exchange: The name of the AMQP exchange.
    :type exchange: str
    :param routing_key: The AMQP routing key, defaults to the name of the task.
    :type routing:key: str

    """
    def deco(func):
        func._exchange = exchange
        func._routing_key = routing_key
        return func
    return deco


class WorkerApplication(object):
    """Defines an application that has a set of tasks that may be executed
    asynchronously by worker processes.

    """

    def __init__(self):
        super(WorkerApplication, self).__init__()

        #: This attribute should be used by clients to call tasks by name. For
        #: example, to call a task registered as ``'do_stuff'``::
        #:
        #:     tasks.do_stuff.delay(3.14159, 'arg2')
        #:
        #: Tasks in this object are objects of type :class:`_TaskCaller` and
        #: so they may be executed asynchronously with
        #: :meth:`~_TaskCaller.delay` and :meth:`~_TaskCaller.apply_async` or
        #: synchronously by calling them like a normal function.
        self.tasks = _TaskSet(self)

    def declare_task(self, name, exchange='', routing_key=None):
        """Declares a task without providing its implementation function. This
        may be used by client applications that know a task exists and how to
        use it, but do not have (or need) access to the task function itself.

        :param name: The full name of the task.
        :type name: str
        :param exchange: The AMQP exchange name to use for routing the task
                         messages.
        :type exchange: str
        :param routing_key: The AMQP routing key to use for routing the task
                            messages. By default, the name of the task will be
                            used. A routing key given to
                            :meth:`~_TaskCaller.apply_async` will always
                            override this value.
        :type routing_key: str

        """
        self.tasks._declare(name, exchange=exchange, routing_key=routing_key)

    def register_task(self, func, name=None, exchange=None, routing_key=None):
        """Registers a single function as an available task. If this function
        is decorated with :func:`routing_info`, its values will be used as the
        defaults for ``exchange`` and ``routing_key``.

        :param func: The function to call when executing the task.
        :type func: collections.Callable
        :param name: A string to uniquely identify the task, ``func.__name__``
                     is used by default.
        :type name: str
        :param exchange: The AMQP exchange name to use for routing the task
                         messages.
        :type exchange: str
        :param routing_key: The AMQP routing key to use for routing the task
                            messages. By default, the name of the task will be
                            used. A routing key given to
                            :meth:`~_TaskCaller.apply_async` will always
                            override this value.
        :type routing_key: str

        """
        if name is None:
            name = func.__name__
        if exchange is None and hasattr(func, '_exchange'):
            exchange = func._exchange
        if routing_key is None and hasattr(func, '_routing_key'):
            routing_key = func._routing_key
        exchange = exchange or ''
        self.tasks._set(func, name, exchange=exchange, routing_key=routing_key)

    def register_module(self, mod, prefix='', what=None, **routing_info):
        """Convenience method for bulk registering tasks in a module.

        :param mod: The module object to register tasks from.
        :type mod: module
        :param prefix: String prefixed to each task registered from the module.
        :type prefix: str
        :param what: List of module attribute names to use as tasks. By
                     default, ``mod.__all__`` is used.
        :param routing_info: Keyword arguments passed in to
                             :meth:`.register_task` and :meth:`.declare_task`
                             to provide the ``exchange`` and ``routing_key``
                             arguments.

        """
        if what is None:
            what = mod.__all__
        for func_identifier in what:
            func = getattr(mod, func_identifier)
            func_name = prefix + func.__name__
            self.register_task(func, func_name, **routing_info)
