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

.. class:: _AsyncResult

   Used to query for the results of an asynchronous operation.

   .. attribute:: correlation_id

      The identifier string used to correlate the AMQP message with the entry
      in the results backend.

   .. attribute:: name

      The name of the task.

   .. attribute:: timestamp

      The UNIX timestamp as returned by :func:`time.time` when this task was
      initially triggered.

   .. method:: get(timeout=None)

      Returns the task result when it becomes available. If the result was an
      exception, the exception is re-raised by this method.

      :param timeout: If this many seconds elapse before the result is ready,
                      this method will stop and return ``None``. The default
                      is to wait indefinitely.
      :type timeout: float
      :raises: NotImplementedError

   .. method:: wait(timeout=None)

      Waits for the task result to become available. It does not return the
      result or re-raise any exceptions.

      :param timeout: Wait at most this many seconds.
      :type timeout: float
      :raises: NotImplementedError

   .. method:: ready()

      Checks if the result is immediately available. If so, the :meth:`.get`
      and :meth:`.wait` methods will return immediately.

      :rtype: bool
      :raises: NotImplementedError

   .. method:: successful()

      Checks if the result is both available and the task result will not
      re-raise an exception.

      :rtype: bool
      :raises: NotImplementedError

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
      :rtype: :class:`_AsyncResult`

   .. method:: apply_async(args, kwargs=None, correlation_id=None,
                           routing_key=None, **log_extra)

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
      :param log_extra: Additional information passed in to the log message
                        indicating the task was queued.
      :type log_extra: keyword arguments
      :returns: A way to retrieve the result of the task's execution when it
                is ready.
      :rtype: :class:`_AsyncResult`

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
from uuid import uuid4
from itertools import repeat

import amqp

from .amqp import AmqpConnection
from .logging import log_info, log_debug

__all__ = ['WorkerApplication']


class _AsyncResult(object):

    def __init__(self, app, correlation_id, name, timestamp):
        super(_AsyncResult, self).__init__()
        self.app = app
        self.correlation_id = correlation_id
        self.name = name
        self.timestamp = timestamp

    def get(self, timeout=None):
        raise NotImplementedError()

    def wait(self, timeout=None):
        raise NotImplementedError()

    def ready(self):
        raise NotImplementedError()

    def successful(self):
        raise NotImplementedError()


class _TaskCaller(object):

    def __init__(self, func, name, app, exchange, routing_key):
        super(_TaskCaller, self).__init__()
        self.func = func
        self.name = name
        self.app = app
        self.exchange = exchange
        if routing_key is None:
            self.routing_key = name
        else:
            self.routing_key = routing_key

    def delay(self, *args, **kwargs):
        return self.apply_async(args, kwargs)

    def apply_async(self, args, kwargs=None, correlation_id=None,
                    routing_key=None,  **log_extra):
        start_time = time.time()
        if correlation_id is None:
            correlation_id = str(uuid4())
        job = {'task': self.name,
               'args': args,
               'kwargs': kwargs}
        job_raw = json.dumps(job)
        msg = amqp.Message(job_raw,
                           content_type='application/json',
                           correlation_id=correlation_id)
        if routing_key is None:
            routing_key = self.routing_key
        with AmqpConnection() as channel:
            channel.basic_publish(msg, exchange=self.exchange,
                                  routing_key=routing_key)
        log_info('Task queued', logger='tasks',
                 id=correlation_id,
                 name=self.name, **log_extra)
        log_debug('Task details', logger='tasks',
                  id=correlation_id,
                  name=self.name, args=args, kwargs=kwargs)
        return _AsyncResult(self.app, correlation_id, self.name, start_time)

    def apply(self, args, kwargs=None, correlation_id=None):
        log_info('Task starting', logger='tasks',
                 id=correlation_id,
                 name=self.name)
        kwargs = kwargs or {}
        start_time = time.time()
        try:
            return self(*args, **kwargs)
        finally:
            elapsed = time.time() - start_time
            log_info('Task finished', logger='tasks',
                     id=correlation_id,
                     name=self.name,
                     elapsed=elapsed)

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


def taskgroup(name):
    """Convenience decorator for ensuring that the task is a member of the
    given taskgroup.

    :param name: The taskgroup to make the task a member of.
    :type name: str

    """
    def deco(func):
        func._taskgroup = name
        return func
    return deco


class WorkerApplication(object):
    """Defines an application that has a set of tasks that may be executed
    asynchronously by worker processes.

    """

    _taskgroups = {}

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

    @classmethod
    def reset_taskgroups(cls):
        """Removes all known taskgroups."""
        cls._taskgroups = {}

    @classmethod
    def declare_taskgroup(cls, name, exchange='', routing_key=None):
        """Associates a name with a set of routing information. Tasks that are
        a member of a given taskgroup will use its routing information. Tasks
        that are not a member of a taskgroup will raise a ``TypeError``
        exception if they are called asynchronously. Taskgroups are shared
        between all instances of this class.

        :param name: The name of the taskgroup.
        :type name: str
        :param exchange: The AMQP exchange name to use for routing the task
                         message.
        :type exchange: str
        :param routing_key: The AMQP routing key used for routing the task
                            message.  If this value is ``None``, the task name
                            will be used instead.
        :type routing_key: str

        """
        cls._taskgroups[name] = {'exchange': exchange,
                                 'routing_key': routing_key}

    def declare_task(self, taskgroup, name):
        """Declares a task without providing its implementation function. This
        may be used by client applications that know a task exists and how to
        use it, but do not have (or need) access to the task function itself.

        :param taskgroup: The taskgroup to use for routing the task messages.
                          Must have been previously declared with
                          :meth:`.declare_taskgroup`.
        :type taskgroup: str
        :param name: The full name of the task.
        :type name: str

        """
        tg_info = self._taskgroups[taskgroup]
        self.tasks._declare(name, **tg_info)

    def register_task(self, func, name=None, taskgroup=None):
        """Registers a single function as an available task.

        :param func: The function to call when executing the task.
        :type func: collections.Callable
        :param name: A string to uniquely identify the task, ``func.__name__``
                     is used by default.
        :type name: str
        :param taskgroup: An optional, previously declared taskgroup name to
                          make the task a member of. This overrides the use of
                          the :func:`~provoke.common.app.taskgroup` decorator.
        :type taskgroup: str

        """
        if name is None:
            name = func.__name__
        tg_info = {'exchange': None, 'routing_key': None}
        if taskgroup is not None:
            tg_info = self._taskgroups[taskgroup]
        elif hasattr(func, '_taskgroup'):
            tg_info = self._taskgroups[func._taskgroup]
        self.tasks._set(func, name, **tg_info)

    def register_module(self, mod, prefix='', taskgroup=None, what=None):
        """Convenience method for bulk registering tasks in a module.

        The module may use a special attribute named ``__declare_tasks__`` to
        specify a list of ``(taskgroup, name)`` tuples, each tuple is passed
        into a call to :meth:`.declare_task`.

        :param mod: The module object to register tasks from.
        :type mod: module
        :param prefix: String prefixed to each task registered from the module.
        :type prefix: str
        :param taskgroup: An optional, previously declared taskgroup name to
                          make the task a member of. This overrides the use of
                          the :func:`~provoke.common.app.taskgroup` decorator.
        :type taskgroup: str
        :param what: List of module attribute names to use as tasks. By
                     default, ``mod.__all__`` is used.

        """
        if what is None:
            what = mod.__all__
        for func_identifier in what:
            func = getattr(mod, func_identifier)
            func_name = prefix + func.__name__
            self.register_task(func, func_name, taskgroup)
        for taskgroup, name in getattr(mod, '__declare_tasks__', []):
            self.declare_task(taskgroup, name)
