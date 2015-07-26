
import unittest
import os
import time
import errno
import signal
import traceback
import threading
import json

from amqp.exceptions import AccessRefused

try:
    from mock import patch, MagicMock, ANY
except ImportError:
    from unittest.mock import patch, MagicMock, ANY

from provoke import worker as worker_mod
from provoke.amqp import AmqpConnection
from provoke.worker import _WorkerProcess, _LocalProcess, \
    WorkerMaster, get_worker_data, get_worker_app, DiscardTask


class JsonMatcher(object):

    def __init__(self, test_case, expected):
        self.test_case = test_case
        self.expected = expected

    def __eq__(self, received):
        obj = json.loads(received)
        self.test_case.assertEqual(self.expected, obj)
        return True


class TestWorkerProcess(unittest.TestCase):

    def setUp(self):
        worker_mod._current_worker_data = {}
        worker_mod._current_worker_app = None

    @patch.object(AmqpConnection, '__enter__')
    @patch.object(AmqpConnection, '__exit__')
    def test_consume(self, amqp_exit_mock, amqp_enter_mock):
        app = MagicMock()
        worker = _WorkerProcess(app, ['testqueue'], 1, exclusive='exclusive')
        channel = MagicMock(callbacks=True)

        def set_done():
            channel.callbacks = False
        channel.connection.drain_events.side_effect = set_done
        amqp_enter_mock.return_value = channel
        worker._consume()
        amqp_enter_mock.assert_called_with()
        amqp_exit_mock.assert_called_with(None, None, None)
        channel.basic_consume.assert_called_with(queue='testqueue',
                                                 consumer_tag='testqueue',
                                                 callback=ANY,
                                                 exclusive='exclusive')
        channel.connection.drain_events.assert_called_with()

    def test_send_result(self):
        channel = MagicMock()
        worker = _WorkerProcess(None, None)
        worker._send_result(channel, 'testid', 'test', {})
        channel.basic_publish.assert_called_with(ANY, exchange='',
                                                 routing_key='test')

    @patch.object(_WorkerProcess, '_send_result')
    def test_on_message(self, send_result_mock):
        app = MagicMock()
        task_cb = MagicMock()
        return_cb = MagicMock()
        app.tasks.func.apply.return_value = 'return'
        worker = _WorkerProcess(app, None, task_callback=task_cb,
                                return_callback=return_cb)
        worker.counter = 0
        channel = MagicMock()
        body = '{"task": "func", "args": [1], "kwargs": {"two": 2}}'
        msg = MagicMock(body=body, correlation_id='testid', reply_to='test')
        worker._on_message(channel, msg)
        task_cb.assert_called_with('func', [1], {'two': 2})
        app.tasks.func.apply.assert_called_with([1], {'two': 2}, 'testid')
        channel.basic_ack.assert_called_with(ANY)
        return_cb.assert_called_with('func', 'return')
        send_result_mock.assert_called_with(channel, 'testid', 'test', ANY)
        self.assertFalse(channel.basic_cancel.called)

    def test_on_message_task_callback(self):
        app = MagicMock()
        task_cb = MagicMock(side_effect=SystemExit)
        worker = _WorkerProcess(app, None, task_callback=task_cb)
        worker.counter = 0
        channel = MagicMock(callbacks={1: None, 2: None})
        body = '{"task": "func", "args": [1], "kwargs": {"two": 2}}'
        msg = MagicMock(body=body, correlation_id='testid')
        self.assertRaises(SystemExit, worker._on_message, channel, msg)
        task_cb.assert_called_with('func', [1], {'two': 2})
        self.assertFalse(app.tasks.func.apply.called)
        channel.basic_reject.assert_called_with(ANY, requeue=True)

    def test_on_message_task_callback_discardtask(self):
        app = MagicMock()
        task_cb = MagicMock(side_effect=DiscardTask)
        return_cb = MagicMock()
        worker = _WorkerProcess(app, None, task_callback=task_cb,
                                return_callback=return_cb)
        worker.counter = 0
        channel = MagicMock(callbacks={1: None, 2: None})
        body = '{"task": "func", "args": [1], "kwargs": {"two": 2}}'
        msg = MagicMock(body=body, correlation_id='testid')
        worker._on_message(channel, msg)
        task_cb.assert_called_with('func', [1], {'two': 2})
        self.assertFalse(app.tasks.func.apply.called)
        self.assertFalse(return_cb.called)
        channel.basic_ack.assert_called_with(ANY)

    def test_on_message_limit(self):
        app = MagicMock()
        worker = _WorkerProcess(app, None, 1)
        worker.counter = 0
        channel = MagicMock(callbacks={1: None, 2: None})

        def clear_callbacks(consumer_tag):
            channel.callbacks = {}
        channel.basic_cancel.side_effect = clear_callbacks
        body = '{"task": "func", "args": [1], "kwargs": {"two": 2}}'
        msg = MagicMock(body=body, correlation_id='testid', reply_to=None)
        worker._on_message(channel, msg)
        app.tasks.func.apply.assert_called_with([1], {'two': 2}, 'testid')
        channel.basic_ack.assert_called_with(ANY)
        channel.basic_cancel.assert_any_call(1)
        channel.basic_cancel.assert_any_call(2)

    @patch.object(_WorkerProcess, '_send_result')
    def test_on_message_task_fail(self, send_result_mock):
        app = MagicMock()
        return_cb = MagicMock()
        app.tasks.func.apply.side_effect = ValueError
        worker = _WorkerProcess(app, None, 2, return_callback=return_cb)
        worker.counter = 0
        channel = MagicMock()
        body = '{"task": "func", "args": [1], "kwargs": {"two": 2}}'
        msg = MagicMock(body=body, correlation_id='testid', reply_to='test')
        self.assertRaises(ValueError, worker._on_message, channel, msg)
        app.tasks.func.apply.assert_called_with([1], {'two': 2}, 'testid')
        return_cb.assert_called_with('func', None)
        send_result_mock.assert_called_with(channel, 'testid', 'test', ANY)
        channel.basic_reject.assert_called_with(ANY, requeue=False)
        self.assertFalse(channel.basic_cancel.called)

    @patch.object(time, 'sleep')
    def test_send_heartbeats(self, sleep_mock):
        sleep_mock.side_effect = [None, Exception, StopIteration]
        worker = _WorkerProcess(None, None, 1)
        worker.active_connection = conn = MagicMock(heartbeat=30.0)
        self.assertRaises(StopIteration, worker._send_heartbeats)
        self.assertEqual(1, conn.send_heartbeat.call_count)
        self.assertEqual(3, sleep_mock.call_count)
        sleep_mock.assert_any_call(15.0)
        sleep_mock.assert_any_call(1.0)

    @patch.object(threading, 'Thread')
    def test_run(self, thread_cls_mock):
        thread_cls_mock.return_value = thread_mock = MagicMock()
        worker_task_callback = MagicMock()
        worker = _WorkerProcess('testapp', ['testqueue'], 10,
                                task_callback=worker_task_callback)
        worker._consume = MagicMock(side_effect=SystemExit)
        worker._run()
        thread_cls_mock.assert_called_with(target=ANY)
        thread_mock.start.assert_called_with()
        self.assertFalse(worker_task_callback.called)
        worker._consume.assert_called_with()

    @patch.object(time, 'sleep')
    @patch.object(threading, 'Thread')
    def test_run_accessrefused(self, thread_cls_mock, sleep_mock):
        thread_cls_mock.return_value = thread_mock = MagicMock()
        worker_task_callback = MagicMock()
        worker = _WorkerProcess('testapp', ['testqueue'], 10,
                                task_callback=worker_task_callback)
        worker._consume = MagicMock(side_effect=[AccessRefused, None])
        worker._run()
        thread_cls_mock.assert_called_with(target=ANY)
        thread_mock.start.assert_called_with()
        self.assertFalse(worker_task_callback.called)
        sleep_mock.assert_called_once_with(ANY)
        worker._consume.assert_called_with()

    @patch.object(traceback, 'print_exc')
    @patch.object(threading, 'Thread')
    def test_run_exception(self, thread_cls_mock, print_exc_mock):
        thread_cls_mock.return_value = thread_mock = MagicMock()
        worker_task_callback = MagicMock()
        worker = _WorkerProcess('testapp', ['testqueue'], 10,
                                task_callback=worker_task_callback)
        worker._consume = MagicMock(side_effect=AssertionError)
        self.assertRaises(AssertionError, worker._run)
        thread_cls_mock.assert_called_with(target=ANY)
        thread_mock.start.assert_called_with()
        self.assertFalse(worker_task_callback.called)
        print_exc_mock.assert_called_with()
        worker._consume.assert_called_with()


class TestLocalProcess(unittest.TestCase):

    @patch.object(traceback, 'print_exc')
    def test_run(self, printexc_mock):
        func1 = MagicMock(return_value='return1')
        func2 = MagicMock(side_effect=ValueError)
        func3 = MagicMock(side_effect=SystemExit)
        self.assertEqual('return1', _LocalProcess(func1)._run())
        self.assertRaises(ValueError, _LocalProcess(func2)._run)
        self.assertEqual(None, _LocalProcess(func3)._run())
        func1.assert_called_once_with()
        func2.assert_called_once_with()
        func3.assert_called_once_with()


class TestWorkerMaster(unittest.TestCase):

    def setUp(self):
        worker_mod._current_worker_data = {}
        worker_mod._current_worker_app = None

    def test_start_callback(self):
        cb = MagicMock(side_effect=Exception)
        worker = MagicMock(queues='testqueues', pid='testpid')
        master = WorkerMaster(start_callback=cb)
        master.start_callback(worker)
        cb.assert_called_with('testqueues', 'testpid')

    def test_exit_callback(self):
        cb = MagicMock(side_effect=Exception)
        worker = MagicMock(queues='testqueues', pid='testpid')
        master = WorkerMaster('testapp', exit_callback=cb)
        master.exit_callback(worker, 'teststatus')
        cb.assert_called_with('testqueues', 'testpid', 'teststatus')

    def test_add_worker(self):
        master = WorkerMaster()
        self.assertEqual([], master.workers)
        master.add_worker('testapp', ['queue1'], exclusive=True)
        self.assertEqual(1, len(master.workers))
        self.assertEqual('testapp', master.workers[0].app)
        self.assertEqual(['queue1'], master.workers[0].queues)
        self.assertEqual(None, master.workers[0].pid)
        master.add_worker('testapp', ['queue2'], num_processes=2)
        self.assertEqual(3, len(master.workers))
        self.assertEqual('testapp', master.workers[0].app)
        self.assertEqual(['queue1'], master.workers[0].queues)
        self.assertTrue(master.workers[0].exclusive)
        self.assertEqual('testapp', master.workers[1].app)
        self.assertEqual(['queue2'], master.workers[1].queues)
        self.assertFalse(master.workers[1].exclusive)
        self.assertEqual('testapp', master.workers[2].app)
        self.assertEqual(['queue2'], master.workers[2].queues)
        self.assertFalse(master.workers[2].exclusive)

    def test_add_local_worker(self):
        master = WorkerMaster()
        self.assertEqual([], master.workers)
        func = MagicMock(return_value='retval')
        master.add_local_worker(func, 2)
        self.assertEqual(2, len(master.workers))
        self.assert_(isinstance(master.workers[0], _LocalProcess))
        self.assert_(isinstance(master.workers[1], _LocalProcess))
        self.assertEqual('retval', master.workers[0]._func())
        self.assertEqual('retval', master.workers[1]._func())

    @patch.object(os, 'waitpid')
    def test_check_workers(self, waitpid_mock):
        exit_cb = MagicMock()
        worker = MagicMock(queues='testqueues', pid='testpid')
        master = WorkerMaster('testapp', exit_callback=exit_cb)
        master.workers = [worker]
        waitpid_mock.return_value = ('testpid', 0)
        self.assertTrue(master._check_workers())
        waitpid_mock.assert_called_with(0, 0)
        exit_cb.assert_called_with('testqueues', 'testpid', 0)
        self.assertEqual(None, worker.pid)

    @patch.object(os, 'waitpid')
    def test_check_workers_no_children(self, waitpid_mock):
        exit_cb = MagicMock()
        worker = MagicMock(queues='testqueues', pid='testpid')
        master = WorkerMaster('testapp', exit_callback=exit_cb)
        master.workers = [worker]
        waitpid_mock.side_effect = OSError(errno.ECHILD, 'No child processes')
        self.assertFalse(master._check_workers())
        waitpid_mock.assert_called_with(0, 0)
        exit_cb.assert_called_with('testqueues', 'testpid', None)
        self.assertEqual(None, worker.pid)

    @patch.object(os, 'waitpid')
    def test_check_workers_other_oserror(self, waitpid_mock):
        exit_cb = MagicMock()
        worker = MagicMock(queues='testqueues', pid='testpid')
        master = WorkerMaster('testapp', exit_callback=exit_cb)
        master.workers = [worker]
        waitpid_mock.side_effect = OSError(99, 'Something else')
        self.assertRaises(OSError, master._check_workers)
        waitpid_mock.assert_called_with(0, 0)
        self.assertEqual('testpid', worker.pid)

    @patch.object(os, 'fork')
    @patch.object(os, '_exit')
    def test_start_worker_child(self, exit_mock, fork_mock):
        app = MagicMock()
        worker = _WorkerProcess(app, ['testqueue'])
        worker._run = MagicMock()
        master = WorkerMaster('testapp')
        fork_mock.return_value = 0
        master._start_worker(worker)
        fork_mock.assert_called_with()
        worker._run.assert_called_with()
        exit_mock.assert_called_with(os.EX_OK)

    @patch.object(os, 'fork')
    def test_start_worker_parent(self, fork_mock):
        worker = MagicMock()
        master = WorkerMaster('testapp')
        fork_mock.return_value = 13
        self.assertEqual(13, master._start_worker(worker))
        fork_mock.assert_called_with()

    @patch.object(os, 'fork')
    @patch.object(os, '_exit')
    def test_get_worker_data(self, exit_mock, fork_mock):
        app = MagicMock()
        worker = _WorkerProcess(app, ['testqueue'])
        worker._run = MagicMock()
        master = WorkerMaster('testapp', worker_data={'test': 'data'})
        fork_mock.return_value = 0
        self.assertRaises(RuntimeError, get_worker_data, 'test')
        master._start_worker(worker)
        self.assertEquals('data', get_worker_data('test'))
        fork_mock.assert_called_with()
        worker._run.assert_called_with()
        exit_mock.assert_called_with(os.EX_OK)

    @patch.object(os, 'fork')
    @patch.object(os, '_exit')
    def test_get_worker_app(self, exit_mock, fork_mock):
        app = MagicMock()
        worker = _WorkerProcess(app, ['testqueue'])
        worker._run = MagicMock()
        master = WorkerMaster('testapp')
        fork_mock.return_value = 0
        self.assertRaises(RuntimeError, get_worker_app)
        master._start_worker(worker)
        self.assertEquals(app, get_worker_app())
        fork_mock.assert_called_with()
        worker._run.assert_called_with()
        exit_mock.assert_called_with(os.EX_OK)

    def test_restart_workers(self):
        start_cb = MagicMock()
        worker1 = MagicMock(queues='testqueues', pid='testpid1')
        worker2 = MagicMock(queues='testqueues', pid=None)
        master = WorkerMaster(start_callback=start_cb)
        master._start_worker = MagicMock(return_value='testpid2')
        master.workers = [worker1, worker2]
        master._restart_workers()
        master._start_worker.assert_called_once_with(worker2)
        start_cb.assert_called_with('testqueues', 'testpid2')

    @patch.object(os, 'kill')
    def test_stop_workers(self, kill_mock):
        kill_mock.side_effect = [None,
                                 OSError(errno.ESRCH, 'No such pid'),
                                 OSError(99, 'Something else')]
        worker1 = MagicMock(pid='testpid1')
        worker2 = MagicMock(pid='testpid2')
        worker3 = MagicMock(pid='testpid3')
        master = WorkerMaster('testapp')
        master.workers = [worker1, worker2, worker3]
        self.assertRaises(OSError, master._stop_workers)
        kill_mock.assert_any_call('testpid1', signal.SIGTERM)
        kill_mock.assert_any_call('testpid2', signal.SIGTERM)
        kill_mock.assert_any_call('testpid3', signal.SIGTERM)

    @patch.object(os, 'waitpid')
    def test_wait(self, waitpid_mock):
        exit_cb = MagicMock()
        waitpid_mock.side_effect = [('testpid1', 0),
                                    OSError(errno.ESRCH, 'No such pid'),
                                    OSError(99, 'Something else')]
        worker1 = MagicMock(queues=None, pid='testpid1')
        worker2 = MagicMock(queues=None, pid='testpid2')
        worker3 = MagicMock(queues=None, pid='testpid3')
        master = WorkerMaster('testapp', exit_callback=exit_cb)
        master.workers = [worker1, worker2, worker3]
        self.assertRaises(OSError, master.wait)
        waitpid_mock.assert_any_call('testpid1', 0)
        waitpid_mock.assert_any_call('testpid2', 0)
        waitpid_mock.assert_any_call('testpid3', 0)
        exit_cb.assert_any_call(None, 'testpid1', 0)
        exit_cb.assert_any_call(None, 'testpid2', None)

    def test_run(self):
        master = WorkerMaster('testapp')
        master._restart_workers = MagicMock(side_effect=[None, ValueError])
        master._check_workers = MagicMock()
        master._stop_workers = MagicMock()
        self.assertRaises(ValueError, master.run)
        master._restart_workers.assert_any_call()
        master._restart_workers.assert_called_with()
        master._check_workers.assert_called_with()
        master._stop_workers.assert_called_with()
