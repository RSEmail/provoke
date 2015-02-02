
import unittest
import os
import time
import errno
import signal
import traceback
import json
from socket import timeout as socket_timeout

from amqp.exceptions import AccessRefused

try:
    from mock import patch, MagicMock, ANY
except ImportError:
    from unittest.mock import patch, MagicMock, ANY

from provoke.amqp import AmqpConnection
from provoke.worker import _WorkerProcess, WorkerMaster, \
    get_worker_data, get_worker_app, DiscardTask


class JsonMatcher(object):

    def __init__(self, test_case, expected):
        self.test_case = test_case
        self.expected = expected

    def __eq__(self, received):
        obj = json.loads(received)
        self.test_case.assertEqual(self.expected, obj)
        return True


class TestWorkerProcess(unittest.TestCase):

    @patch.object(AmqpConnection, '__enter__')
    @patch.object(AmqpConnection, '__exit__')
    def test_consume(self, amqp_exit_mock, amqp_enter_mock):
        app = MagicMock()
        worker = _WorkerProcess(app, ['testqueue'], 1, None, None, 'exclusive')
        channel = MagicMock(callbacks=True)

        def set_done(timeout):
            channel.callbacks = False
            raise socket_timeout
        channel.connection.drain_events.side_effect = set_done
        amqp_enter_mock.return_value = channel
        worker._consume()
        amqp_enter_mock.assert_called_with()
        channel.basic_consume.assert_called_with(queue='testqueue',
                                                 consumer_tag='testqueue',
                                                 callback=ANY,
                                                 exclusive='exclusive')
        channel.connection.drain_events.assert_called_with(timeout=10.0)
        channel.connection.send_heartbeat.assert_called_with()
        amqp_exit_mock.assert_called_with(None, None, None)

    def test_send_result(self):
        channel = MagicMock()
        worker = _WorkerProcess(None, None, None, None, None, False)
        worker._send_result(channel, 'test', {})
        channel.basic_publish.assert_called_with(ANY, exchange='',
                                                 routing_key='test')

    @patch.object(_WorkerProcess, '_send_result')
    def test_on_message(self, send_result_mock):
        app = MagicMock()
        task_cb = MagicMock()
        return_cb = MagicMock()
        app.tasks.func.apply.return_value = 'return'
        worker = _WorkerProcess(app, None, None, task_cb, return_cb, False)
        worker.counter = 0
        channel = MagicMock()
        body = '{"task": "func", "args": [1], "kwargs": {"two": 2}}'
        msg = MagicMock(body=body, correlation_id='testid', reply_to='test')
        worker._on_message(channel, msg)
        task_cb.assert_called_with('func', [1], {'two': 2})
        app.tasks.func.apply.assert_called_with([1], {'two': 2}, 'testid')
        channel.basic_ack.assert_called_with(ANY)
        return_cb.assert_called_with('func', 'return')
        send_result_mock.assert_called_with(channel, 'test', ANY)
        self.assertFalse(channel.basic_cancel.called)

    def test_on_message_task_callback(self):
        app = MagicMock()
        task_cb = MagicMock(side_effect=SystemExit)
        worker = _WorkerProcess(app, None, None, task_cb, None, False)
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
        worker = _WorkerProcess(app, None, None, task_cb, return_cb,
                                False)
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
        worker = _WorkerProcess(app, None, 1, None, None, False)
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
        worker = _WorkerProcess(app, None, 2, None, return_cb, False)
        worker.counter = 0
        channel = MagicMock()
        body = '{"task": "func", "args": [1], "kwargs": {"two": 2}}'
        msg = MagicMock(body=body, correlation_id='testid', reply_to='test')
        self.assertRaises(ValueError, worker._on_message, channel, msg)
        app.tasks.func.apply.assert_called_with([1], {'two': 2}, 'testid')
        return_cb.assert_called_with('func', None)
        send_result_mock.assert_called_with(channel, 'test', ANY)
        channel.basic_reject.assert_called_with(ANY, requeue=False)
        self.assertFalse(channel.basic_cancel.called)

    def test_run(self):
        worker_task_callback = MagicMock()
        worker = _WorkerProcess('testapp', ['testqueue'], 10,
                                worker_task_callback, None, False)
        worker._consume = MagicMock(side_effect=SystemExit)
        worker._run()
        self.assertFalse(worker_task_callback.called)
        worker._consume.assert_called_with()

    @patch.object(time, 'sleep')
    def test_run_accessrefused(self, sleep_mock):
        worker_task_callback = MagicMock()
        worker = _WorkerProcess('testapp', ['testqueue'], 10,
                                worker_task_callback, None, False)
        worker._consume = MagicMock(side_effect=[AccessRefused, None])
        worker._run()
        self.assertFalse(worker_task_callback.called)
        sleep_mock.assert_called_once_with(ANY)
        worker._consume.assert_called_with()

    @patch.object(traceback, 'print_exc')
    def test_run_exception(self, print_exc_mock):
        worker_task_callback = MagicMock()
        worker = _WorkerProcess('testapp', ['testqueue'], 10,
                                worker_task_callback, None, False)
        worker._consume = MagicMock(side_effect=AssertionError)
        self.assertRaises(AssertionError, worker._run)
        self.assertFalse(worker_task_callback.called)
        print_exc_mock.assert_called_with()
        worker._consume.assert_called_with()


class TestWorkerMaster(unittest.TestCase):

    def test_start_callback(self):
        cb = MagicMock(side_effect=Exception)
        worker = MagicMock(queues='testqueues', pid='testpid')
        master = WorkerMaster('testapp', start_callback=cb)
        master.start_callback(worker)
        cb.assert_called_with('testqueues', 'testpid')

    def test_exit_callback(self):
        cb = MagicMock(side_effect=Exception)
        worker = MagicMock(queues='testqueues', pid='testpid')
        master = WorkerMaster('testapp', exit_callback=cb)
        master.exit_callback(worker, 'teststatus')
        cb.assert_called_with('testqueues', 'testpid', 'teststatus')

    def test_add_worker(self):
        master = WorkerMaster('testapp')
        self.assertEqual([], master.workers)
        master.add_worker(['queue1'], exclusive=True)
        self.assertEqual(1, len(master.workers))
        self.assertEqual('testapp', master.workers[0].app)
        self.assertEqual(['queue1'], master.workers[0].queues)
        self.assertEqual(None, master.workers[0].pid)
        master.add_worker(['queue2'], num_processes=2)
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
        worker = _WorkerProcess(app, ['testqueue'], None, None, None, False)
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
        worker = _WorkerProcess(app, ['testqueue'], None, None, None, False)
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
        worker = _WorkerProcess(app, ['testqueue'], None, None, None, False)
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
        master = WorkerMaster('testapp', start_callback=start_cb)
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
