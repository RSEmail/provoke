
import unittest
import time
import json
from socket import timeout as socket_timeout
from multiprocessing import TimeoutError

try:
    from mock import patch, MagicMock, ANY
except ImportError:
    from unittest.mock import patch, MagicMock, ANY
import amqp

from provoke.amqp import AmqpConnection
from provoke.app import AsyncResult, _TaskCaller, _TaskSet, \
    taskgroup, WorkerApplication


class JsonMatcher(object):

    def __init__(self, test_case, expected):
        self.test_case = test_case
        self.expected = expected

    def __eq__(self, received):
        obj = json.loads(received)
        self.test_case.assertEqual(self.expected, obj)
        return True


class TestAsyncResult(unittest.TestCase):

    def test_attributes(self):
        res = AsyncResult('testid')
        self.assertEqual('testid', res.correlation_id)
        self.assertRaises(AttributeError, getattr, res, 'args')
        self.assertRaises(AttributeError, getattr, res, 'name')
        self.assertRaises(AttributeError, getattr, res, 'returned')
        self.assertRaises(AttributeError, getattr, res, 'exception')
        self.assertRaises(AttributeError, getattr, res, 'traceback')
        res._result = {'args': 123, 'kwargs': 456, 'task_name': 'test',
                       'exception': {'traceback': 'test traceback'}}
        self.assertEqual((123, 456), res.args)
        self.assertEqual('test', res.name)
        res._return = 789
        self.assertEqual(789, res.returned)
        del res._return
        res._exc = 987
        self.assertEqual(987, res.exception)
        self.assertEqual('test traceback', res.traceback)

    def test_on_message(self):
        res = AsyncResult(None)
        msg = MagicMock(body='{"task_name": "test", "return": 123}')
        res._on_message(msg)
        self.assertEqual(123, res._return)
        self.assertFalse(hasattr(res, '_exc'))

    def test_on_message_fail(self):
        res = AsyncResult(None)
        body = '{"task_name": "test", "exception": {"value": "I123\\n."}}'
        msg = MagicMock(body=body)
        res._on_message(msg)
        self.assertEqual(123, res._exc)
        self.assertFalse(hasattr(res, '_return'))

    def test_get_cached(self):
        res = AsyncResult(None)
        res._result = True
        res._return = 123
        self.assertEqual(123, res.get())
        del res._return
        res._exc = ValueError
        self.assertRaises(ValueError, res.get)

    @patch.object(AmqpConnection, '__enter__')
    @patch.object(AmqpConnection, '__exit__')
    def test_get(self, exit_mock, enter_mock):
        enter_mock.return_value = channel = MagicMock()
        exit_mock.return_value = None
        res = AsyncResult('test')

        def finish(timeout):
            res._result = True
            res._return = 123
        channel.connection.drain_events.side_effect = finish
        self.assertEqual(123, res.get())
        exit_mock.assert_called_with(None, None, None)
        channel.basic_consume.assert_called_with(queue='result_test',
                                                 no_ack=True, callback=ANY)
        channel.connection.drain_events.assert_called_with(timeout=10.0)
        channel.queue_delete.assert_called_with('result_test')
        self.assertFalse(channel.connection.send_heartbeat.called)

    @patch.object(AmqpConnection, '__enter__')
    @patch.object(AmqpConnection, '__exit__')
    @patch.object(AsyncResult, '_on_message')
    def test_get_nonblock(self, on_msg_mock, exit_mock, enter_mock):
        enter_mock.return_value = channel = MagicMock()
        exit_mock.return_value = None
        channel.basic_get.return_value = msg = MagicMock()
        res = AsyncResult('test')

        def finish(timeout):
            res._result = True
            res._return = 123
        on_msg_mock.side_effect = finish
        self.assertEqual(123, res.get(0.0))
        exit_mock.assert_called_with(None, None, None)
        on_msg_mock.assert_called_with(msg)
        channel.basic_get.assert_called_with(queue='result_test', no_ack=True)
        channel.queue_delete.assert_called_with('result_test')

    @patch.object(AmqpConnection, '__enter__')
    @patch.object(AmqpConnection, '__exit__')
    def test_get_keyerror(self, exit_mock, enter_mock):
        enter_mock.return_value = channel = MagicMock()
        exit_mock.return_value = None
        channel.basic_consume.side_effect = amqp.exceptions.NotFound
        res = AsyncResult('test')
        self.assertRaises(KeyError, res.get)
        channel.basic_consume.assert_called_with(queue='result_test',
                                                 no_ack=True, callback=ANY)
        self.assertFalse(channel.connection.drain_events.called)
        self.assertFalse(channel.queue_delete.called)
        self.assertFalse(channel.connection.send_heartbeat.called)

    @patch.object(AmqpConnection, '__enter__')
    @patch.object(AmqpConnection, '__exit__')
    @patch.object(time, 'time')
    def test_get_timeout(self, time_mock, exit_mock, enter_mock):
        enter_mock.return_value = channel = MagicMock()
        time_mock.side_effect = [0.0, 20.0]
        channel.connection.drain_events.side_effect = socket_timeout
        res = AsyncResult('test')
        self.assertRaises(TimeoutError, res.get, 10.0)
        channel.basic_consume.assert_called_with(queue='result_test',
                                                 no_ack=True, callback=ANY)
        channel.connection.drain_events.assert_called_with(timeout=0.0)
        channel.connection.send_heartbeat.assert_called_with()

    @patch.object(AsyncResult, 'get')
    def test_wait(self, get_mock):
        res = AsyncResult(None)
        res._return = True
        res.wait()
        del res._return
        get_mock.side_effect = ValueError
        res.wait()

    @patch.object(AsyncResult, 'wait')
    def test_ready(self, wait_mock):
        res = AsyncResult(None)
        self.assertFalse(res.ready())
        res._result = True
        self.assertTrue(res.ready())
        res._exc = True
        self.assertTrue(res.ready())
        wait_mock.assert_called_once_with(0.0)

    @patch.object(AsyncResult, 'ready')
    def test_successful(self, ready_mock):
        res = AsyncResult(None)
        ready_mock.return_value = False
        self.assertFalse(res.successful())
        ready_mock.return_value = True
        res._return = True
        self.assertTrue(res.successful())
        del res._return
        res._exc = True
        self.assertFalse(res.successful())


class TestTaskCall(unittest.TestCase):

    def test_delay(self):
        call = _TaskCaller('a', 'b', 'c', 'd', 'e')
        call.apply_async = MagicMock()
        call.delay('stuff', 13, test='what')
        call.apply_async.assert_called_with(('stuff', 13), {'test': 'what'})

    @patch.object(AmqpConnection, '__enter__')
    @patch.object(AmqpConnection, '__exit__')
    @patch.object(amqp, 'Message')
    def test_apply_async(self, msg_mock, amqp_exit_mock, amqp_enter_mock):
        channel = MagicMock()
        amqp_enter_mock.return_value = channel
        msg_mock.return_value = 52
        app = MagicMock(result_queue_ttl=0)
        func = MagicMock(_send_result=True)
        call = _TaskCaller(func, 'testname', app, 'testexchange', 'testqueue')
        ret = call.apply_async(('one', 'two'), {'three': 'four'},
                               send_result=True)
        self.assertTrue(isinstance(ret, AsyncResult))
        body_matcher = JsonMatcher(self, {'task': 'testname',
                                          'args': ['one', 'two'],
                                          'kwargs': {'three': 'four'}})
        msg_mock.assert_called_with(body_matcher,
                                    content_type='application/json',
                                    reply_to=ANY,
                                    correlation_id=ANY)
        amqp_enter_mock.assert_called_with()
        channel.basic_publish.assert_called_with(52, exchange='testexchange',
                                                 routing_key='testqueue')
        amqp_exit_mock.assert_called_with(None, None, None)

    def test_apply(self):
        func = MagicMock(return_value=13)
        call = _TaskCaller(func, 'a', 'b', 'c', 'd')
        ret = call.apply(('one', 'two'), {'three': 'four'})
        self.assertEqual(13, ret)
        func.assert_called_with('one', 'two', three='four')

    def test_call(self):
        func = MagicMock(return_value=13)
        call = _TaskCaller(func, 'a', 'b', 'c', 'd')
        ret = call('one', 'two', three='four')
        self.assertEqual(13, ret)
        func.assert_called_with('one', 'two', three='four')


class TestTaskSet(unittest.TestCase):

    def test_set(self):
        set = _TaskSet('testapp')
        set._set('testfunc', 'testname', 'testexchange', 'testrouting')
        self.assertEqual(1, len(set))
        self.assertTrue('testname' in set)
        self.assertEqual('testfunc', set._tasks['testname'].func)
        self.assertEqual('testname', set._tasks['testname'].name)
        self.assertEqual('testapp', set._tasks['testname'].app)
        self.assertEqual('testrouting', set._tasks['testname'].routing_key)
        self.assertEqual('testexchange', set._tasks['testname'].exchange)

    def test_declare(self):
        set = _TaskSet('testapp')
        set._set('testfunc1', 'testtask1', 'testexchange1', 'testrouting1')
        set._declare('testtask1', 'testexchange1', 'testrouting1')
        set._declare('testtask2', 'testexchange2', 'testrouting2')
        self.assertEqual(2, len(set))

    def test_getattr(self):
        set = _TaskSet('testparams')
        set._set('func1', 'one', 'testexchange', 'testrouting')
        set._set('func2', 'two', 'testexchange', 'testrouting')
        self.assertEqual('func1', set.one.func)
        self.assertEqual('func2', set.two.func)
        self.assertRaises(AttributeError, getattr, set, 'three')

    def test_repr(self):
        set = _TaskSet('testparams')
        set._set('func1', 'one', 'testexchange', 'testrouting')
        set._set('func2', 'two', 'testexchange', 'testrouting')
        self.assertTrue(repr(set).startswith('<registered task set '))
        self.assertTrue(repr(set).endswith('>'))


class TestWorkerApplication(unittest.TestCase):

    def test_declare_task(self):
        app = WorkerApplication()
        app.declare_taskgroup('testgroup', 'testexchange', 'testroutingkey')
        app.tasks = MagicMock()
        app.declare_task('taskname', 'testgroup')
        app.tasks._declare.assert_called_with('taskname',
                                              exchange='testexchange',
                                              routing_key='testroutingkey')

    def test_register_task(self):
        @taskgroup('testgroup')
        def func1():
            pass

        def func2():
            pass
        app = WorkerApplication()
        app.declare_taskgroup('testgroup', 'testexchange', 'testroutingkey')
        app.tasks = MagicMock()
        app.register_task(func1, 'funcone')
        app.tasks._set.assert_called_with(func1, 'funcone',
                                          exchange='testexchange',
                                          routing_key='testroutingkey')
        app.register_task(func2, taskgroup='testgroup')
        app.tasks._set.assert_called_with(func2, 'func2',
                                          exchange='testexchange',
                                          routing_key='testroutingkey')

    def test_register_module(self):
        app = WorkerApplication()
        app.declare_task = MagicMock()
        app.register_task = MagicMock()
        mod = MagicMock()
        mod.__all__ = ['func1', 'func2']
        mod.__declare_tasks__ = [('testgroup', 'testtask')]
        mod.func1 = MagicMock(__name__='func1')
        mod.func2 = MagicMock(__name__='func2')
        app.register_module(mod, 't_')
        app.register_task.assert_any_call(mod.func1, 't_func1', None)
        app.register_task.assert_any_call(mod.func2, 't_func2', None)
        app.declare_task.assert_called_with('testtask', 'testgroup')
