
import unittest
import json

from mock import patch, MagicMock, ANY
import amqp

from provoke.common.amqp import AmqpConnection
from provoke.common.app import _AsyncResult, _TaskCaller, \
    _TaskSet, taskgroup, WorkerApplication


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
        res = _AsyncResult('testapp', 'testid', 'testname', 1234.0)
        self.assertEqual('testapp', res.app)
        self.assertEqual('testid', res.correlation_id)
        self.assertEqual('testname', res.name)
        self.assertEqual(1234.0, res.timestamp)

    def test_get(self):
        res = _AsyncResult(None, None, None, None)
        self.assertRaises(NotImplementedError, res.get)

    def test_wait(self):
        res = _AsyncResult(None, None, None, None)
        self.assertRaises(NotImplementedError, res.wait)

    def test_ready(self):
        res = _AsyncResult(None, None, None, None)
        self.assertRaises(NotImplementedError, res.ready)

    def test_successful(self):
        res = _AsyncResult(None, None, None, None)
        self.assertRaises(NotImplementedError, res.successful)


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
        app = MagicMock()
        call = _TaskCaller('testfunc', 'testname', app, 'testexchange',
                          'testqueue')
        ret = call.apply_async(('one', 'two'), {'three': 'four'})
        self.assertTrue(isinstance(ret, _AsyncResult))
        body_matcher = JsonMatcher(self, {'task_name': 'testname',
                                          'args': ['one', 'two'],
                                          'kwargs': {'three': 'four'}})
        msg_mock.assert_called_with(body_matcher,
                                    content_type='application/json',
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
        app.declare_task('testgroup', 'taskname')
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
        app.declare_task.assert_called_with('testgroup', 'testtask')
