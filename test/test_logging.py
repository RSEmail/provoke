
import unittest
import sys
import logging
import logging.handlers

try:
    from mock import patch, MagicMock, ANY
except ImportError:
    from unittest.mock import patch, MagicMock, ANY

from provoke.logging import setup_logging, \
    log_exception, log_debug, log_info, log_warning, log_error


class TestLoggingFunctions(unittest.TestCase):

    @patch.object(logging, 'getLogger')
    @patch.object(logging, 'StreamHandler')
    @patch.object(logging.handlers, 'SysLogHandler')
    def test_setup_logging(self, sysloghandler_mock, streamhandler_mock,
                           getlogger_mock):
        getlogger_mock.return_value = root_logger = MagicMock()
        streamhandler_mock.return_value = stdout_handler = MagicMock()
        sysloghandler_mock.return_value = syslog_handler = MagicMock()
        setup_logging(False, 'testaddress', 'testfac')
        self.assertEqual(3, getlogger_mock.call_count)
        getlogger_mock.assert_called_with('')
        streamhandler_mock.assert_called_with(sys.stdout)
        stdout_handler.setLevel.assert_called_with(logging.WARNING)
        stdout_handler.setFormatter.assert_called_with(ANY)
        sysloghandler_mock.assert_called_with(address='testaddress',
                                              facility='testfac')
        syslog_handler.setLevel.assert_called_with(logging.INFO)
        syslog_handler.setFormatter.assert_called_with(ANY)
        root_logger.setLevel.assert_called_with(logging.DEBUG)
        root_logger.addHandler.assert_any_call(stdout_handler)
        root_logger.addHandler.assert_any_call(syslog_handler)

    @patch.object(logging, 'getLogger')
    def test_log_debug(self, getlogger_mock):
        getlogger_mock.return_value = logger = MagicMock()
        log_debug('testmsg', logger='test', test='asdf')
        getlogger_mock.assert_called_with('provoke.test')
        logger.debug.assert_called_with('testmsg: {"test": "asdf"}')

    @patch.object(logging, 'getLogger')
    def test_log_info(self, getlogger_mock):
        getlogger_mock.return_value = logger = MagicMock()
        log_info('testmsg', logger='test', test='asdf')
        getlogger_mock.assert_called_with('provoke.test')
        logger.info.assert_called_with('testmsg: {"test": "asdf"}')

    @patch.object(logging, 'getLogger')
    def test_log_warning(self, getlogger_mock):
        getlogger_mock.return_value = logger = MagicMock()
        log_warning('testmsg', logger='test', test='asdf')
        getlogger_mock.assert_called_with('provoke.test')
        logger.warning.assert_called_with('testmsg: {"test": "asdf"}')

    @patch.object(logging, 'getLogger')
    def test_log_error(self, getlogger_mock):
        getlogger_mock.return_value = logger = MagicMock()
        log_error('testmsg', logger='test', test='asdf')
        getlogger_mock.assert_called_with('provoke.test')
        logger.error.assert_called_with('testmsg: {"test": "asdf"}')

    @patch.object(logging, 'getLogger')
    def test_log_exception(self, getlogger_mock):
        getlogger_mock.return_value = logger = MagicMock()
        try:
            raise Exception('testexcmsg')
        except Exception:
            log_exception('testmsg')
        getlogger_mock.assert_called_with('provoke')
        logger.error.assert_called_with(ANY)
        log_msg = logger.error.call_args[0][0]
        self.assertTrue(log_msg.startswith('testmsg: '))
        self.assertTrue('"type": "Exception"' in log_msg)
        self.assertTrue('"value": "testexcmsg"' in log_msg)
