
from __future__ import unicode_literals

import os.path
import unittest
from tempfile import NamedTemporaryFile

from mock import patch, MagicMock, ANY
from flask import Flask
from passlib.apache import HtpasswdFile

from provoke.api import auth


class TestAuth(unittest.TestCase):

    def setUp(self):
        app = Flask(__name__)
        app.testing = True
        @app.route('/authtest')
        def authtest_route():
            return ''
        app.url_value_preprocessor(auth.require_auth)
        self.client = app.test_client()
        htpasswd = NamedTemporaryFile()
        htpasswd.write('testuser:{SSHA}GUHOTEmm8rm4O3ljh/11wWUNFFcOIcTY\n')
        htpasswd.flush()
        auth.initialize_auth(os.path.dirname(htpasswd.name),
                             os.path.basename(htpasswd.name))
        htpasswd.close()

    def test_auth_missing(self):
        ret = self.client.get('/authtest')
        self.assertEqual(401, ret.status_code)

    def test_auth_failure(self):
        environ = {'HTTP_AUTHORIZATION': 'basic dGVzdHVzZXI6YmFkcGFzcw=='}
        ret = self.client.get('/authtest', environ_overrides=environ)
        self.assertEqual(401, ret.status_code)

    def test_auth_success(self):
        environ = {'HTTP_AUTHORIZATION': 'basic dGVzdHVzZXI6dGVzdHBhc3M='}
        ret = self.client.get('/authtest', environ_overrides=environ)
        self.assertEqual(200, ret.status_code)


# vim:et:fdm=marker:sts=4:sw=4:ts=4
