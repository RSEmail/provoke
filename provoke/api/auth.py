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

"""Provides a set of routines to enforce authentication in API routes, based on
credentials stored in an Apache-style htpasswd file.

"""

from __future__ import absolute_import

import os.path
from base64 import b64decode
from functools import wraps

from flask import request
from werkzeug.exceptions import Unauthorized
from passlib.apache import HtpasswdFile
from passlib.context import CryptContext

from ..common.logging import log_info

__all__ = ['initialize_auth', 'require_auth']


_htpasswd = None
_allow_anonymous = False


def initialize_auth(config_dir, filename='htpasswd', anonymous=False):
    """Initializes the authentication system with credential information from
    an htpasswd file.

    :param config_dir: The configuration directory where the file is located.
    :type config_dir: str
    :param filename: The name of the htpasswd file in the configuration
                     directory.
    :type filename: str
    :param optional: If ``True``, anonymous authentication will be allowed.
    :type optional: bool

    """
    global _htpasswd, _allow_anonymous
    path = os.path.join(config_dir, filename)
    context = CryptContext(['ldap_salted_sha1', 'plaintext'])
    _htpasswd = HtpasswdFile(path, context=context)
    _allow_anonymous = anonymous


def _get_credentials():
    auth_info = request.environ.get('HTTP_AUTHORIZATION', '')
    auth_type, _, auth_data = auth_info.partition(' ')
    if auth_type.lower() == 'basic':
        user, _, password = b64decode(auth_data).partition(':')
        return user, password or ''
    return '', ''


def _check_credentials(user, password):
    if _allow_anonymous and not user:
        log_info('Authentication successful', logger='auth', anonymous=True)
        return
    if not _htpasswd.verify(user, password):
        log_info('Authentication failure', logger='auth', user=user)
        raise Unauthorized('Authentication is required')
    log_info('Authentication successful', logger='auth', user=user)


def require_auth(endpoint, values):
    """Flask preprocessor that enforces authentication from an htpasswd file.

    """
    user, password = _get_credentials()
    _check_credentials(user, password)
