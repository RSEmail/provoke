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

"""Provides utilities used by the other resource implementations.

"""

from __future__ import unicode_literals, absolute_import

import traceback
import collections
from functools import wraps

from jsonschema import Draft4Validator, ValidationError, FormatChecker
from flask import request, json, current_app
from werkzeug.urls import url_unquote
from werkzeug.exceptions import HTTPException, BadRequest, UnsupportedMediaType
from werkzeug.wrappers import BaseResponse

from ..common.logging import log_error, log_warning


def unquote_url_values(endpoint, values):
    """Preprocessor that URL-decodes the values given in the URL.

    """
    for key, val in values.items():
        if isinstance(val, basestring):
            values[key] = url_unquote(val)


def load_schema(schema):
    """Validates the given schema and returns an associated schema validator
    object that can check other objects' conformance to the schema.

    :param schema: The JSON schema object.
    :returns: The object loaded in from the JSON schema file.

    """
    Draft4Validator.check_schema(schema)
    return Draft4Validator(schema, format_checker=FormatChecker())


def validate_json_request(schema):
    """Pulls the data from the request body, attempts to decode it as a JSON
    object, and then validates the response against the given schema.

    :param schema: An object giving the JSON schema for validation.
    :returns: The resulting, validated object from the request data.
    :raises: ValueError, ValidationError

    """
    if request.mimetype != 'application/json':
        raise UnsupportedMediaType('Only application/json is accepted.')
    data = request.json
    try:
        schema.validate(data)
    except ValidationError as exc:
        raise BadRequest(str(exc))
    return data


class JsonResponse(BaseResponse):
    """Implements a :class:`~werkzeug.wrappers.BaseResponse` such that a
    dictionary can be given the response data and it will be JSON encoded
    before transmission. The response will also include ``Content-Type:
    application/json`` in that case.

    :param response: If this argument is a :class:`~collections.Mapping` type,
                     then it is JSON encoded before the response is sent.
                     Otherwise, it is passed directly through to the response.
    :param args: Arguments passed into :class:`~werkzeug.wrappers.BaseResponse`
                 constructor.
    :type args: positional arguments
    :param kwargs: Arguments passed into
                   :class:`~werkzeug.wrappers.BaseResponse` constructor.
    :type kwargs: keyword arguments

    """

    def __init__(self, response=None, *args, **kwargs):
        if isinstance(response, collections.Mapping):
            response = json.dumps(response)
            kwargs.setdefault('content_type', 'application/json')
        super(JsonResponse, self).__init__(response, *args, **kwargs)


def json_response(old_f):
    """Decorator that expects a two-item tuple to be returned from the
    function, encodes the first item as a JSON blob, and returns the result as
    a Flask Response object.

    """
    @wraps(old_f)
    def new_f(*args, **kwargs):
        """Replaces the old function"""
        ret = old_f(*args, **kwargs)
        return JsonResponse(*ret)
    return new_f


def json_error_handler(exc):
    """Flask error handler that generates a :class:`~flask.Response` object
    from the given raised exception.

    :param exc: The exception object that was raised by the request.
    :type exc: Exception
    :rtype: :class:`flask.Response`

    """
    error = {'type': exc.__class__.__name__,
             'message': unicode(exc)}
    if isinstance(exc, HTTPException):
        error['code'] = exc.code
        if hasattr(exc, 'description'):
            error['message'] = exc.description
        log_warning('HTTP Exception', type=exc.name, value=error['message'])
    else:
        traceback.print_exc()
        log_error('Unhandled exception occured',
                  type=error['type'], value=error['message'])
        error['code'] = 500
        error['traceback'] = traceback.format_exc()
    response = JsonResponse(error)
    response.status_code = error['code']
    return response
