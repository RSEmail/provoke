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

"""Provides a subclass of :class:`flask.Blueprint` that is customized for use
as a provoke RESTful API blueprint.

"""

from __future__ import unicode_literals, absolute_import

from flask import Blueprint as FlaskBlueprint
from flask.views import MethodView
from werkzeug.exceptions import default_exceptions

from .util import unquote_url_values, json_error_handler

__all__ = ['Blueprint', 'RouteBase']


class RouteBase(MethodView):
    """Intended as a parent class for implementing an API route. This class
    inherits from Flask's :class:`~flask.views.MethodView`, so you should
    implement methods that correpond to the HTTP verb, e.g. ``.post()`` for
    ``POST`` requests.

    """

    def __init__(self, worker_app):
        super(RouteBase, self).__init__()
        self.worker_app = worker_app


class Blueprint(FlaskBlueprint):
    """This blueprint subclass is customized for RESTful practices that use
    JSON payloads for requests and responses. It has a URL preprocessor that
    URL-decodes any parameters given in the URL of a request. Exceptions are
    handled with :class:`~provoke.api.util.json_error_handler`.

    It's up to the request handler methods to ensure they return JSON responses
    under normal circumstances. You can use the
    :func:`~provoke.api.util.json_response` decorator to ensure dict objects
    are serialized into JSON responses.

    :param worker_app: The application backend that knows how to enqueue and
                       execute tasks. This object will be accessible to routes
                       as their ``self.worker_app`` attribute.
    :type worker_app: :class:`~provoke.common.app.WorkerApplication`

    """

    def __init__(self, worker_app=None, *args, **kwargs):
        super(Blueprint, self).__init__(*args, **kwargs)
        self.url_value_preprocessor(unquote_url_values)
        self.errorhandler(Exception)(json_error_handler)
        for code in default_exceptions.keys():
            if code != 500:
                self.errorhandler(code)(json_error_handler)
        self.worker_app = worker_app

    def register_route(self, route, view_class, view_name=None):
        """Blueprint route registration convenience function. It registers
        ``view_class`` to handle requests on the given route.

        :param route: The URL route template to register.
        :type route: str
        :param view_class: The class used to handle requests on the route.
        :type view_class: :class:`flask.views.View`
        :param view_name: Custom view name to use instead of
                          ``view_class.__name__``.
        :type view_name: str

        """
        view_name = view_class.__name__
        if issubclass(view_class, RouteBase):
            view_func = view_class.as_view(view_name, self.worker_app)
        else:
            raise TypeError('Expected sub-class of RouteBase.')
        self.add_url_rule(route, view_func=view_func)
