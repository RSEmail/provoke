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

from __future__ import absolute_import

import flask
from flask.views import MethodView

from ..common.app import WorkerApplication

__all__ = ['RoutesBase']


class RoutesBase(MethodView):

    _worker_app = None
    tasks = []

    @classmethod
    def _build_worker_app(cls):
        cls._worker_app = app = WorkerApplication()
        taskgroups = flask.current_app.config['PROVOKE_TASKGROUPS']
        for taskgroup, info in taskgroups.items():
            app.declare_taskgroup(taskgroup, **info)
        for taskgroup, task_name in cls.tasks:
            app.declare_task(taskgroup, task_name)
        return app

    @property
    def worker_app(self):
        if self._worker_app:
            return self._worker_app
        return self._build_worker_app()
