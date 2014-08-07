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

"""This module provides the actual Flask application. This app should be
imported and routes/blueprints registered against it.

"""

from __future__ import absolute_import

import os

from flask import Flask

__all__ = ['get_flask_app']


def get_flask_app(app, config_dir='/etc/provoke', syslog_facility='local5',
                  debug=False):
    """Builds and returns a ``Flask`` object which serves as the WSGI
    application for the API.

    :param app: The application backend that knows how to enqueue and execute
                tasks.
    :type app: :class:`~provoke.common.app.WorkerApplication`
    :param config_dir: Serves as the root directory where config files are
                       found.
    :type config_dir: str
    :param syslog_facility: The syslog facility where API logs are sent.
    :type syslog_facility: str
    :param debug: Whether flask should run in debug mode and use debug-level
                  logging.
    :type debug: bool

    """
    app = Flask(__name__,
                debug=debug,
                instance_path=config_dir,
                instance_relative_config=True)
    app.logger_name = 'provoke.api'
    app.config.from_envvar('PROVOKE_FLASK_SETTINGS', silent=True)
    app.config['PROVOKE_APP'] = app

    configparser = read_configuration_dir(config_dir)
    config = load_configuration(configparser)

    initialize_auth(config_dir)

    setup_logging(debug=app.debug, syslog_facility=syslog_facility)
