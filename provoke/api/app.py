
"""This module provides the actual Flask application and can be executed in
debug mode to serve the API.

"""

from __future__ import absolute_import

import os
from ConfigParser import SafeConfigParser

from flask import Flask

from provoke.common.config import load_configuration, read_configuration_dir
from provoke.common.logging import setup_logging

__all__ = ['app']


config_dir = os.getenv('PROVOKE_CONFIG_DIR', '/etc/provoke')

app = Flask(__name__,
            instance_path=config_dir,
            instance_relative_config=True)
app.logger_name = 'provoke.api'
app.config.from_envvar('PROVOKE_FLASK_SETTINGS', silent=True)

configparser = read_configuration_dir(config_dir)
config = load_configuration(configparser)

setup_logging(debug=app.debug, syslog_facility='local5')

app.config['PROVOKE_TASKGROUPS'] = {}

if __name__ == '__main__':
    app.run(debug=True)


# vim:et:fdm=marker:sts=4:sw=4:ts=4
