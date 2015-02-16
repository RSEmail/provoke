
import sys

from setuptools import setup, find_packages


if sys.version_info[0] == 2:
    mysql = 'MySQL-python'
else:
    mysql = 'PyMySQL'


setup(name='provoke',
      version='0.3.2',
      author='Ian Good',
      author_email='ian.good@rackspace.com',
      description='Lightweight, asynchronous function execution in Python '
                  'using AMQP.',
      packages=find_packages(),
      install_requires=['amqp', 'six'],
      extras_require={
          'mysql': [mysql],
      },
      entry_points={
          'provoke.workers': ['example = provoke.example.worker:register'],
          'console_scripts': ['provoke-worker = provoke.worker.main:main']})
