
from setuptools import setup, find_packages


setup(name='provoke',
      version='0.2.1',
      author='Ian Good',
      author_email='ian.good@rackspace.com',
      description='Lightweight, asynchronous function execution in Python '
                  'using AMQP.',
      packages=find_packages(),
      install_requires=['amqp'],
      extras_require={
          'mysql': ['MySQL-python'],
      },
      entry_points={'console_scripts': [
          'provoke-worker = provoke.worker.main:main',
      ]})
