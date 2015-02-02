
from setuptools import setup, find_packages


setup(name='provoke',
      version='0.2.2',
      author='Ian Good',
      author_email='ian.good@rackspace.com',
      description='Lightweight, asynchronous function execution in Python '
                  'using AMQP.',
      packages=find_packages(),
      install_requires=['amqp', 'six'],
      extras_require={
          'mysql': ['PyMySQL'],
      },
      entry_points={'console_scripts': [
          'provoke-worker = provoke.worker.main:main',
      ]})
