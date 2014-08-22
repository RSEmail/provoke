
from setuptools import setup, find_packages


setup(name='provoke',
      version='0.0.0',
      author='Ian Good',
      author_email='ian.good@rackspace.com',
      packages=find_packages(),
      install_requires=[],
      extras_require={
          'amqp': ['amqp'],
          'api': ['flask', 'jsonschema', 'passlib'],
          'mysql': ['MySQL-python'],
      })
