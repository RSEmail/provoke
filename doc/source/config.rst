
Provoke Configuration
=====================

.. |ConfigParser| replace:: :py:class:`~configparser.ConfigParser`

A configuration file is not required for using provoke, but it does add some
distinctly necessary control for larger applications.

Config files can be passed in using the ``--config`` option of *provoke*. They
are loaded using Python's built-in |ConfigParser| class.  Provoke will read its
configuration and then pass the |ConfigParser| object to the plugin init
function.

``[daemon]`` Section
--------------------

* ``logging_config``: String

  If given, specifies the full path to a file containing logging configuration.
  This file name is passed in to the :py:class:`logging.config.fileConfig`
  function.

  .. seealso:: `16.7.3. Configuration file format <https://docs.python.org/3.4/library/logging.config.html#configuration-file-format>`_

* ``max_fd``: Integer

  If given, this number will become the maximum file descriptor limit for the
  provoke worker processes.

  .. seealso:: :py:func:`resource.setrlimit`

* ``pidfile``: String

  If given, this file will be created and populated with the PID of the master
  process that manages provoke worker processes. If the worker is killed, this
  file is removed.

* ``stdout``, ``stderr``, ``stdin``: String

  These options, when used, refer to filenames that will be used for standard
  IO streams when daemonized. The files are opened in append mode.

* ``user``, ``group``: String

  The provoke master process will maintain the privileges it had when it was
  started. Child worker processes, when these options are given, will drop
  privileges to the user and group specified.

  .. seealso:: :py:func:`os.setuid`, :py:func:`os.setgid`

* ``umask``: String

  When given, child processes will set their umask to the given value. This
  value may be in octal format, e.g. ``0o755``.

``[amqp]`` Section
------------------

* ``host``: String
* ``port``: Integer
* ``virtual_host``: String
* ``user``: String
* ``password``: String
* ``heartbeat``: Float
* ``connect_timeout``: Float

External Connection Sections
----------------------------

Provoke comes with some convenience modules for configuring and using external
resources using various protocols. For protocols that support it, these
connections will be pooled and recycled.

Rather than passing around connection information everywhere that needs it,
these configuration sections make connections available by name.

MySQL Connections
"""""""""""""""""

* ``host``: String
* ``port``: Integer
* ``user``: String
* ``password``: String
* ``database``: String
* ``charset``: String
* ``unix_socket``: String
* ``connect_timeout``: Integer

Example section::

  [database:my-database]
  host = mysql1.database.fqdn
  database = stuff

Example usage::

  with MySQLConnection('my-database') as my_db:
      cur = my_db.conn.cursor()
      try:
          cur.execute("""SELECT ...""")
      finally:
          cur.close()

.. note:: Python2.6+ uses `MySQL-python <https://pypi.python.org/pypi/MySQL-python/>`_,
   and Python 3+ uses `pymysql <https://pypi.python.org/pypi/PyMySQL>`_.

HTTP Connections
""""""""""""""""

* ``host``: String
* ``port``: Integer
* ``user``: String

  When given, combines with the ``password`` option to create an
  ``Authorization`` header with ``Basic`` credentials.

* ``password``: String
* ``timeout``: Integer
* ``ssl``: Boolean
* ``key_file``: String
* ``cert_file``: String

Example section::

  [http:my-api]
  host = api1.http.fqdn

Example usage::

  with HttpConnection('my-api') as conn, headers:
      conn.request('GET', '/', headers=headers)
      res = conn.getresponse()
      assert res.status == 200

