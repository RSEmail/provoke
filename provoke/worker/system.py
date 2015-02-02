
"""Contains functions to simplify the usual daemonization procedures for long-
running processes.

"""

from __future__ import absolute_import

import os
import os.path
import sys
from pwd import getpwnam
from grp import getgrnam

__all__ = ['daemonize', 'redirect_stdio', 'drop_privileges', 'PidFile']


def daemonize():
    """Daemonizes the current process using the standard double-fork.
    This function does not affect standard input, output, or error.

    :returns: The PID of the daemonized process.

    """

    # Fork once.
    try:
        pid = os.fork()
        if pid > 0:
            os._exit(0)
    except OSError:
        return

    # Set some options to detach from the terminal.
    os.chdir('/')
    os.setsid()
    os.umask(0)

    # Fork again.
    try:
        pid = os.fork()
        if pid > 0:
            os._exit(0)
    except OSError:
        return

    os.setsid()
    return os.getpid()


def redirect_stdio(stdout=None, stderr=None, stdin=None):
    """Redirects standard output, error, and input to the given
    filenames. Standard output and error are opened in append-mode, and
    standard input is opened in read-only mode. Leaving any parameter
    blank leaves that stream alone.

    :param stdout: filename to append the standard output stream into.
    :param stderr: filename to append the standard error stream into.
    :param stdin: filename to read from as the standard input stream.

    """

    # Find the OS /dev/null equivalent.
    nullfile = getattr(os, 'devnull', '/dev/null')

    # Redirect all standard I/O to /dev/null.
    sys.stdout.flush()
    sys.stderr.flush()
    si = open(stdin or nullfile, 'r')
    so = open(stdout or nullfile, 'a+')
    se = open(stderr or nullfile, 'a+', 0)
    os.dup2(si.fileno(), sys.stdin.fileno())
    os.dup2(so.fileno(), sys.stdout.fileno())
    os.dup2(se.fileno(), sys.stderr.fileno())

    # Set permissions as necessary.
    if stdin:
        os.fchmod(si.fileno(), 0o600)
    if stdout:
        os.fchmod(so.fileno(), 0o600)
    if stderr:
        os.fchmod(se.fileno(), 0o600)


def drop_privileges(user=None, group=None, mask=None):
    """Uses system calls to drop privileges to the given user and group.  This
    is useful for security purposes, once root-only ports like 25 are opened.

    .. seealso:: :func:`os.setuid`, :func:`os.setgid`, :func:`os.umask`

    :param user: user name (from /etc/passwd) or UID.
    :param group: group name (from /etc/group) or GID.
    :param mask: file mode creation mask.

    """
    if group:
        try:
            gid = int(group)
        except ValueError:
            gid = getgrnam(group).gr_gid
        os.setgid(gid)
    if user:
        try:
            uid = int(user)
        except ValueError:
            uid = getpwnam(user).pw_uid
        os.setuid(uid)
    if mask:
        os.umask(mask)


class PidFile(object):
    """Context manager which creates a PID file containing the current process
    id, runs the context, and then removes the PID file.

    An :py:exc:`OSError` exceptions when creating the PID file will be
    propogated without executing the context.

    :param filename: The filename to use for the PID file. If ``None`` is
                     given, the context is simply executed with no PID file
                     created.

    """

    def __init__(self, filename=None):
        super(PidFile, self).__init__()
        if not filename:
            self.filename = None
        else:
            self.filename = os.path.abspath(filename)

    def __enter__(self):
        if self.filename:
            with open(self.filename, 'w') as pid:
                pid.write('{0}\n'.format(os.getpid()))
            return self.filename

    def __exit__(self, exc_type, exc_value, traceback):
        if self.filename:
            try:
                os.unlink(self.filename)
            except OSError:
                pass
