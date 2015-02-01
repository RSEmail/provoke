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

import pkg_resources

__all__ = ['__version__', 'import_attr']

__version__ = pkg_resources.require('provoke')[0].version


def import_attr(what, default_attr=None):
    """Attempts to load an attribute from a module. The ``what`` argument must
    at least include an absolute, importable module name, and optionally may
    include a ``:`` separating an attribute name to load from the module.

    :param what: An absolute, importable module name, optionally with an
                 attribute name separated by a colon character.
    :type what: str
    :param default_attr: The name of the attribute to load, if no attribute
                         name was given in ``what``.
    :type default_attr: str
    :raises: ImportError, AttributeError, ValueError

    """
    importable, _, attr = what.partition(':')
    attr = attr or default_attr
    mod = __import__(importable, fromlist=[attr], level=0)
    return getattr(mod, attr)
