provoke
=======

Foundation for building a web API for calling Python functions asynchronously.

## Development

Start by creating a [virtualenv][1] in a clone of the repository:

    virtualenv .venv
    source .venv/bin/activate

Install the package in development mode. **Note:** Do not use `sudo`!

    python setup.py develop

The easiest way to run the tests is with `nosetests`. You need to install it
into the virtual environment, even if it is installed system-wide.

    pip install -r test/requirements.txt
    nosetests

[1]: http://www.virtualenv.org/en/latest/
