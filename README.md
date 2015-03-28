
provoke
=======

Lightweight, asynchronous function execution in Python using AMQP. Provoke was
inspired by [Celery][4], but intends to be *much* smaller and less invasive.

[![Build Status](https://travis-ci.org/icgood/provoke.svg?branch=master)](https://travis-ci.org/icgood/provoke)

##### [Documentation](http://provoke.readthedocs.org/)

### Installation

```
sudo pip install provoke
```

## Usage

The `provoke-worker` script manages a set of processes, each capable of pulling
task messages from an AMQP queue and executing them. The script uses an
external Python script, installed as plugins, to load the information about
processes and tasks.

To try it out, you will need a basic RabbitMQ broker running on localhost. When
you're ready, start up the provoke worker with the worker example plugin:

```
provoke-worker example
```

In another terminal, use the client example to send a task for execution:

```
python -m provoke.example.client
```

Try running the client example with some command-line arguments, and pay
attention to the different PIDs running the tasks.

As easy as that, you have four processes executing a simple task!

Definitely check out the source code for the [worker example][2] and
[client example][3] to see how it's done.

## Development

Start by creating a [virtualenv][1] in a clone of the repository:

    virtualenv .venv
    source .venv/bin/activate

Install the package in development mode. **Note:** Do not use `sudo`!

    python setup.py develop

The easiest way to run the tests is with `nosetests`. You need to install it
into the virtual environment, even if it is installed system-wide.

    pip install -r test/requirements.txt
    pip install MySQL-python
    nosetests -v

[1]: http://www.virtualenv.org/en/latest/
[2]: provoke/example/worker.py
[3]: provoke/example/client.py
[4]: http://www.celeryproject.org/
