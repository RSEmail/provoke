provoke
=======

Foundation for building a web API for calling Python functions asynchronously.

## API Usage

## Worker Usage

The `provoke-worker` script manages a set of processes, each capable of
pulling task messages from an AMQP queue and executing them. The script uses
an external Python script, given in configuration or on the command-line, to
load the information about processes and tasks.

To try it out, you will need a basic RabbitMQ broker running on localhost, with
a single queue: `work_queue`.

Next, create `test.py` and populate it with this:

```python
import os

from provoke.common.app import WorkerApplication
from provoke.worker import WorkerMaster


def do_work(one, two):
    print '===> PID {0}: doing work!'.format(os.getpid())
    print one
    print two
    print '===> PID {0}: done!'.format(os.getpid())


app = WorkerApplication()
app.register_task(do_work)

master = WorkerMaster(app)
master.add_worker(['work_queue'], num_processes=4)
```

Start up the provoke worker:

```
provoke-worker --worker-master test:master
```

Finally, send messages to your AMQP `work_queue`:

```json
{
  "task": "do_work",
  "args": ["Hello", "World!"]
}
```

As easy as that, you have four processes executing a simple task!

## Development

Start by creating a [virtualenv][1] in a clone of the repository:

    virtualenv .venv
    source .venv/bin/activate

Install the package in development mode. **Note:** Do not use `sudo`!

    python setup.py develop

The easiest way to run the tests is with `nosetests`. You need to install it
into the virtual environment, even if it is installed system-wide.

    pip install -r test/requirements.txt
    nosetests -v

[1]: http://www.virtualenv.org/en/latest/
