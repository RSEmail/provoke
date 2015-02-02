from __future__ import print_function

import os

from provoke.amqp import AmqpConnection
from provoke.app import WorkerApplication
from provoke.worker import WorkerMaster


def do_work(*args):
    result = ' '.join(args)
    print('===> PID {0}: doing work!'.format(os.getpid()))
    print('Result:', result)
    print('===> PID {0}: done!'.format(os.getpid()))
    return result


with AmqpConnection() as channel:
    channel.queue_declare('do_work')

app = WorkerApplication()
app.register_task(do_work)

master = WorkerMaster(app)
master.add_worker(['do_work'], num_processes=4)
