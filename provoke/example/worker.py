from __future__ import print_function

import os

from provoke.app import WorkerApplication
from provoke.amqp import AmqpConnection


def do_work(*received):
    result = list(reversed(received))
    print('===> PID {0}: doing work!'.format(os.getpid()))
    print('Received:', ' '.join(received))
    print('Result:', ' '.join(result))
    print('===> PID {0}: done!'.format(os.getpid()))
    return result


def register(master, config):
    with AmqpConnection() as channel:
        channel.queue_declare('do_work')

    app = WorkerApplication()
    app.register_task(do_work)
    master.add_worker(app, ['do_work'], num_processes=4)
