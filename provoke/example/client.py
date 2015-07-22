
from __future__ import print_function

import sys

from provoke.app import WorkerApplication
from provoke.amqp import AmqpConnection

with AmqpConnection() as channel:
    channel.queue_declare('do_work')

app = WorkerApplication()
app.declare_task('do_work')

args = sys.argv[1:] or ['World!', 'Hello']
print('Sending:', ' '.join(args))
res = app.tasks.do_work.apply_async(args, send_result=True)
print('Received result:', ' '.join(res.get()))
res.delete()
