
from __future__ import print_function

import sys

from provoke.app import WorkerApplication

app = WorkerApplication()
app.declare_task('do_work')

args = sys.argv[1:] or ['Hello', 'World!']
res = app.tasks.do_work.apply_async(args, send_result=True)
print('Received result:', res.get())
