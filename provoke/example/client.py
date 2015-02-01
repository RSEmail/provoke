
import sys

from provoke.common.app import WorkerApplication

app = WorkerApplication()
app.declare_taskgroup('test', routing_key='do_work')
app.declare_task('test', 'do_work')

args = sys.argv[1:] or ['Hello', 'World!']
app.tasks.do_work.apply_async(args)
