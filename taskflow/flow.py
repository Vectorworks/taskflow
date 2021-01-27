from .tasks.task import BaseTask


class Flow(object):
    STATUS_NOT_STARTED = 'not-started'
    STATUS_RUNNING = 'running'
    STATUS_HALTED = 'halted'
    STATUS_COMPLETE = 'complete'

    def __init__(self, root_task: BaseTask):
        self.root_task = root_task
        self.status = self.STATUS_NOT_STARTED

        self.root_task.set_ids()

    def run(self, *args, **kwargs):
        while True:
            task = self._get_next()
            if not task:
                break
            task.run(*args, **kwargs)

        return self.root_task.last_result

    def step(self):
        task = self._get_next()
        if not task:
            return

        task.run()
        return task

    def _execute(self, task, *args, **kwargs):
        result = None
        for sub_task in task:
            if sub_task != task:
                self._execute(sub_task, *args, **kwargs)
            result = sub_task.run(*args, **kwargs)

        return result

    def _get_next(self):
        task = next(iter(self.root_task), None)
        while task and not task.ready_to_run:
            task = next(iter(task), None)

        return task

    def to_dict(self):
        return {
            'root_task': self.root_task.to_dict(),
            'status': self.status
        }
