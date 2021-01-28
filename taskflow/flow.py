from uuid import uuid4

from .tasks.task import BaseTask


class Flow(object):
    def __init__(self, task: BaseTask):
        self.uid = uuid4()
        self.root_task = task.local_root
        self.root_task.set_ids()

    def run(self, **kwargs):
        while True:
            task = self._get_next(self.root_task)
            if not task:
                break
            task.run(**kwargs)

        return self.root_task.leaf.result

    def step(self):
        task = self._get_next(self.root_task)
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

    def _get_next(self, task):
        sub_tasks = task.get_all_tasks()
        for sub_task in sub_tasks:
            if sub_task == task:
                if sub_task.status == BaseTask.STATUS_PENDING:
                    return task
            else:
                next_task = self._get_next(sub_task)
                if next_task:
                    return next_task

        if task.next:
            return self._get_next(task.next)

        return None

    def to_dict(self):
        return {
            'root_task': self.root_task.to_dict(),
        }

    def to_list(self):
        return self.root_task.to_list()

    def from_list(self, task_list):
        pass