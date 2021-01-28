from copy import deepcopy
from uuid import uuid4

from taskflow.tasks import BaseTask
from .type_helpers import type_from_string


class Flow(object):
    def __init__(self, task: BaseTask, friendly_name=None):
        self.uid = uuid4()
        self.friendly_name = friendly_name or ''
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

    @classmethod
    def from_list(cls, task_list: list):
        # tasks come in a possibly randomly ordered list
        # we need to figure out the correct order for creating them starting with leaves

        created = {}
        last_created = None
        remaining_tasks = deepcopy(task_list)

        while remaining_tasks:
            for task_data in remaining_tasks:
                task_depends_on = ([task_data['prev']] if task_data['prev'] else []) + \
                                  (task_data.get('sub_tasks') or [])

                if all(depends_on in created for depends_on in task_depends_on):
                    task_type = type_from_string(task_data['class'])
                    task_data['sub_tasks'] = [created[task_id] for task_id in task_data.get('sub_tasks', [])]
                    task_data['prev'] = created[task_data['prev']] if task_data['prev'] else None

                    last_created = task_type.from_dict(task_data)

                    created[last_created.id] = last_created
                    remaining_tasks.remove(task_data)
                    break

        return Flow(last_created)
