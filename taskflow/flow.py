from copy import deepcopy
from uuid import uuid4

from .tasks import BaseTask
from .type_helpers import type_from_string


class Flow(object):
    def __init__(self, task: BaseTask, uid=None, friendly_name=None):
        self.uid = uid or uuid4()
        self.friendly_name = friendly_name or ""
        self.root_task = BaseTask.find_root(task)

        # when deserializing, tasks will already have ids, that we want to preserve
        if not self.root_task.id:
            self.root_task.set_ids()

    @property
    def is_halted(self):
        # if we encounter a halted task, we want to halt the whole flow
        return self.root_task.leaf.is_halted

    @property
    def is_complete(self):
        return self.root_task.leaf.status == BaseTask.STATUS_COMPLETE

    def run(self, **kwargs):
        while True:
            task = self._get_next(self.root_task)
            if not task:
                break
            task.run(**kwargs)

        return self.root_task.leaf.result

    def step(self, **kwargs):
        while True:
            task = self._get_next(self.root_task)
            if not task:
                return

            if self._before_task_run(task):
                break

        task.run(**kwargs)
        self._after_task_run(task)

        return task

    def _before_task_run(self, _task):
        """
        Allow inheritors to choose not to run the particular task by returning False
        """
        return True

    def _after_task_run(self, _task):
        """
        Allow inheritors to run code after a task has been run
        """
        return None

    def _get_next(self, task):
        if self.is_halted:
            return None

        sub_tasks = task.get_all_tasks()
        for sub_task in sub_tasks:
            if sub_task == task:
                if sub_task.status == BaseTask.STATUS_PENDING:
                    return task
            else:
                next_task = self._get_next(sub_task)
                if next_task:
                    return next_task

        if task.status == BaseTask.STATUS_COMPLETE and task.next:
            return self._get_next(task.next)

        return None

    def to_list(self):
        return self.root_task.to_list()

    @classmethod
    def from_list(cls, task_list: list, uid=None, friendly_name=None):
        # tasks come in a possibly randomly ordered list
        # we need to figure out the correct order for creating them starting with leaves

        created = {}
        last_created = None
        remaining_tasks = deepcopy(task_list)

        while remaining_tasks:
            for task_data in remaining_tasks:
                task_depends_on = ([task_data["prev"]] if task_data["prev"] else []) + (
                    task_data.get("sub_tasks") or []
                )

                if all(depends_on in created for depends_on in task_depends_on):
                    task_type = type_from_string(task_data["class"])
                    task_data["sub_tasks"] = [created[task_id] for task_id in (task_data.get("sub_tasks") or [])]
                    task_data["prev"] = created[task_data["prev"]] if task_data["prev"] else None

                    last_created = task_type.from_data(task_data)

                    created[last_created.id] = last_created
                    remaining_tasks.remove(task_data)
                    break

        return cls(BaseTask.find_root(last_created), uid=uid, friendly_name=friendly_name)
