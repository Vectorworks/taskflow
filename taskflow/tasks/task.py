import inspect


class BaseTask(object):
    STATUS_PENDING = 'pending'
    STATUS_RUNNING = 'running'
    STATUS_HALTED = 'halted'
    STATUS_COMPLETE = 'complete'

    def __init__(self, max_retries=None):
        self.max_retries = max_retries or 0
        self._runs = 0
        self._status = self.STATUS_PENDING
        self._last_result = None
        self._id = None
        self._depends_on = []

    @property
    def id(self):
        return self._id

    @property
    def depends_on(self):
        return self._depends_on

    @property
    def status(self):
        return self._status

    @property
    def last_result(self):
        return self._last_result

    @property
    def ready_to_run(self):
        return True

    def set_ids(self, starting_id=1):
        self._id = starting_id
        return starting_id + 1

    def run(self, *args, **kwargs):
        raise NotImplemented

    def __iter__(self):
        return iter(self._get_pending_tasks())

    def _get_pending_tasks(self):
        return [self] if self._status == self.STATUS_PENDING else []

    def to_dict(self):
        return {
            '__class__': self.__class__.__name__,
            'max_retries': self.max_retries,
            '_id': self._id,
            '_depends_on': self._depends_on,
            '_runs': self._runs,
            '_status': self._status,
            '_last_result': self._last_result,
        }


class SimpleTask(BaseTask):
    def __init__(self, func, max_retries=None):
        super().__init__(max_retries=max_retries)
        self._func = func

    def run(self, *args, **kwargs):
        self._runs += 1
        self._status = self.STATUS_RUNNING
        try:
            self._last_result = self._func(*args, **kwargs)
            self._status = self.STATUS_COMPLETE
            return self._last_result
        except Exception:
            self._status = self.STATUS_HALTED if self._runs > self.max_retries else self.STATUS_RUNNING

    def __str__(self):
        return f'{self._func}:{self.max_retries}'

    def to_dict(self):
        result = super().to_dict()
        result.update({
            '_func': f'{self._get_func_module_name()}.{self._get_func_name()}'
        })

        return result

    def _get_func_module_name(self):
        func = getattr(self._func, '__func__', self._func)
        return func.__module__

    def _get_func_name(self):
        func = getattr(self._func, '__func__', self._func)
        return func.__qualname__


class CompositeTask(SimpleTask):
    def __init__(self, func, sub_tasks=None, independent_subtasks=False):
        super().__init__(func)
        self._sub_tasks = sub_tasks or []
        self._independent_subtasks = independent_subtasks

    def set_ids(self, starting_id=1):
        current_id = super().set_ids(starting_id=starting_id)

        for index, sub_task in enumerate(self._sub_tasks):
            current_id = sub_task.set_ids(starting_id=current_id)
            if index and self._independent_subtasks:
                sub_task.depends_on.push(self._sub_tasks[index - 1].id)

        self.depends_on.extend([sub_task.id for sub_task in self._sub_tasks])
        return current_id

    def _get_pending_tasks(self):
        return [task for task in self._sub_tasks if task.status == self.STATUS_PENDING] + super()._get_pending_tasks()

    def run(self, *args, **kwargs):
        args, kwargs = self._override_arguments(*args, **kwargs)
        return super().run(*args, **kwargs)

    @property
    def ready_to_run(self):
        return not [task for task in self._sub_tasks if task.status == self.STATUS_PENDING]

    def _override_arguments(self, *args, **kwargs):
        return [task.last_result for task in self._sub_tasks], kwargs

    def to_dict(self):
        result = super().to_dict()
        result.update({
            '_sub_tasks': [sub_task.to_dict() for sub_task in self._sub_tasks],
            '_independent_subtasks': self._independent_subtasks,
        })

        return result


# class RepeatTask(BaseTask):
#     def __init__(self, task: BaseTask, count: int):
#         super().__init__()
#         self.task = task
#         self.count = count
#
#     def run(self, *args, **kwargs):
#         return [self.task.run(*args, **kwargs) for i in range(self.count)]
