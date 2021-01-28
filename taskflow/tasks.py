from taskflow.defaults import Defaults
from taskflow.type_helpers import function_name, function_module_name, type_to_string, function_from_string


class BaseTask(object):
    STATUS_PENDING = 'pending'
    STATUS_RUNNING = 'running'
    STATUS_HALTED = 'halted'
    STATUS_COMPLETE = 'complete'

    is_standalone = True

    def __init__(self, max_retries=None):
        self.max_retries = max_retries if max_retries is not None else Defaults.max_retries
        self._runs = 0
        self._status = self.STATUS_PENDING
        self._result = None
        self._id = None

        self._prev = None
        self._next = None
        self._parent = None

    @property
    def id(self):
        return self._id

    @property
    def status(self):
        return self._status

    @property
    def result(self):
        return self._result

    @property
    def prev(self):
        return self._prev

    @property
    def next(self):
        return self._next

    @property
    def local_root(self):
        root = self
        while root.prev:
            root = root.prev

        return root

    @property
    def leaf(self):
        leaf = self
        while leaf.next:
            leaf = leaf.next

        return leaf

    def set_ids(self, starting_id=1):
        current_id = starting_id

        self._id = current_id
        current_id += 1

        if self._next:
            current_id = self._next.set_ids(starting_id=current_id)

        return current_id

    def run(self, **kwargs):
        raise NotImplemented

    def _override_arguments(self, *args, **kwargs):
        if self.prev:
            return self.prev.result, kwargs

        if self._parent:
            return self._parent._override_arguments(*args, **kwargs)

        return args, kwargs

    def get_all_tasks(self):
        return [self]

    def then(self, task):
        if self._next:
            raise RuntimeError(
                'Unsupported operation. Multiple then operations are not support. Use a CompositeTask instead.')

        self._next = task
        task._prev = self
        return task

    def to_dict(self):
        result = self._get_single_task_dict()
        result.update({
            'next': self._next.to_dict() if self._next else None
        })

        return result

    @classmethod
    def from_dict(cls, task_data):
        result = cls()

        result.max_retries = task_data['max_retries']
        result._runs = task_data['runs']
        result._status = task_data['status']
        result._result = task_data['result']
        result._id = task_data['id']

        if task_data['prev']:
            task_data['prev'].then(result)

        return result

    def _get_single_task_dict(self):
        return {
            'class': type_to_string(type(self)),
            'max_retries': self.max_retries,
            'id': self._id,
            'runs': self._runs,
            'status': self.status,
            'result': self._result,
            'is_standalone': self.is_standalone
        }

    def to_list(self):
        result = [self._get_single_task_dict()]
        result[0].update({
            'prev': self._prev.id if self._prev else None
        })

        if self._next:
            result.extend(self._next.to_list())

        return result


class Task(BaseTask):
    def __init__(self, func=None, args=None, max_retries=None):
        super().__init__(max_retries=max_retries)
        self._func = func
        self._args = args or []

    def run(self, **kwargs):
        # overriding args with the prev result
        # use kwargs for persistent parameters to all Tasks
        self._status = self.STATUS_RUNNING
        args, kwargs = self._override_arguments(*self._args, **kwargs)

        self._runs += 1
        try:
            self._result = self._func(*args, **kwargs)
            self._status = self.STATUS_COMPLETE
            return self._result
        except Exception:
            self._status = self.STATUS_HALTED if self._runs > self.max_retries else self.STATUS_PENDING

    def __str__(self):
        return f'{self._func}:{self.max_retries}'

    def _get_single_task_dict(self):
        result = super()._get_single_task_dict()
        result.update({
            'func': f'{function_module_name(self._func)}.{function_name(self._func)}',
            'args': self._args
        })

        return result

    @classmethod
    def from_dict(cls, task_data):
        result = super().from_dict(task_data)
        result._func = function_from_string(task_data['func'])
        result._args = task_data['args']
        return result

    @classmethod
    def when(cls, *tasks):
        return CompositeTask(*tasks)


class CompositeTask(BaseTask):
    is_standalone = False

    def __init__(self, *sub_tasks):
        super().__init__()
        self._sub_tasks = [sub_task.local_root for sub_task in sub_tasks or []]
        for sub_task in self._sub_tasks:
            sub_task._parent = self

        # not a standalone task, so only the calculated property makes sense
        self._status = None

    @property
    def status(self):
        if any(sub_task.status == self.STATUS_HALTED for sub_task in self._sub_tasks):
            return self.STATUS_HALTED
        elif any(sub_task.status in [self.STATUS_PENDING, self.STATUS_RUNNING] for sub_task in self._sub_tasks):
            return self.STATUS_PENDING
        return self.STATUS_COMPLETE

    @property
    def result(self):
        return [sub_task.leaf.result for sub_task in self._sub_tasks]

    def set_ids(self, starting_id=1):
        current_id = starting_id

        for index, sub_task in enumerate(self._sub_tasks):
            current_id = sub_task.set_ids(starting_id=current_id)

        return super().set_ids(starting_id=current_id)

    def get_all_tasks(self):
        # todo: clone maybe?
        return self._sub_tasks

    def run(self, **kwargs):
        raise RuntimeError('Composite tasks cannot be run directly')

    def to_dict(self):
        result = super().to_dict()
        result.update({
            'sub_tasks': [sub_task.to_dict() for sub_task in self._sub_tasks],
        })

        return result

    def to_list(self):
        result = []
        for sub_task in self._sub_tasks:
            result += sub_task.to_list()

        base_list = super().to_list()
        base_list[0].update({
            'sub_tasks': [sub_task.id for sub_task in self._sub_tasks]
        })

        return result + base_list

    @classmethod
    def from_dict(cls, task_data):
        result = super().from_dict(task_data)
        result._sub_tasks = [sub_task.local_root for sub_task in task_data['sub_tasks'] or []]
        for sub_task in result._sub_tasks:
            sub_task._parent = result

        return result