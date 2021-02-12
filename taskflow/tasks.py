from taskflow.defaults import Defaults
from taskflow.type_helpers import function_from_string, function_to_string, type_to_string


class BaseTask(object):
    STATUS_PENDING = 'pending'
    STATUS_RUNNING = 'running'
    STATUS_HALTED = 'halted'
    STATUS_COMPLETE = 'complete'

    is_standalone = True

    def __init__(self, max_runs=None, needs_prev_result=True, name=None):
        self.max_runs = max_runs if max_runs is not None else Defaults.max_runs
        self._runs = 0
        self._status = self.STATUS_PENDING
        self._result = None
        self._error = None
        self._id = None
        self._name = name
        self._needs_prev_result = needs_prev_result

        self._prev = None
        self._next = None
        self._parent = None

    @property
    def id(self):
        return self._id

    @property
    def name(self):
        return self._name

    @property
    def needs_prev_result(self):
        return self._needs_prev_result

    @property
    def status(self):
        return self._status

    @property
    def error(self):
        return self._error

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
    def parent(self):
        return self._parent

    @property
    def runs(self):
        return self._runs

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

    @property
    def is_halted(self):
        if self.status == self.STATUS_HALTED:
            return True

        return self.prev.is_halted if self.prev else False

    def set_ids(self, starting_id=1):
        current_id = starting_id

        self._id = current_id
        current_id += 1

        if self._next:
            current_id = self._next.set_ids(starting_id=current_id)

        return current_id

    def run(self, **kwargs):
        raise NotImplementedError

    def _override_arguments(self, *args, **kwargs):
        if self._needs_prev_result:
            if self.prev:
                prev_result = [self.prev.result]
                return list(args) + prev_result, kwargs

            if self._parent:
                return self._parent._override_arguments(*args, **kwargs)

        return args, kwargs

    def get_all_tasks(self):
        return [self]

    @classmethod
    def find_root(cls, task):
        while task.prev or task.parent:
            task = task.prev or task.parent
        return task

    def then(self, task):
        if self._next:
            raise RuntimeError(
                'Unsupported operation. Multiple then operations are not support. Use a CompositeTask instead.')

        task = task.local_root
        self._next = task
        task._prev = self
        return task.leaf

    def _get_task_data(self):
        return {
            'class': type_to_string(type(self)),
            'max_runs': self.max_runs,
            'id': self._id,
            'name': self._name,
            'needs_prev_result': self._needs_prev_result,
            'runs': self._runs,
            'status': self._status,
            'result': self._result,
            'is_standalone': self.is_standalone
        }

    def to_list(self):
        result = [self._get_task_data()]
        result[0].update({
            'prev': self._prev.id if self._prev else None
        })

        if self._next:
            result.extend(self._next.to_list())

        return result

    @classmethod
    def from_data(cls, task_data):
        result = cls()

        result.max_runs = task_data['max_runs']
        result._runs = task_data['runs']
        result._status = task_data['status']
        result._result = task_data['result']
        result._id = task_data['id']
        result._name = task_data['name']
        result._needs_prev_result = task_data['needs_prev_result']

        if task_data['prev']:
            task_data['prev'].then(result)

        return result

    def __str__(self):
        return self._name if self._name else type_to_string(type(self))


class Task(BaseTask):
    def __init__(self, func=None, args=None, max_runs=None, needs_prev_result=True, name=None):
        super().__init__(max_runs=max_runs, needs_prev_result=needs_prev_result, name=name)
        self._func = func
        self._args = args or []

    @property
    def func_name(self):
        return function_to_string(self._func)

    @property
    def args(self):
        return self._args

    def run(self, **kwargs):
        # overriding args with the prev result
        # use kwargs for persistent parameters to all Tasks
        self._status = self.STATUS_RUNNING
        args, kwargs = self._override_arguments(*self._args, **kwargs)

        self._runs += 1
        try:
            self._result = self._func(*args, **kwargs)
            self._status = self.STATUS_COMPLETE
            self._error = None
            return self._result
        except Exception as ex:
            self._status = self.STATUS_HALTED if self._runs >= self.max_runs else self.STATUS_PENDING
            self._error = ex

    def __str__(self):
        return self._name if self._name else f'{function_to_string(self._func)}:{self._args}'

    def _get_task_data(self):
        result = super()._get_task_data()
        result.update({
            'func': function_to_string(self._func),
            'args': self._args
        })

        return result

    @classmethod
    def from_data(cls, task_data):
        result = super().from_data(task_data)
        result._func = function_from_string(task_data['func'])
        result._args = task_data['args']
        return result

    @classmethod
    def when(cls, *tasks, **kwargs):
        return CompositeTask(*tasks, **kwargs)


class CompositeTask(BaseTask):
    is_standalone = False

    def __init__(self, *sub_tasks, needs_prev_result=True, name=None):
        super().__init__(needs_prev_result=needs_prev_result, name=name)
        self._sub_tasks = [sub_task.local_root for sub_task in sub_tasks or []]
        for sub_task in self._sub_tasks:
            sub_task._parent = self

        # not a standalone task, so only the calculated property makes sense
        self._status = None

    @property
    def status(self):
        if any(sub_task.leaf.status == self.STATUS_HALTED for sub_task in self._sub_tasks):
            return self.STATUS_HALTED
        elif all(sub_task.leaf.status == self.STATUS_COMPLETE for sub_task in self._sub_tasks):
            return self.STATUS_COMPLETE
        return self.STATUS_PENDING

    @property
    def is_halted(self):
        if any(sub_task.leaf.is_halted for sub_task in self._sub_tasks):
            return True

        return super().is_halted

    @property
    def result(self):
        return [sub_task.leaf.result for sub_task in self._sub_tasks]

    def set_ids(self, starting_id=1):
        current_id = starting_id

        for index, sub_task in enumerate(self._sub_tasks):
            current_id = sub_task.set_ids(starting_id=current_id)

        return super().set_ids(starting_id=current_id)

    def get_all_tasks(self):
        return self._sub_tasks[:]

    def run(self, **kwargs):
        raise RuntimeError('Composite tasks cannot be run directly')

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
    def from_data(cls, task_data):
        result = super().from_data(task_data)
        result._sub_tasks = [sub_task.local_root for sub_task in task_data['sub_tasks'] or []]
        for sub_task in result._sub_tasks:
            sub_task._parent = result

        return result
