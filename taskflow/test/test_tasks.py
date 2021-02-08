import random

import pytest

from taskflow.defaults import Defaults
from taskflow.tasks import BaseTask, CompositeTask, Task

from .fixtures import Handlers


class TestBaseTask(object):
    def test_init__empty(self):
        task = BaseTask()
        assert task.max_runs == Defaults.max_runs
        assert task._runs == 0
        assert task._status == BaseTask.STATUS_PENDING
        assert task._result is None
        assert task._id is None
        assert task._name is None
        assert task._prev is None
        assert task._next is None
        assert task._parent is None

    def test_init__max_runs(self):
        task = BaseTask(max_runs=25)
        assert task.max_runs == 25
        assert task._runs == 0
        assert task._status == BaseTask.STATUS_PENDING
        assert task._result is None
        assert task._id is None
        assert task._name is None
        assert task._prev is None
        assert task._next is None
        assert task._parent is None

    def test_init__name(self):
        task = BaseTask(name='Test name')
        assert task.max_runs == Defaults.max_runs
        assert task._runs == 0
        assert task._status == BaseTask.STATUS_PENDING
        assert task._result is None
        assert task._id is None
        assert task._name == 'Test name'
        assert task._needs_prev_result is True
        assert task._prev is None
        assert task._next is None
        assert task._parent is None

    def test_id(self):
        task = BaseTask()
        task._id = random.random()
        assert task.id == task._id

    def test_name(self):
        task = BaseTask()
        task._name = 'some name'
        assert task.name == task._name

    def test_needs_prev_results(self):
        task = BaseTask()
        task._needs_prev_result = False
        assert task.needs_prev_result == task._needs_prev_result

    def test_status(self):
        task = BaseTask()
        task._status = BaseTask.STATUS_HALTED
        assert task.status == task._status

    def test_result(self):
        task = BaseTask()
        task._result = 12345
        assert task.result == task._result

    def test_error(self):
        task = BaseTask()
        task._error = 'err'
        assert task.error == task._error

    def test_runs(self):
        task = BaseTask()
        task._runs = 5
        assert task.runs == task._runs

    def test_prev(self):
        task = BaseTask()
        task._prev = 'Illegal'
        assert task.prev == task._prev

    def test_next(self):
        task = BaseTask()
        task._next = 'Illegal'
        assert task.next == task._next

    def test_prent(self):
        task = BaseTask()
        task._parent = 'Illegal'
        assert task.parent == task._parent

    def test_then(self):
        root = BaseTask()
        mid = BaseTask()
        leaf = BaseTask()

        result = root.then(mid).then(leaf)
        assert result == leaf

        assert root.prev is None
        assert root.next == mid

        assert mid.prev == root
        assert mid.next == leaf

        assert leaf.prev == mid
        assert leaf.next is None

        assert root.leaf == leaf
        assert leaf.local_root == root

    def test_then_again(self):
        root = BaseTask()
        mid = BaseTask()
        leaf = BaseTask()

        with pytest.raises(RuntimeError):
            root.then(mid)
            root.then(leaf)

    def test_is_halted_not(self):
        task = BaseTask()
        task._status = BaseTask.STATUS_PENDING
        assert not task.is_halted

    def test_is_halted_direct(self):
        task = BaseTask()
        task._status = BaseTask.STATUS_HALTED
        assert task.is_halted

    def test_is_halted_prev(self):
        root = BaseTask()
        mid = BaseTask()
        leaf = BaseTask()

        root.then(mid).then(leaf)
        root._status = BaseTask.STATUS_HALTED
        assert leaf.is_halted

    def test_set_ids(self):
        root = BaseTask()
        mid = BaseTask()
        leaf = BaseTask()
        root.then(mid).then(leaf)

        result = root.set_ids(starting_id=10)
        assert root.id == 10
        assert mid.id == 11
        assert leaf.id == 12
        assert result == 13

    def test_run(self):
        task = BaseTask()
        with pytest.raises(NotImplementedError):
            task.run()

    def test_override_arguments_no_override(self):
        task = BaseTask()
        args, kwargs = task._override_arguments(*(1, 2), **{'1': 1, '2': 2})
        assert tuple(args) == (1, 2)
        assert kwargs == {'1': 1, '2': 2}

    def test_override_arguments_prev(self):
        root = BaseTask()
        leaf = BaseTask()
        root.then(leaf)
        root._result = 12345

        args, kwargs = leaf._override_arguments(*(1, 2), **{'1': 1, '2': 2})
        assert tuple(args) == (1, 2, 12345)
        assert kwargs == {'1': 1, '2': 2}

    def test_override_arguments_prev_no_needs_prev_result(self):
        root = BaseTask()
        leaf = BaseTask()
        root.then(leaf)
        root._result = 12345

        leaf._needs_prev_result = False
        args, kwargs = leaf._override_arguments(*(1, 2), **{'1': 1, '2': 2})
        assert tuple(args) == (1, 2)
        assert kwargs == {'1': 1, '2': 2}

    def test_override_arguments_parent(self):
        root = BaseTask()
        leaf = BaseTask()
        root.then(leaf)
        root._result = 12345

        task = BaseTask()
        task._parent = leaf

        args, kwargs = task._override_arguments(*(1, 2), **{'1': 1, '2': 2})
        assert tuple(args) == (1, 2, 12345)
        assert kwargs == {'1': 1, '2': 2}

    def test_override_arguments_parent_no_needs_prev_result(self):
        root = BaseTask()
        leaf = BaseTask()
        root.then(leaf)
        root._result = 12345

        task = BaseTask()
        task._parent = leaf

        leaf._needs_prev_result = False
        args, kwargs = task._override_arguments(*(1, 2), **{'1': 1, '2': 2})
        assert tuple(args) == (1, 2)
        assert kwargs == {'1': 1, '2': 2}

    def test_get_all_tasks(self):
        task = BaseTask()
        assert task.get_all_tasks() == [task]

    def test_find_root(self):
        root = BaseTask()
        leaf = BaseTask()
        root.then(leaf)

        task = BaseTask()
        task._parent = leaf

        assert BaseTask.find_root(leaf) == root

    def test_get_task_data(self):
        task = BaseTask()
        task.max_runs = 5
        task._runs = 3
        task._status = BaseTask.STATUS_RUNNING
        task._id = 4
        task._name = 'asdf'

        data = task._get_task_data()
        assert data['class'] == 'taskflow.tasks.BaseTask'
        assert data['max_runs'] == task.max_runs
        assert data['id'] == task.id
        assert data['name'] == task.name
        assert data['runs'] == task._runs
        assert data['status'] == task._status
        assert data['result'] == task._result
        assert data['is_standalone'] == task.is_standalone

    def test_to_list(self):
        root = BaseTask()
        mid = BaseTask()
        leaf = BaseTask()
        root.then(mid).then(leaf)
        root.set_ids(starting_id=10)

        tasks_data = root.to_list()
        assert len(tasks_data) == 3

        assert tasks_data[0]['id'] == 10
        assert tasks_data[0]['prev'] is None

        assert tasks_data[1]['id'] == 11
        assert tasks_data[1]['prev'] == 10

        assert tasks_data[2]['id'] == 12
        assert tasks_data[2]['prev'] == 11

    def test_from_data(self):
        task = BaseTask()
        task.max_runs = 5
        task._runs = 3
        task._status = BaseTask.STATUS_RUNNING
        task._id = 4
        task._name = 'asdf'
        task.set_ids()

        data = task.to_list()[0]
        prev_task = BaseTask()
        data['prev'] = prev_task
        task1 = BaseTask.from_data(data)

        assert type(task1) == BaseTask
        assert task1.max_runs == task.max_runs
        assert task1._id == task._id
        assert task1._name == task._name
        assert task1._runs == task._runs
        assert task1._status == task._status
        assert task1._result == task._result
        assert task1.prev == prev_task

    def test_str(self):
        task = BaseTask()
        assert str(task) == 'taskflow.tasks.BaseTask'

    def test_str_name(self):
        task = BaseTask(name='test')
        assert str(task) == 'test'


class TestTask(object):
    def test_init(self):
        task = Task(func=Handlers.repeat, args=(1, 2), max_runs=25, name='test')
        assert task.max_runs == 25
        assert task.name == 'test'
        assert task._func == Handlers.repeat
        assert task._args == (1, 2)

    def test_func_name(self):
        task = Task(func=Handlers.repeat)
        assert task.func_name == 'taskflow.test.fixtures.Handlers.repeat'

    def test_args(self):
        task = Task(func=Handlers.repeat, args=(1, 2,))
        assert task.args == (1, 2,)

    def test_run(self, mocker):
        mocker.patch('taskflow.test.fixtures.Handlers.repeat')
        task = Task(func=Handlers.repeat, args=(1, 2))
        task.run(**{'1': 1})

        assert task.status == BaseTask.STATUS_COMPLETE
        assert task.error is None
        assert task._runs == 1
        Handlers.repeat.assert_called_once_with(*(1, 2), **{'1': 1})

    def test_run_fail(self, mocker):
        mocker.patch('taskflow.test.fixtures.Handlers.repeat', side_effect=Exception('Boom'))
        task = Task(func=Handlers.repeat, args=(1, 2))
        task.run()

        assert task.status == BaseTask.STATUS_PENDING
        assert isinstance(task.error, Exception)
        assert task._runs == 1

    def test_run_fail_halt(self, mocker):
        mocker.patch('taskflow.test.fixtures.Handlers.repeat', side_effect=Exception('Boom'))
        task = Task(func=Handlers.repeat, args=(1, 2), max_runs=1)
        task.run()

        assert task.status == BaseTask.STATUS_HALTED
        assert task._runs == 1

    def test_str(self):
        task = Task(func=Handlers.repeat, args=(1, 2))
        assert str(task) == f'taskflow.test.fixtures.Handlers.repeat:{(1, 2)}'

    def test_str_name(self):
        task = Task(func=Handlers.repeat, args=(1, 2), name='test')
        assert str(task) == 'test'

    def test_get_task_data(self):
        task = Task(func=Handlers.repeat, args=(1, 2), name='test')
        data = task._get_task_data()
        assert data['name'] == 'test'
        assert data['func'] == 'taskflow.test.fixtures.Handlers.repeat'
        assert data['args'] == (1, 2)

    def test_from_data(self):
        task = Task(func=Handlers.repeat, args=(1, 2))
        data = task.to_list()[0]
        task1 = Task.from_data(data)

        assert type(task1) == Task
        assert task1._func == Handlers.repeat
        assert task1._args == (1, 2)

    def test_when(self):
        tasks = [Task(func=Handlers.repeat), Task(func=Handlers.repeat)]
        composite = Task.when(*tasks)
        assert type(composite) == CompositeTask
        assert composite._sub_tasks == tasks


class TestComposite(object):
    def test_init(self):
        sub1 = Task(func=Handlers.repeat)
        sub2 = Task(func=Handlers.repeat)
        sub21 = Task(func=Handlers.repeat)
        sub2.then(sub21)

        task = CompositeTask(sub1, sub21, name='composite-test')
        assert task.name == 'composite-test'
        assert task._sub_tasks == [sub1, sub2]
        assert sub1.parent == task
        assert sub2.parent == task
        assert task._status is None

    def test_status(self):
        sub1 = Task(func=Handlers.repeat)
        sub2 = Task(func=Handlers.repeat)

        task = CompositeTask(sub1, sub2)
        assert task.status == BaseTask.STATUS_PENDING

        sub1._status = BaseTask.STATUS_RUNNING
        assert task.status == BaseTask.STATUS_PENDING

        sub2._status = BaseTask.STATUS_COMPLETE
        assert task.status == BaseTask.STATUS_PENDING

        sub1._status = BaseTask.STATUS_HALTED
        assert task.status == BaseTask.STATUS_HALTED

        sub1._status = BaseTask.STATUS_COMPLETE
        assert task.status == BaseTask.STATUS_COMPLETE

    def test_is_halted(self):
        sub1 = Task(func=Handlers.repeat)
        sub2 = Task(func=Handlers.repeat)
        sub21 = Task(func=Handlers.repeat)
        sub2.then(sub21)

        task = CompositeTask(sub1, sub2)
        assert not task.is_halted

        sub21._status = BaseTask.STATUS_HALTED
        assert task.is_halted

    def test_is_halted_prev(self):
        task1 = Task(func=Handlers.repeat)
        task2 = CompositeTask(Task(func=Handlers.repeat), Task(func=Handlers.repeat))
        task1.then(task2)
        assert not task2.is_halted

        task1._status = BaseTask.STATUS_HALTED
        assert task2.is_halted

    def test_result(self):
        sub1 = Task(func=Handlers.repeat)
        sub1._result = 1
        sub2 = Task(func=Handlers.repeat)
        sub2._result = 2
        sub21 = Task(func=Handlers.repeat)
        sub21._result = 21
        sub2.then(sub21)

        task = CompositeTask(sub1, sub2)
        assert task.result == [1, 21]

    def test_set_ids(self):
        sub1 = Task(func=Handlers.repeat)
        sub2 = Task(func=Handlers.repeat)
        sub21 = Task(func=Handlers.repeat)
        sub2.then(sub21)

        task = CompositeTask(sub1, sub2)
        result = task.set_ids(starting_id=10)

        assert sub1.id == 10
        assert sub2.id == 11
        assert sub21.id == 12
        assert task.id == 13
        assert result == 14

    def test_get_all_tasks(self):
        sub1 = Task(func=Handlers.repeat)
        sub2 = Task(func=Handlers.repeat)
        sub21 = Task(func=Handlers.repeat)
        sub2.then(sub21)
        task = CompositeTask(sub1, sub2)

        all_tasks = task.get_all_tasks()
        assert all_tasks == [sub1, sub2]
        assert id(all_tasks) != id(task._sub_tasks)

    def test_run(self):
        task = CompositeTask(Task(func=Handlers.repeat))
        with pytest.raises(RuntimeError):
            task.run()

    def test_to_list(self):
        sub1 = Task(func=Handlers.repeat)
        sub1._id = 1
        sub2 = Task(func=Handlers.repeat)
        sub2._id = 2
        sub21 = Task(func=Handlers.repeat)
        sub21._id = 3
        sub2.then(sub21)

        task = CompositeTask(sub1, sub2)
        task._id = 4

        then = Task(func=Handlers.repeat)
        then._id = 5
        task.then(then)

        tasks_data = task.to_list()
        assert len(tasks_data) == 5
        assert tasks_data[0]['id'] == 1
        assert tasks_data[1]['id'] == 2
        assert tasks_data[2]['id'] == 3
        assert tasks_data[3]['id'] == 4
        assert tasks_data[4]['id'] == 5
        assert tasks_data[3]['sub_tasks'] == [1, 2]

    def test_from_data(self):
        task = CompositeTask(Task(func=Handlers.repeat), Task(func=Handlers.repeat))
        task.set_ids(starting_id=1)

        data = task.to_list()[0]
        data['sub_tasks'] = [Task(func=Handlers.repeat), Task(func=Handlers.repeat)]

        task1 = CompositeTask.from_data(data)
        assert task1._sub_tasks == data['sub_tasks']
        assert data['sub_tasks'][0].parent == task1
        assert data['sub_tasks'][1].parent == task1
