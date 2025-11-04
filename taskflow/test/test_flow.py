from random import shuffle
from uuid import uuid4

from taskflow.flow import Flow
from taskflow.tasks import BaseTask, CompositeTask, Task

from .fixtures import Handlers


class TestFlow(object):
    def test_init(self, mocker):
        mocker.patch("taskflow.tasks.BaseTask.set_ids")

        uid = uuid4()
        root = BaseTask()

        flow = Flow(root.then(BaseTask()), uid=uid, friendly_name="test-flow")

        assert flow.root_task == root
        assert flow.uid == uid
        assert flow.friendly_name == "test-flow"
        root.set_ids.assert_called_once_with()

    def test_is_halted(self):
        root = BaseTask()
        then = BaseTask()
        root.then(then)

        flow = Flow(then)
        assert not flow.is_halted

        then._status = BaseTask.STATUS_HALTED
        assert flow.is_halted

    def test_is_complete(self):
        root = BaseTask()
        then = BaseTask()
        root.then(then)

        flow = Flow(then)
        assert not flow.is_complete

        root._status = BaseTask.STATUS_COMPLETE
        assert not flow.is_complete

        then._status = BaseTask.STATUS_COMPLETE
        assert flow.is_complete

    def test_run(self):
        root = Task(Handlers.repeat, args=(4,))
        then = Task(Handlers.repeat, args=(5,))
        leaf = Task(Handlers.repeat, args=(6,))

        flow = Flow(root.then(then).then(leaf))
        result = flow.run()

        assert flow.is_complete
        assert result == leaf.result
        assert result == (6, (5, (4,)))

    def test_step(self):
        root = Task(Handlers.repeat, args=(4,))
        then = Task(Handlers.repeat, args=(5,))
        leaf = Task(Handlers.repeat, args=(6,))

        flow = Flow(root.then(then).then(leaf))

        result = flow.step()
        assert not flow.is_complete
        assert result == root

        result = flow.step()
        assert not flow.is_complete
        assert result == then

        result = flow.step()
        assert flow.is_complete
        assert result == leaf
        assert leaf.result == (6, (5, (4,)))

    def test_step_before_task_run(self, mocker):
        root = Task(Handlers.repeat, args=(4,))
        leaf = Task(Handlers.repeat, args=(6,))

        def before_task_run(self, task):
            task._status = BaseTask.STATUS_RUNNING
            return False

        flow = Flow(root.then(leaf))
        mocker.patch("taskflow.flow.Flow._before_task_run", before_task_run)

        result = flow.step()
        assert not flow.is_complete
        assert result is None

    def test_get_next(self):
        task1 = Task(Handlers.repeat, args=(1,))
        task2 = task1.then(Task(Handlers.repeat, args=(2,)))
        task31 = Task(Handlers.repeat, args=(31,))
        task32 = Task(Handlers.repeat, args=(32,))
        task3 = task2.then(CompositeTask(task31, task32))
        task4 = task3.then(Task(Handlers.repeat, args=(4,)))

        flow = Flow(task1)
        result = flow._get_next(task1)
        assert result == task1

        task1._status = BaseTask.STATUS_RUNNING
        result = flow._get_next(task1)
        assert result is None

        task1._status = BaseTask.STATUS_HALTED
        result = flow._get_next(task1)
        assert result is None

        task1._status = BaseTask.STATUS_COMPLETE
        task1._result = 1
        result = flow._get_next(task1)
        assert result == task2

        task2._status = BaseTask.STATUS_COMPLETE
        task1._result = 2
        result = flow._get_next(task1)
        assert result == task31

        task31._status = BaseTask.STATUS_COMPLETE
        task31._result = 31
        result = flow._get_next(task1)
        assert result == task32

        task32._status = BaseTask.STATUS_RUNNING
        result = flow._get_next(task1)
        assert result is None

        task32._status = BaseTask.STATUS_COMPLETE
        task32._result = 32
        result = flow._get_next(task1)
        assert result == task4

    def test_to_list(self):
        task1 = Task(Handlers.repeat, args=(1,))
        flow = Flow(
            task1.then(Task(Handlers.repeat))
            .then(CompositeTask(Task(Handlers.repeat), Task(Handlers.repeat)))
            .then(Task(Handlers.repeat))
        )

        assert task1.to_list() == flow.to_list()

    def test_from_list(self):
        task1 = Task(Handlers.repeat, args=(1,))
        flow1 = Flow(
            task1.then(Task(Handlers.repeat))
            .then(CompositeTask(Task(Handlers.repeat), Task(Handlers.repeat)))
            .then(Task(Handlers.repeat))
        )

        data_list = flow1.to_list()
        shuffle(data_list)
        flow2 = Flow.from_list(data_list, uid=uuid4(), friendly_name="Copy")

        assert flow2.to_list() == flow1.to_list()
