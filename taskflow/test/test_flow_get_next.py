"""
Comprehensive test suite for Flow._get_next method.

Covers:
- Simple chained tasks with different states (pending, running, complete, halted)
- Composite tasks with various child states
- Chained composite tasks with different states
"""

from taskflow.flow import Flow
from taskflow.tasks import BaseTask, CompositeTask, Task

from .fixtures import Handlers


def _make_simple_chain(n=3):
    """Create a simple chain: task1 -> task2 -> task3 (or more)."""
    tasks = [Task(Handlers.repeat, args=(i,)) for i in range(1, n + 1)]
    for i in range(len(tasks) - 1):
        tasks[i].then(tasks[i + 1])
    return tasks


def _make_flow_with_composite():
    """Create flow: task1 -> task2 -> Composite(task31, task32) -> task4."""
    task1 = Task(Handlers.repeat, args=(1,))
    task2 = task1.then(Task(Handlers.repeat, args=(2,)))
    task31 = Task(Handlers.repeat, args=(31,))
    task32 = Task(Handlers.repeat, args=(32,))
    task3 = task2.then(CompositeTask(task31, task32))
    task4 = task3.then(Task(Handlers.repeat, args=(4,)))
    flow = Flow(task1)
    return flow, task1, task2, task31, task32, task3, task4


def _reset_task(task, status=BaseTask.STATUS_PENDING, result=None):
    """Reset a task's state."""
    task._status = status
    task._result = result


class TestGetNextSimpleChain:
    """Tests for _get_next with simple chained tasks (no composites)."""

    def test_all_pending_returns_first_task(self):
        task1, task2, task3 = _make_simple_chain()
        flow = Flow(task1)
        assert flow._get_next(task1) == task1

    def test_first_pending_returns_first(self):
        task1, task2, task3 = _make_simple_chain()
        flow = Flow(task1)
        _reset_task(task2, BaseTask.STATUS_PENDING)
        _reset_task(task3, BaseTask.STATUS_PENDING)
        assert flow._get_next(task1) == task1

    def test_first_complete_second_pending_returns_second(self):
        task1, task2, task3 = _make_simple_chain()
        flow = Flow(task1)
        _reset_task(task1, BaseTask.STATUS_COMPLETE, 1)
        assert flow._get_next(task1) == task2

    def test_first_running_returns_none(self):
        task1, task2, task3 = _make_simple_chain()
        flow = Flow(task1)
        _reset_task(task1, BaseTask.STATUS_RUNNING)
        assert flow._get_next(task1) is None

    def test_first_halted_returns_none(self):
        task1, task2, task3 = _make_simple_chain()
        flow = Flow(task1)
        _reset_task(task1, BaseTask.STATUS_HALTED)
        assert flow._get_next(task1) is None

    def test_second_running_returns_none(self):
        task1, task2, task3 = _make_simple_chain()
        flow = Flow(task1)
        _reset_task(task1, BaseTask.STATUS_COMPLETE, 1)
        _reset_task(task2, BaseTask.STATUS_RUNNING)
        assert flow._get_next(task1) is None

    def test_second_halted_returns_none(self):
        task1, task2, task3 = _make_simple_chain()
        flow = Flow(task1)
        _reset_task(task1, BaseTask.STATUS_COMPLETE, 1)
        _reset_task(task2, BaseTask.STATUS_HALTED)
        assert flow._get_next(task1) is None

    def test_all_complete_returns_none(self):
        task1, task2, task3 = _make_simple_chain()
        flow = Flow(task1)
        _reset_task(task1, BaseTask.STATUS_COMPLETE, 1)
        _reset_task(task2, BaseTask.STATUS_COMPLETE, 2)
        _reset_task(task3, BaseTask.STATUS_COMPLETE, 3)
        assert flow._get_next(task1) is None

    def test_chain_of_four_progression(self):
        tasks = _make_simple_chain(4)
        task1, task2, task3, task4 = tasks
        flow = Flow(task1)

        assert flow._get_next(task1) == task1
        _reset_task(task1, BaseTask.STATUS_COMPLETE, 1)
        assert flow._get_next(task1) == task2
        _reset_task(task2, BaseTask.STATUS_COMPLETE, 2)
        assert flow._get_next(task1) == task3
        _reset_task(task3, BaseTask.STATUS_COMPLETE, 3)
        assert flow._get_next(task1) == task4
        _reset_task(task4, BaseTask.STATUS_COMPLETE, 4)
        assert flow._get_next(task1) is None


class TestGetNextSingleComposite:
    """Tests for _get_next with a single CompositeTask and various child states."""

    def test_both_children_pending_returns_first_child(self):
        flow, task1, task2, task31, task32, task3, task4 = _make_flow_with_composite()
        _reset_task(task1, BaseTask.STATUS_COMPLETE, 1)
        _reset_task(task2, BaseTask.STATUS_COMPLETE, 2)
        result = flow._get_next(task1)
        assert result == task31

    def test_first_child_complete_second_pending_returns_second(self):
        flow, task1, task2, task31, task32, task3, task4 = _make_flow_with_composite()
        _reset_task(task1, BaseTask.STATUS_COMPLETE, 1)
        _reset_task(task2, BaseTask.STATUS_COMPLETE, 2)
        _reset_task(task31, BaseTask.STATUS_COMPLETE, 31)
        assert flow._get_next(task1) == task32

    def test_one_child_running_one_pending_returns_pending(self):
        flow, task1, task2, task31, task32, task3, task4 = _make_flow_with_composite()
        _reset_task(task1, BaseTask.STATUS_COMPLETE, 1)
        _reset_task(task2, BaseTask.STATUS_COMPLETE, 2)
        _reset_task(task31, BaseTask.STATUS_RUNNING)
        assert flow._get_next(task1) == task32

    def test_one_child_running_one_complete_returns_none(self):
        """Regression: avoids infinite loop when no pending task exists."""
        flow, task1, task2, task31, task32, task3, task4 = _make_flow_with_composite()
        _reset_task(task1, BaseTask.STATUS_COMPLETE, 1)
        _reset_task(task2, BaseTask.STATUS_COMPLETE, 2)
        _reset_task(task31, BaseTask.STATUS_RUNNING)
        _reset_task(task32, BaseTask.STATUS_COMPLETE, 32)
        assert flow._get_next(task1) is None

    def test_both_children_running_returns_none(self):
        flow, task1, task2, task31, task32, task3, task4 = _make_flow_with_composite()
        _reset_task(task1, BaseTask.STATUS_COMPLETE, 1)
        _reset_task(task2, BaseTask.STATUS_COMPLETE, 2)
        _reset_task(task31, BaseTask.STATUS_RUNNING)
        _reset_task(task32, BaseTask.STATUS_RUNNING)
        assert flow._get_next(task1) is None

    def test_one_child_halted_returns_none(self):
        flow, task1, task2, task31, task32, task3, task4 = _make_flow_with_composite()
        _reset_task(task1, BaseTask.STATUS_COMPLETE, 1)
        _reset_task(task2, BaseTask.STATUS_COMPLETE, 2)
        _reset_task(task31, BaseTask.STATUS_HALTED)
        assert flow._get_next(task1) is None

    def test_both_children_complete_returns_next_in_chain(self):
        flow, task1, task2, task31, task32, task3, task4 = _make_flow_with_composite()
        _reset_task(task1, BaseTask.STATUS_COMPLETE, 1)
        _reset_task(task2, BaseTask.STATUS_COMPLETE, 2)
        _reset_task(task31, BaseTask.STATUS_COMPLETE, 31)
        _reset_task(task32, BaseTask.STATUS_COMPLETE, 32)
        assert flow._get_next(task1) == task4

    def test_composite_with_three_children_returns_first_pending(self):
        task1 = Task(Handlers.repeat, args=(1,))
        task2 = task1.then(Task(Handlers.repeat, args=(2,)))
        t31 = Task(Handlers.repeat, args=(31,))
        t32 = Task(Handlers.repeat, args=(32,))
        t33 = Task(Handlers.repeat, args=(33,))
        task3 = task2.then(CompositeTask(t31, t32, t33))
        flow = Flow(task1)
        _reset_task(task1, BaseTask.STATUS_COMPLETE, 1)
        _reset_task(task2, BaseTask.STATUS_COMPLETE, 2)
        assert flow._get_next(task1) == t31
        _reset_task(t31, BaseTask.STATUS_COMPLETE, 31)
        assert flow._get_next(task1) == t32
        _reset_task(t32, BaseTask.STATUS_COMPLETE, 32)
        assert flow._get_next(task1) == t33


class TestGetNextChainedComposites:
    """Tests for _get_next with multiple composites chained together."""

    def test_first_composite_both_pending_returns_first_child(self):
        task1 = Task(Handlers.repeat, args=(1,))
        task2 = task1.then(Task(Handlers.repeat, args=(2,)))
        c1a = Task(Handlers.repeat, args=(101,))
        c1b = Task(Handlers.repeat, args=(102,))
        comp1 = task2.then(CompositeTask(c1a, c1b))
        c2a = Task(Handlers.repeat, args=(201,))
        c2b = Task(Handlers.repeat, args=(202,))
        comp2 = comp1.then(CompositeTask(c2a, c2b))
        flow = Flow(task1)
        _reset_task(task1, BaseTask.STATUS_COMPLETE, 1)
        _reset_task(task2, BaseTask.STATUS_COMPLETE, 2)
        assert flow._get_next(task1) == c1a

    def test_first_composite_complete_second_has_pending_returns_second_composite_child(
        self,
    ):
        task1 = Task(Handlers.repeat, args=(1,))
        task2 = task1.then(Task(Handlers.repeat, args=(2,)))
        c1a = Task(Handlers.repeat, args=(101,))
        c1b = Task(Handlers.repeat, args=(102,))
        comp1 = task2.then(CompositeTask(c1a, c1b))
        c2a = Task(Handlers.repeat, args=(201,))
        c2b = Task(Handlers.repeat, args=(202,))
        comp2 = comp1.then(CompositeTask(c2a, c2b))
        flow = Flow(task1)
        _reset_task(task1, BaseTask.STATUS_COMPLETE, 1)
        _reset_task(task2, BaseTask.STATUS_COMPLETE, 2)
        _reset_task(c1a, BaseTask.STATUS_COMPLETE, 101)
        _reset_task(c1b, BaseTask.STATUS_COMPLETE, 102)
        assert flow._get_next(task1) == c2a

    def test_first_composite_one_running_second_composite_pending_returns_none(self):
        """First composite blocks; second composite's children not yet reachable."""
        task1 = Task(Handlers.repeat, args=(1,))
        task2 = task1.then(Task(Handlers.repeat, args=(2,)))
        c1a = Task(Handlers.repeat, args=(101,))
        c1b = Task(Handlers.repeat, args=(102,))
        comp1 = task2.then(CompositeTask(c1a, c1b))
        c2a = Task(Handlers.repeat, args=(201,))
        c2b = Task(Handlers.repeat, args=(202,))
        comp2 = comp1.then(CompositeTask(c2a, c2b))
        flow = Flow(task1)
        _reset_task(task1, BaseTask.STATUS_COMPLETE, 1)
        _reset_task(task2, BaseTask.STATUS_COMPLETE, 2)
        _reset_task(c1a, BaseTask.STATUS_RUNNING)
        _reset_task(c1b, BaseTask.STATUS_COMPLETE, 102)
        assert flow._get_next(task1) is None

    def test_first_composite_one_running_one_pending_returns_pending(self):
        task1 = Task(Handlers.repeat, args=(1,))
        task2 = task1.then(Task(Handlers.repeat, args=(2,)))
        c1a = Task(Handlers.repeat, args=(101,))
        c1b = Task(Handlers.repeat, args=(102,))
        comp1 = task2.then(CompositeTask(c1a, c1b))
        task4 = comp1.then(Task(Handlers.repeat, args=(4,)))
        flow = Flow(task1)
        _reset_task(task1, BaseTask.STATUS_COMPLETE, 1)
        _reset_task(task2, BaseTask.STATUS_COMPLETE, 2)
        _reset_task(c1a, BaseTask.STATUS_RUNNING)
        assert flow._get_next(task1) == c1b

    def test_both_composites_complete_returns_final_task(self):
        task1 = Task(Handlers.repeat, args=(1,))
        task2 = task1.then(Task(Handlers.repeat, args=(2,)))
        c1a = Task(Handlers.repeat, args=(101,))
        c1b = Task(Handlers.repeat, args=(102,))
        comp1 = task2.then(CompositeTask(c1a, c1b))
        task4 = comp1.then(Task(Handlers.repeat, args=(4,)))
        flow = Flow(task1)
        _reset_task(task1, BaseTask.STATUS_COMPLETE, 1)
        _reset_task(task2, BaseTask.STATUS_COMPLETE, 2)
        _reset_task(c1a, BaseTask.STATUS_COMPLETE, 101)
        _reset_task(c1b, BaseTask.STATUS_COMPLETE, 102)
        assert flow._get_next(task1) == task4
