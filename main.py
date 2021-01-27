import inspect
import json

from taskflow.tasks.task import *
from taskflow.flow import Flow
from taskflow.test_funcs import Test, product


def run():
    flow = Flow(CompositeTask(
        Test.sum,
        sub_tasks=[
            SimpleTask(Test.mul_5_2),
            CompositeTask(
                product,
                sub_tasks=[
                    SimpleTask(lambda *args, **kwargs: 5 + 6),
                    SimpleTask(lambda *args, **kwargs: 2 + 4)
                ]
            ),
            SimpleTask(lambda *args, **kwargs: 4),
        ]
    ))

    print(json.dumps(flow.to_dict(), indent=2))

    while True:
        task = flow.step()
        if not task:
            break

        print(f'{task._func} -> {task.last_result}')


if __name__ == '__main__':
    print(run())
