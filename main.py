import inspect
import json

from taskflow.tasks.task import *
from taskflow.flow import Flow
from taskflow.test_funcs import Test, product


def run():
    flow = Flow(
        Task.when(
            Task(product, args=[5, 2]),
            Task.when(
                Task(Test.sum, args=[5, 6]),
                Task(Test.sum, args=[2, 4])
            ).then(
                Task(product)
            ),
            Task(Test.const, args=[4]),
        ).then(
            Task(Test.sum)
        )
    )

    for row in flow.to_list():
        print(row)

    print()
    while True:
        task = flow.step()
        if not task:
            break

        print(f'{task.id} -> {task.result}')


if __name__ == '__main__':
    run()
