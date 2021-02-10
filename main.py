import random

from taskflow.tasks import *
from taskflow.flow import Flow
from test_funcs import Test, product


def get_test_tasks():
    # 5*2 + (5+6)*(2+4) + 4
    return (
        Task.when(
            Task(product, args=([5, 2],), name='5*2'),
            Task.when(
                Task(Test.sum, args=([5, 6],), name='5+6'),
                Task(Test.sum, args=([2, 4],), name='2+4')
            ).then(
                Task(product, name='product')
            ),
            Task(Test.const, args=(4,), name='4'),
        ).then(
            Task(Test.sum, name='sum')
        )
    )


def run():
    flow = Flow(get_test_tasks())

    while True:
        print()
        print(flow.root_task.id)
        for row in flow.to_list():
            print(row)

        task = flow.step()
        if not task:
            break

        print(f'{task.id} -> {task.result}')
        print(f'Is halted: {flow.is_halted}')
        print(f'Is complete: {flow.is_complete}')

        # for testing recreate the flow from serialization
        task_list = flow.to_list()
        random.shuffle(task_list)
        flow = Flow.from_list(task_list)


if __name__ == '__main__':
    run()
