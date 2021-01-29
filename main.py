import random

from taskflow.tasks import *
from taskflow.flow import Flow
from taskflow.test_funcs import Test, product


def run():
    # 5*2 + (5+6)*(2+4) + 4
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


# flow = Flow(
#     Task(
#         Test.const, args=[12]
#     ).then(
#         Task(Test.sum, args=[1, 2])
#     ).then(
#         Task(Test.sum, args=[5])
#     ).then(
#         Task(product, args=[3])
#     )
# )
#
# flow = Flow(
#     Task.when(
#         Task(Test.const, args=[12]),
#         Task(Test.sum, args=[1, 2]),
#         Task(Test.sum, args=[5]),
#         Task(product, args=[3]),
#     ).then(
#         Task(Test.const)
#     )
# )
