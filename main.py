from taskflow.tasks import *
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

    while True:
        print()
        for row in flow.to_list():
            print(row)

        task = flow.step()
        if not task:
            break

        print(f'{task.id} -> {task.result}')

        # for testing recreate the flow from serialization
        flow = Flow.from_list(flow.to_list())


if __name__ == '__main__':
    run()
