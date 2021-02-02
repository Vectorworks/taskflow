# taskflow

[![Build Status](https://travis-ci.com/Vectorworks/taskflow.svg?branch=master)](https://travis-ci.com/Vectorworks/taskflow)

taskflow is a simple workflow library, designed to be easily extended to support asynchronous distributed execution.   

The logic is centered around **Task**s. Tasks can be chained or executed in parallel. To execute the tasks in the correct order you will need to add the to a **Flow** object.


## Usages
### Single task synchronous example
```python
from taskflow import Flow, Task

flow = Flow(Task(func=lambda x: x * x, args=(5,)))
flow.run()

25
```

### Chaining tasks
To chain tasks, use the **then** method. This will make sure these tasks are executed in the correct order. Tasks passed in then will receive the previous tasks result as a parameter, alongside any explicit parameters.

```python
from taskflow import Flow, Task

flow = Flow(
    Task(
        func=lambda x: x * x, args=(5,)
    ).then(
        Task(func=lambda *args: sum(args), args=(10,))
    )
)
flow.run()

35
```