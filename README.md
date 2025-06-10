# taskflow

## Install
In the project root, run:

```bash
pip install .
```
or
```bash
pip install taskflow
```
or add this to your `requirements.txt` file:

`taskflow @ git+ssh://git@github.com/Vectorworks/taskflow.git@v{tag}`

## Development
If you are using a virtual environment, make sure to activate it before running the following commands.

To run tox locally you need only the dev requirements, which can be installed with:

```bash
pip install -e .[dev]
```
or

```bash
pip install -r requirements-dev.txt
```


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
