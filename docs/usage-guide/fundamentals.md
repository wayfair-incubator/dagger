# Usage Guide

This section provides detailed descriptions of how `dagger` can be used. If you are new to `dagger`, the
[Getting Started][getting-started] page provides a gradual introduction of the basic functionality with examples.

## Task Support
Tasks are the building blocks to define workflows. `dagger` supports the following types of tasks:

### KafkaCommandTask

This task is used to send a request/message on a Kafka Topic defined using the template builder. This type of task is a
child task in the execution graph and can be extended by implementing the method

```python
@abc.abstractmethod
async def execute(
    self, runtime_parameters: Dict[str, str], workflow_instance: ITask = None
) -> None:
```

### KafkaListenerTask

This task waits/halts the execution of the DAG until a message is received on the defined Kafka topic(in the template
definition). Each task is created using the DAG builder defines a durable key to correlate each received message on the
topic against listener tasks. The Engine handles the complexity of invoking the appropriate task instance based on the
key in the payload.

A listener task needs to implement the following methods

```python

 @abc.abstractmethod
    async def on_message(
        self, runtime_parameters: Dict[Any, Any], *args: Any, **kwargs: Any
    ) -> bool:  
        ...
    
async def get_correlatable_key_from_payload(
    self, payload: Any
) -> TaskLookupKey:
```

The `get_correlatable_key_from_payload` method extracts the key value by parsing the payload received on the Kafka topic.
Using this key `dagger` looks up the appropriate task from the list of tasks waiting on this event and invokes `on_message` on each one
of them. The default implementation of this task just sets this task to `COMPLETED`

`dagger` provides the flexibility to implement any other type of listener task by implementing the following interface

```python
class SensorTask(ITask[KT, VT]):
```

along with a custom `TaskTemplateBuilder`

### TriggerTask

This task waits/halts the execution of the DAG until current time >= the trigger time on the task and then invokes
the `execute` method defined by the task

A trigger task needs to implement the following method

```python
@abc.abstractmethod
async def execute(
    self, runtime_parameters: Dict[str, str], workflow_instance: ITask = None
) -> None:  
    """Executes the ITask."""
    ...
```

`dagger` provides a `TriggerTaskTemplateBuilder` helper to model the task in the DAG.
The `set_time_to_execute_lookup_key` on this builder is used to define the key to lookup the trigger time provided in
the runtime parameters of the task

### DecisionTask

This type of task is similar to the `case..switch` statement in a programming language. It returns the next task to
execute based on the execution logic. A decision task needs to implement

```python
@abc.abstractmethod
async def evaluate(self, **kwargs: Any) -> Optional[UUID]:
    ...
```

This method returns the UUID of the next task to execute in the execution path

The Engine provides a `DecisionTaskTemplateBuilder` to model a decision task in the DAG

### MonitoredTask

dagger provides the `IMonitoredTask` interface which can be implemented on any task to provide a way to monitor that task.
When the monitor is triggered based on a trigger, the `process_monitored_task` method is invoked on the `MonitoringTask`.
Dagger comes built in with `MonitoredProcessTemplateDAGInstance`

### ParallelCompositeTask

SUB-DAG Task to execute parallel tasks and wait until all of them are in a terminal state before progressing to the next task
This task can be embedded as a child of the root node or a process node

### IntervalTask

A type of Task to Trigger at a trigger time and execute multiple times until the execution completes. The
task is retried until the timeout is reached periodically after the trigger time

### MonitoringTask

A Type of TriggerTask that executes at s specific time and checks on the monitored task to execute some
domain specific logic

### SensorTask

A type of task that halts execution of the workflow until a condition is met. When the condition is met
the on_message method on this task is invoked

### IMonitoredTask
An Abstract interface to enable monitoring of a task. Any Task that implements this interface will need to setup
a MonitoringTask by implementing. Dagger ships with a default implementation using ``

```python
@abc.abstractmethod
async def setup_monitoring_task(
    self, workflow_instance: ITask
) -> None:
    ...
```

### RESTful API

The framework provides a RESTFul API to retrieve the status of root task instances. Root task is the instance created
using the `TaskTemplate`
which then has multiple, chained ProcessTasks and child tasks(KafkaCommand and KafkaListener tasks)

```json
    http://<hostname>:6066/tasks/instances

    [
        {
            "child_dags": [],
            "child_tasks": [
                {
                    "child_dags": [
                        "89bbf26c-0727-11ea-96e5-0242ac150004",
                        "89bc1486-0727-11ea-96e5-0242ac150004"
                    ],
                    "correlatable_key": null,
                    "id": "89bbedd0-0727-11ea-96e5-0242ac150004",
                    "lastupdated": 1573767727,
                    "parent_id": "89bbe43e-0727-11ea-96e5-0242ac150004",
                    "process_name": "PAYMENT",
                    "runtime_parameters": {
                        "order_number": "ID000",
                        "customer": "ID000"
                    },
                    "status": {
                        "code": "COMPLETED",
                        "value": "Complete"
                    },
                    "task_type": "NON_ROOT",
                    "time_completed": 1573767727,
                    "time_created": 1573767624,
                    "time_submitted": 1573767698
                },
                {
                    "child_dags": [
                        "89bc3984-0727-11ea-96e5-0242ac150004",
                        "89bc482a-0727-11ea-96e5-0242ac150004"
                    ],
                    "correlatable_key": null,
                    "id": "89bc35f6-0727-11ea-96e5-0242ac150004",
                    "lastupdated": 1573767727,
                    "parent_id": "89bbe43e-0727-11ea-96e5-0242ac150004",
                    "process_name": "SHIPPING",
                    "runtime_parameters": {
                        "order_number": "ID000",
                        "customer": "ID000"
                    },
                    "status": {
                        "code": "EXECUTING",
                        "value": "Executing"
                    },
                    "task_type": "NON_ROOT",
                    "time_completed": 0,
                    "time_created": 1573767624,
                    "time_submitted": 1573767727
                }
            ],
            "correlatable_key": null,
            "id": "89bbe43e-0727-11ea-96e5-0242ac150004",
            "lastupdated": 1573767624,
            "parent_id": null,
            "runtime_parameters": {
                "order_number": "ID000",
                "customer": "ID000"
            },
            "status": {
                "code": "EXECUTING",
                "value": "Executing"
            },
            "task_type": "ROOT",
            "time_completed": 0,
            "time_created": 1573767624,
            "time_submitted": 1573767698
        }]

```

## How to stop a workflow

### Step 1. Implement the stop method in the task modeled in the workflow

```python
    async def stop(self) -> None:
        print("Stop called")
```

### Step 2. Invoke stop on the workflow instance

```python
    workflow_instance: ITemplateDAGInstance = await dagger_instance.get_instance(id=workflow_id)
    await workflow_instance.stop()
```

This will invoke `stop` on tasks in `EXECUTING` state

## Detailed Sections

* [getting-started][getting-started]

[getting-started]: ../getting-started.md
