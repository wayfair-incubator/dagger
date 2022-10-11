
# Dagger

[![Release](https://img.shields.io/github/v/release/wayfair-incubator/dagger?display_name=tag)](CHANGELOG.md)
[![Lint](https://github.com/wayfair-incubator/dagger/actions/workflows/lint.yml/badge.svg?branch=main)](https://github.com/wayfair-incubator/dagger/actions/workflows/lint.yml)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.0-4baaaa.svg)](CODE_OF_CONDUCT.md)
[![Maintainer](https://img.shields.io/badge/Maintainer-Wayfair-7F187F)](https://wayfair.github.io)

## About The Project

**Dagger** is a distributed, scalable, durable, and highly available orchestration engine to execute asynchronous and
synchronous long-running business logic in a scalable and resilient way.

Dagger requires Python 3.7 or later for the new `async/await`_ syntax, and variable type annotations.

Here's an example of how to use the library to build and run a workflow:

```python
import logging
from uuid import uuid1
from dagger.service.services import Dagger
from dagger.modeler.definition import *
from dagger.templates.template import *

logger = logging.getLogger(__name__)

workflow_engine = Dagger(broker="kafka://localhost:9092", datadir="/tmp/data/")

@Dagger.register_template('OrderWorkflow')
def order_template(template_name: str) -> ITemplateDAG:


    # Create empty default Template
    template_builder = DefaultTemplateBuilder(Dagger.app)
    template_builder.set_name(template_name)
    template_builder.set_type(DefaultTemplateDAGInstance)

    # First process in template (first node in DAG) named waving
    payment_process_builder = ProcessTemplateDagBuilder(Dagger.app)
    payment_process_builder.set_name("PAYMENT")

    # First task and topic in payment process
    payment_command_topic =  Dagger.create_topic("PAYMENT_COMMAND", key_type=bytes, value_type=bytes)
    payment_command_task_template_builder = KafkaCommandTaskTemplateBuilder(Dagger.app)
    payment_command_task_template_builder.set_topic(payment_command_topic)
    payment_command_task_template_builder.set_type(PaymentKafkaCommandTask)

    # Second task and topic in payment process
    payment_topic: Topic =  Dagger.create_topic("PAYMENT_LISTENER", key_type=bytes, value_type=bytes)
    payment_listener_task_template_builder = KafkaListenerTaskTemplateBuilder(Dagger.app)
    payment_listener_task_template_builder.set_topic(payment_topic)
    payment_listener_task_template_builder.set_type(PaymentKafkaListenerTask)
    
    # Link and build tasks in payment process (define root task and order, essentially just created a child DAG inside the parent DAG)
    payment_listener_task_template = payment_listener_task_template_builder.build()
    payment_command_task_template_builder.set_next(payment_listener_task_template)
    payment_command_task_template = payment_command_task_template_builder.build()
    payment_process_builder.set_root_task(payment_command_task_template)
    payment_process_builder.set_type(DefaultProcessTemplateDAGInstance)

    # Build more processes like above

    [...]

    # Link and build processes in DAG (define root task and order) Assuming one more process called "SHIPPING" was created, this would be the flow:
    shipping_template = shipping_process_builder.build()
    payment_process_builder.set_next_process(shipping_template)
    payment_template = payment_process_builder.build()
    template_builder.set_root(payment_template)

    btemplate = template_builder.build()
    return btemplate

# Starts the worker
workflow_engine.main()
```

The ``register_template`` decorator defines a "DAG processor" that essentially defines the various processes and child
tasks the DAG executes. In the example above the code creates a named template ``OrderWorkflow``  and associates
a ``PAYMENT`` process with 2 child tasks ``PAYMENT_LISTENER`` and ``PAYMENT_COMMAND``. The ``SHIPPING`` process follows
with similarly named topics and processes and the template defines the root process and links them in a DAG (Directed
Acyclic Graph) structure

The application can define as many DAG'S it needs to model using the ``register_template``
decorator. process-engine populates all the DAG templates in the codebase decorated with `register_template`

Here's and example of how to create an instance of a specific DAG:

```python
template = workflow_engine.get_template('BulkTemplate')
runtime_parameters:Dict[str, str] = dict()
runtime_parameters['customer_name']= `EXAMPLE_CUSTOMER`
runtime_parameters['order_number'] = 'EXAMPLE_ORDER' 
template_instance = await template.create_instance(uuid1(), runtime_parameters)
```

To begin execution of the DAG instance created above

```python
await workflow_engine.submit(template_instance)
```

This begins the actual execution of the tasks created by the template definition and executes them in the sequence as
defined in the template. The engine currently supports the following types of tasks:

## KafkaCommandTask

This task is used to send a request/message on a Kafka Topic defined using the template builder. This type of task is a
child task in the execution graph and can be extended by implementing the method

```python
@abc.abstractmethod
async def execute(self) -> None:
    ...
```

## KafkaListenerTask

This task waits/halts the execution of the DAG until a message is received on the defined Kafka topic(in the template
definition). Each task is created using the DAG builder defines a durable key to correlate each received message on the
topic against listener tasks. The Engine handles the complexity of invoking the appropriate task instance based on the
key in the payload.

A listener task needs to implement the following methods

```python

 @abc.abstractmethod
 async def on_message(self, *args: Any, **kwargs: Any) -> None :
    ...
    
 @abc.abstractmethod
 async def get_correlatable_key(self, payload: Any) -> TaskLookupKey: 
    ...
```

The get_correlatable_key method extracts the key by parsing the payload received on the Kafka topic. Using this key the
DAGGER looks up the appropriate task from the list of tasks waiting on this event and invokes `on_message` on each one
of them. The default implementation of this task just sets this task and `COMPLETED`

The engine provides the flexibility to implement any other type of listener task by implementing the following interface

```python
class SensorTask(ITask[KT, VT]):
```

along with a custom `TaskTemplateBuilder`

```python
class TaskTemplateBuilder:
app: Service

def __init__(self, app: Service) -> None :
    self.app = app

@abc.abstractmethod
def set_type(self, task_type:Type[ITask]) -> TaskTemplateBuilder:
    ...

@abc.abstractmethod
def build(self) -> TaskTemplate:
    ...

```

## TriggerTask

This task waits/halts the execution of the DAG until current time >= the trigger time on the task

A trigger task needs to implement the following method

```python
@abc.abstractmethod
async def execute(self) -> None:
    ...
```

The engine provides a `TriggerTaskTemplateBuilder` helper to model the task in the DAG.
The `set_time_to_execute_lookup_key` on this builder is used to define the key to lookup the trigger time provided in
the runtime parameters of the task

## DecisionTask

This type of task is similar to the `case..switch` statement in a programming language. It returns the next task to
execute based on the execution logic. A decision task needs to implement

```python
@abc.abstractmethod
async def evaluate(self, **kwargs: Any) -> Optional[UUID]:
    ...
```

This method returns the UUID of the next task to execute in the execution path

The Engine provides a `DecisionTaskTemplateBuilder` to model a decision task in the DAG

## RESTful API

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

Dagger supports any type of stream data: bytes, Unicode and serialized structures, but also comes with "Models" that use
modern Python syntax to describe how keys and values in streams are serialized. For more details on supported models
refer to
<https://faust.readthedocs.io/en/latest/userguide/models.html>

## OpenTelemetry

Dagger has support for open telemetry. To enable open telemetry the client application has to initialise the tracer
implementation and set the flag enable_telemetry while initializing dagger

## Dagger is

## Simple

Dagger is extremely easy to use. To get started applications need to install this library, define a DAG using the
default templates or extending them based on the use case, creating instances of these DAG's and scheduling them for
execution. The library hides all the complexity of producing and consuming from Kafka, maintaining Kafka Streams
topology processing and also persistence and recovery of created tasks

## Highly Available

Dagger is highly available and can survive network problems and server crashes. In the case of node failure, it can
automatically recover the state store(representing task data)
or failover to a standby node

## Distributed

Start more instances of your application as needed to distribute the load on the system

## Fast

A single-core worker instance can already process tens of thousands of tasks every second. Dagger uses a fast key-value
lookup store based on rocksDB replicated to kafka topics for fault tolerance

## Installation

You can install dagger via the Wayfair artifactory or from source.

To install using `pip`:
```shell
pip install py-dagger
```

dagger has a dependency on `faust` for kafka stream processing

## FAQ

## Which version of python is supported?

dagger supports python version >= 3.7

## What kafka versions are supported?

dagger supports kafka with version >= 0.10.

## Roadmap

See the [open issues](https://github.com/wayfair-incubator/dagger/issues) for a list of proposed features (and known
issues).

## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any
contributions you make are **greatly appreciated**. For detailed contributing guidelines, please
see [CONTRIBUTING.md](CONTRIBUTING.md)

## License

Distributed under the `MIT LICENSE` License. See `LICENSE` for more information.

## Contact

Vikram Patki - vpatki@wayfair.com

Project Link: [https://github.com/wayfair-incubator/dagger](https://github.com/wayfair-incubator/dagger)

## Acknowledgements

This template was adapted from
[https://github.com/othneildrew/Best-README-Template](https://github.com/othneildrew/Best-README-Template).
