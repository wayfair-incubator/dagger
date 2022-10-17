[![Release](https://img.shields.io/github/v/release/wayfair-incubator/dagger?display_name=tag)](CHANGELOG.md)
[![Lint](https://github.com/wayfair-incubator/dagger/actions/workflows/lint.yml/badge.svg?branch=main)](https://github.com/wayfair-incubator/dagger/actions/workflows/lint.yml)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.0-4baaaa.svg)](CODE_OF_CONDUCT.md)
[![Maintainer](https://img.shields.io/badge/Maintainer-Wayfair-7F187F)](https://wayfair.github.io)

# Dagger

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

workflow_engine = Dagger(broker="kafka://localhost:9092", store="aerospike://", datadir="/tmp/data/")

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

    # Build more processes like above and link them

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
a ``PAYMENT`` process with 2 child tasks ``PAYMENT_LISTENER`` using ``PaymentKafkaListenerTask`` and ``PAYMENT_COMMAND``
using the ``PaymentKafkaCommandTask`` definition. The ``SHIPPING`` process follows
after the ``PAYMENT`` process with similarly named topics and processes and the template defines the root process and 
links them in a DAG (Directed Acyclic Graph) structure

The application can define as many DAG'S it needs to model using the ``register_template``
decorator. dagger populates all the DAG templates in the codebase decorated with `register_template`

Here's and example of how to create an instance of a specific DAG:

```python
template = workflow_engine.get_template('OrderWorkflow')
runtime_parameters:Dict[str, str] = dict()
runtime_parameters['customer_name']= `EXAMPLE_CUSTOMER`
runtime_parameters['order_number'] = 'EXAMPLE_ORDER' 
workflow_instance = await template.create_instance(uuid1(), runtime_parameters)
```

To begin execution of the DAG instance created above

```python
await workflow_engine.submit(workflow_instance)
```

This begins the actual execution of the tasks created by the template definition and executes them in the sequence as
defined in the template.

## Documentation

Check out the [project documentation][dagger-docs].

For an overview on how repository structure and how to work with the code base, read the
[Development Guide][development-docs].


[dagger-docs]: https://wayfair-incubator.github.io/dagger/latest/
[development-docs]: https://wayfair-incubator.github.io/dagger/latest/development-guide/