# Why Dagger?

`dagger` started as an internal library used at [Wayfair][wayfair] by the Fulfillment Engineering Team. It is now an open
source project with the hope that it will provide benefit from the greater Python community.

## What problem did dagger initially solve?

With our move to microservices, we chose orchestration over choreography to orchestrate between microservices. To achieve
orchestration between microservices, the engineering team built dagger to model and execute workflows at scale. The reason
why we chose to built dagger was

* Ability to modify runtime parameters after execution of the workflow
* Built as a lightweight library
* Highly available, Scalable and Fault Tolerant
* Supports kafka out of the box for asynchronous API's
* Supports long running tasks
* Simple deployment model - no DB setup and dependency
* Flexible model to support any kind of workflows
* Event sources all workflow updates

## Alternatives

`dagger` is not the only library that exists which provides a way to codify prompting a user for answers to a set of
questions. This section compares `dagger` with some frameworks which were created to achieve this task.

### Airflow

[Airflow][airflow] is an workflow engine implemented in python. However it does not support long running tasks and has
no integration with Kafka. It also uses a single RDBMS as a data store thus not giving us the scale out feature we were
looking for

### Cadence

[cadence][cadence] is another workflow engine with support for java and go. It however needs cassandra/mysql as the
data store and does not have any kafka support built in. It does not offer a python client

[wayfair]: https://www.wayfair.com/
[airflow]: https://airflow.apache.org/
[cadence]: https://cadenceworkflow.io/
