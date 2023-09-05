from __future__ import annotations

import abc
import random
import time
import uuid
from typing import Any, List, Optional, Type
from uuid import UUID

from faust import Topic
from mode import Service

from dagger.exceptions.exceptions import InvalidTaskType, InvalidTriggerTimeForTask
from dagger.tasks.task import (
    KT,
    VT,
    DecisionTask,
    IntervalTask,
    IProcessTemplateDAGInstance,
    ITask,
    ITemplateDAGInstance,
    KafkaAgent,
    KafkaCommandTask,
    KafkaListenerTask,
    TaskOperator,
    TaskStatus,
    TaskStatusEnum,
    TaskType,
    TriggerTask,
)
from dagger.utils.utils import DAGIDGenerator


class ITemplateDAG:
    """Skeleton class defining the attributes and functions of a template instance."""

    name: str
    root_process_dag: IProcessTemplateDAG
    app: Service
    template_type: Type[ITemplateDAGInstance]

    def __init__(
        self,
        dag: IProcessTemplateDAG,
        name: str,
        app: Service,
        template_type: Type[ITemplateDAGInstance],
    ) -> None:
        """
        ITemplateDAG Constructor
        :param dag: The definition of the first process to execute
        :param name: the name of the Workflow
        :param app: the Dagger instance
        :param template_type: the type of the Workflow to instantiate
        """
        self.app = app
        self.root_process_dag = dag
        self.name = name
        self.template_type = template_type

    @abc.abstractmethod
    async def create_instance(
        self,
        id: UUID,
        partition_key_lookup: str,
        *,
        repartition: bool = True,
        seed: random.Random = None,
        submit_task: bool = False,
        **kwargs,
    ) -> ITemplateDAGInstance:  # pragma: no cover
        """Method for creating an instance of a workflow definition
        :param id: The id of the workflow
        :param partition_key_lookup: Kafka topic partition key associated with the instance of the workflow. The key
        needs to be defined within the runtime parameters
        :param repartition: Flag indicating if the creation of this instance needs to be stored on the current node or
        by the owner of the partition defined by the partition_key_lookup
        :param seed: the seed to use to create all internal instances of the workflow
        :param submit_task: if True also submit the task for execution
        :param **kwargs: Other keyword arguments
        :return: An instance of the workflow
        """
        ...

    @abc.abstractmethod
    def set_dynamic_builders_for_process_template(
        self, name: str, process_template_builders
    ):
        """
        Use these builders only when the processes of the workflow definition need to be determined at runtime.
        Using these the processes in the workflow definition can be defined at runtime based on the runtime parameters
        of the workflow

        :param name: Then name of the dynamic process builder
        :param process_template_builders: the ProcessTemplateDagBuilder dynamic process builders
        """
        ...


class IProcessTemplateDAG:
    """Skeleton class defining the attributes and functions of a process instance."""

    root_task_dag: TaskTemplate
    next_process_dag: List[IProcessTemplateDAG]
    app: Service
    name: str
    process_type: Type[ITask]

    def __init__(
        self,
        next_process_dag: List[IProcessTemplateDAG],
        app: Service,
        name: str,
        process_type: Type[ITask],
        root_task_dag: Optional[TaskTemplate],
        max_run_duration: int,
    ) -> None:
        """
        Constructor
        :param next_process_dag: The list of next processes to execute in this workflow
        :param app: The Dagger instance
        :param name: the name of the process
        :param process_type: the type of the process to instantiate
        :param root_task_dag: The workflow type instance(definition)
        :param max_run_duration: The maximum time this process can execute until its marked as failure(timeout)
        """
        self.next_process_dag = next_process_dag
        self.app = app
        self.name = name
        self.process_type = process_type
        self.root_task_dag = root_task_dag  # type: ignore
        self.max_run_duration = max_run_duration

    @abc.abstractmethod
    async def create_instance(
        self,
        id: UUID,
        parent_id: UUID,
        parent_name: str,
        partition_key_lookup: str,
        *,
        repartition: bool = True,
        seed: random.Random = None,
        workflow_instance: ITemplateDAGInstance = None,
        **kwargs,
    ) -> IProcessTemplateDAGInstance:  # pragma: no cover
        """Method for creating an instance of a process instance
        :param id: The id of the instance
        :param parent_id: the id of the parent of this process(The workflow instance)
        :param parent_name: the name of the workflow
        :param partition_key_lookup: The kafka partitioning key to look up
        :param repartition: If true the instance is serialized and stored in the owner of the parition owned by the
        partioning key
        :param seed: The seed to use to create any child instances
        :param workflow_instance: the instance of the workflow
        :param **kwargs: Other keyword arguments
        :return: the instance of the Process
        """
        ...

    @abc.abstractmethod
    def set_dynamic_process_builders(
        self, process_template_builders
    ) -> None:  # pragma: no cover
        """
        Sets the dynamic process builders on the process instance determined at runtime. Only used when the processes
        of the workflow cannot be determined statically and needs to be defined at runtime
        :param process_template_builders: the list of ProcessTemplateDagBuilder
        """
        ...

    @abc.abstractmethod
    def set_parallel_process_template_dags(
        self, parallel_process_templates
    ) -> None:  # pragma: no cover
        """Method to set child_process_task_templates for parallel execution

        :param parallel_process_templates: List of parallel process templates
        """
        ...


class IDynamicProcessTemplateDAG(IProcessTemplateDAG):
    """Skeleton class defining the attributes and functions of dynamic processes and task instances."""

    app: Service
    max_run_duration: int
    name: str

    def __init__(
        self,
        next_process_dags: List[IProcessTemplateDAG],
        app: Service,
        name: str,
        max_run_duration: int,
    ) -> None:
        """
        Init method
        :param next_process_dags: The next Process to execute after the current process is complete
        :param app: The Dagger instance
        :param name: The name of the process
        :param max_run_duration: the timeout on the process
        """
        self.next_process_dag = next_process_dags
        self.app = app
        self.max_run_duration = max_run_duration
        self.name = name

    async def create_instance(
        self,
        id: UUID,
        parent_id: UUID,
        parent_name: str,
        partition_key_lookup: str,
        *,
        repartition: bool = True,
        seed: random.Random = None,
        workflow_instance: ITemplateDAGInstance = None,
        **kwargs,
    ) -> IProcessTemplateDAGInstance:  # pragma: no cover
        """Method for creating an dynamic instance(s) of a template and return the head of the list
        :param id: The id of the instance to create
        :param parent_id: the id of the parent instance
        :param parent_name: the name of the parent workflow
        :param partition_key_lookup: The kafka partitioning key to use serialize the storage of this instance
        :param repartition: If true, the instance is stored on the node owning the parition defined by the partioning
        key
        :param seed: the seed to use any create any child instances
        :param workflow_instance: the instance of the workflow
        :param **kwargs: Other keyword arguments
        """
        ...

    def set_dynamic_process_builders(self, process_template_builders) -> None:
        self._dynamic_process_builders = process_template_builders

    def set_parallel_process_template_dags(
        self, parallel_process_templates: List[IProcessTemplateDAG]
    ) -> None:  # pragma: no cover
        raise NotImplementedError(
            "IDynamicProcessTemplateDAG does not implement this method"
        )


class TaskTemplate:
    """Skeleton class defining the attributes and functions of process and task instances."""

    app: Service
    next_task_dag: List[TaskTemplate]

    @abc.abstractmethod
    async def create_instance(
        self,
        id: UUID,
        parent_id: UUID,
        parent_name: str,
        partition_key_lookup: str,
        *,
        repartition: bool = True,
        seed: random.Random = None,
        workflow_instance: ITemplateDAGInstance = None,
        **kwargs,
    ) -> ITask:  # pragma: no cover
        """Method for creating an instance of a template definition
        :param id: the ID of the instance
        :param parent_id: the ID of the parent instance if any
        :param parent_name: the name of the parent instance
        :param partition_key_lookup: The kafka paritioning key
        :param repartition: If true the instance is serialized and stored by the node owning the parition defined by
        the partitioning key
        :param seed: the seed to use to create any child instanes
        :param workflow_instance: the workflow object
        :param **kwargs: Other keyword arguments
        """
        ...


class DefaultTaskTemplate(TaskTemplate):
    """Default implementation of task template."""

    _type: Type[ITask]

    def __init__(
        self,
        app: Service,
        type: Type[ITask],
        name: str,
        task_dag_template: List[TaskTemplate],
        allow_skip_to: bool,
        reprocess_on_message: bool = False,
    ) -> None:
        """
        Init method
        :param app: The Dagger Instance
        :param type: The type of the task defined within the workflow
        :param name: the name of the task
        :param task_dag_template: The next task to execute after this task
        :param allow_skip_to: Set to true if processing of the worklow can skip to this task
        :param reprocess_on_message: the task is executed when invoked irresepective of the state of the task
        """
        self.app = app
        self._type = type
        self.next_task_dag = task_dag_template
        self.name = name
        self.allow_skip_to = allow_skip_to
        self.reprocess_on_message = reprocess_on_message

    async def create_instance(
        self,
        id: UUID,
        parent_id: UUID,
        parent_name: str,
        partition_key_lookup: str,
        *,
        repartition: bool = True,
        seed: random.Random = None,
        workflow_instance: ITemplateDAGInstance = None,
        **kwargs: Any,
    ) -> ITask:
        """
        Creates an instance of the task defined by this template
        :param id: the id of the instance to create
        :param parent_id: the id of the parent of this task to be created
        :param parent_name: the name of the task of the parent of the task to be created
        :param partition_key_lookup: the kafka partioning key if this instance needs to be repartitioned
        :param repartition: If true, the instance is stored in the node owning the partition defined by the paritioning
        key
        :param seed: the seed to use to create any child instances
        :param workflow_instance: the workflow object
        :param **kwargs: other keywork arguments
        """
        task_instance = self._type(id=id, parent_id=parent_id)
        return await self._setup_instance(
            task_instance,
            parent_id=parent_id,
            parent_name=parent_name,
            partition_key_lookup=partition_key_lookup,
            repartition=repartition,
            seed=seed,
            workflow_instance=workflow_instance,
            **kwargs,
        )

    async def _setup_instance(
        self,
        task_instance: ITask,
        parent_id: UUID,
        parent_name: str,
        partition_key_lookup: str,
        *,
        repartition: bool = True,
        seed: random.Random = None,
        workflow_instance: ITemplateDAGInstance = None,
        **kwargs: Any,
    ) -> ITask:
        """Follows from create instance. Sets up next tasks and sends to tasks topic.

        :param task_instance: Instance of ITask.
        :param parent_id: Id of parent ITask.
        :param parent_name: the name of the parent task
        :param partition_key_lookup: Kafka topic partition key associated with the ITask.
         :param partition_key_lookup: the kafka partioning key if this instance needs to be repartitioned
        :param repartition: If true, the instance is stored in the node owning the partition defined by the paritioning
        key
        :param seed: the seed to use to create any child instances
        :param workflow_instance: the workflow object
        :param **kwargs: other keywork arguments
        :return: Instance of ITask.
        """
        task_instance.time_created = int(time.time())
        task_instance.status = TaskStatus(
            code=TaskStatusEnum.NOT_STARTED.name, value=TaskStatusEnum.NOT_STARTED.value
        )
        task_instance.task_name = f"{parent_name}.{self.name}"
        task_instance.next_dags = list()
        task_instance.allow_skip_to = self.allow_skip_to
        task_instance.reprocess_on_message = self.reprocess_on_message
        if workflow_instance:
            workflow_instance.add_task(task=task_instance)
        for next_task_template in self.next_task_dag:
            dag_id = (
                DAGIDGenerator.generate_dag_id_from_seed(seed) if seed else uuid.uuid1()
            )
            next_task_instance = await next_task_template.create_instance(
                dag_id,
                parent_id=parent_id,
                parent_name=parent_name,
                partition_key_lookup=partition_key_lookup,
                repartition=repartition,
                seed=seed,
                workflow_instance=workflow_instance,
                **kwargs,
            )
            task_instance.next_dags.append(next_task_instance.id)
        if task_instance.correlatable_key:
            await self.app._insert_correletable_key_task(  # type: ignore
                task_instance=task_instance, workflow_instance=workflow_instance
            )  # type: ignore
        if isinstance(task_instance, TriggerTask) and task_instance.time_to_execute:
            await self.app._store_trigger_instance(task_instance=task_instance, workflow_instance=workflow_instance)  # type: ignore
        return task_instance


class DefaultKafkaTaskTemplate(DefaultTaskTemplate):
    """Default implementation of KafkaCommandTask."""

    topic: Topic

    def __init__(
        self,
        app: Service,
        type: Type[KafkaCommandTask[KT, VT]],
        name: str,
        topic: Topic,
        task_dag_templates: List[TaskTemplate],
        allow_skip_to: bool,
        reprocess_on_message: bool = False,
    ) -> None:
        """
        Init method
        :param app: Dagger instance
        :param type: The type of KafkaCommandTask
        :param name: the name of the task
        :param topic: The topic to send the command on
        :param task_dag_templates: the next task to execute after this task
        :param allow_skip_to: If true the execution of the workflow can jump to this task
        :param reprocess_on_message: if true, the task is executed irrespective of the state of this task
        """
        super().__init__(
            app=app,
            type=type,
            name=name,
            task_dag_template=task_dag_templates,
            allow_skip_to=allow_skip_to,
            reprocess_on_message=reprocess_on_message,
        )
        self.topic = topic

    async def create_instance(
        self,
        id: UUID,
        parent_id: UUID,
        parent_name: str,
        partition_key_lookup: str,
        *,
        repartition: bool = True,
        seed: random.Random = None,
        workflow_instance: ITemplateDAGInstance = None,
        **kwargs: Any,
    ) -> ITask:
        """Method for creating an instance of a kafkaTask
        :param id: The id of the instance
        :param parent_id: the id of the parent of this process(The workflow instance)
        :param parent_name: the name of the workflow
        :param partition_key_lookup: The kafka partitioning key to look up
        :param repartition: If true the instance is serialized and stored in the owner of the parition owned by the
        partioning key
        :param seed: The seed to use to create any child instances
        :param workflow_instance: the instance of the workflow
        :param **kwargs: Other keyword arguments
        :return: the instance of the Process
        """
        self.app.add_topic(self.topic.get_topic_name(), self.topic)  # type: ignore
        task_instance = self._type(
            id=id, topic=self.topic.get_topic_name(), parent_id=parent_id
        )
        return await super()._setup_instance(
            task_instance,
            parent_id=parent_id,
            parent_name=parent_name,
            partition_key_lookup=partition_key_lookup,
            repartition=repartition,
            seed=seed,
            workflow_instance=workflow_instance,
            **kwargs,
        )


class DefaultTriggerTaskTemplate(DefaultTaskTemplate):
    """Default implementation of TriggerTask."""

    time_to_execute_lookup_key: str

    def __init__(
        self,
        app: Service,
        type: Type[TriggerTask[KT, VT]],
        name: str,
        time_to_execute_key: str,
        task_dag_templates: List[TaskTemplate],
        allow_skip_to: bool,
    ) -> None:
        """
        Init method
        :param app: The dagger instance
        :param type: The type of TriggerTask
        :param name: the name of the task
        :param time_to_execute_key: the key lookup in runtime paramters to execute the trigger
        :param allow_skip_to: Flag For skipping serial execution
        """
        super().__init__(
            app=app,
            type=type,
            name=name,
            task_dag_template=task_dag_templates,
            allow_skip_to=allow_skip_to,
        )
        self.time_to_execute_lookup_key = time_to_execute_key

    async def create_instance(
        self,
        id: UUID,
        parent_id: UUID,
        parent_name: str,
        partition_key_lookup: str,
        *,
        repartition: bool = True,
        seed: random.Random = None,
        workflow_instance: ITemplateDAGInstance = None,
        **kwargs: Any,
    ) -> ITask:
        """Method for creating an instance of a TriggerTask
        :param id: The id of the instance
        :param parent_id: the id of the parent of this process(The workflow instance)
        :param parent_name: the name of the workflow
        :param partition_key_lookup: The kafka partitioning key to look up
        :param repartition: If true the instance is serialized and stored in the owner of the parition owned by the
        partioning key
        :param seed: The seed to use to create any child instances
        :param workflow_instance: the instance of the workflow
        :param **kwargs: Other keyword arguments
        :return: the instance of the Process
        """
        time_to_execute = (
            workflow_instance.runtime_parameters.get(
                self.time_to_execute_lookup_key, None
            )
            if workflow_instance
            else None
        )
        if self.time_to_execute_lookup_key is None or time_to_execute is None:
            raise InvalidTriggerTimeForTask(f"Task in invalid state {id}")
        task_instance = self._type(
            id=id,
            time_to_execute=time_to_execute,
            parent_id=parent_id,
        )
        return await super()._setup_instance(
            task_instance,
            parent_id=parent_id,
            parent_name=parent_name,
            partition_key_lookup=partition_key_lookup,
            repartition=repartition,
            seed=seed,
            workflow_instance=workflow_instance,
            **kwargs,
        )


class DefaultIntervalTaskTemplate(DefaultTaskTemplate):
    """Default implementation of IntervalTask."""

    time_to_execute_lookup_key: str
    time_to_force_complete_lookup_key: str
    interval_execute_period_key: str

    def __init__(
        self,
        app: Service,
        type: Type[IntervalTask[KT, VT]],
        name: str,
        time_to_execute_key: str,
        time_to_force_complete_key: str,
        interval_execute_period_key: str,
        task_dag_templates: List[TaskTemplate],
        allow_skip_to: bool,
    ) -> None:
        """
        Init method
        :param app: Dagger instance
        :param type: The type of IntervalTask
        :param name: the name of the task
        :param time_to_execute_key: the key to lookup in the runtime paramters to trigger the task
        :param time_to_force_complete_key: the key to lookup to timeout the task
        :param interval_execute_period_key: the frequency of execution from trigger time to timeout until the task
        succeeds
        :param task_dag_templates: the next task to execute
        :param allow_skip_to: Flag to skip serial execution
        """
        super().__init__(
            app=app,
            type=type,
            name=name,
            task_dag_template=task_dag_templates,
            allow_skip_to=allow_skip_to,
        )
        self.time_to_execute_lookup_key = time_to_execute_key
        self.time_to_force_complete_lookup_key = time_to_force_complete_key
        self.interval_execute_period_key = interval_execute_period_key

    async def create_instance(
        self,
        id: UUID,
        parent_id: UUID,
        parent_name: str,
        partition_key_lookup: str,
        *,
        repartition: bool = True,
        seed: random.Random = None,
        workflow_instance: ITemplateDAGInstance = None,
        **kwargs,
    ) -> ITask:
        """Method for creating an instance of IntervalTask
        :param id: The id of the instance
        :param parent_id: the id of the parent of this process(The workflow instance)
        :param parent_name: the name of the workflow
        :param partition_key_lookup: The kafka partitioning key to look up
        :param repartition: If true the instance is serialized and stored in the owner of the parition owned by the
        partioning key
        :param seed: The seed to use to create any child instances
        :param workflow_instance: the instance of the workflow
        :param **kwargs: Other keyword arguments
        :return: the instance of the Process
        """
        time_to_execute_lookup_key_runtime = (
            workflow_instance.runtime_parameters.get(
                self.time_to_execute_lookup_key, None
            )
            if workflow_instance
            else None
        )
        time_to_force_complete_lookup_key_runtime = (
            workflow_instance.runtime_parameters.get(
                self.time_to_force_complete_lookup_key, None
            )
            if workflow_instance
            else None
        )
        interval_execute_period_key_runtime = (
            workflow_instance.runtime_parameters.get(
                self.interval_execute_period_key, None
            )
            if workflow_instance
            else None
        )
        if (
            self.time_to_execute_lookup_key
            and time_to_execute_lookup_key_runtime is None
        ):
            raise InvalidTriggerTimeForTask(f"Task in invalid state {id}")
        if (
            self.time_to_force_complete_lookup_key is None
            or time_to_force_complete_lookup_key_runtime is None
        ):
            raise InvalidTriggerTimeForTask(f"Task in invalid state {id}")
        if (
            self.interval_execute_period_key is None
            or interval_execute_period_key_runtime is None
        ):
            raise InvalidTriggerTimeForTask(f"Task in invalid state {id}")
        task_instance = self._type(
            id=id,
            time_to_execute=time_to_execute_lookup_key_runtime
            if self.time_to_execute_lookup_key
            else None,
            time_to_force_complete=time_to_force_complete_lookup_key_runtime,
            interval_execute_period=interval_execute_period_key_runtime,
            parent_id=parent_id,
        )
        if workflow_instance:
            workflow_instance.add_task(task=task_instance)
        return await super()._setup_instance(
            task_instance,
            parent_id=parent_id,
            parent_name=parent_name,
            partition_key_lookup=partition_key_lookup,
            repartition=repartition,
            seed=seed,
            workflow_instance=workflow_instance,
            **kwargs,
        )


class ParallelCompositeTaskTemplate(DefaultTaskTemplate):
    """
    Template to define the structure of parallel tasks to execute within a workflow
    """

    child_task_templates: List[TaskTemplate]
    parallel_operator_type: TaskOperator

    def __init__(
        self,
        app: Service,
        type: Type[ITask],
        name: str,
        task_dag_templates: List[TaskTemplate],
        child_task_templates: List[TaskTemplate],
        allow_skip_to: bool,
        reprocess_on_message: bool = False,
        parallel_operator_type: TaskOperator = TaskOperator.JOIN_ALL,
    ) -> None:
        """
        Init method
        :param app: The dagger instance
        :param type: The type of ParallelCompositeTask
        :param name: The name of the ParallelCompositeTask
        :param task_dag_templates: the next task to execute after the parallel task completes
        :param child_task_templates: the set of parallel tasks to execute
        :param allow_skip_to: Skip serial execution of the workflow if this is set
        :param reprocess_on_message: Re-execute the task irrespective of the state of the task
        :param parallel_operator_type: Wait for all parallel tasks to complete or just one before transitioning to the
        next task in the workflow
        """
        super().__init__(
            app=app,
            type=type,
            name=name,
            task_dag_template=task_dag_templates,
            allow_skip_to=allow_skip_to,
            reprocess_on_message=reprocess_on_message,
        )
        self.child_task_templates = child_task_templates
        self.parallel_operator_type = parallel_operator_type

    async def _setup_instance(
        self,
        task_instance: ITask,
        parent_id: UUID,
        parent_name: str,
        partition_key_lookup: str,
        *,
        repartition: bool = True,
        seed: random.Random = None,
        workflow_instance: ITemplateDAGInstance = None,
        **kwargs: Any,
    ) -> ITask:
        """Follows from create instance. Sets up next tasks and sends to tasks topic.

        :param task_instance: Instance of ITask.
        :param parent_id: Id of parent ITask.
        :param parent_name: the name of the parent task
        :param partition_key_lookup: Kafka topic partition key associated with the ITask.
         :param partition_key_lookup: the kafka partioning key if this instance needs to be repartitioned
        :param repartition: If true, the instance is stored in the node owning the partition defined by the paritioning
        key
        :param seed: the seed to use to create any child instances
        :param workflow_instance: the workflow object
        :param **kwargs: other keywork arguments
        :return: Instance of ITask.
        """
        task_instance.time_created = int(time.time())
        task_instance.status = TaskStatus(
            code=TaskStatusEnum.NOT_STARTED.name, value=TaskStatusEnum.NOT_STARTED.value
        )
        task_instance.task_name = f"{parent_name}.{self.name}"
        task_instance.next_dags = list()
        task_instance.task_type = TaskType.PARALLEL_COMPOSITE.name
        task_instance.parallel_child_task_list = list()
        task_instance.operator_type = self.parallel_operator_type.name
        task_instance.allow_skip_to = self.allow_skip_to
        task_instance.reprocess_on_message = self.reprocess_on_message
        if workflow_instance:
            workflow_instance.add_task(task=task_instance)
        for next_task_template in self.next_task_dag:
            dag_id = (
                DAGIDGenerator.generate_dag_id_from_seed(seed) if seed else uuid.uuid1()
            )
            next_task_instance = await next_task_template.create_instance(
                id=dag_id,
                parent_id=parent_id,
                parent_name=parent_name,
                partition_key_lookup=partition_key_lookup,
                repartition=repartition,
                seed=seed,
                workflow_instance=workflow_instance,
                **kwargs,
            )
            task_instance.next_dags.append(next_task_instance.id)
        for next_task_template in self.child_task_templates:
            dag_id = (
                DAGIDGenerator.generate_dag_id_from_seed(seed) if seed else uuid.uuid1()
            )
            next_task_instance = await next_task_template.create_instance(
                id=dag_id,
                parent_id=task_instance.id,
                parent_name=task_instance.task_name,
                partition_key_lookup=partition_key_lookup,
                repartition=repartition,
                seed=seed,
                workflow_instance=workflow_instance,
                **kwargs,
            )
            task_instance.parallel_child_task_list.append(next_task_instance.id)
        return task_instance


class TaskTemplateBuilder:
    """Skeleton builder class used to build the definition of a task object
    within a workflow
    """

    app: Service
    task_dags: List[TaskTemplate]
    allow_skip_to: bool
    reprocess_on_message: bool

    def __init__(self, app: Service) -> None:
        """
        :param app: The Dagger object
        """
        self.app = app
        self.task_dags = list()
        self.name = "task"
        self.allow_skip_to = False
        self.reprocess_on_message = False

    @abc.abstractmethod
    def set_type(
        self, task_type: Type[ITask]
    ) -> TaskTemplateBuilder:  # pragma: no cover
        """Sets the type of task
        :param task_type: the type of the task to instantiate from the builder
        :return: An instance of the updated template builder.
        """
        ...

    @abc.abstractmethod
    def set_name(self, name: str) -> TaskTemplateBuilder:  # pragma: no cover
        """Set the name of task
        :param name: the name of the Task
        :return: An instance of the updated template builder.
        """
        ...

    def set_next(
        self, task_template: TaskTemplate
    ) -> TaskTemplateBuilder:  # pragma: no cover
        """Set the next task in the process definition within the workflow
        :param task_template: the next task template definition
        :return: An instance of the updated template builder.
        """
        self.task_dags.append(task_template)
        return self

    def set_allow_skip_to(
        self, allow_skip_to: bool
    ) -> TaskTemplateBuilder:  # pragma: no cover
        """Set whether or not this task is allowed to be executed out of order (skipped to)
        :param allow_skip_to: If set to true a Sensor task can be executed if an event is received out of order
        :return: An instance of the updated template builder.
        """
        self.allow_skip_to = allow_skip_to
        return self

    @abc.abstractmethod
    def build(self) -> TaskTemplate:  # pragma: no cover
        """Builds the TaskTemplate object
        :return: The TaskTemplate instance to add to the ProcessBuilder definition
        """
        ...


class DefaultTaskTemplateBuilder(TaskTemplateBuilder):
    """Default Implementation of TaskTemplateBuilder"""

    _type: Type[ITask]
    name: str

    def __init__(self, app: Service) -> None:
        super().__init__(app)

    def set_type(self, task_type: Type[ITask[KT, VT]]) -> TaskTemplateBuilder:
        if issubclass(task_type, KafkaListenerTask) or issubclass(
            task_type, KafkaCommandTask
        ):
            raise InvalidTaskType(f"Invalid task type {task_type}")
        self._type = task_type
        return self

    def set_name(self, name: str) -> TaskTemplateBuilder:
        self.name = name
        return self

    def build(self) -> TaskTemplate:
        return DefaultTaskTemplate(
            app=self.app,
            type=self._type,
            name=self.name,
            task_dag_template=self.task_dags,
            allow_skip_to=self.allow_skip_to,
            reprocess_on_message=self.reprocess_on_message,
        )


class ParallelCompositeTaskTemplateBuilder(DefaultTaskTemplateBuilder):
    """
    A type of DefaultTaskTemplateBuilder to create ParallelTasks
    """

    parallel_tasks_templates: List[TaskTemplate]
    operator: TaskOperator

    def __init__(
        self, app: Service, no_of_parallel_tasks: int, task_template: TaskTemplate
    ) -> None:
        super().__init__(app)
        self.parallel_tasks_templates = self.get_parallel_tasks_templates(
            no_of_parallel_tasks, task_template
        )
        self.operator = TaskOperator.JOIN_ALL

    def add_parallel_task(self, task_template: TaskTemplate) -> TaskTemplateBuilder:
        self.parallel_tasks_templates.append(task_template)
        return self

    def get_parallel_tasks_templates(
        self, no_of_parallel_tasks: int, task_template: TaskTemplate
    ) -> List[TaskTemplate]:
        parallel_tasks_templates = list()

        while no_of_parallel_tasks > 0:
            parallel_tasks_templates.append(task_template)
            no_of_parallel_tasks -= 1
        return parallel_tasks_templates

    def set_task_operator(self, operator: TaskOperator) -> TaskTemplateBuilder:
        self.operator = operator
        return self

    def build(self) -> TaskTemplate:
        return ParallelCompositeTaskTemplate(
            app=self.app,
            type=self._type,
            name=self.name,
            task_dag_templates=self.task_dags,
            allow_skip_to=self.allow_skip_to,
            reprocess_on_message=self.reprocess_on_message,
            parallel_operator_type=self.operator,
            child_task_templates=self.parallel_tasks_templates,
        )


class KafkaCommandTaskTemplateBuilder(DefaultTaskTemplateBuilder):
    """
    A type of DefaultTaskTemplateBuilder to define KafkaCommandTasks
    """

    _type: Type[KafkaCommandTask]
    _topic: Topic
    name: str

    def __init__(self, app: Service) -> None:
        super().__init__(app)

    def set_type(self, task_type: Type[ITask]) -> TaskTemplateBuilder:
        if not issubclass(task_type, KafkaCommandTask):
            raise InvalidTaskType(f"Invalid task type {task_type}")
        self._type = task_type
        return self

    def set_topic(self, topic: Topic) -> TaskTemplateBuilder:
        self._topic = topic
        return self

    def build(self) -> TaskTemplate:
        return DefaultKafkaTaskTemplate(
            app=self.app,
            type=self._type,
            name=self.name,
            topic=self._topic,
            task_dag_templates=self.task_dags,
            allow_skip_to=self.allow_skip_to,
        )


class DecisionTaskTemplateBuilder(DefaultTaskTemplateBuilder):
    """
    A type of DefaultTaskTemplateBuilder to define DecisionTasks
    """

    def __init__(self, app: Service) -> None:
        super().__init__(app)

    def set_type(self, task_type: Type[ITask]) -> TaskTemplateBuilder:
        if not issubclass(task_type, DecisionTask):
            raise InvalidTaskType(f"Invalid task type {task_type}")
        self._type = task_type
        return self


class TriggerTaskTemplateBuilder(DefaultTaskTemplateBuilder):
    """
    A type of DefaultTaskTemplateBuilder to define TriggerTasks
    """

    _time_to_execute_key: str

    def set_time_to_execute_lookup_key(self, key: str) -> TriggerTaskTemplateBuilder:
        self._time_to_execute_key = key
        return self

    def __init__(self, app: Service) -> None:
        super().__init__(app)

    def set_type(self, task_type: Type[ITask]) -> TriggerTaskTemplateBuilder:
        if not issubclass(task_type, TriggerTask):
            raise InvalidTaskType(f"Invalid task type {task_type}")
        self._type = task_type
        return self

    def build(self) -> TaskTemplate:
        return DefaultTriggerTaskTemplate(
            app=self.app,
            type=self._type,
            name=self.name,
            time_to_execute_key=self._time_to_execute_key,
            task_dag_templates=self.task_dags,
            allow_skip_to=self.allow_skip_to,
        )


class IntervalTaskTemplateBuilder(TriggerTaskTemplateBuilder):
    """
    A type of DefaultTaskTemplateBuilder to define IntervalTasks
    """

    _time_to_execute_key: Optional[str] = None  # type: ignore
    _time_to_force_complete_key: str
    _interval_execute_period_key: str

    def set_interval_execute_period_lookup_key(
        self, key: str
    ) -> IntervalTaskTemplateBuilder:
        self._interval_execute_period_key = key
        return self

    def set_time_to_force_complete_lookup_key(
        self, key: str
    ) -> IntervalTaskTemplateBuilder:
        self._time_to_force_complete_key = key
        return self

    def build(self) -> TaskTemplate:
        return DefaultIntervalTaskTemplate(  # type: ignore
            app=self.app,
            type=self._type,
            name=self.name,
            time_to_execute_key=self._time_to_execute_key,  # type: ignore
            time_to_force_complete_key=self._time_to_force_complete_key,
            interval_execute_period_key=self._interval_execute_period_key,
            task_dag_templates=self.task_dags,
            allow_skip_to=self.allow_skip_to,
        )


class KafkaListenerTaskTemplateBuilder(DefaultTaskTemplateBuilder):
    """
    A type of DefaultTaskTemplateBuilder to define KafkaListenerTasks
    """

    _topic: Topic
    _concurrency: int = 1

    def __init__(self, app: Service) -> None:
        super().__init__(app)
        self._concurrency = 1

    def set_type(self, task_type: Type[ITask]) -> TaskTemplateBuilder:
        if not issubclass(task_type, KafkaListenerTask):
            raise InvalidTaskType(f"Invalid task type {task_type}")
        self._type = task_type
        return self

    def set_topic(self, topic: Topic) -> TaskTemplateBuilder:
        self._topic = topic
        return self

    def set_concurrency(self, concurrency: int):
        self._concurrency = concurrency

    def set_reprocess_on_message(
        self, reprocess_on_message: bool
    ) -> TaskTemplateBuilder:  # pragma: no cover
        """Set whether or not this task should reprocess on_message if a correlated message is reprocessed."""
        self.reprocess_on_message = reprocess_on_message
        return self

    def build(self) -> TaskTemplate:
        # create an internal instance of KAFKAListener task

        internal_task = self._type(uuid.uuid1())
        agent = KafkaAgent(app=self.app, topic=self._topic, task=internal_task)
        agent.decorate(self.app.faust_app, concurrency=self._concurrency)  # type: ignore
        return DefaultKafkaTaskTemplate(
            app=self.app,
            type=self._type,
            name=self.name,
            task_dag_templates=self.task_dags,
            allow_skip_to=self.allow_skip_to,
            reprocess_on_message=self.reprocess_on_message,
            topic=self._topic,
        )


class IProcessTemplateDAGBuilder:
    """Skeleton builder class used to build a process definition within a workfow."""

    app: Service
    root_task_dag: TaskTemplate
    next_process_dag: List[IProcessTemplateDAG]

    def __init__(self, app: Service) -> None:
        """
        :param app: The Dagger instance
        """
        self.app = app
        self.next_process_dag = list()

    @abc.abstractmethod
    def set_root_task(
        self, task: TaskTemplate
    ) -> IProcessTemplateDAGBuilder:  # pragma: no cover
        """Set the first task in the process definition of the workflow.

        :param task: TaskTemplate to be set as the first task of the process execution.
        :return: An instance of the updated process template builder.
        """
        ...

    @abc.abstractmethod
    def set_next_process(
        self, task: IProcessTemplateDAG
    ) -> IProcessTemplateDAGBuilder:  # pragma: no cover
        """Set the next process in the execution of the workflow defintion.

        :param task: TaskTemplate to be set as the next process.
        :return: An instance of the updated process template builder.
        """
        ...

    @abc.abstractmethod
    def set_name(
        self, process_name: str
    ) -> IProcessTemplateDAGBuilder:  # pragma: no cover
        """Sets the name of the process.

        :param process_name: Name of the process.
        :return: An instance of the updated process template builder.
        """
        ...

    @abc.abstractmethod
    def set_type(
        self, process_type: Type[ITask]
    ) -> IProcessTemplateDAGBuilder:  # pragma: no cover
        """Sets the type of the process.

        :param process_type: The type of the process.
        :return: An instance of the updated process template builder.
        """
        ...

    @abc.abstractmethod
    def build(self) -> IProcessTemplateDAG:  # pragma: no cover
        """Builds the IProcessTemplateDAG object
        :return: the instance of IProcessTemplateDAG to create the workflow definition
        """
        ...


class ITemplateDAGBuilder:

    """
    Base class to define the structure of workflow definition
    """

    app: Service

    def __init__(self, app: Service) -> None:
        """
        :param app: The Dagger app instance
        """
        self.app = app

    @abc.abstractmethod
    def set_root(
        self, template: IProcessTemplateDAG
    ) -> ITemplateDAGBuilder:  # pragma: no cover
        """Sets the first process to execute in the template.

        :param template: Instance of a process template containing the defintion of the process.
        :return: An instance of the updated template builder.
        """
        ...

    @abc.abstractmethod
    def build(self) -> ITemplateDAG:  # pragma: no cover
        """Builds the ITemplateDAGBuilder object.
        :return: The instance of ITemplateDAG
        """
        ...

    @abc.abstractmethod
    def set_name(self, name: str) -> ITemplateDAGBuilder:  # pragma: no cover
        """Sets the name of the template.

        :param name: Name of the template.
        :return: An instance of the updated template builder.
        """
        ...

    @abc.abstractmethod
    def set_type(
        self, template_type: Type[ITemplateDAGInstance]
    ) -> ITemplateDAGBuilder:  # pragma: no cover
        """Sets the type of the process.

        :param template_type: The type of the process.
        :return: An instance of the updated template builder.
        """
        ...
