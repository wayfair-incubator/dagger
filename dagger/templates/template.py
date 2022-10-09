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
        **kwargs,
    ) -> ITemplateDAGInstance:  # pragma: no cover
        """Method for creating an instance of a template"""
        ...

    @abc.abstractmethod
    def set_dynamic_builders_for_process_template(
        self, name: str, process_template_builders
    ):
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
        root_task_dag: TaskTemplate,
        max_run_duration: int,
    ) -> None:
        self.next_process_dag = next_process_dag
        self.app = app
        self.name = name
        self.process_type = process_type
        self.root_task_dag = root_task_dag
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
        """Method for creating an instance of a template"""
        ...

    @abc.abstractmethod
    def set_dynamic_process_builders(
        self, process_template_builders
    ) -> None:  # pragma: no cover
        ...

    @abc.abstractmethod
    def set_parallel_process_template_dags(
        self, parallel_process_templates
    ) -> None:  # pragma: no cover
        """Method to set child_process_task_templates

        Args:
            parallel_process_templates (List[IProcessTemplateDAG]): List of parallel process templates
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
        """Method for creating an dynamic instance(s) of a template and return the head of the list"""
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
        """Method for creating an instance of a template"""
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

        Args:
            task_instance (ITask): Instance of ITask.
            parent_id (UUID): Id of parent ITask.
            partition_key_lookup (str): Kafka topic partition key associated with the ITask.

        Returns:
            ITask: Instance of ITask.
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
        if (
            self.time_to_execute_lookup_key is None
            or workflow_instance.runtime_parameters.get(
                self.time_to_execute_lookup_key, None
            )
            is None
        ):
            raise InvalidTriggerTimeForTask(f"Task in invalid state {id}")
        task_instance = self._type(
            id=id,
            time_to_execute=workflow_instance.runtime_parameters[
                self.time_to_execute_lookup_key
            ],
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
        if (
            self.time_to_execute_lookup_key
            and workflow_instance.runtime_parameters.get(
                self.time_to_execute_lookup_key, None
            )
            is None
        ):
            raise InvalidTriggerTimeForTask(f"Task in invalid state {id}")
        if (
            self.time_to_force_complete_lookup_key is None
            or workflow_instance.runtime_parameters.get(
                self.time_to_force_complete_lookup_key, None
            )
            is None
        ):
            raise InvalidTriggerTimeForTask(f"Task in invalid state {id}")
        if (
            self.interval_execute_period_key is None
            or workflow_instance.runtime_parameters.get(
                self.interval_execute_period_key, None
            )
            is None
        ):
            raise InvalidTriggerTimeForTask(f"Task in invalid state {id}")
        task_instance = self._type(
            id=id,
            time_to_execute=workflow_instance.runtime_parameters[
                self.time_to_force_complete_lookup_key
            ]
            if self.time_to_execute_lookup_key
            else None,
            time_to_force_complete=workflow_instance.runtime_parameters[
                self.time_to_force_complete_lookup_key
            ],
            interval_execute_period=workflow_instance.runtime_parameters[
                self.interval_execute_period_key
            ],
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

        Args:
            task_instance (ITask): Instance of ITask.
            parent_id (UUID): Id of parent ITask.
            partition_key_lookup (str): Kafka topic partition key associated with the ITask.

        Returns:
            ITask: Instance of ITask.
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
    """Skeleton builder class used to build a task object."""

    app: Service
    task_dags: List[TaskTemplate]
    allow_skip_to: bool
    reprocess_on_message: bool

    def __init__(self, app: Service) -> None:
        self.app = app
        self.task_dags = list()
        self.name = "task"
        self.allow_skip_to = False
        self.reprocess_on_message = False

    @abc.abstractmethod
    def set_type(
        self, task_type: Type[ITask]
    ) -> TaskTemplateBuilder:  # pragma: no cover
        """Set the type of task."""
        ...

    @abc.abstractmethod
    def set_name(self, name: str) -> TaskTemplateBuilder:  # pragma: no cover
        """Set the type of task."""
        ...

    def set_next(
        self, task_template: TaskTemplate
    ) -> TaskTemplateBuilder:  # pragma: no cover
        """Set the next task."""
        self.task_dags.append(task_template)
        return self

    def set_allow_skip_to(
        self, allow_skip_to: bool
    ) -> TaskTemplateBuilder:  # pragma: no cover
        """Set whether or not this task is allowed to be executed out of order (skipped to)"""
        self.allow_skip_to = allow_skip_to
        return self

    @abc.abstractmethod
    def build(self) -> TaskTemplate:  # pragma: no cover
        """Builds the TaskTemplate object."""
        ...


class DefaultTaskTemplateBuilder(TaskTemplateBuilder):
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
    def __init__(self, app: Service) -> None:
        super().__init__(app)

    def set_type(self, task_type: Type[ITask]) -> TaskTemplateBuilder:
        if not issubclass(task_type, DecisionTask):
            raise InvalidTaskType(f"Invalid task type {task_type}")
        self._type = task_type
        return self


class TriggerTaskTemplateBuilder(DefaultTaskTemplateBuilder):
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
    _time_to_execute_key: Optional[str] = None
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
        return DefaultIntervalTaskTemplate(
            app=self.app,
            type=self._type,
            name=self.name,
            time_to_execute_key=self._time_to_execute_key,
            time_to_force_complete_key=self._time_to_force_complete_key,
            interval_execute_period_key=self._interval_execute_period_key,
            task_dag_templates=self.task_dags,
            allow_skip_to=self.allow_skip_to,
        )


class KafkaListenerTaskTemplateBuilder(DefaultTaskTemplateBuilder):
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
    """Skeleton builder class used to build a process object."""

    app: Service
    root_task_dag: TaskTemplate
    next_process_dag: List[IProcessTemplateDAG]

    def __init__(self, app: Service) -> None:
        self.app = app
        self.next_process_dag = list()

    @abc.abstractmethod
    def set_root_task(
        self, task: TaskTemplate
    ) -> IProcessTemplateDAGBuilder:  # pragma: no cover
        """Set the first task in the process.

        Args:
            task (TaskTemplate): TaskTemplate to be set as the first task.

        Returns:
            IProcessTemplateDAGBuilder: An instance of the updated process template builder.
        """
        ...

    @abc.abstractmethod
    def set_next_process(
        self, task: IProcessTemplateDAG
    ) -> IProcessTemplateDAGBuilder:  # pragma: no cover
        """Set the next process.

        Args:
            task (TaskTemplate): TaskTemplate to be set as the next process.

        Returns:
            IProcessTemplateDAGBuilder: An instance of the updated process template builder.
        """
        ...

    @abc.abstractmethod
    def set_name(
        self, process_name: str
    ) -> IProcessTemplateDAGBuilder:  # pragma: no cover
        """Sets the name of the process.

        Args:
            process_name (str): Name of the process.

        Returns:
            IProcessTemplateDAGBuilder: An instance of the updated process template builder.
        """
        ...

    @abc.abstractmethod
    def set_type(
        self, process_type: Type[ITask]
    ) -> IProcessTemplateDAGBuilder:  # pragma: no cover
        """Sets the type of the process.

        Args:
            process_type (Type[IProcessTemplateDAGInstance]): The type of the process.

        Returns:
            IProcessTemplateDAGBuilder: An instance of the updated process template builder.
        """
        ...

    @abc.abstractmethod
    def build(self) -> IProcessTemplateDAG:  # pragma: no cover
        """Builds the IProcessTemplateDAG object."""
        ...


class ITemplateDAGBuilder:
    app: Service

    def __init__(self, app: Service) -> None:
        self.app = app

    @abc.abstractmethod
    def set_root(
        self, template: IProcessTemplateDAG
    ) -> ITemplateDAGBuilder:  # pragma: no cover
        """Sets the first process in the template.

        Args:
            template (IProcessTemplateDAG): Instance of a process template.

        Returns:
            ITemplateDAGBuilder: An instance of the updated template builder.
        """
        ...

    @abc.abstractmethod
    def build(self) -> ITemplateDAG:  # pragma: no cover
        """Builds the template object."""
        ...

    @abc.abstractmethod
    def set_name(self, name: str) -> ITemplateDAGBuilder:  # pragma: no cover
        """Sets the name of the template.

        Args:
            name (str): Name of the template.

        Returns:
            ITemplateDAGBuilder: An instance of the updated template builder.
        """
        ...

    @abc.abstractmethod
    def set_type(
        self, template_type: Type[ITemplateDAGInstance]
    ) -> ITemplateDAGBuilder:  # pragma: no cover
        """Sets the type of the process.

        Args:
            template_type (Type[ITemplateDAGInstance]): The type of the process.

        Returns:
            ITemplateDAGBuilder: An instance of the updated template builder.
        """
        ...