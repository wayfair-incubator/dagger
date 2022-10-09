from __future__ import annotations

import asyncio
import json
import logging
import time
import traceback
import uuid
from random import Random
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Dict,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Set,
)
from uuid import UUID
from weakref import WeakValueDictionary

import faust
import jsonpickle
import mode
from aiohttp.web import HTTPNotFound
from faust import App, Monitor, Record, Sensor, Topic, TopicT
from faust.sensors.datadog import DatadogMonitor
from faust.types.web import ResourceOptions
from faust.web import Blueprint, Request, Response, View
from mode import Service
from mode.utils.types.trees import NodeT
from schema_registry.client import SchemaRegistryClient  # type: ignore
from schema_registry.serializers import MessageSerializer  # type: ignore

from dagger.exceptions.exceptions import TemplateDoesNotExist
from dagger.executor.executor import ExecutorStrategy, SerialExecutorStrategy
from dagger.service.engineconfig import AerospikeConfig, EngineConfig, StoreEnum
from dagger.store.stores import AerospikeStore, IStore, RocksDBStore
from dagger.tasks.task import (
    CorrelatableMapValue,
    CorreletableKeyTasks,
    CorreletableLookUpKey,
    ITask,
    ITemplateDAGInstance,
    MonitoringTask,
    SensorTask,
    SystemTimerTask,
    TaskLookupKey,
    TaskStatusEnum,
    TriggerTask,
)
from dagger.templates.template import IProcessTemplateDAG, ITemplateDAG

logger = logging.getLogger(__name__)
RegisterFun = Callable[[Any], None]
RESTART_WAIT_TIME = 1800


class Dagger(Service):
    faust_app: App
    app: Dagger
    schema_registry_client: SchemaRegistryClient
    message_serializer: MessageSerializer
    dd_sensor: Optional[Monitor]
    config: EngineConfig
    aerospike_config: AerospikeConfig
    bootstrap_topic: TopicT
    tasks_topic: TopicT
    task_update_topic: Optional[TopicT]
    topics: Dict[str, TopicT] = dict()
    template_dags: Dict[str, ITemplateDAG]
    process_templates: Dict[str, IProcessTemplateDAG]
    _store: IStore
    _execution_strategy: ExecutorStrategy
    tasks_blueprint: Blueprint
    start_time: int
    kafka_broker_list: list
    max_tasks_per_trigger: int
    restart_tasks_on_boot: bool
    trigger_interval: int = 60
    enable_changelog: bool = True
    max_correletable_keys_in_values: int = 15000
    delete_workflow_on_complete: bool = False
    kwargs: Any
    workflows_weak_ref_map: MutableMapping[str, ITemplateDAGInstance]
    LOCK_STRIPE_SIZE = 1000
    asyncio_locks: Dict[int, asyncio.Lock] = {}
    task_update_callbacks: List[Callable[[ITemplateDAGInstance], Awaitable[None]]]

    def __init__(
        self,
        *,
        broker: str = None,
        datadir: str = None,
        store: str = StoreEnum.AEROSPIKE.value,
        application_name: str = "dagger",
        package_name: str = "dagger",
        kafka_partitions: int = 1,
        task_update_topic: Optional[str] = None,
        tasks_topic: str = "dagger_task_topic",
        bootstrap_topic: str = "dagger_bootstrap",
        beacon: NodeT = None,
        kafka_broker_list=None,
        loop: asyncio.AbstractEventLoop = None,
        tracing_sensor: Sensor = None,
        datadog_sensor: DatadogMonitor = None,
        trigger_interval=60,
        max_tasks_per_trigger=2000,
        restart_tasks_on_boot=True,
        aerospike_config: AerospikeConfig = None,
        enable_changelog: bool = True,
        max_correletable_keys_in_values: int = 15000,
        schema_registry_url: str = None,
        message_serializer: MessageSerializer = None,
        delete_workflow_on_complete: bool = False,
        task_update_callbacks: List[
            Callable[[ITemplateDAGInstance], Awaitable[None]]
        ] = [],
        **kwargs: Any,
    ) -> None:
        """Initialize an instance of Dagger

        Args:
            broker (str, optional): Kafka broker address i.e. kafka://0.0.0.0:9092. Defaults to None.
            datadir (str, optional): Directory where db data will reside. Defaults to None.
            store (str, optional): DB to use. Defaults to "rocksdb://".
            application_name (str, optional): Name of application. Defaults to "dagger".
            package_name (str, optional): Name of package. Defaults to "dagger".
            kafka_partitions (int, optional): Number of Kafka partitions. Defaults to 1.
            task_update_topic (str, optional): Name of topic where tasks that have updated in status will be sent. Defaults to "task_update_topic".
            tasks_topic (str, optional): Name of topic where new tasks will be sent for execution. Defaults to "dagger_task_topic".
            bootstrap_topic (str, optional): Name of topic where tasks after restart will be sent for execution. Defaults to "dagger_task_topic".
            beacon (NodeT, optional): Beacon used to track services in a dependency graph. Defaults to None.
            loop (asyncio.AbstractEventLoop, optional): Asyncio event loop to attach to. Defaults to None.
            tracing_sensor: Tracing Sensor to use for OpenTelemetry. The global tracer has to be initialized in the client. Defaults to None
            datadog_sensor: datadog statsD sensor
            aerospike_config: Config for Aerospike if enabled
            enable_changelog: Flag to enable/disable events on the table changelog topic
            max_correletable_keys_in_values: maximum number of ids in the value part to chunk
            schema_registry_url: the schema registry URK
            message_serializer: the message serializer instance using the schema registry
            delete_workflow_on_complete: deletes the workflow instance when complete
            task_update_callbacks: callbacks when a workflow instance is updated
            kwargs (Any): Other Faust keyword arguments.
        """
        self.started_flag = False
        self.restart_tasks_on_boot = restart_tasks_on_boot
        self.config = EngineConfig(
            BROKER=broker,
            DATADIR=datadir,
            STORE=store,
            APPLICATION_NAME=application_name,
            PACKAGE_NAME=package_name,
            KAFKA_PARTITIONS=kafka_partitions,
            KWARGS=kwargs,
        )
        self.aerospike_config = aerospike_config
        super().__init__(beacon=beacon, loop=loop)
        self.app = self
        self.dd_sensor = datadog_sensor
        self.bootstrap_topic: TopicT = self.faust_app.topic(
            bootstrap_topic, value_type=str
        )

        self.tasks_topic: TopicT = self.faust_app.topic(tasks_topic, value_type=str)

        self.task_update_topic: Optional[TopicT] = (
            self.faust_app.topic(task_update_topic, value_type=str)
            if task_update_topic
            else None
        )

        self.faust_app.agent(self.tasks_topic, name=self.tasks_topic.get_topic_name())(
            self._process_tasks_create_event
        )

        self.trigger_interval = trigger_interval

        self.start_time = int(time.time())
        if tracing_sensor:
            self.faust_app.sensors.add(tracing_sensor)
        if datadog_sensor:
            self.faust_app.sensors.add(datadog_sensor)
        self.max_tasks_per_trigger = max_tasks_per_trigger
        self.kafka_broker_list = kafka_broker_list
        self.enable_changelog = enable_changelog
        self.kwargs = kwargs
        if hasattr(self, "_store") and self._store:
            self._store.initialize()
            self._store.enable_changelog = self.enable_changelog
        self.max_correletable_keys_in_values = max_correletable_keys_in_values
        self.delete_workflow_on_complete = delete_workflow_on_complete
        if schema_registry_url:
            self.schema_registry_client = SchemaRegistryClient(url=schema_registry_url)
            self.message_serializer = MessageSerializer(
                schemaregistry_client=self.schema_registry_client
            )
        self.workflows_weak_ref_map = WeakValueDictionary()
        self.task_update_callbacks = task_update_callbacks
        for i in range(Dagger.LOCK_STRIPE_SIZE):
            self.asyncio_locks[i] = asyncio.Lock()

    def get_template(self, template_name: str) -> ITemplateDAG:
        """Get the instance of a template given it's name.

        Args:
            template_name (str): Name of template

        Raises:
            TemplateDoesNotExist: Template does not exist

        Returns:
            ITemplateDAG: Instance of Template
        """
        if template_name not in self.template_dags:
            raise TemplateDoesNotExist()
        return self.template_dags[template_name]

    def __create_app(self) -> faust.App:
        """Initializes instance of Faust

        Returns:
            faust.App: Instance of Faust
        """
        return App(
            self.config.APPLICATION_NAME,
            broker=self.config.BROKER,
            version=1,
            autodiscover=True,
            origin=self.config.PACKAGE_NAME,  # imported name for this project (import proj -> "proj")
            store=self.config.STORE,
            datadir=self.config.DATADIR,
            value_serializer="raw",
            web_in_thread=True,
            web_cors_options={
                "*": ResourceOptions(
                    allow_credentials=True,
                    allow_methods="*",
                    expose_headers="*",
                    allow_headers="*",
                )
            },
            **self.config.KWARGS,
        )  # mark application as ready for kubernetes

    def __post_init__(self) -> None:
        self.faust_app = self.__create_app()
        logger.info("Faust app initialized.")
        Dagger.app = self
        self.template_dags = dict()
        self.process_templates = dict()
        self.started_flag = False
        self.add_dependency(self.faust_app)
        if self.config.STORE == StoreEnum.AEROSPIKE.value:
            self._store = AerospikeStore(self)
        elif self.config.STORE == StoreEnum.ROCKSDB.value:
            self._store = RocksDBStore(self)
        self._execution_strategy = SerialExecutorStrategy(self)
        global tasks_blueprint
        self.tasks_blueprint = tasks_blueprint
        self.tasks_blueprint.register(self.faust_app, url_prefix="/tasks")
        global service_blueprint
        self.service_blueprint = service_blueprint
        self.service_blueprint.register(self.faust_app, url_prefix="/service")

    async def on_start(self) -> None:
        logger.info("Dagger initializing")

    async def on_started(self) -> None:
        self.started_flag = True
        logger.info("Dagger initialized")

    async def _process_system_timer_task(self):
        timer_task = SystemTimerTask(id=uuid.uuid1())
        await self._store.process_system_timer_task(timer_task=timer_task)

    async def _submit_task_on_bootstrap_topic(self, task: ITask):
        for topic_object in self.faust_app.consumer._active_partitions:  # type: ignore
            if topic_object.topic == self.bootstrap_topic.get_topic_name():
                await self.bootstrap_topic.send(
                    key=str(task.id), partition=topic_object.partition, value=task
                )
                logger.info(
                    f"Submitted system task {task.id} type {type(task).__name__}"
                )

    @classmethod
    def create_topic(
        cls, topic_name: str, key_type: type = str, value_type: type = str
    ) -> TopicT:
        """Create a Kafka topic using Faust (which will also create a rocksdb table)

        Args:
            topic_name (str): Name of topic
            key_type (type, optional):  Key type for the topic. Defaults to str.
            value_type (type, optional): Value type for the topic. Defaults to str.

        Returns:
            TopicT: Instance of Faust Topic
        """
        logger.info(f"Creating topic {topic_name}")
        topic_instance: TopicT = None
        if topic_name in Dagger.app.topics:
            logger.warning(f"Topic {topic_name} is already created")
            topic_instance = Dagger.app.topics[topic_name]
        else:
            topic_instance = Dagger.app.faust_app.topic(
                topic_name, key_type=key_type, value_type=value_type
            )
            Dagger.app.topics[topic_name] = topic_instance
        return topic_instance

    async def get_monitoring_task(
        self, task: ITask, workflow_instance: ITemplateDAGInstance
    ) -> Optional[MonitoringTask]:
        """
        Returns the monitoring task associated with this task

        :param task: the task to check for
        :return: the monitoring task instance or None
        """
        monitoring_task_id = getattr(task, "monitoring_task_id", None)
        if monitoring_task_id:
            return workflow_instance.get_task(id=monitoring_task_id)
        return None

    @classmethod
    def register_template(cls, template_name: str):
        """Registers a template as a path in Dagger.

        Args:
            template_name (str): Name of function that returns a built template.
        """

        def wrapped(wrapped_fun: RegisterFun):
            logger.info("registering " + template_name)
            Dagger.app.template_dags[template_name] = wrapped_fun(template_name)

        return wrapped

    @classmethod
    def register_process_template(cls, process_template_name: str):
        """Registers a process template in Dagger.

        Args:
            template_name (str): Name of function that returns a built template.
        """

        def wrapped(wrapped_fun: RegisterFun):
            logger.info("registering " + process_template_name)
            Dagger.app.process_templates[process_template_name] = wrapped_fun(
                process_template_name
            )

        return wrapped

    async def _get_tasks_by_correlatable_key(  # noqa: C901
        self, lookup_key: TaskLookupKey, get_completed: bool = False
    ) -> AsyncGenerator[Optional[ITask, ITask], None]:  # type: ignore
        """Get a task based on the lookup key associated with the task

        Args:
            lookup_key (TaskLookupKey): Key, value associated with a task

        Returns:
            Optional[ITask, ITask]: Instance of workflow task  and the SensorTask
        """
        cor_instance: CorreletableKeyTasks = await self._store.get_table_value(
            self._store.correletable_keys_table, str(lookup_key[1])
        )
        cor_instance_list: List[CorreletableKeyTasks] = []
        if cor_instance:
            task_ids_to_remove: List[CorreletableLookUpKey] = list()
            lookup_keys: Set[CorreletableLookUpKey] = set()
            while cor_instance:
                cor_instance.lookup_keys = set(cor_instance.lookup_keys)
                cor_instance_list.append(cor_instance)
                lookup_keys.update(cor_instance.lookup_keys)
                for lookup_key in cor_instance.lookup_keys:
                    workflow_instance = await self._invoke_store_get_value_for_key_with_timer(
                        str(lookup_key.workflow_id)  # type: ignore
                    )  # type: ignore
                    task: ITask = None
                    if workflow_instance and isinstance(workflow_instance, ITask):
                        if (
                            workflow_instance.status.code
                            != TaskStatusEnum.COMPLETED.name
                            or (
                                get_completed
                                and workflow_instance.status.code
                                == TaskStatusEnum.COMPLETED.name
                            )
                        ):
                            task = workflow_instance.get_task(id=lookup_key.task_id)  # type: ignore
                            if task is not None:

                                yield workflow_instance, task
                            else:
                                logger.warning(f"task {lookup_key.task_id} not found in {workflow_instance}")  # type: ignore
                                task_ids_to_remove.append(lookup_key)
                    elif task is None:
                        logger.warning(f"None objectType in correletable value key {lookup_key.task_id} value {task}")  # type: ignore
                        task_ids_to_remove.append(lookup_key)
                    elif not isinstance(task, ITask):
                        logger.error(
                            f"Invalid objectType in kvTable key {lookup_key.task_id} value {task}"
                        )
                if cor_instance.overflow_key:
                    cor_instance = await self._store.get_table_value(
                        self._store.correletable_keys_table, cor_instance.overflow_key
                    )
                else:
                    cor_instance = None
            if len(task_ids_to_remove) > 0:
                lookup_keys.difference_update(task_ids_to_remove)
                await self.persist_tasks_ids_for_correletable_keys(
                    lookup_keys=lookup_keys, cor_instances=cor_instance_list
                )

    async def _store_root_template_instance(
        self, root_template_instance: ITemplateDAGInstance
    ):
        """Store instance of root template instance in the datastore.

        Args:
            root_template_instance (ITemplateDAGInstance): Instance of root template
        """
        await self._invoke_store_insert_key_value_with_timer(
            str(root_template_instance.id), root_template_instance
        )

    async def _invoke_store_insert_key_value_with_timer(self, key: str, value: Record):
        start_time = self.faust_app.loop.time()
        await self._store.insert_key_value(str(key), value)
        end_time = self.faust_app.loop.time() - start_time
        if self.dd_sensor:
            self.dd_sensor.client.histogram(metric="insert_key_value", value=end_time)

    async def _store_trigger_instance(
        self, task_instance: TriggerTask, workflow_instance: ITemplateDAGInstance
    ):
        await self._store.store_trigger_instance(
            task_instance=task_instance, wokflow_instance=workflow_instance
        )

    async def _insert_correletable_key_task(
        self, task_instance: ITask, workflow_instance: ITemplateDAGInstance
    ):
        if task_instance.correlatable_key:
            correletable_key = workflow_instance.runtime_parameters.get(
                task_instance.correlatable_key, None
            )
            await self.update_correletable_key_for_task(
                task_instance,
                str(correletable_key),
                new_task=True,
                workflow_instance=workflow_instance,
            )

    async def update_correletable_key_for_task(
        self,
        task_instance: ITask,
        key: str = None,
        new_task: bool = False,
        workflow_instance: ITemplateDAGInstance = None,
    ):
        """Updates the correletable key in the datastore.

        Args:
            itask_instance (ITask): Instance of itask.
            task_instance (str): the key to be updated
        """
        workflow_instance.sensor_tasks_to_correletable_map[
            task_instance.get_id()
        ] = CorrelatableMapValue(task_instance.correlatable_key, key)
        if task_instance.correlatable_key and key and task_instance.topic:
            cor_instances: List[CorreletableKeyTasks] = []
            new_key = f"{key}_{task_instance.topic}"
            logger.debug(f"updating correlatable key {new_key}")
            if not new_task:
                await self.remove_task_from_correletable_keys_table(
                    task=task_instance, workflow_instance=workflow_instance
                )
            cor_instance = await self._store.get_table_value(
                self._store.correletable_keys_table, new_key
            )
            if not cor_instance:
                cor_instance = CorreletableKeyTasks()
                cor_instance.lookup_keys = set()
                cor_instances.append(cor_instance)
                cor_instance.key = new_key

            else:
                cor_instances = await self.get_correletable_key_instances(
                    cor_instance=cor_instance
                )
            terminal_cor_instance: CorreletableKeyTasks = cor_instances[-1]

            terminal_cor_instance.lookup_keys = set(terminal_cor_instance.lookup_keys)
            await self.chunk_and_store_correlatable_tasks(
                cor_instance=terminal_cor_instance,
                value=task_instance.id,
                workflow_id=workflow_instance.id,
            )

    async def chunk_and_store_correlatable_tasks(
        self, cor_instance: CorreletableKeyTasks, value: UUID, workflow_id: UUID
    ):
        if len(cor_instance.lookup_keys) >= self.max_correletable_keys_in_values:
            logger.info(f"Chunking CorreletableKeyTasks for {cor_instance.key}")
            new_chunk_id: str = str(uuid.uuid1())
            new_cor_instance = CorreletableKeyTasks()
            new_cor_instance.lookup_keys = set()
            value_to_add = CorreletableLookUpKey(workflow_id, value)
            new_cor_instance.lookup_keys.add(value_to_add)
            new_cor_instance.key = new_chunk_id
            cor_instance.overflow_key = new_chunk_id
            await self._store.set_table_value(
                self._store.correletable_keys_table,
                new_cor_instance.key,
                new_cor_instance,
            )
        else:
            cor_instance.lookup_keys = set(cor_instance.lookup_keys)
            value_to_add = CorreletableLookUpKey(workflow_id, value)
            cor_instance.lookup_keys.add(value_to_add)

        await self._store.set_table_value(
            self._store.correletable_keys_table, cor_instance.key, cor_instance
        )

    async def get_correletable_key_instances(
        self, cor_instance: CorreletableKeyTasks
    ) -> List[CorreletableKeyTasks]:
        cor_instances: List[CorreletableKeyTasks] = []
        cor_instances.append(cor_instance)
        while cor_instance and cor_instance.overflow_key:
            overflow_key: str = cor_instance.overflow_key
            cor_instance = await self._store.get_table_value(
                self._store.correletable_keys_table, cor_instance.overflow_key
            )
            if cor_instance:
                cor_instances.append(cor_instance)
            else:
                logger.warning(f"Correletable key instance not found {overflow_key}")
        return cor_instances

    async def remove_task_from_correletable_keys_table(
        self, task: ITask, workflow_instance: ITemplateDAGInstance
    ):
        if workflow_instance.runtime_parameters:
            current_key = workflow_instance.runtime_parameters.get(
                task.correlatable_key, None
            )
            if (
                task.correlatable_key
                and current_key
                and isinstance(task, SensorTask)
                and getattr(task, "_topic", None)
            ):
                new_key = f"{current_key}_{task._topic.get_topic_name()}"
                logger.debug(
                    f"Removing task {task.id} from correletable_keys_table with key {new_key}"
                )
                cor_instance: CorreletableKeyTasks = await self._store.get_table_value(
                    self._store.correletable_keys_table, new_key
                )
                if cor_instance:
                    cor_instances: List[
                        CorreletableKeyTasks
                    ] = await self.get_correletable_key_instances(
                        cor_instance=cor_instance
                    )
                    workflow_task_ids_to_persist: Set[CorreletableLookUpKey] = set()
                    for c_instance in cor_instances:
                        workflow_task_ids_to_persist.update(set(c_instance.lookup_keys))
                        remove_key = CorreletableLookUpKey(
                            workflow_instance.id, task.id
                        )
                    if remove_key in workflow_task_ids_to_persist:
                        workflow_task_ids_to_persist.remove(remove_key)
                        await self.persist_tasks_ids_for_correletable_keys(
                            lookup_keys=workflow_task_ids_to_persist,
                            cor_instances=cor_instances,
                        )

    async def persist_tasks_ids_for_correletable_keys(
        self,
        lookup_keys: Set[CorreletableKeyTasks],
        cor_instances: List[CorreletableKeyTasks],
    ) -> None:
        workflow_task_ids_list = list(lookup_keys)
        if not cor_instances:
            logger.error("Empty CorreketableKeyTasks")
            return
        chunks = [
            workflow_task_ids_list[i : i + self.max_correletable_keys_in_values]  # noqa
            for i in range(
                0, len(workflow_task_ids_list), self.max_correletable_keys_in_values
            )
        ]  # noqa

        if len(chunks) > len(cor_instances):
            logger.error("Error in length of tasks for Correletable keys")
            return

        index: int = 0
        cor_instance = cor_instances[index]
        for chunk in chunks:
            cor_instance = cor_instances[index]
            cor_instance.lookup_keys = set(chunk)
            await self._store.set_table_value(
                self._store.correletable_keys_table, cor_instance.key, cor_instance
            )
            index += 1
        prev_cor_instance: CorreletableKeyTasks = cor_instance
        while index < len(cor_instances):
            next_cor_instance: CorreletableKeyTasks = cor_instances[index]
            await self._store.del_table_value(
                self._store.correletable_keys_table, next_cor_instance.key
            )
            if prev_cor_instance and prev_cor_instance != next_cor_instance:
                prev_cor_instance.overflow_key = None
                await self._store.set_table_value(
                    self._store.correletable_keys_table,
                    prev_cor_instance.key,
                    prev_cor_instance,
                )
            prev_cor_instance = None
            index += 1

    async def _remove_root_template_instance(
        self, root_template_instance: ITemplateDAGInstance
    ):
        """Removes an instance of a root template from the datastore.

        Args:
            root_template_instance (ITemplateDAGInstance): Instance of root template.
        """
        await self._store.remove_key_value(str(root_template_instance.get_id()))
        logger.debug(
            f"Removed template instance with id: {str(root_template_instance.get_id())}"
        )

    async def _invoke_store_get_value_for_key_with_timer(self, key: str):
        start_time = self.faust_app.loop.time()
        random: Random = Random()
        random.seed(str(key))
        partition: int = random.randint(0, Dagger.LOCK_STRIPE_SIZE)  # nosec
        async with self.asyncio_locks.get(partition):
            value = self.workflows_weak_ref_map.get(key, None)
            if value:
                return value
            value = await self._store.get_value_for_key(key)
            if value is not None:
                self.workflows_weak_ref_map[key] = value
        end_time = self.faust_app.loop.time() - start_time
        if self.dd_sensor:
            self.dd_sensor.client.histogram(metric="get_value_for_key", value=end_time)
        return value

    async def get_instance(self, id: UUID, log: bool = True) -> ITask:
        """Get an instance of an ITask given it's id.

        Args:
            id (UUID): Id of the ITask.
            log (bool): suppress logging

        Returns:
            ITask: Instance of the ITask.
        """
        task = await self._invoke_store_get_value_for_key_with_timer(str(id))
        if not task:
            try:
                raise Exception
            except Exception:
                if log:
                    logger.warning(
                        f"Task with id: {str(id)} was not found in the kv-table. Returning None instead. {traceback.format_stack()}"
                    )
        return task

    def get_db_options(self) -> Mapping[str, Any]:
        return self._store.get_db_options()

    async def _update_instance(self, task: ITemplateDAGInstance) -> None:
        """Update the state of an ITask.

        Args:
            task (ITask): Instance of ITask.
        """

        await self._invoke_store_insert_key_value_with_timer(str(task.id), task)
        logger.debug(
            f"State update for ITask with id: {str(task.id)} stored to datastore."
        )

    async def submit(self, task: ITask, *, repartition: bool = True) -> None:
        """Submits an ITask for execution.

        Args:
            task (ITask): Instance of ITask.
        """
        logger.debug(f"Submitting task with id: {str(task.get_id())}")
        await self._execution_strategy.submit(task, repartition=repartition)

    def add_topic(self, topic_name: str, topic: Topic) -> None:
        """Associate a topic instance with a name.

        Args:
            topic_name (str): Name of topic.
            topic (Topic): Instance of Topic.
        """
        self.topics[topic_name] = topic

    def get_topic(self, topic_name: str) -> TopicT:
        """Get a topic based on the associated name.

        Args:
            topic_name (str): Name of topic.

        Returns:
            TopicT: Instance of Topic.
        """
        return self.topics[topic_name]

    def main(self, override_logging=False) -> None:
        """Main method that initializes the Dagger worker. This method is blocking."""
        worker = mode.Worker(self, daemon=True, override_logging=override_logging)
        worker.execute_from_commandline()

    async def _store_and_create_task(self, task):
        if isinstance(task, ITemplateDAGInstance):
            await self._store_root_template_instance(task)
            if task.status.code == TaskStatusEnum.SUBMITTED.name:
                await task.start(workflow_instance=task)

    async def _process_tasks_create_event(self, stream):
        """Upon creation of tasks, store them in the datastore.

        Args:
            stream ([type]): The stream instance that triggers this event.
        """
        async for taskjson in stream:
            task: ITask = ITask.loads(taskjson)
            await self._store_and_create_task(task)
            logger.debug(f"Task with id: {str(task.get_id())}")

    async def _process_bootstrap_tasks(self, stream):
        """Used to process stalled ITasks upon restart.

        Args:
            stream ([type]): Stream instance that triggers this event.
        """

        async for taskjson in stream:
            task = ITask.loads(taskjson)
            logger.info(
                f"Task with id: {str(task.get_id())} received on bootstrap topic. Starting..."
            )
            await task.start()


service_blueprint = Blueprint("service_state")


@service_blueprint.route("/ready", name="task_resource")
class ServiceStateView(View):
    async def get(self, request: Request) -> Response:
        """REST endpoint that is just used to tell when the Dagger worker has started."""
        return self.text(
            json.dumps({"is_ready": Dagger.app.started_flag}),
            content_type="application/json",
        )


tasks_blueprint = Blueprint("template_tasks")


class TaskDTO(ITask):

    child_tasks: List[ITask] = list()


@tasks_blueprint.route("/instance/{dag_id}", name="dag_by_id")
class DagProcessView(View):
    """REST endpoint to get the details of a process dag."""

    async def get(self, request: Request, dag_id) -> Response:
        try:
            dag_obj = await Dagger.app.get_instance(dag_id)
            if not dag_obj:
                raise KeyError
            return self.text(
                jsonpickle.encode(dag_obj, unpicklable=False),  # type: ignore
                content_type="application/json",
            )
        except KeyError:
            logger.error("Dag not found for given id")
            raise HTTPNotFound()
        except Exception:
            logger.error("Exception occurred while fetching details.", exc_info=True)
            raise Exception


@tasks_blueprint.route("/instances", name="task_resource")
class TemplateProcessView(View):
    """REST endpoint that details the current instances of templates and their processes."""

    tasks: List[TaskDTO]

    async def get(self, request: Request) -> Response:
        try:
            self.tasks = list()
            for task in Dagger.app._store.kv_table.values():  # pragma: no cover
                self.tasks.append(task)
            return self.text(
                jsonpickle.encode(self.tasks, unpicklable=False),
                content_type="application/json",
            )
        except Exception as ex:
            logger.warning(f"Error {ex}")
        return None