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
import jsonpickle  # type: ignore
import mode
from aiohttp.web import HTTPNotFound
from faust import App, Monitor, Record, Sensor, Topic, TopicT
from faust.sensors.datadog import DatadogMonitor
from faust.types.codecs import CodecArg
from faust.types.models import ModelArg
from faust.types.web import ResourceOptions
from faust.web import Blueprint, Request, Response, View
from mode import Service
from mode.utils.types.trees import NodeT  # type: ignore
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
    """
    The Dagger class to create the workflow engine.
    """

    faust_app: App
    app: Dagger
    schema_registry_client: SchemaRegistryClient
    message_serializer: MessageSerializer
    dd_sensor: Optional[Monitor]
    config: EngineConfig
    aerospike_config: Optional[AerospikeConfig]
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
    logging_config = None

    def __init__(
        self,
        *,
        broker: str,
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
        logging_config=None,
        **kwargs: Any,
    ) -> None:
        """Initialize an instance of Dagger.

        :param broker: Kafka broker address i.e. kafka://0.0.0.0:9092. Defaults to None.
        :param datadir: Directory where db data will reside. Defaults to None.
        :param store: DB to use. Defaults to "rocksdb://".
        :param application_name: Name of application. Defaults to "dagger".
        :param package_name: Name of package. Defaults to "dagger".
        :param kafka_partitions: Number of Kafka partitions. Defaults to 1.
        :param task_update_topic: Name of topic where tasks that have updated in status will be sent. Defaults to "task_update_topic".
        :param tasks_topic: Name of topic where new tasks will be sent for execution. Defaults to "dagger_task_topic".
        :param bootstrap_topic: Name of topic where tasks after restart will be sent for execution. Defaults to "dagger_task_topic".
        :param beacon: Beacon used to track services in a dependency graph. Defaults to None.
        :param loop: Asyncio event loop to attach to. Defaults to None.
        :param tracing_sensor: Tracing Sensor to use for OpenTelemetry. The global tracer has to be initialized in the client. Defaults to None
        :param datadog_sensor: datadog statsD sensor
        :param aerospike_config: Config for Aerospike if enabled
        :param enable_changelog: Flag to enable/disable events on the table changelog topic
        :param max_correletable_keys_in_values: maximum number of ids in the value part to chunk
        :param schema_registry_url: the schema registry URK
        :param message_serializer: the message serializer instance using the schema registry
        :param delete_workflow_on_complete: deletes the workflow instance when complete
        :param task_update_callbacks: callbacks when a workflow instance is updated
        :param logging_config: the logging config to use
        :param **kwargs: Other Faust keyword arguments.
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
        self.logging_config = logging_config
        for i in range(Dagger.LOCK_STRIPE_SIZE):
            self.asyncio_locks[i] = asyncio.Lock()

    def get_template(self, template_name: str) -> ITemplateDAG:
        """Get the instance of a template that contains the workflow definition given it's name.

        :param template_name: Name of template

        :raises TemplateDoesNotExist: Template does not exist

        :return: Instance of Template that contains the workflow definition
        """
        if template_name not in self.template_dags:
            raise TemplateDoesNotExist()
        return self.template_dags[template_name]

    def __create_app(self) -> faust.App:
        """Initializes instance of Faust

        :return: Instance of Faust
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
            logging_config=self.logging_config,
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
        """
        This method is called after initialization of dagger
        """
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
        cls,
        topic_name: str,
        key_type: Optional[ModelArg] = None,
        value_type: Optional[ModelArg] = None,
        key_serializer: CodecArg = None,
        value_serializer: CodecArg = None,
    ) -> TopicT:
        """Create a Kafka topic using Faust

        :param topic_name: Name of topic
        :param key_type:  Key type for the topic. Defaults to str.
        :param value_type: Value type for the topic. Defaults to str.
        :return: Instance of Faust Topic

        """
        logger.info(f"Creating topic {topic_name}")
        topic_instance: Optional[TopicT] = None
        if topic_name in Dagger.app.topics:
            logger.warning(f"Topic {topic_name} is already created")
            topic_instance = Dagger.app.topics[topic_name]
        else:
            topic_instance = Dagger.app.faust_app.topic(
                topic_name,
                key_type=key_type,
                value_type=value_type,
                key_serializer=key_serializer,
                value_serializer=value_serializer,
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
        """
        this method is used to register a workflow definition with Dagger. Refer to the examples
        in the documentation

        :param template_name: Name of workflow to register
        """

        def wrapped(wrapped_fun: RegisterFun):
            logger.info("registering " + template_name)
            Dagger.app.template_dags[template_name] = wrapped_fun(template_name)  # type: ignore

        return wrapped

    @classmethod
    def register_process_template(cls, process_template_name: str):
        """Registers a process template in Dagger.

        :param process_template_name (str): Name of process to register in dagger.
        """

        def wrapped(wrapped_fun: RegisterFun):
            logger.info("registering " + process_template_name)
            Dagger.app.process_templates[process_template_name] = wrapped_fun(  # type: ignore
                process_template_name
            )

        return wrapped

    async def _get_tasks_by_correlatable_key(  # noqa: C901
        self, lookup_key: TaskLookupKey, get_completed: bool = False
    ) -> AsyncGenerator[Optional[ITask, ITask], None]:  # type: ignore
        """Get a task based on the lookup key associated with the task

        :param lookup_key: Key, value associated with a task
        :param get_completed: If false it looks up only SensorTasks which are not in terminal state. If true ir returns
        tasks which are in terminal state(COMPLETED,STOPPED, FAILED, SKIPPED)
        :return: Instance of workflow task  and the SensorTask
        """
        cor_instance: Optional[
            CorreletableKeyTasks
        ] = await self._store.get_table_value(
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
                    task: Optional[ITask] = None
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

        :param root_template_instance: Instance of root template aka workflow instance
        """
        await self._invoke_store_insert_key_value_with_timer(
            str(root_template_instance.id), root_template_instance
        )

    async def _invoke_store_insert_key_value_with_timer(self, key: str, value: Record):
        start_time = self.faust_app.loop.time()
        await self._store.insert_key_value(str(key), value)
        end_time = self.faust_app.loop.time() - start_time
        if self.dd_sensor:
            self.dd_sensor.client.histogram(metric="insert_key_value", value=end_time)  # type: ignore

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
        """Updates the correletable key for a SensorTask within the datastore.

        :param task_instance: the SensorTask for which the correletable key is updated
        :param key: the correletable key name to update.
        :param new_task: If the task is not new, then the entire runtime paramters and subsequent tasks need to be updated
        :param workflow_instance: the workflow instance
        """
        workflow_instance.sensor_tasks_to_correletable_map[  # type: ignore
            task_instance.get_id()
        ] = CorrelatableMapValue(task_instance.correlatable_key, key)
        if (
            task_instance.correlatable_key
            and key
            and task_instance.topic
            and workflow_instance
        ):
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
        """
        This method chunks the value of the key to the list so that we don't overflow the limit of the value size
        on the datastore used by dagger defined by max_correletable_keys_in_values

        :param cor_instance: the CorreletableKeyTasks instance
        :param value: the new value to add to the lookup_keys
        :param workflow_id: the id of the workflow to which the cor_instance belongs to
        """
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
        """
        Gathers all the chunked values of the cor_instance
        :param cor_instance: the CorreletableKeyTasks to gather
        :return: A list of all the chunked CorreletableKeyTasks
        """
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
        """
        Removes a task from the correletable keys table
        :param task: the task to remove from the correletable keys table
        :param workflow_instance: the workflow instance
        """
        if workflow_instance.runtime_parameters:
            current_key = (
                workflow_instance.runtime_parameters.get(task.correlatable_key, None)
                if task.correlatable_key
                else None
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
        """ """
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
        prev_cor_instance: Optional[CorreletableKeyTasks] = cor_instance
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

        :param root_template_instance: Instance of root template.
        """
        await self._store.remove_key_value(str(root_template_instance.get_id()))
        logger.debug(
            f"Removed template instance with id: {str(root_template_instance.get_id())}"
        )

    async def _invoke_store_get_value_for_key_with_timer(self, key: str):
        start_time = self.faust_app.loop.time()
        random: Random = Random()
        random.seed(str(key))
        partition: int = random.randint(0, Dagger.LOCK_STRIPE_SIZE - 1)  # nosec
        if self.asyncio_locks and self.asyncio_locks.get(partition):
            async with self.asyncio_locks.get(partition):  # type: ignore
                value = self.workflows_weak_ref_map.get(key, None)
                if value:
                    return value
                value = await self._store.get_value_for_key(key)
                if value is not None:
                    self.workflows_weak_ref_map[key] = value
        end_time = self.faust_app.loop.time() - start_time
        if self.dd_sensor:
            self.dd_sensor.client.histogram(metric="get_value_for_key", value=end_time)  # type: ignore
        return value

    async def get_instance(self, id: UUID, log: bool = True) -> ITask:
        """Get an instance of an ITask given it's id.

        :param id: Id of the ITask.
        :param log: suppress logging if set to True

        :return: Instance of the ITask.
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
        """
        Get the DB options on the dagger store
        :return: the DB options
        """
        return self._store.get_db_options()

    async def _update_instance(self, task: ITemplateDAGInstance) -> None:
        """Update the state of an workflow instance in the datastore.
        :param task: the workflow instance to store
        """

        await self._invoke_store_insert_key_value_with_timer(str(task.id), task)
        logger.debug(
            f"State update for ITask with id: {str(task.id)} stored to datastore."
        )

    async def submit(self, task: ITask, *, repartition: bool = True) -> None:
        """Submits a workflow instance for execution.

        :param task: The workflow instance.
        :param repartition: if True it uses the the repartitioning key to submit the task for execution on the
        configured kafka topic. If false, it creates the workflow on the same node and submits it for execution
        """
        logger.debug(f"Submitting task with id: {str(task.get_id())}")
        await self._execution_strategy.submit(task, repartition=repartition)

    def add_topic(self, topic_name: str, topic: Topic) -> None:
        """Associate a topic instance with a name.

        :param topic_name: Name of topic.
        :param topic: Instance of Topic.
        """
        self.topics[topic_name] = topic

    def get_topic(self, topic_name: str) -> TopicT:
        """Get a topic based on the associated name.

        :param topic_name: Name of topic.
        :return: the instance of the topic

        """
        return self.topics[topic_name]

    def main(self, override_logging=False) -> None:
        """Main method that initializes the Dagger worker. This method is blocking."""
        worker = mode.Worker(self, daemon=True, override_logging=override_logging)
        worker.execute_from_commandline()

    async def _store_and_create_task(self, task):
        if isinstance(task, ITemplateDAGInstance):
            if task.status.code == TaskStatusEnum.SUBMITTED.name:
                await task.start(workflow_instance=task)
            await self._store_root_template_instance(task)

    async def _process_tasks_create_event(self, stream):
        """Upon creation of tasks, store them in the datastore.

        :param stream: The stream instance that triggers this event.
        """
        async for taskjson in stream:
            task: ITask = ITask.loads(taskjson)
            await self._store_and_create_task(task)
            logger.debug(f"Task with id: {str(task.get_id())}")

    async def _process_bootstrap_tasks(self, stream):
        """Used to process stalled ITasks upon restart.

        :param stream: The stream instance that triggers this event.
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
    """
    Class to represent the state of the application
    """

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
    """REST endpoint to get the details of a workflow instance"""

    async def get(self, request: Request, dag_id) -> Response:
        """
        Returns the details of the workflow request specified in the request

        :param request: the HTTP request
        :param dag_id: the workflow instance ID
        :return: the JSON representation of the workflow instance
        """
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
        """
        Get all the workflow instances
        :param request: the HTTP request
        """
        try:
            self.tasks = list()
            for task in Dagger.app._store.kv_table.values():  # pragma: no cover
                self.tasks.append(task)

        except Exception as ex:
            logger.warning(f"Error {ex}")
        return self.text(
            jsonpickle.encode(self.tasks, unpicklable=False),
            content_type="application/json",
        )
