from __future__ import annotations

import abc
import asyncio
import json
import logging
import time
from typing import Any, AsyncGenerator, Mapping, Set

import aerospike  # type: ignore
import faust.serializers.schemas  # type: ignore
import jsonpickle  # type: ignore
from aerospike_helpers import expressions as exp  # type: ignore
from faust import Record, Table
from faust.types import TP
from mode import Service

from dagger.service.engineconfig import ROCKS_DB_OPTIONS
from dagger.tasks.task import (
    TERMINAL_STATUSES,
    IntervalTask,
    ITask,
    ITemplateDAGInstance,
    SystemTimerTask,
    TaskStatusEnum,
    Trigger,
    TriggerTask,
)

logger = logging.getLogger(__name__)


class IStore:
    """Store consists of a root node table and key/value table mappings.

    The root node table holds instances of templates. They key/value table pairs ids with their respective
    ITask instances, and the subject-predicate-object tables to ensure faster lookups for like terms.

    task_cache: Cache with a max size and ttl to cache tasks until we get the ROOT task to build workflows"""

    app: Service
    kv_table: Table[str, ITask]
    triggers_table: Table
    correletable_keys_table: Table
    enable_changelog: bool = True

    def __init__(self, app: Service) -> None:
        self.app = app

    async def insert_key_value(self, key: str, value: ITemplateDAGInstance) -> None:

        value.lastupdated = int(time.time())
        value.update_count += 1

        if self.app.task_update_topic:  # type: ignore
            update_key_lookup = value.partition_key_lookup
            update_key = (
                value.runtime_parameters.get(update_key_lookup, None)
                if update_key_lookup
                else None
            )
            payload = jsonpickle.encode(value.asdict())
            future = await self.app.task_update_topic.send(key=update_key, value=payload)  # type: ignore
            await future
        for fn_callback in self.app.task_update_callbacks:  # type: ignore
            await fn_callback(value)

        await self.set_table_value(self.kv_table, key, value)

    async def remove_key_value(self, key: str) -> None:  # pragma: no cover
        await self.del_table_value(self.kv_table, key)

    @abc.abstractmethod
    async def insert_trigger(self, value: Trigger) -> None:  # pragma: no cover
        ...

    @abc.abstractmethod
    async def remove_trigger(self, value: Trigger) -> None:  # pragma: no cover
        ...

    @abc.abstractmethod
    async def get_value_for_key(self, key: str) -> Record:  # pragma: no cover
        ...

    @abc.abstractmethod
    async def get_valid_triggers(self):  # pragma: no cover
        ...

    @abc.abstractmethod
    async def get_trigger(
        self, key, use_partition: bool = False
    ) -> Trigger:  # pragma: no cover
        ...

    def set_value(self, table, key, value):
        table.data[key] = value

    def get_value(self, table: Table, key):
        return table.get(key, None)

    def del_value(self, table, key):
        return table.data.pop(key, None)

    async def get_table_value(self, table, key):
        return table.get(key, None)

    async def set_table_value(self, table, key, value):
        table[key] = value

    async def del_table_value(self, table, key):
        return table.pop(key, None)

    @abc.abstractmethod
    def initialize(self, *args) -> None:  # pragma: no cover
        ...

    async def process_system_timer_task(self, timer_task: SystemTimerTask) -> None:
        try:
            await timer_task.execute(runtime_parameters={}, workflow_instance=None)
        except Exception as ex:
            logger.warning(f"Exception executing timer task {ex}")

    async def store_trigger_instance(
        self, task_instance: TriggerTask, wokflow_instance: ITemplateDAGInstance
    ) -> None:
        trigger_instance = await self._create_trigger_instance_from_trigger_task(
            task_instance=task_instance, wokflow_instance=wokflow_instance
        )
        await self.insert_trigger(trigger_instance)
        logger.debug(
            f"Stored trigger task with id: {str(task_instance.get_id())} and time to execute: {str(task_instance.time_to_execute)} in the datastore."
        )

    @abc.abstractmethod
    def get_db_options(self) -> Mapping[str, Any]:  # pragma: no cover
        ...

    async def _create_trigger_instance_from_trigger_task(
        self, task_instance: TriggerTask, wokflow_instance: ITemplateDAGInstance
    ) -> Trigger:
        trigger_instance = Trigger()
        trigger_instance.id = task_instance.id
        trigger_instance.trigger_time = task_instance.time_to_execute
        trigger_instance.workflow_id = wokflow_instance.id
        return trigger_instance

    async def process_trigger_task_complete(
        self, task: TriggerTask, wokflow_instance: ITemplateDAGInstance
    ) -> None:
        if task and wokflow_instance:
            trigger: Trigger = await self._create_trigger_instance_from_trigger_task(
                task_instance=task, wokflow_instance=wokflow_instance
            )
            if trigger:
                await self.remove_trigger(trigger)

    async def execute_system_timer_task(self) -> None:  # pragma: no cover
        async for trigger in self.get_valid_triggers():
            # get all tasks with this trigger
            logger.info(f"Found valid trigger {trigger.id}")
            workflow_instance = await self.app.get_instance(trigger.workflow_id)  # type: ignore
            task = None
            if workflow_instance:
                task = workflow_instance.get_task(id=trigger.id)  # type: ignore
                finished = True
            if task and task.status.code == TaskStatusEnum.EXECUTING.name:
                if issubclass(task.__class__, IntervalTask):
                    finished = await task.start(workflow_instance)
                else:
                    await task.start(workflow_instance)
                await self.app._update_instance(task=workflow_instance)  # type: ignore
                if finished:
                    await self.remove_trigger(trigger)
            if not task or task.status.code in TERMINAL_STATUSES:
                await self.remove_trigger(trigger)


class AerospikeStore(IStore):
    TRIGGER_TIMESTAMP: str = "ts"

    def __init__(self, app: Service) -> None:
        super().__init__(app)
        self.app = app
        self.kv_table = self.app.faust_app.Table(  # type: ignore
            f"{self.app.config.APPLICATION_NAME}_kv_table",  # type: ignore
            key_type=str,
            partitions=app.config.KAFKA_PARTITIONS,  # type: ignore
            options=self.app.aerospike_config.as_options(),  # type: ignore
        )
        self.kv_table.value_serializer = "raw"
        self.triggers_table = self.app.faust_app.Table(  # type: ignore
            f"{self.app.config.APPLICATION_NAME}_triggers_value_table",  # type: ignore
            key_type=str,
            partitions=app.config.KAFKA_PARTITIONS,  # type: ignore
            options=self.app.aerospike_config.as_options(),  # type: ignore
        )
        self.triggers_table.value_serializer = "raw"

        self.correletable_keys_table = self.app.faust_app.Table(  # type: ignore
            f"{self.app.config.APPLICATION_NAME}_correletable_keys_table",  # type: ignore
            key_type=str,
            partitions=app.config.KAFKA_PARTITIONS,  # type: ignore
            options=self.app.aerospike_config.as_options(),  # type: ignore
        )
        self.correletable_keys_table.value_serializer = "raw"

    async def remove_trigger(self, key: Trigger) -> None:
        await self.del_table_value(table=self.triggers_table, key=key.get_trigger_key())

    async def get_value_for_key(self, key: str) -> Record:
        return await self.get_table_value(table=self.kv_table, key=key)

    async def insert_trigger(self, value: Trigger) -> None:
        await self.app.loop.run_in_executor(None, self._insert_trigger, value)  # type: ignore
        if self.enable_changelog:
            self.triggers_table.use_partitioner = True
            self.triggers_table.on_key_set(value.id, value)

    def _insert_trigger(self, value: Trigger):
        try:
            key = (
                self.app.aerospike_config.NAMESPACE,  # type: ignore
                self.triggers_table.name,
                self.triggers_table.data._encode_key(value.get_trigger_key()),
            )
            vt = {
                self.triggers_table.data.BIN_KEY: self.triggers_table.data._encode_value(
                    value
                ),
                self.TRIGGER_TIMESTAMP: value.trigger_time,
            }
            self.triggers_table.data.client.put(
                key=key,
                bins=vt,
                meta={"ttl": self.app.aerospike_config.TTL},  # type: ignore
                policy={
                    "exists": aerospike.POLICY_EXISTS_IGNORE,
                    "key": aerospike.POLICY_KEY_SEND,
                },
            )
        except Exception as ex:
            logger.error(
                f"Error in set for table {self.triggers_table.name} exception {ex}"
            )
            raise ex

    async def get_trigger(
        self, key, use_partition: bool = False
    ) -> Trigger:  # pragma: no cover
        return await self.get_table_value(self.triggers_table, key)

    async def get_valid_triggers(self) -> AsyncGenerator[Trigger, None]:
        try:
            current_time = int(time.time())
            query = self.triggers_table.data.client.query(
                namespace=self.app.aerospike_config.NAMESPACE, set=self.triggers_table.name  # type: ignore
            )
            query.max_records = 5000
            query.paginate()
            expr = exp.LT(exp.IntBin(self.TRIGGER_TIMESTAMP), current_time).compile()

            policy = {"expressions": expr}
            loop = asyncio.get_running_loop()

            done = False
            while not done:
                results = await loop.run_in_executor(None, query.results, policy)
                done = query.is_done()
                for result in results:
                    (key, meta, bins) = result
                    await asyncio.sleep(0)
                    if bins:
                        trigger = self.triggers_table.data._decode_value(
                            (bins[self.triggers_table.data.BIN_KEY])
                        )
                        yield trigger

        except Exception as ex:
            logger.error(
                f"Error in _itervalues for table {self.triggers_table.name} exception {ex}"
            )
            raise ex

        end_time = int(time.time())
        logger.info(f"get_valid_triggers took {(end_time - current_time)}")

    def initialize(self, *args) -> None:
        self.app.faust_app.timer(interval=self.app.trigger_interval, on_leader=True)(  # type: ignore
            self.app._process_system_timer_task  # type: ignore
        )

    def get_db_options(self) -> Mapping[str, Any]:
        return self.app.aerospike_config.as_options()  # type: ignore

    async def get_table_value(self, table, key):
        loop = asyncio.get_running_loop()
        return_value = await loop.run_in_executor(None, self.get_value, table, key)
        if self.enable_changelog:
            table.on_key_get(key)
        return return_value

    async def set_table_value(self, table, key, value):
        await self.app.loop.run_in_executor(None, self.set_value, table, key, value)  # type: ignore
        if self.enable_changelog:
            table.use_partitioner = True

    async def del_table_value(self, table, key):
        return_value = await self.app.loop.run_in_executor(None, self.del_value, table, key)  # type: ignore
        if self.enable_changelog:
            table.use_partitioner = True
            table.on_key_del(key)
        return return_value


class RocksDBStore(IStore):

    DELIM_VAUE = "|"
    migrated_partitions: Set[int]

    def __init__(self, app: Service) -> None:
        super().__init__(app)
        self.app = app
        self.kv_table = self.app.faust_app.Table(  # type: ignore
            "kv_table",
            key_type=str,
            partitions=self.app.config.KAFKA_PARTITIONS,  # type: ignore
            options=ROCKS_DB_OPTIONS,
        )
        self.kv_table.value_serializer = "raw"

        self.triggers_table = self.app.faust_app.Table(  # type: ignore
            "triggers_table",
            key_type=str,
            partitions=self.app.config.KAFKA_PARTITIONS,  # type: ignore
            options=ROCKS_DB_OPTIONS,
        )
        self.triggers_table.value_serializer = "raw"
        self.correletable_keys_table = self.app.faust_app.Table(  # type: ignore
            "correletable_keys_table",
            key_type=str,
            partitions=self.app.config.KAFKA_PARTITIONS,  # type: ignore
            options=ROCKS_DB_OPTIONS,
        )
        self.correletable_keys_table.value_serializer = "raw"

    def get_db_options(self) -> Mapping[str, Any]:
        return ROCKS_DB_OPTIONS

    async def remove_trigger(self, key: Trigger) -> None:
        self.triggers_table.pop(key.get_trigger_key(), None)
        logger.debug(f"Removed trigger {key.get_trigger_key()}")

    async def get_value_for_key(self, key: str) -> Record:
        return self.kv_table.get(key, None)  # type: ignore

    async def insert_trigger(self, value: Trigger) -> None:
        self.triggers_table[value.get_trigger_key()] = value

    async def get_trigger(
        self, key, use_partition: bool = False
    ) -> Trigger:  # pragma: no cover
        if not use_partition:
            return self.triggers_table.get(key, None)
        else:
            event = faust.current_event()
            partition = event.message.partition  # type: ignore
            db = self.triggers_table.data._db_for_partition(partition)
            value = db.get(str(key).encode())
            if value:
                value = Trigger.loads(json.loads(value))
            return value

    async def get_valid_triggers(self) -> AsyncGenerator[Trigger, None]:
        logger.debug("get_valid_triggers start")
        current_time = int(time.time())
        event = faust.current_event()
        partition = event.message.partition  # type: ignore
        topic = self.triggers_table._changelog_topic_name()
        tp = TP(topic=topic, partition=partition)
        logger.debug(f"get_valid_triggers function. partition: {str(partition)}.")
        db = self.triggers_table.data._db_for_partition(partition)
        it = db.iteritems()
        it.seek_to_first()

        for item in it:
            try:
                await asyncio.sleep(0)
                if tp not in self.app.faust_app.assignor.assigned_actives():  # type: ignore
                    logger.info(
                        f"Partition assignment changed when executing Trigger tasks {tp}"
                    )
                    break
                trigger: Trigger = Trigger.loads(json.loads(item[1]))
                if trigger.trigger_time and trigger.trigger_time <= int(current_time):
                    yield trigger
                else:
                    break
            except Exception as ex:  # pragma: no cover
                logger.warning(f"Error iterating {ex}")
        end_time = int(time.time())
        logger.info(f"get_valid_triggers took {(end_time-current_time)}")

    def initialize(self, *args) -> None:
        self.app.faust_app.agent(  # type: ignore
            self.app.bootstrap_topic, name=self.app.bootstrap_topic.get_topic_name()  # type: ignore
        )(
            self.app._process_bootstrap_tasks  # type: ignore
        )
        self.app.faust_app.timer(interval=self.app.trigger_interval, on_leader=False)(  # type: ignore
            self.app._process_system_timer_task  # type: ignore
        )
