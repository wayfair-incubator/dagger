import asyncio
import json
import time
import uuid

import asynctest
import faust
import pytest
from asynctest import CoroutineMock, MagicMock, call
from cachetools import TTLCache
from faust.types import TP

import dagger
from dagger.store.stores import AerospikeStore, RocksDBStore
from dagger.tasks.task import (
    DefaultTemplateDAGInstance,
    ITask,
    ITemplateDAGInstance,
    SystemTimerTask,
    TaskStatus,
    TaskStatusEnum,
    TaskType,
    Trigger,
)


class TestAeroSpikeStore:
    @pytest.fixture()
    def workflow_instance_fixture(self):
        return DefaultTemplateDAGInstance(uuid.uuid1())

    @pytest.fixture()
    async def store(self):
        app = MagicMock()
        store = AerospikeStore(app=app)
        store.kv_table = MagicMock()
        store.triggers_table = MagicMock()
        store.correletable_keys_table = MagicMock()
        store.set_table_value = CoroutineMock()
        store.app.dd_sensor = MagicMock()
        store.app.dd_sensor.client.histogram = MagicMock()
        store.task_cache = TTLCache(maxsize=500, ttl=600)
        return store

    @pytest.fixture()
    def system_timer_fixture(self):
        task = SystemTimerTask(uuid.uuid1())
        task.status = TaskStatus()
        return task

    @pytest.mark.asyncio
    async def test_remove_trigger(self, store: AerospikeStore):
        trigger: Trigger = Trigger()
        trigger.id = "id1"
        trigger.workflow_id = "wk_id"
        cur_time = time.time()
        trigger.trigger_time = cur_time
        trigger.task_ids = None
        store.del_table_value = CoroutineMock()
        await store.remove_trigger(key=trigger)
        store.del_table_value.assert_called_with(
            table=store.triggers_table, key=trigger.get_trigger_key()
        )

    @pytest.mark.asyncio
    async def test_del_table_value_called(self, store: AerospikeStore):
        store.app.loop.run_in_executor = CoroutineMock(return_value=None)
        store.kv_table.on_key_del = MagicMock()
        store.del_value = MagicMock()

        await store.del_table_value(table=store.kv_table, key="k1")
        store.app.loop.run_in_executor.assert_called_with(
            None, store.del_value, store.kv_table, "k1"
        )
        assert store.kv_table.on_key_del.called

    @pytest.mark.asyncio
    async def test_insert_key_value(self, store: AerospikeStore):
        store.app.faust_app.loop.time = MagicMock(return_value=time.time())
        store.app.task_update_topic = MagicMock()
        store.app.task_change_topic = MagicMock()
        callback_method = CoroutineMock()
        store.app.task_update_callbacks = [callback_method]
        # Set mocks
        old_value = ITemplateDAGInstance(uuid.uuid1())
        old_value.status = TaskStatus(
            code=TaskStatusEnum.NOT_STARTED.name, value=TaskStatusEnum.NOT_STARTED.value
        )
        old_value.runtime_parameters = dict()
        store.get_table_value = CoroutineMock(return_value=old_value)
        mock_future = asyncio.Future()
        mock_result = asyncio.Future()
        mock_result.set_result(1)
        mock_future.set_result(mock_result)
        store.app.task_update_topic.send = CoroutineMock(return_value=mock_future)

        value = ITemplateDAGInstance(uuid.uuid1())
        value.status = TaskStatus(
            code=TaskStatusEnum.EXECUTING.name, value=TaskStatusEnum.EXECUTING.value
        )
        value.task_type = TaskType.SUB_DAG.name
        value.runtime_parameters = dict()
        await store.insert_key_value(key=value.id, value=value)
        assert store.app.task_update_topic.send.called
        assert store.set_table_value.called
        assert callback_method.called
        store.set_table_value.assert_called_with(store.kv_table, value.id, value)

        # Reset mocks
        store.app.task_update_topic.send = CoroutineMock(return_value=mock_future)
        store.app.task_change_topic.send = CoroutineMock(return_value=mock_future)
        store.set_table_value = CoroutineMock()

        store.get_table_value = CoroutineMock(return_value=value)
        updated_value = ITemplateDAGInstance(uuid.uuid1())
        updated_value.status = TaskStatus(
            code=TaskStatusEnum.COMPLETED.name, value=TaskStatusEnum.COMPLETED.value
        )
        updated_value.task_type = TaskType.SUB_DAG.name
        updated_value.runtime_parameters = {"hu_id": "EX8886661"}
        await store.insert_key_value(key=updated_value.id, value=updated_value)
        assert store.app.task_update_topic.send.called
        assert store.set_table_value.called
        store.set_table_value.assert_called_with(
            store.kv_table, updated_value.id, updated_value
        )

        # Reset mocks
        store.app.task_update_topic.send = CoroutineMock(return_value=mock_future)
        store.set_table_value = CoroutineMock()

        value.task_type = TaskType.ROOT.name
        await store.insert_key_value(key=value.id, value=value)
        assert store.app.task_update_topic.send.called
        store.set_table_value.assert_called_with(store.kv_table, value.id, value)
        # Reset mocks
        store.app.task_update_topic.send = CoroutineMock(return_value=mock_future)
        store.app.task_change_topic.send = CoroutineMock(return_value=mock_future)
        store.set_table_value = CoroutineMock()

        old_value = ITemplateDAGInstance(uuid.uuid1())
        old_value.status = TaskStatus(
            code=TaskStatusEnum.NOT_STARTED.name, value=TaskStatusEnum.NOT_STARTED.value
        )
        old_value.task_type = TaskType.ROOT.name
        old_value.runtime_parameters = dict()

        value = ITemplateDAGInstance(uuid.uuid1())
        value.status = TaskStatus(
            code=TaskStatusEnum.EXECUTING.name, value=TaskStatusEnum.EXECUTING.value
        )
        value.task_type = TaskType.ROOT.name
        value.runtime_parameters = {"changed_value": "changed_value"}
        store.get_table_value = CoroutineMock(return_value=updated_value)
        await store.insert_key_value(key=value.id, value=value)
        assert store.app.task_update_topic.send.called

        store.set_table_value.assert_called_with(store.kv_table, value.id, value)

    @pytest.mark.asyncio
    async def test_remove_key_value(self, store: AerospikeStore):
        store.del_table_value = CoroutineMock()
        await store.remove_key_value(key="k1")
        store.del_table_value.assert_has_calls([call(store.kv_table, "k1")])

    @pytest.mark.asyncio
    async def test_get_value_for_key(self, store: AerospikeStore):
        ret_mock = MagicMock()
        store.get_table_value = CoroutineMock(return_value=ret_mock)
        value = await store.get_value_for_key(key="k1")
        assert value == ret_mock
        store.get_table_value.assert_called_with(table=store.kv_table, key="k1")

    @pytest.mark.asyncio
    async def test_insert_trigger(self, store: AerospikeStore):
        store.set_table_value = CoroutineMock()
        value = Trigger()
        value.id = "id1"
        store.app.loop.run_in_executor = CoroutineMock()
        store.triggers_table.on_key_set = CoroutineMock()
        await store.insert_trigger(value)
        assert store.app.loop.run_in_executor.called
        store.triggers_table.on_key_set.assert_called_with(value.id, value)

    @pytest.mark.asyncio
    async def test_insert_trigger_disable_changelog(self, store: AerospikeStore):
        store.set_table_value = CoroutineMock()
        store.enable_changelog = False
        value = Trigger()
        value.id = "id1"
        store.app.loop.run_in_executor = CoroutineMock()
        store.triggers_table.on_key_set = CoroutineMock()
        await store.insert_trigger(value)
        assert store.app.loop.run_in_executor.called
        assert not store.triggers_table.on_key_set.called

    @pytest.mark.asyncio
    async def test_insert_trigger2(self, store: AerospikeStore):
        value = Trigger()
        value.id = "id1"
        value.trigger_time = time.time()
        store.triggers_table.data.client.put = MagicMock()
        store._insert_trigger(value=value)
        assert store.triggers_table.data.client.put.called
        store.triggers_table.data.client.put = MagicMock(side_effect=Exception)
        with pytest.raises(Exception):
            store._insert_trigger(value=value)

    @pytest.mark.asyncio
    async def test_get_trigger(self, store: AerospikeStore):
        value = Trigger()
        value.id = "id1"
        store.get_table_value = CoroutineMock(return_value=value)

        ret = await store.get_trigger(value.id)
        assert ret == value
        store.get_table_value.assert_called_with(store.triggers_table, value.id)

    @pytest.mark.asyncio
    async def test_get_valid_triggers(self, store: AerospikeStore):
        query = MagicMock()
        value = Trigger()
        value.id = "id1"
        store.triggers_table.data.BIN_KEY = "key"
        store.triggers_table.data.client.query = MagicMock(return_value=query)
        query.results = MagicMock(
            return_value=[(MagicMock(), MagicMock(), {"key": value})]
        )
        query.is_done = MagicMock(return_type=True)
        store.triggers_table.data._decode_value = MagicMock(return_value=value)
        ret = list()
        async for val in store.get_valid_triggers():
            ret.append(val)
        assert value in ret
        assert len(ret) == 1
        query.results = MagicMock(side_effect=Exception)
        with pytest.raises(Exception):
            async for val in store.get_valid_triggers():
                pass

    @pytest.mark.asyncio
    async def test_set_value(self, store: AerospikeStore):
        store.kv_table.data = dict()
        store.set_value(store.kv_table, "k1", "v1")
        assert store.kv_table.data["k1"] == "v1"

    @pytest.mark.asyncio
    async def test_get_value(self, store: AerospikeStore):
        store.kv_table.get = MagicMock()
        store.get_value(store.kv_table, "k1")
        assert store.kv_table.get.called

    @pytest.mark.asyncio
    async def test_del_value(self, store: AerospikeStore):
        store.kv_table.data = dict()
        store.kv_table["k1"] = "v1"
        store.del_value(store.kv_table, "k1")
        assert store.kv_table.data.get("k1", None) is None

    @pytest.mark.asyncio
    async def test_get_table_value(self, store: AerospikeStore):
        get_return = MagicMock()
        store.get_value = MagicMock(return_value=get_return)
        store.kv_table.on_key_get = MagicMock()
        val = await store.get_table_value(table=store.kv_table, key="k1")
        assert val == get_return

    @pytest.mark.asyncio
    async def test_del_table_value(self, store: AerospikeStore):
        store.kv_table.data = dict()
        store.kv_table.data["k1"] = "v1"
        store.kv_table.on_key_del = MagicMock()
        store.app.loop.run_in_executor = CoroutineMock(return_value="v1")
        val = await store.del_table_value(table=store.kv_table, key="k1")
        assert val == "v1"

    @pytest.mark.asyncio
    async def test_store_trigger_instance(self, store, workflow_instance_fixture):
        store.get_trigger = CoroutineMock(return_value=None)
        store.insert_trigger = CoroutineMock()
        trigger_instance = MagicMock()
        await store.store_trigger_instance(trigger_instance, workflow_instance_fixture)
        assert store.insert_trigger.called

    @pytest.mark.asyncio
    async def test_process_system_timer_task(self, store):
        timer_task = MagicMock()
        timer_task.execute = CoroutineMock()
        await store.process_system_timer_task(timer_task)
        assert timer_task.execute.called

    @pytest.mark.asyncio
    async def test_process_trigger_task_complete(
        self, store, workflow_instance_fixture
    ):
        trigger = MagicMock()
        store.remove_trigger = CoroutineMock()
        await store.process_trigger_task_complete(trigger, workflow_instance_fixture)
        assert store.remove_trigger.assert_awaited_once

    @pytest.mark.asyncio
    async def test_exec_timer_task(
        self, store, system_timer_fixture, workflow_instance_fixture
    ):
        assert system_timer_fixture.status.code == TaskStatusEnum.NOT_STARTED.name
        mock_trigger_task = Trigger({"ID1"}, time.time())

        mock_trigger = asynctest.MagicMock()
        mock_trigger.id = system_timer_fixture.id
        mock_trigger.__aiter__.return_value = [mock_trigger_task]
        store.get_valid_triggers = MagicMock()
        store.get_valid_triggers.return_value = mock_trigger

        store.get_trigger = CoroutineMock(return_value=mock_trigger)
        task_mock = ITask(uuid.uuid1())
        mock_trigger_task.id = task_mock.id
        workflow_instance_fixture.add_task(task_mock)
        task_mock.start = CoroutineMock()
        task_mock.status.code = TaskStatusEnum.EXECUTING.name
        store.app.get_instance = CoroutineMock(return_value=workflow_instance_fixture)
        workflow_instance_fixture.add_task(system_timer_fixture)
        store.app._update_instance = CoroutineMock()
        store.remove_trigger = CoroutineMock()
        store.insert_trigger = CoroutineMock()
        assert system_timer_fixture.get_id() == system_timer_fixture.id
        dagger.service.services.Dagger.app = MagicMock()
        dagger.service.services.Dagger.app._store = store
        await system_timer_fixture.start(workflow_instance=workflow_instance_fixture)
        assert task_mock.start.called
        assert system_timer_fixture.status.code == TaskStatusEnum.EXECUTING.name
        assert store.remove_trigger.called


class TestRocksDBStore:
    @pytest.fixture()
    async def store(self):
        app = MagicMock()
        store = RocksDBStore(app=app)
        store.kv_table = MagicMock()
        store.triggers_table = MagicMock()
        store.correletable_keys_table = MagicMock()
        store.app.dd_sensor = MagicMock()
        store.app.dd_sensor.client.histogram = MagicMock()
        store.task_cache = TTLCache(maxsize=500, ttl=600)
        return store

    @pytest.fixture()
    def system_timer_fixture(self):
        return SystemTimerTask(CoroutineMock())

    def mock_prep(self, store_data_fixture, encode: bool = False, codec=json):
        obj = MagicMock(dict)

        def iterable_items(**kwargs):
            dumped_dict = MagicMock()
            if encode:
                dumped_dict.__iter__.return_value = [
                    [item[0].encode(), codec.dumps(item[1])]
                    for item in store_data_fixture.items()
                ]
            else:
                dumped_dict.__iter__.return_value = [
                    [codec.dumps(item[0]), codec.dumps(item[1])]
                    for item in store_data_fixture.items()
                ]
            return dumped_dict

        obj.items = iterable_items
        obj.iteritems = iterable_items
        return obj

    @pytest.mark.asyncio
    async def test_get_triggers(self, store):
        cur_time = int(time.time())
        future_time = cur_time + 20
        past_time = cur_time - 20000
        mock_trigger1 = Trigger(past_time, "id5", "id6")
        mock_trigger2 = Trigger(cur_time, "id1", "id2")
        mock_trigger3 = Trigger(future_time, "id3", "id4")
        triggers_table_fixture = {
            str(past_time): mock_trigger1.dumps(),
            str(cur_time): mock_trigger2.dumps(),
            str(future_time): mock_trigger3.dumps(),
        }
        obj = self.mock_prep(triggers_table_fixture, encode=True, codec=json)
        store.triggers_table.data._db_for_partition = MagicMock(return_value=obj)
        store.triggers_table._changelog_topic_name = MagicMock(return_value="test")
        mock_event = MagicMock()
        mock_event.message = MagicMock()
        mock_event.message.partition = 0
        faust.current_event = MagicMock(return_value=mock_event)
        asyncio.sleep = CoroutineMock()
        store.app.faust_app.assignor.assigned_actives = MagicMock(
            return_value=[TP(topic="test", partition=0)]
        )
        expected_outputs = [mock_trigger1, mock_trigger2]
        async for sub in store.get_valid_triggers():
            assert sub in expected_outputs
            expected_outputs.remove(sub)
        assert len(expected_outputs) == 0

    @pytest.mark.asyncio
    async def test_insert_remove_key_value(self, store):
        store.kv_table = dict()
        task = MagicMock()
        task.lastupdated = 0
        task.status.code = TaskStatusEnum.NOT_STARTED.name
        task.id = uuid.uuid1()
        task.task_type = TaskType.LEAF.name
        mock_future = asyncio.Future()
        mock_result = asyncio.Future()
        mock_result.set_result(1)
        mock_future.set_result(mock_result)
        store.app.task_update_topic.send = CoroutineMock(return_value=mock_future)
        await store.insert_key_value(str(task.id), task)
        assert store.kv_table[str(task.id)] == task
        await store.remove_key_value(str(task.id))
        assert store.kv_table.get(str(task.id)) is None

    @pytest.mark.asyncio
    async def test_insert_remove_trigger(self, store: RocksDBStore):
        store.triggers_table = dict()
        mock_trigger1 = Trigger(123, uuid.uuid1(), uuid.uuid1())
        await store.insert_trigger(mock_trigger1)
        assert store.triggers_table[mock_trigger1.get_trigger_key()] == mock_trigger1
        await store.remove_trigger(mock_trigger1)
        assert store.triggers_table.get(mock_trigger1.get_trigger_key()) is None

    @pytest.mark.asyncio
    async def test_store_trigger_instance(self, store):
        store.get_trigger = CoroutineMock(return_value=None)
        store.insert_trigger = CoroutineMock()
        trigger_instance = MagicMock()
        await store.store_trigger_instance(trigger_instance, MagicMock())
        assert store.insert_trigger.called

    @pytest.mark.asyncio
    async def test_process_trigger_task_complete(self, store):
        trigger = MagicMock()
        store.get_trigger = CoroutineMock(return_value=trigger)
        store.remove_trigger = CoroutineMock()
        await store.process_trigger_task_complete(MagicMock(), MagicMock())
        assert store.remove_trigger.called
