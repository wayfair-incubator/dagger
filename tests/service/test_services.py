import asyncio
import json
import uuid

import aerospike
import asynctest
import faust
import mode
import pytest
from asynctest import CoroutineMock, MagicMock

import dagger
from dagger.exceptions.exceptions import TemplateDoesNotExist
from dagger.service.engineconfig import AerospikeConfig, EngineConfig, StoreEnum
from dagger.service.services import Dagger, ServiceStateView, TemplateProcessView
from dagger.store.stores import AerospikeStore, IStore
from dagger.tasks.task import (
    CorrelatableMapValue,
    CorreletableKeyTasks,
    CorreletableLookUpKey,
    DefaultProcessTemplateDAGInstance,
    DefaultTemplateDAGInstance,
    IntervalTask,
    ITask,
    KafkaListenerTask,
    SensorTask,
    TaskStatus,
    TaskStatusEnum,
    Trigger,
    TriggerTask,
)


class TestDagger:
    @pytest.fixture()
    def workflow_instance_fixture(self):
        return DefaultTemplateDAGInstance(uuid.uuid1())

    @pytest.fixture()
    def pe_fixture(self):
        aerospike_config = AerospikeConfig(
            HOSTS=[(3000)],
            POLICIES={"key": aerospike.POLICY_KEY_SEND},
            NAMESPACE="test",
            USERNAME="",
            PASSWORD="",
            TTL=-1,
            KWARGS=None,
        )
        dagger = Dagger(
            broker="localhost",
            completed_topic="completed_topic",
            task_create_topic="task_create_topic",
            datadir="/tmp/data/",
            aerospike_config=aerospike_config,
            enable_changelog=False,
        )
        dagger.kafka_broker_list = ["test"]
        dagger.bootstrap_topic.get_topic_name = MagicMock(
            return_value="bootstrap_topic"
        )
        return dagger

    @pytest.mark.asyncio
    async def test_get_template_error(self, pe_fixture):
        with pytest.raises(TemplateDoesNotExist):
            pe_fixture.get_template("test")

    @pytest.mark.asyncio
    async def test_get_template_success(self, pe_fixture):
        mock = MagicMock()
        pe_fixture.template_dags["test"] = mock
        assert pe_fixture.get_template("test") == mock

    @pytest.mark.asyncio
    async def test_get_on_start(self, pe_fixture):
        pe_fixture.restart_tasks = CoroutineMock()
        pe_fixture._submit_task_on_bootstrap_topic = CoroutineMock()
        pe_fixture.restart_tasks_on_boot = False
        asyncio.sleep = CoroutineMock()
        await pe_fixture.on_start()
        await pe_fixture.on_started()
        assert pe_fixture.started_flag is True

    @pytest.mark.asyncio
    async def test_process_timer_task(self, pe_fixture):
        pe_fixture._submit_task_on_bootstrap_topic = CoroutineMock()
        pe_fixture._store.get_valid_triggers = CoroutineMock()
        await pe_fixture._process_system_timer_task()
        assert pe_fixture._store.get_valid_triggers.called

    @pytest.mark.asyncio
    async def test_submit_bootstrap_task(self, pe_fixture):
        pe_fixture.bootstrap_topic = MagicMock()
        pe_fixture.bootstrap_topic.get_topic_name = MagicMock(return_value="bootstrap")

        topic_object = MagicMock()
        topic_object.partition = 0
        topic_object.topic = "bootstrap"
        pe_fixture.faust_app.consumer._active_partitions = [topic_object]
        pe_fixture.bootstrap_topic.send = asynctest.CoroutineMock()
        await pe_fixture._submit_task_on_bootstrap_topic(MagicMock())
        assert pe_fixture.bootstrap_topic.send.called

    @pytest.mark.asyncio
    async def test_process_engine_create_topic(self):
        topic = MagicMock()
        Dagger.app.faust_app.topic = MagicMock(return_value=topic)
        ret_topic = Dagger.create_topic(topic_name="test")
        assert ret_topic == topic
        dagger.service.services.logger.warning = MagicMock()
        ret_topic = Dagger.create_topic(topic_name="test")
        assert dagger.service.services.logger.warning.called

    @pytest.mark.asyncio
    async def test_process_engine_register_template(self, pe_fixture):
        template = MagicMock()
        mock = MagicMock(return_value=template)
        Dagger.register_template("testtemplate")(mock)
        assert pe_fixture.template_dags["testtemplate"] == template

    @pytest.mark.asyncio
    async def test_process_engine_register_process_template(self, pe_fixture):
        template = MagicMock()
        mock = MagicMock(return_value=template)
        Dagger.register_process_template("testprocesstemplate")(mock)
        assert pe_fixture.process_templates["testprocesstemplate"] == template

    @pytest.mark.asyncio
    async def test_process_engine_get_task_by_key_not_found(self, pe_fixture):
        mock = MagicMock()
        mock.__aiter__.return_value = range(0)
        pe_fixture._store.correletable_keys_table = dict()
        async for _ in pe_fixture._get_tasks_by_correlatable_key(MagicMock()):
            assert False
            break
        else:
            assert True

    @pytest.mark.asyncio
    async def test_process_engine_get_task_by_key_found(
        self, pe_fixture, workflow_instance_fixture
    ):
        mock = MagicMock()
        mock.__aiter__.return_value = range(1)
        cor_instance = CorreletableKeyTasks()
        task_id = uuid.uuid1()
        lookup_key = CorreletableLookUpKey(workflow_instance_fixture.id, task_id)
        cor_instance.lookup_keys = set()
        cor_instance.lookup_keys.add(lookup_key)
        cor_instance.overflow_key = None
        task = ITask(task_id)
        workflow_instance_fixture.add_task(task)
        task.status.code = TaskStatusEnum.EXECUTING.name
        pe_fixture._store.get_table_value = CoroutineMock(return_value=cor_instance)
        pe_fixture._invoke_store_get_value_for_key_with_timer = CoroutineMock(
            return_value=workflow_instance_fixture
        )
        async for wf, ret_value in pe_fixture._get_tasks_by_correlatable_key(
            MagicMock()
        ):
            assert ret_value is task

    @pytest.mark.asyncio
    async def test_process_engine__store_root_template_instance(self, pe_fixture):
        pe_fixture._store.insert_key_value = CoroutineMock()
        pe_fixture._store.insert_root_node = CoroutineMock()
        await pe_fixture._store_root_template_instance(MagicMock())
        assert pe_fixture._store.insert_key_value.called

    @pytest.mark.asyncio
    async def test_process_engine__store_task_instance(
        self, pe_fixture, workflow_instance_fixture
    ):
        pe_fixture._store.insert_key_value = CoroutineMock()
        pe_fixture._store.insert_trigger = CoroutineMock()
        pe_fixture.update_correletable_key_for_task = CoroutineMock()
        listener_task = KafkaListenerTask(uuid.uuid1())
        listener_task.correlatable_key = 123
        workflow_instance_fixture.add_task(listener_task)
        workflow_instance_fixture.runtime_parameters = dict()
        workflow_instance_fixture.runtime_parameters[123] = 123
        faust.current_event = MagicMock()
        await pe_fixture._insert_correletable_key_task(
            listener_task, workflow_instance_fixture
        )
        assert pe_fixture.update_correletable_key_for_task.called
        assert not pe_fixture._store.insert_trigger.called
        trigger_task = TriggerTask(CoroutineMock())
        trigger_task.time_to_execute = 123
        pe_fixture._store.insert_trigger = CoroutineMock()
        await pe_fixture._store_trigger_instance(
            trigger_task, workflow_instance=workflow_instance_fixture
        )
        assert pe_fixture._store.insert_trigger.called
        trigger_task = IntervalTask(CoroutineMock())
        trigger_task.time_to_execute = 123
        trigger_task.interval_execute_period = 12
        assert pe_fixture._store.insert_trigger.called

    @pytest.mark.asyncio
    async def test_process_engine_get_instance(self, pe_fixture):
        task = MagicMock()
        pe_fixture._invoke_store_get_value_for_key_with_timer = CoroutineMock(
            return_value=task
        )
        ret_val = await pe_fixture.get_instance("123")
        assert ret_val == task

    @pytest.mark.asyncio
    async def test_process_engine__update_instance(self, pe_fixture):
        pe_fixture._store.insert_key_value = CoroutineMock()
        pe_fixture._store.insert_root_node = CoroutineMock()
        listener_task = KafkaListenerTask(CoroutineMock())
        await pe_fixture._update_instance(listener_task)
        assert pe_fixture._store.insert_key_value.called
        assert not pe_fixture._store.insert_root_node.called
        root_task = DefaultTemplateDAGInstance(CoroutineMock())
        await pe_fixture._update_instance(root_task)

    @pytest.mark.asyncio
    async def test_process_engine_submit(self, pe_fixture):
        pe_fixture._execution_strategy.submit = CoroutineMock()
        listener_task = KafkaListenerTask(CoroutineMock())
        await pe_fixture.submit(listener_task)
        assert pe_fixture._execution_strategy.submit.called

    @pytest.mark.asyncio
    async def test_process_engine_test_topics(self, pe_fixture):
        topic = MagicMock()
        pe_fixture.add_topic(topic_name="test", topic=topic)
        ret_value = pe_fixture.get_topic("test")
        assert topic == ret_value

    @pytest.mark.asyncio
    async def test_process_engine_test_main(self, pe_fixture):
        worker = MagicMock()
        worker.execute_from_commandline = MagicMock()
        mode.Worker = MagicMock(return_value=worker)
        pe_fixture.main()
        assert worker.execute_from_commandline.called

    @pytest.mark.asyncio
    async def test_process_engine__process_tasks_create_event_template(
        self, pe_fixture
    ):
        pe_fixture._store_root_template_instance = CoroutineMock()
        pe_fixture._store_process_instance = CoroutineMock()
        pe_fixture._store_task_instance = CoroutineMock()
        stream = MagicMock()
        stream.__aiter__.return_value = range(1)
        mock_template_instance = DefaultTemplateDAGInstance(CoroutineMock())
        mock_template_instance.status = TaskStatus(
            code=TaskStatusEnum.SUBMITTED.name, value=TaskStatusEnum.SUBMITTED.value
        )
        mock_template_instance.start = CoroutineMock()
        ITask.loads = MagicMock(return_value=mock_template_instance)
        await pe_fixture._process_tasks_create_event(stream)
        assert pe_fixture._store_root_template_instance.called
        assert not pe_fixture._store_process_instance.called
        assert not pe_fixture._store_task_instance.called

    @pytest.mark.asyncio
    async def test_process_engine__process_tasks_create_event_process(
        self, pe_fixture: Dagger
    ):
        pe_fixture._store_root_template_instance = CoroutineMock()
        stream = MagicMock()
        stream.__aiter__.return_value = range(1)
        ITask.loads = MagicMock(
            return_value=DefaultProcessTemplateDAGInstance(uuid.uuid1())
        )
        await pe_fixture._process_tasks_create_event(stream)
        assert not pe_fixture._store_root_template_instance.called

    @pytest.mark.asyncio
    async def test_process_engine__process_tasks_create_event_task(self, pe_fixture):
        pe_fixture._store_root_template_instance = CoroutineMock()
        pe_fixture.update_correletable_key_for_task = CoroutineMock()
        stream = MagicMock()
        stream.__aiter__.return_value = range(1)
        mock_task = KafkaListenerTask(uuid.uuid1())
        mock_task.status = TaskStatus(
            TaskStatusEnum.NOT_STARTED.name, TaskStatusEnum.NOT_STARTED.value
        )
        ITask.loads = MagicMock(return_value=mock_task)
        pe_fixture.update_correletable_key_for_task = CoroutineMock()
        await pe_fixture._process_tasks_create_event(stream)
        assert not pe_fixture._store_root_template_instance.called

    @pytest.mark.asyncio
    async def test_process_engine_process_bootstrap_events(self, pe_fixture):
        listener = KafkaListenerTask(CoroutineMock())
        listener.start = CoroutineMock()
        stream = MagicMock()
        stream.__aiter__.return_value = range(1)
        ITask.loads = MagicMock(return_value=listener)
        pe_fixture.cleanup_old_triggers = CoroutineMock()
        await pe_fixture._process_bootstrap_tasks(stream)
        assert listener.start.called

    @pytest.mark.asyncio
    async def test_process_engine_update_correletable_key(
        self, pe_fixture: Dagger, workflow_instance_fixture
    ):
        listener = KafkaListenerTask(uuid.uuid1())
        listener.correlatable_key = "k1"
        workflow_instance_fixture.runtime_parameters = dict()
        workflow_instance_fixture.runtime_parameters["k1"] = "v1"
        listener._topic = MagicMock()
        listener._topic.get_topic_name = MagicMock(return_value="topic")
        listener.topic = "topic"
        pe_fixture._store.correletable_keys_table = dict()
        pe_fixture._store.set_table_value = CoroutineMock()
        pe_fixture.remove_task_from_correletable_keys_table = CoroutineMock()
        await pe_fixture.update_correletable_key_for_task(
            listener, None, workflow_instance=workflow_instance_fixture
        )
        assert not pe_fixture.remove_task_from_correletable_keys_table.called
        await pe_fixture.update_correletable_key_for_task(
            listener, "value", workflow_instance=workflow_instance_fixture
        )
        assert pe_fixture.remove_task_from_correletable_keys_table.called
        assert pe_fixture._store.set_table_value.called
        pe_fixture.remove_task_from_correletable_keys_table = MagicMock()
        await pe_fixture.update_correletable_key_for_task(
            listener,
            "value",
            new_task=True,
            workflow_instance=workflow_instance_fixture,
        )
        assert not pe_fixture.remove_task_from_correletable_keys_table.called

    @pytest.mark.asyncio
    async def test_remove_root_template_instance(self, pe_fixture: Dagger):
        pe_fixture._remove_itask_instance = CoroutineMock()
        pe_fixture._store.remove_key_value = CoroutineMock()
        await pe_fixture._remove_root_template_instance(MagicMock())
        assert pe_fixture._store.remove_key_value.called

    @pytest.mark.asyncio
    async def test_store_trigger_instance_aerospike(
        self, pe_fixture: Dagger, workflow_instance_fixture
    ):
        pe_fixture.config = EngineConfig(
            BROKER="test",
            DATADIR="datadir",
            STORE=StoreEnum.AEROSPIKE.value,
            APPLICATION_NAME="test",
            PACKAGE_NAME="test",
            KAFKA_PARTITIONS=5,
            KWARGS=None,
        )
        trigger = TriggerTask(uuid.uuid1())
        workflow_instance_fixture.add_task(trigger)
        trigger.time_to_execute = 123
        trigger_instance = Trigger()
        trigger_instance.trigger_time = 123
        pe_fixture._store.get_trigger = CoroutineMock(return_value=None)
        pe_fixture._store.insert_trigger = CoroutineMock()
        pe_fixture._store.store_trigger_instance = CoroutineMock()
        await pe_fixture._store_trigger_instance(
            trigger_instance, workflow_instance=workflow_instance_fixture
        )
        assert pe_fixture._store.store_trigger_instance.called

    @pytest.mark.asyncio
    async def test_get_tasks_by_correlatable_key_task_not_found(
        self, pe_fixture: Dagger, workflow_instance_fixture
    ):
        cr_tasks = CorreletableKeyTasks()
        task_id = uuid.uuid1()
        lookup_key = CorreletableLookUpKey(workflow_instance_fixture.id, task_id)
        cr_tasks.lookup_keys = set()
        cr_tasks.lookup_keys.add(lookup_key)
        pe_fixture._store.del_table_value = CoroutineMock()
        pe_fixture._store.get_value_for_key = CoroutineMock(
            return_value=workflow_instance_fixture
        )
        pe_fixture._store.get_table_value = CoroutineMock(return_value=cr_tasks)
        async for _, _ in pe_fixture._get_tasks_by_correlatable_key(
            lookup_key=("k", "v")
        ):
            assert False
        assert pe_fixture._store.del_table_value.called

    @pytest.mark.asyncio
    async def test_get_db_options(self, pe_fixture: Dagger):

        pe_fixture.config = EngineConfig(
            BROKER="test",
            DATADIR="datadir",
            STORE=StoreEnum.AEROSPIKE.value,
            APPLICATION_NAME="test",
            PACKAGE_NAME="test",
            KAFKA_PARTITIONS=5,
            KWARGS=None,
        )

        pe_fixture.aerospike_config = MagicMock()
        aero_opts = MagicMock()
        pe_fixture.aerospike_config.as_options = MagicMock(return_value=aero_opts)
        pe_fixture._store = AerospikeStore(CoroutineMock())
        pe_fixture._store.get_db_options = MagicMock(return_value=aero_opts)
        db_options = pe_fixture.get_db_options()
        assert db_options == aero_opts

    @pytest.mark.asyncio
    async def test_chunk_and_store_correlatable_tasks_no_overflow(
        self, pe_fixture: Dagger, workflow_instance_fixture
    ):
        pe_fixture.max_correletable_keys_in_values = 2
        cor_instance: CorreletableKeyTasks = CorreletableKeyTasks()
        cor_instance.key = "key"
        task_id = uuid.uuid1()
        lookup_key = CorreletableLookUpKey(workflow_instance_fixture.id, task_id)
        cor_instance.lookup_keys = set()
        cor_instance.lookup_keys.add(lookup_key)
        pe_fixture._store.set_table_value = CoroutineMock()
        await pe_fixture.chunk_and_store_correlatable_tasks(
            cor_instance=cor_instance,
            value=uuid.uuid1(),
            workflow_id=workflow_instance_fixture.id,
        )
        assert pe_fixture._store.set_table_value.call_count == 1
        assert cor_instance.overflow_key is None

    @pytest.mark.asyncio
    async def test_chunk_and_store_correlatable_tasks_overflow(
        self, pe_fixture: Dagger, workflow_instance_fixture
    ):
        pe_fixture.max_correletable_keys_in_values = 2
        cor_instance: CorreletableKeyTasks = CorreletableKeyTasks()
        cor_instance.key = "key"
        task_id = uuid.uuid1()
        lookup_key = CorreletableLookUpKey(workflow_instance_fixture.id, task_id)
        lookup_key2 = CorreletableLookUpKey(workflow_instance_fixture.id, uuid.uuid1())
        cor_instance.lookup_keys = set()
        cor_instance.lookup_keys.add(lookup_key)
        cor_instance.lookup_keys.add(lookup_key2)
        pe_fixture._store.set_table_value = CoroutineMock()
        await pe_fixture.chunk_and_store_correlatable_tasks(
            cor_instance=cor_instance,
            value=uuid.uuid1(),
            workflow_id=workflow_instance_fixture.id,
        )
        assert pe_fixture._store.set_table_value.call_count == 2
        assert cor_instance.overflow_key is not None

    @pytest.fixture
    def correletable_table_fixture(self, workflow_instance_fixture):

        c1: CorreletableKeyTasks = CorreletableKeyTasks()
        c1.key = "ck1_topic1"
        overflow_key1 = uuid.uuid1()
        c1.overflow_key = overflow_key1
        lookup_key1 = CorreletableLookUpKey(workflow_instance_fixture.id, uuid.uuid1())
        c1.lookup_keys = set()
        c1.lookup_keys.add(lookup_key1)
        lookup_key2 = CorreletableLookUpKey(workflow_instance_fixture.id, uuid.uuid1())
        c1.lookup_keys.add(lookup_key2)

        c2: CorreletableKeyTasks = CorreletableKeyTasks()
        c2.key = overflow_key1
        lookup_key3 = CorreletableLookUpKey(workflow_instance_fixture.id, uuid.uuid1())
        lookup_key4 = CorreletableLookUpKey(workflow_instance_fixture.id, uuid.uuid1())
        c2.lookup_keys.add(lookup_key4)
        c2.lookup_keys.add(lookup_key3)
        return {c1.key: c1, c2.key: c2}

    @pytest.mark.asyncio
    async def test_update_correletable_key_for_task(
        self, pe_fixture: Dagger, correletable_table_fixture, workflow_instance_fixture
    ):
        pe_fixture._store = IStore(pe_fixture)
        pe_fixture.max_correletable_keys_in_values = 2
        pe_fixture._store.correletable_keys_table = correletable_table_fixture
        mock_task = SensorTask(uuid.uuid1())
        mock_task.id = uuid.uuid1()
        mock_task.correlatable_key = "correlatable_key"
        mock_task.runtime_parameters = {mock_task.correlatable_key: "ck1"}
        mock_task._topic = MagicMock()
        mock_task._topic.get_topic_name = MagicMock(return_value="topic1")
        mock_task.topic = "topic1"
        workflow_instance_fixture.add_task(mock_task)

        workflow_instance_fixture.runtime_parameters = dict()
        workflow_instance_fixture.runtime_parameters = {
            mock_task.correlatable_key: "ck1"
        }
        workflow_instance_fixture.sensor_tasks_to_correletable_map = dict()
        workflow_instance_fixture.sensor_tasks_to_correletable_map[
            mock_task.id
        ] = CorrelatableMapValue(mock_task.correlatable_key, "ck1")

        await pe_fixture.update_correletable_key_for_task(
            task_instance=mock_task,
            key="ck1",
            new_task=True,
            workflow_instance=workflow_instance_fixture,
        )
        assert len(pe_fixture._store.correletable_keys_table) == 3
        c1: CorreletableKeyTasks = pe_fixture._store.correletable_keys_table[
            "ck1_topic1"
        ]
        c2: CorreletableKeyTasks = pe_fixture._store.correletable_keys_table[
            c1.overflow_key
        ]
        assert c2.key == c1.overflow_key
        c3: CorreletableKeyTasks = pe_fixture._store.correletable_keys_table[
            c2.overflow_key
        ]
        assert c3.key == c2.overflow_key
        assert c3.overflow_key is None

        await pe_fixture.update_correletable_key_for_task(
            task_instance=mock_task,
            key="ck2",
            new_task=False,
            workflow_instance=workflow_instance_fixture,
        )
        assert len(pe_fixture._store.correletable_keys_table) == 3
        c1: CorreletableKeyTasks = pe_fixture._store.correletable_keys_table[
            "ck1_topic1"
        ]
        c2: CorreletableKeyTasks = pe_fixture._store.correletable_keys_table[
            c1.overflow_key
        ]
        assert c2.key == c1.overflow_key
        assert c2.overflow_key is None
        c3: CorreletableKeyTasks = pe_fixture._store.correletable_keys_table[
            "ck2_topic1"
        ]
        assert c3.key == "ck2_topic1"
        assert c3.overflow_key is None
        assert len(c3.lookup_keys) == 1

    @pytest.mark.asyncio
    async def test_persist_tasks_ids_empty_cor_instances(
        self, pe_fixture: Dagger, correletable_table_fixture
    ):
        pe_fixture._store = IStore(pe_fixture)
        pe_fixture.max_correletable_keys_in_values = 2
        current_task_id = uuid.uuid1()
        tasks_ids = [current_task_id]
        await pe_fixture.persist_tasks_ids_for_correletable_keys(
            lookup_keys=tasks_ids, cor_instances=[]
        )
        assert current_task_id in tasks_ids

    @pytest.mark.asyncio
    async def test_persist_tasks_ids_more_chunks(
        self, pe_fixture: Dagger, correletable_table_fixture
    ):
        pe_fixture._store = IStore(pe_fixture)
        pe_fixture.max_correletable_keys_in_values = 2
        current_task_id = uuid.uuid1()
        tasks_ids = [current_task_id, uuid.uuid1(), uuid.uuid1()]
        await pe_fixture.persist_tasks_ids_for_correletable_keys(
            lookup_keys=tasks_ids, cor_instances=[MagicMock()]
        )
        assert len(tasks_ids) == 3

    @pytest.mark.asyncio
    async def test_persist_tasks_ids_success(
        self, pe_fixture: Dagger, correletable_table_fixture
    ):
        pe_fixture._store = IStore(pe_fixture)
        pe_fixture.max_correletable_keys_in_values = 2
        pe_fixture._store.correletable_keys_table = correletable_table_fixture
        ck = []
        for value in correletable_table_fixture.values():
            ck.append(value)
        await pe_fixture.persist_tasks_ids_for_correletable_keys(
            lookup_keys=[], cor_instances=ck
        )
        assert len(correletable_table_fixture) == 0


class TestServiceStateView:
    @pytest.fixture()
    def ssview_fixture(self):
        return ServiceStateView(app=MagicMock(), web=MagicMock())

    @pytest.mark.asyncio
    async def test_ssview_get(self, ssview_fixture):
        ssview_fixture.text = MagicMock()
        Dagger.app = MagicMock()
        Dagger.app.started_flag = True
        await ssview_fixture.get(request=MagicMock())
        assert ssview_fixture.text.called


class TestTemplateProcessView:
    @pytest.fixture()
    def task_view_fixture(self):
        return TemplateProcessView(app=MagicMock(), web=MagicMock())

    @pytest.fixture()
    def pe_fixture(self):
        return Dagger(broker="localhost", datadir="/tmp/data/")

    def mock_prep(self, store_data_fixture):
        obj = MagicMock(dict)

        def iterable_items(**kwargs):
            dumped_dict = MagicMock()
            dumped_dict.__iter__.return_value = [
                [json.dumps(item[0]), json.dumps(item[1])]
                for item in store_data_fixture.items()
            ]
            return dumped_dict

        obj.items = iterable_items
        obj.iteritems = iterable_items
        return obj

    @pytest.mark.asyncio
    async def test_task_view_get_no_offset(self, task_view_fixture):
        request = MagicMock()
        request.query = dict()
        task_view_fixture.text = MagicMock()
        Dagger.app = MagicMock()
        Dagger.app.started_flag = True

        template_instance = DefaultTemplateDAGInstance(id=uuid.uuid1())
        Dagger.app._store.kv_table = {template_instance.id: template_instance}
        process_instance = DefaultProcessTemplateDAGInstance(id=uuid.uuid1())
        process_instance.next_dags = []
        template_instance.root_dag = uuid.uuid1()
        template_instance.add_task(process_instance)
        await task_view_fixture.get(request=request)
        assert task_view_fixture.text.called
        assert len(task_view_fixture.tasks) == 1
