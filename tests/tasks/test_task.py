import time
import uuid
from uuid import UUID, uuid1  # noqa: F401

import asynctest
import jsonpickle
import pytest
from asynctest import CoroutineMock, MagicMock

import dagger
import dagger.service.services
from dagger.tasks.task import (
    COMPLETE_BY_KEY,
    CorrelatableMapValue,
    DecisionTask,
    DefaultMonitoringTask,
    DefaultProcessTemplateDAGInstance,
    DefaultTemplateDAGInstance,
    IntervalTask,
    ITask,
    KafkaAgent,
    KafkaCommandTask,
    KafkaListenerTask,
    MonitoredProcessTemplateDAGInstance,
    ParallelCompositeTask,
    SensorTask,
    SkipOnMaxDurationTask,
    SystemTimerTask,
    TaskOperator,
    TaskStatus,
    TaskStatusEnum,
    Trigger,
    TriggerTask,
)

test = 2 * 1024**3
test = test


class TestTasks:
    @pytest.fixture()
    def template_fixture(self):
        return DefaultTemplateDAGInstance(uuid1())

    @pytest.fixture()
    def executor_fixture(self):
        return KafkaCommandTask(uuid1())

    @pytest.fixture()
    def decision_fixture(self):
        return DecisionTask(uuid1())

    @pytest.fixture()
    def sensor_fixture(self):
        return KafkaListenerTask(uuid1())

    @pytest.fixture()
    def trigger_fixture(self):
        return TriggerTask(uuid1())

    @pytest.fixture()
    def interval_fixture(self):
        return IntervalTask(uuid1())

    @pytest.fixture()
    def system_timer_fixture(self):
        return SystemTimerTask(uuid1())

    @pytest.fixture()
    def parallel_composite_task_fixture(self):
        dagger.service.services.Dagger.app = MagicMock()
        return ParallelCompositeTask(uuid.uuid1())

    @pytest.fixture()
    def workflow_instance_fixture(self):
        return DefaultTemplateDAGInstance(uuid.uuid1())

    @pytest.mark.asyncio
    async def test_parallel_composite_task_stop(self, parallel_composite_task_fixture):
        try:
            await parallel_composite_task_fixture.stop()
        except Exception:
            pytest.fail("Error should not be thrown")

    @pytest.mark.asyncio
    async def test_parallel_composite_task_execute(
        self, parallel_composite_task_fixture
    ):
        try:
            await parallel_composite_task_fixture.execute(runtime_parameters={})
            assert (
                parallel_composite_task_fixture.status.code
                == TaskStatusEnum.EXECUTING.name
            )
        except Exception:
            pytest.fail("Error should not be thrown")

    @pytest.mark.asyncio
    async def test_parallel_composite_task_start_terminal(
        self, parallel_composite_task_fixture, workflow_instance_fixture
    ):
        try:
            parallel_composite_task_fixture.status = TaskStatus(
                code=TaskStatusEnum.COMPLETED.name, value=TaskStatusEnum.COMPLETED.value
            )
            parallel_composite_task_fixture.execute = CoroutineMock()
            parallel_composite_task_fixture.on_complete = CoroutineMock()
            await parallel_composite_task_fixture.start(
                workflow_instance=workflow_instance_fixture
            )
            assert not parallel_composite_task_fixture.execute.called
            assert parallel_composite_task_fixture.on_complete.called
        except Exception:
            pytest.fail("Error should not be thrown")

    @pytest.mark.asyncio
    async def test_parallel_composite_task_start_non_terminal(
        self, parallel_composite_task_fixture, workflow_instance_fixture
    ):
        try:
            parallel_composite_task_fixture.status = TaskStatus(
                code=TaskStatusEnum.SUBMITTED.name, value=TaskStatusEnum.SUBMITTED.value
            )
            child_task1 = MagicMock()
            child_task1.start = CoroutineMock()
            child_task2 = MagicMock()
            child_task2.start = CoroutineMock()
            id1 = uuid1()
            id2 = uuid1()
            parallel_composite_task_fixture.parallel_child_task_list = [id1, id2]
            dagger.service.services.Dagger.app._update_instance = CoroutineMock()
            dagger.service.services.Dagger.app.get_instance = CoroutineMock(
                side_effect=lambda x, log: child_task1 if x == id1 else child_task2
            )
            parallel_composite_task_fixture.execute = CoroutineMock()
            parallel_composite_task_fixture.on_complete = CoroutineMock()
            dagger.service.services.Dagger.app._update_instance = CoroutineMock()
            workflow_instance_fixture.tasks[id1] = child_task1
            workflow_instance_fixture.tasks[id2] = child_task2

            await parallel_composite_task_fixture.start(
                workflow_instance=workflow_instance_fixture
            )
            assert parallel_composite_task_fixture.execute.called
            assert dagger.service.services.Dagger.app._update_instance.called
            assert not parallel_composite_task_fixture.on_complete.called
            assert child_task1.start.called
            assert child_task2.start.called
            assert dagger.service.services.Dagger.app._update_instance.called
        except Exception:
            pytest.fail("Error should not be thrown")

    @pytest.mark.asyncio
    async def test_parallel_composite_task_start_non_terminal_executing(
        self, parallel_composite_task_fixture, workflow_instance_fixture
    ):
        try:
            parallel_composite_task_fixture.status = TaskStatus(
                code=TaskStatusEnum.EXECUTING.name, value=TaskStatusEnum.EXECUTING.value
            )
            child_task1 = MagicMock()
            child_task1.start = CoroutineMock()
            child_task2 = MagicMock()
            child_task2.start = CoroutineMock()
            id1 = uuid1()
            id2 = uuid1()
            parallel_composite_task_fixture.parallel_child_task_list = [id1, id2]
            dagger.service.services.Dagger.app._update_instance = CoroutineMock()
            workflow_instance_fixture.tasks[id1] = child_task1
            workflow_instance_fixture.tasks[id2] = child_task2
            parallel_composite_task_fixture.execute = CoroutineMock()
            parallel_composite_task_fixture.on_complete = CoroutineMock()
            await parallel_composite_task_fixture.start(
                workflow_instance=workflow_instance_fixture
            )
            assert not parallel_composite_task_fixture.execute.called
            assert not parallel_composite_task_fixture.on_complete.called
            assert child_task1.start.called
            assert child_task2.start.called
            assert not dagger.service.services.Dagger.app._update_instance.called
        except Exception:
            pytest.fail("Error should not be thrown")

    @pytest.mark.asyncio
    async def test_parallel_composite_task_notify_same_status(
        self, parallel_composite_task_fixture
    ):
        parallel_composite_task_fixture.status = TaskStatus(
            code=TaskStatusEnum.COMPLETED.name, value=TaskStatusEnum.COMPLETED.value
        )
        child_task1 = MagicMock()
        child_task1.start = CoroutineMock()
        child_task2 = MagicMock()
        child_task2.start = CoroutineMock()
        id1 = uuid1()
        id2 = uuid1()
        parallel_composite_task_fixture.parallel_child_task_list = [id1, id2]
        dagger.service.services.Dagger.app.get_instance = CoroutineMock()
        await parallel_composite_task_fixture.notify(
            parallel_composite_task_fixture.status
        )
        assert not dagger.service.services.Dagger.app.get_instance.called

    @pytest.mark.asyncio
    async def test_parallel_composite_task_notify_join_all_both_complete(
        self, parallel_composite_task_fixture, workflow_instance_fixture
    ):
        parallel_composite_task_fixture.status = TaskStatus(
            code=TaskStatusEnum.EXECUTING.name, value=TaskStatusEnum.EXECUTING.value
        )
        parallel_composite_task_fixture.operator_type = TaskOperator.JOIN_ALL.name

        child_task1 = MagicMock()
        child_task1.start = CoroutineMock()
        child_task1.status.code = TaskStatusEnum.COMPLETED.name
        child_task2 = MagicMock()
        child_task2.status.code = TaskStatusEnum.COMPLETED.name

        child_task2.start = CoroutineMock()
        id1 = uuid1()
        id2 = uuid1()
        parallel_composite_task_fixture.on_complete = CoroutineMock()
        parallel_composite_task_fixture.parallel_child_task_list = [id1, id2]
        workflow_instance_fixture.tasks[id1] = child_task1
        workflow_instance_fixture.tasks[id2] = child_task2
        await parallel_composite_task_fixture.notify(
            TaskStatus(
                code=TaskStatusEnum.COMPLETED.name, value=TaskStatusEnum.COMPLETED.value
            ),
            workflow_instance=workflow_instance_fixture,
        )
        assert parallel_composite_task_fixture.on_complete.called

    @pytest.mark.asyncio
    async def test_parallel_composite_task_notify_join_not_both_complete(
        self, parallel_composite_task_fixture, workflow_instance_fixture
    ):
        parallel_composite_task_fixture.status = TaskStatus(
            code=TaskStatusEnum.EXECUTING.name, value=TaskStatusEnum.EXECUTING.value
        )
        parallel_composite_task_fixture.operator_type = TaskOperator.JOIN_ALL.name

        child_task1 = MagicMock()
        child_task1.start = CoroutineMock()
        child_task1.status.code = TaskStatusEnum.COMPLETED.name
        child_task2 = MagicMock()
        child_task2.status.code = TaskStatusEnum.EXECUTING.name

        child_task2.start = CoroutineMock()
        id1 = uuid1()
        id2 = uuid1()
        parallel_composite_task_fixture.on_complete = CoroutineMock()
        parallel_composite_task_fixture.parallel_child_task_list = [id1, id2]
        workflow_instance_fixture.tasks[id1] = child_task1
        workflow_instance_fixture.tasks[id2] = child_task2
        await parallel_composite_task_fixture.notify(
            TaskStatus(
                code=TaskStatusEnum.COMPLETED.name, value=TaskStatusEnum.COMPLETED.value
            ),
            workflow_instance=workflow_instance_fixture,
        )
        assert not parallel_composite_task_fixture.on_complete.called

    @pytest.mark.asyncio
    async def test_parallel_composite_task_notify_atleast_one_none_complete(
        self, parallel_composite_task_fixture, workflow_instance_fixture
    ):
        parallel_composite_task_fixture.status = TaskStatus(
            code=TaskStatusEnum.EXECUTING.name, value=TaskStatusEnum.EXECUTING.value
        )
        parallel_composite_task_fixture.operator_type = TaskOperator.ATLEAST_ONE.name

        child_task1 = MagicMock()
        child_task1.start = CoroutineMock()
        child_task1.status.code = TaskStatusEnum.EXECUTING.name
        child_task2 = MagicMock()
        child_task2.status.code = TaskStatusEnum.EXECUTING.name

        child_task2.start = CoroutineMock()
        id1 = uuid1()
        id2 = uuid1()
        parallel_composite_task_fixture.on_complete = CoroutineMock()
        parallel_composite_task_fixture.parallel_child_task_list = [id1, id2]
        workflow_instance_fixture.tasks[id1] = child_task1
        workflow_instance_fixture.tasks[id2] = child_task2
        await parallel_composite_task_fixture.notify(
            TaskStatus(
                code=TaskStatusEnum.COMPLETED.name, value=TaskStatusEnum.COMPLETED.value
            ),
            workflow_instance=workflow_instance_fixture,
        )
        assert not parallel_composite_task_fixture.on_complete.called

    @pytest.mark.asyncio
    async def test_parallel_composite_task_notify_atleast_one_one_complete(
        self, parallel_composite_task_fixture, workflow_instance_fixture
    ):
        parallel_composite_task_fixture.status = TaskStatus(
            code=TaskStatusEnum.EXECUTING.name, value=TaskStatusEnum.EXECUTING.value
        )
        parallel_composite_task_fixture.operator_type = TaskOperator.ATLEAST_ONE.name

        child_task1 = MagicMock()
        child_task1.start = CoroutineMock()
        child_task1.status.code = TaskStatusEnum.EXECUTING.name
        child_task2 = MagicMock()
        child_task2.status.code = TaskStatusEnum.COMPLETED.name

        child_task2.start = CoroutineMock()
        id1 = uuid1()
        id2 = uuid1()
        parallel_composite_task_fixture.on_complete = CoroutineMock()
        parallel_composite_task_fixture.parallel_child_task_list = [id1, id2]
        workflow_instance_fixture.tasks[id1] = child_task1
        workflow_instance_fixture.tasks[id2] = child_task2
        await parallel_composite_task_fixture.notify(
            TaskStatus(
                code=TaskStatusEnum.COMPLETED.name, value=TaskStatusEnum.COMPLETED.value
            ),
            workflow_instance=workflow_instance_fixture,
        )
        assert parallel_composite_task_fixture.on_complete.called

    @pytest.mark.asyncio
    async def test_get_remaining_tasks(
        self, template_fixture, executor_fixture, sensor_fixture, decision_fixture
    ):
        dagger.service.services.Dagger.app.get_instance = CoroutineMock(
            side_effect=lambda m: m
        )
        first_process = DefaultProcessTemplateDAGInstance(uuid1())
        second_process = DefaultProcessTemplateDAGInstance(uuid1())
        template_fixture.root_dag = first_process.id
        template_fixture.add_task(first_process)
        first_process.root_dag = executor_fixture.id
        template_fixture.add_task(executor_fixture)
        second_process.root_dag = decision_fixture.id
        template_fixture.add_task(decision_fixture)
        template_fixture.add_task(sensor_fixture)
        template_fixture.add_task(second_process)
        first_process.next_dags = [second_process.id]
        second_process.next_dags = []

        executor_fixture.next_dags = [sensor_fixture.id]
        executor_fixture.root_dag = None
        sensor_fixture.next_dags = []
        sensor_fixture.root_dag = None
        decision_fixture.next_dags = []
        decision_fixture.root_dag = None

        remaining_tasks = await template_fixture.get_remaining_tasks(
            template_fixture.root_dag, workflow_instance=template_fixture, tasks=[]
        )
        assert remaining_tasks == [
            executor_fixture,
            sensor_fixture,
            first_process,
            decision_fixture,
            second_process,
        ]

        sensor_fixture.get_id = MagicMock(return_value=1)
        remaining_tasks = await template_fixture.get_remaining_tasks(
            template_fixture.root_dag,
            workflow_instance=template_fixture,
            tasks=[],
            end_task_id=1,
        )
        assert remaining_tasks == [executor_fixture, sensor_fixture]

        executor_fixture.get_id = MagicMock(return_value=2)
        remaining_tasks = await template_fixture.get_remaining_tasks(
            template_fixture.root_dag,
            workflow_instance=template_fixture,
            tasks=[],
            end_task_id=2,
        )
        assert remaining_tasks == [executor_fixture]

        first_process.get_id = MagicMock(return_value=3)
        remaining_tasks = await template_fixture.get_remaining_tasks(
            template_fixture.root_dag,
            workflow_instance=template_fixture,
            tasks=[],
            end_task_id=3,
        )
        assert remaining_tasks == [executor_fixture, sensor_fixture, first_process]

        decision_fixture.get_id = MagicMock(return_value=4)
        remaining_tasks = await template_fixture.get_remaining_tasks(
            template_fixture.root_dag,
            workflow_instance=template_fixture,
            tasks=[],
            end_task_id=4,
        )
        assert remaining_tasks == [
            executor_fixture,
            sensor_fixture,
            first_process,
            decision_fixture,
        ]

        second_process.get_id = MagicMock(return_value=5)
        remaining_tasks = await template_fixture.get_remaining_tasks(
            template_fixture.root_dag,
            workflow_instance=template_fixture,
            tasks=[],
            end_task_id=5,
        )
        assert remaining_tasks == [
            executor_fixture,
            sensor_fixture,
            first_process,
            decision_fixture,
            second_process,
        ]

    @pytest.mark.asyncio
    async def test_merge_runtime_parameters_template_instance(self, template_fixture):
        dagger.service.services.Dagger.app = CoroutineMock()
        dagger.service.services.Dagger.app._update_instance = CoroutineMock()
        template_fixture.runtime_parameters = {"k1": "10", "k2": "20"}
        remaining_task = SensorTask(uuid1())
        template_fixture.add_task(remaining_task)
        template_fixture.sensor_tasks_to_correletable_map[
            remaining_task.id
        ] = CorrelatableMapValue("k1", "0")
        remaining_task.status.code = TaskStatusEnum.NOT_STARTED.name
        remaining_task._update_correletable_key = CoroutineMock()
        template_fixture.get_remaining_tasks = CoroutineMock(
            return_value=[remaining_task]
        )
        await template_fixture._update_global_runtime_parameters()
        assert template_fixture.runtime_parameters == {"k1": "10", "k2": "20"}
        assert remaining_task._update_correletable_key.called
        assert template_fixture.sensor_tasks_to_correletable_map[
            remaining_task.id
        ] == CorrelatableMapValue("k1", "10")

    @pytest.mark.asyncio
    async def test_executortask(self, executor_fixture, workflow_instance_fixture):
        assert executor_fixture.status.code == TaskStatusEnum.NOT_STARTED.name
        dagger.service.services.Dagger.app = CoroutineMock()
        dagger.service.services.Dagger.app._update_instance = CoroutineMock()
        mock_status = MagicMock()
        jsonpickle.encode = MagicMock()
        mock_status.status.code = TaskStatusEnum.SKIPPED.name
        dagger.service.services.Dagger.app.get_instance = CoroutineMock(
            return_value=None
        )
        assert executor_fixture.time_completed == 0
        executor_fixture.next_dags = []
        parent_task = DefaultProcessTemplateDAGInstance(uuid1())
        workflow_instance_fixture.add_task(parent_task)
        executor_fixture.parent_id = parent_task.id
        mock_parent_notify_func = CoroutineMock
        mock_parent_notify_func.notify = CoroutineMock()
        mock_parent_node_func = executor_fixture.get_parent_node = CoroutineMock()
        mock_parent_node_func.side_effect = mock_parent_notify_func
        parent_task.notify = CoroutineMock()
        assert executor_fixture.get_id() == executor_fixture.id
        await executor_fixture.start(workflow_instance=workflow_instance_fixture)
        assert dagger.service.services.Dagger.app._update_instance.called
        assert executor_fixture.status.code == TaskStatusEnum.COMPLETED.name
        assert executor_fixture.time_completed != 0
        assert parent_task.notify.called
        with pytest.raises(NotImplementedError):
            await executor_fixture.evaluate()
        with pytest.raises(NotImplementedError):
            await executor_fixture.on_message(
                workflow_instance_fixture.runtime_parameters, {}
            )

    @pytest.mark.asyncio
    async def test_decisiontask(self, decision_fixture, workflow_instance_fixture):
        assert decision_fixture.status.code == TaskStatusEnum.NOT_STARTED.name
        dagger.service.services.Dagger.app._update_instance = CoroutineMock()
        mock_status = MagicMock()
        mock_status.status.code = TaskStatusEnum.NOT_STARTED.name
        dagger.service.services.Dagger.app.get_instance = CoroutineMock(
            return_value=mock_status
        )
        decision_fixture.next_dags = []
        decision_fixture.on_complete = CoroutineMock()
        workflow_instance_fixture.runtime_parameters = {}
        assert decision_fixture.get_id() == decision_fixture.id
        await decision_fixture.start(workflow_instance=workflow_instance_fixture)
        assert dagger.service.services.Dagger.app._update_instance.called
        assert decision_fixture.on_complete.called
        with pytest.raises(NotImplementedError):
            await decision_fixture.execute(
                runtime_parameters={}, workflow_instance=workflow_instance_fixture
            )
        with pytest.raises(NotImplementedError):
            await decision_fixture.on_message(workflow_instance_fixture, {})

    @pytest.mark.asyncio
    async def test_template_on_complete(self, template_fixture):
        assert template_fixture.status.code == TaskStatusEnum.NOT_STARTED.name
        dagger.service.services.Dagger.app._update_instance = CoroutineMock()
        mock_task = MagicMock()
        mock_task.root_dag = None
        mock_task.next_dags = []
        mock_task.next_task_dag = None
        dagger.service.services.Dagger.app.get_instance.side_effect = [
            mock_task,
            KeyError,
        ]
        dagger.service.services.Dagger.app._remove_itask_instance = CoroutineMock()
        dagger.service.services.Dagger.app._remove_root_template_instance = (
            CoroutineMock()
        )
        dagger.service.services.Dagger.app._remove_root_template_instance = (
            CoroutineMock()
        )
        monitoring_task = MagicMock()
        dagger.service.services.Dagger.app.get_monitoring_task = CoroutineMock(
            return_value=monitoring_task
        )
        monitoring_task.on_complete = CoroutineMock()
        template_fixture.runtime_parameters = MagicMock()
        template_fixture.next_dags = []
        assert template_fixture.get_id() == template_fixture.id
        await template_fixture.on_complete(workflow_instance=template_fixture)
        assert template_fixture.status.code == TaskStatusEnum.COMPLETED.name
        cur_time = int(time.time())
        assert template_fixture.time_completed <= cur_time

    @pytest.mark.asyncio
    async def test_template_on_complete_time_set(self, template_fixture):
        assert template_fixture.status.code == TaskStatusEnum.NOT_STARTED.name
        dagger.service.services.Dagger.app._update_instance = CoroutineMock()
        mock_task = MagicMock()
        mock_task.root_dag = None
        mock_task.next_dags = []
        mock_task.next_task_dag = None
        dagger.service.services.Dagger.app.get_instance.side_effect = [
            mock_task,
            KeyError,
        ]
        monitoring_task = MagicMock()
        dagger.service.services.Dagger.app.get_monitoring_task = CoroutineMock(
            return_value=monitoring_task
        )
        monitoring_task.on_complete = CoroutineMock()
        template_fixture.runtime_parameters = MagicMock()
        template_fixture.next_dags = []
        template_fixture.time_completed = 123
        assert template_fixture.get_id() == template_fixture.id
        await template_fixture.on_complete(workflow_instance=template_fixture)
        assert template_fixture.status.code == TaskStatusEnum.COMPLETED.name
        assert template_fixture.time_completed == 123

    @pytest.mark.asyncio
    async def test_sensortask(self, sensor_fixture, workflow_instance_fixture):
        assert sensor_fixture.status.code == TaskStatusEnum.NOT_STARTED.name
        dagger.service.services.Dagger.app._update_instance = CoroutineMock()
        sensor_fixture.on_complete = CoroutineMock()
        assert sensor_fixture.get_id() == sensor_fixture.id
        payload = dict()
        payload["1"] = 1
        ret_val = sensor_fixture.get_correlatable_key(payload)
        assert payload == ret_val
        await sensor_fixture.start(workflow_instance=workflow_instance_fixture)
        assert dagger.service.services.Dagger.app._update_instance.called
        assert not sensor_fixture.on_complete.called
        with pytest.raises(NotImplementedError):
            await sensor_fixture.execute(
                {}, workflow_instance=workflow_instance_fixture
            )
        with pytest.raises(NotImplementedError):
            await sensor_fixture.evaluate()

    @pytest.mark.asyncio
    async def test_sensortask_update_key_equal(
        self, sensor_fixture: SensorTask, workflow_instance_fixture
    ):
        template_instance = MagicMock()
        workflow_instance_fixture.runtime_parameters = {"k1": "v1", "k2": "v2"}
        dagger.service.services.Dagger.app.get_root_template_instance = CoroutineMock(
            return_value=workflow_instance_fixture
        )
        sensor_fixture.correlatable_key = "k2"
        dagger.service.services.Dagger.app.update_correletable_key_for_task = (
            CoroutineMock()
        )
        await sensor_fixture._update_correletable_key(template_instance)
        assert (
            dagger.service.services.Dagger.app.update_correletable_key_for_task.called
        )

    @pytest.mark.asyncio
    async def test_sensortask_update_key_both_none(
        self, sensor_fixture: SensorTask, workflow_instance_fixture
    ):
        workflow_instance_fixture.runtime_parameters = {"k1": "v1", "k2": "v2"}
        dagger.service.services.Dagger.app.get_root_template_instance = CoroutineMock(
            return_value=workflow_instance_fixture
        )
        sensor_fixture.correlatable_key = "k3"
        sensor_fixture.runtime_parameters = {"k1": "v1"}
        dagger.service.services.Dagger.app.update_correletable_key_for_task = (
            CoroutineMock()
        )
        await sensor_fixture._update_correletable_key(workflow_instance_fixture)
        assert (
            dagger.service.services.Dagger.app.update_correletable_key_for_task.called
        )

    @pytest.mark.asyncio
    async def test_future_triggertask(self, trigger_fixture, workflow_instance_fixture):
        assert trigger_fixture.status.code == TaskStatusEnum.NOT_STARTED.name
        trigger_fixture.time_to_execute = int(time.time()) + 1000
        dagger.service.services.Dagger.app._update_instance = CoroutineMock()
        assert trigger_fixture.get_id() == trigger_fixture.id
        await trigger_fixture.start(workflow_instance=workflow_instance_fixture)
        assert trigger_fixture.status.code == TaskStatusEnum.EXECUTING.name

    @pytest.mark.asyncio
    async def test_current_triggertask(
        self, trigger_fixture, workflow_instance_fixture
    ):
        assert trigger_fixture.status.code == TaskStatusEnum.NOT_STARTED.name
        trigger_fixture.time_to_execute = int(time.time()) - 1
        dagger.service.services.Dagger.app._update_instance = CoroutineMock()
        dagger.service.services.Dagger.app._store.get_trigger = CoroutineMock()
        dagger.service.services.Dagger.app._store.insert_trigger = CoroutineMock()
        dagger.service.services.Dagger.app._store.process_trigger_task_complete = (
            CoroutineMock()
        )
        assert trigger_fixture.get_id() == trigger_fixture.id
        await trigger_fixture.start(workflow_instance=workflow_instance_fixture)
        assert dagger.service.services.Dagger.app._update_instance.called
        assert trigger_fixture.status.code == TaskStatusEnum.COMPLETED.name
        assert (
            dagger.service.services.Dagger.app._store.process_trigger_task_complete.called
        )

    @pytest.mark.asyncio
    async def test_future_interval_fixture(
        self, interval_fixture, workflow_instance_fixture
    ):
        assert interval_fixture.status.code == TaskStatusEnum.NOT_STARTED.name
        interval_fixture.time_to_execute = int(time.time()) + 1000
        dagger.service.services.Dagger.app._update_instance = CoroutineMock()
        assert interval_fixture.get_id() == interval_fixture.id
        await interval_fixture.start(workflow_instance=workflow_instance_fixture)
        assert dagger.service.services.Dagger.app._update_instance.called
        assert interval_fixture.status.code == TaskStatusEnum.EXECUTING.name

    @pytest.mark.asyncio
    async def test_current_interval_fixture(
        self, interval_fixture, workflow_instance_fixture
    ):
        assert interval_fixture.status.code == TaskStatusEnum.NOT_STARTED.name
        interval_fixture.time_to_execute = int(time.time()) - 1
        dagger.service.services.Dagger.app._update_instance = CoroutineMock()
        dagger.service.services.Dagger.app._store.get_trigger = CoroutineMock()
        dagger.service.services.Dagger.app._store.insert_trigger = CoroutineMock()
        assert interval_fixture.get_id() == interval_fixture.id
        await interval_fixture.start(workflow_instance=workflow_instance_fixture)
        assert dagger.service.services.Dagger.app._update_instance.called
        assert interval_fixture.status.code == TaskStatusEnum.COMPLETED.name

    @pytest.mark.asyncio
    async def test_systemtimer_trigger_aerospike(
        self, system_timer_fixture, workflow_instance_fixture
    ):
        assert system_timer_fixture.status.code == TaskStatusEnum.NOT_STARTED.name
        mock = asynctest.MagicMock()
        mock.task_ids = ["id1"]
        mock.__aiter__.return_value = [mock]
        dagger.service.services.Dagger.app._store.get_valid_triggers.return_value = mock
        mock_trigger = Trigger({"ID1"}, time.time())
        dagger.service.services.Dagger.app._store.get_trigger = CoroutineMock(
            return_value=mock_trigger
        )
        task_mock = MagicMock()
        task_mock.start = CoroutineMock()
        task_mock.status.code = TaskStatusEnum.EXECUTING.name
        dagger.service.services.Dagger.app._store.execute_system_timer_task = (
            CoroutineMock()
        )
        assert system_timer_fixture.get_id() == system_timer_fixture.id
        await system_timer_fixture.start(workflow_instance=workflow_instance_fixture)
        assert (
            dagger.service.services.Dagger.app._store.execute_system_timer_task.called
        )
        with pytest.raises(NotImplementedError):
            await system_timer_fixture.on_message(workflow_instance_fixture, {})
        with pytest.raises(NotImplementedError):
            await system_timer_fixture.evaluate()
        with pytest.raises(NotImplementedError):
            await system_timer_fixture.get_correlatable_key(MagicMock())
        with pytest.raises(NotImplementedError):
            await system_timer_fixture.on_complete(
                workflow_instance=workflow_instance_fixture
            )

    @pytest.fixture()
    def default_process_instance_fixture(self):
        return DefaultProcessTemplateDAGInstance(uuid1())

    @pytest.mark.asyncio
    async def test_process_instance_task(
        self, default_process_instance_fixture, workflow_instance_fixture
    ):
        default_process_instance_fixture.max_run_duration = 300
        assert (
            default_process_instance_fixture.status.code
            == TaskStatusEnum.NOT_STARTED.name
        )
        dagger.service.services.Dagger.app._update_instance = CoroutineMock()
        mock = asynctest.MagicMock()
        mock.start = CoroutineMock()
        mock.merge_local_runtime_parameters = CoroutineMock()
        dagger.service.services.Dagger.app.get_instance = CoroutineMock(
            return_value=mock
        )
        dagger.service.services.Dagger.app._store_task_instance = CoroutineMock()
        dagger.service.services.Dagger.app._store_trigger_instance = CoroutineMock()
        await default_process_instance_fixture.start(
            workflow_instance=workflow_instance_fixture
        )
        assert (
            default_process_instance_fixture.status.code
            == TaskStatusEnum.EXECUTING.name
        )
        default_process_instance_fixture.on_complete = CoroutineMock()
        await default_process_instance_fixture.notify(
            status=TaskStatus(
                code=TaskStatusEnum.COMPLETED.name, value=TaskStatusEnum.COMPLETED.value
            ),
            workflow_instance=workflow_instance_fixture,
        )
        assert default_process_instance_fixture.on_complete.called
        assert dagger.service.services.Dagger.app._store_trigger_instance.called
        await default_process_instance_fixture.stop()
        with pytest.raises(NotImplementedError):
            await default_process_instance_fixture.get_correlatable_key(MagicMock())
        with pytest.raises(NotImplementedError):
            await default_process_instance_fixture.on_message(
                runtime_parameters=workflow_instance_fixture.runtime_parameters
            )
        with pytest.raises(NotImplementedError):
            await default_process_instance_fixture.evaluate()

    @pytest.fixture()
    def default_template_instance_fixture(self):
        return DefaultTemplateDAGInstance(CoroutineMock())

    @pytest.mark.asyncio
    async def test_default_template_instance_task(self, workflow_instance_fixture):
        assert workflow_instance_fixture.status.code == TaskStatusEnum.NOT_STARTED.name
        dagger.service.services.Dagger.app._update_instance = CoroutineMock()
        mock = asynctest.MagicMock()
        dagger.service.services.Dagger.app.get_instance.return_value = mock
        mock.start = CoroutineMock()
        mock.merge_local_runtime_parameters = CoroutineMock()
        await workflow_instance_fixture.start(
            workflow_instance=workflow_instance_fixture
        )
        assert workflow_instance_fixture.status.code == TaskStatusEnum.EXECUTING.name
        workflow_instance_fixture.on_complete = CoroutineMock()
        await workflow_instance_fixture.notify(
            status=TaskStatus(
                code=TaskStatusEnum.COMPLETED.name, value=TaskStatusEnum.COMPLETED.value
            ),
            workflow_instance=workflow_instance_fixture,
        )
        assert workflow_instance_fixture.on_complete.called
        with pytest.raises(NotImplementedError):
            await workflow_instance_fixture.get_correlatable_key(MagicMock())
        with pytest.raises(NotImplementedError):
            await workflow_instance_fixture.on_message(workflow_instance_fixture)
        with pytest.raises(NotImplementedError):
            await workflow_instance_fixture.evaluate()

    @pytest.fixture()
    def default_monitoring_instance_fixture(self):
        return TestDefaultMonitoringClass(uuid1())

    @pytest.fixture()
    def max_run_duration_monitoring_instance_fixture(self):
        return SkipOnMaxDurationTask(CoroutineMock())

    @pytest.mark.asyncio
    async def test_default_monitoring_task_on_complete(self, workflow_instance_fixture):
        dagger.service.services.Dagger.app._remove_root_template_instance = (
            CoroutineMock()
        )
        dagger.service.services.Dagger.app.delete_workflow_on_complete = True
        await workflow_instance_fixture.on_complete(
            workflow_instance=workflow_instance_fixture
        )
        assert dagger.service.services.Dagger.app._remove_root_template_instance.called

    @pytest.mark.asyncio
    async def test_max_run_duration_monitoring_task_on_complete(
        self,
        max_run_duration_monitoring_instance_fixture,
        default_process_instance_fixture,
        workflow_instance_fixture,
    ):
        max_run_duration_monitoring_instance_fixture.get_remaining_tasks = (
            CoroutineMock(return_value=[])
        )
        workflow_instance_fixture.status.code = TaskStatusEnum.EXECUTING.name
        await max_run_duration_monitoring_instance_fixture.process_monitored_task(
            default_process_instance_fixture,
            workflow_instance=workflow_instance_fixture,
        )
        assert max_run_duration_monitoring_instance_fixture.get_remaining_tasks.called

    @pytest.mark.asyncio
    async def test_default_monitoring_task_on_complete_not_called(
        self, default_monitoring_instance_fixture, workflow_instance_fixture
    ):
        monitored_task = MagicMock()
        monitored_task.status.code = TaskStatusEnum.COMPLETED.name
        dagger.service.services.Dagger.app.get_instance = CoroutineMock(
            return_value=monitored_task
        )
        default_monitoring_instance_fixture.process_monitored_task = CoroutineMock()
        await default_monitoring_instance_fixture.execute(
            runtime_parameters=workflow_instance_fixture.runtime_parameters,
            workflow_instance=workflow_instance_fixture,
        )
        assert (
            default_monitoring_instance_fixture.process_monitored_task.called is False
        )

    @pytest.mark.asyncio
    async def test_default_monitoring_task_on_complete_called(
        self,
        default_process_instance_fixture,
        default_monitoring_instance_fixture,
        workflow_instance_fixture,
    ):
        default_monitoring_instance_fixture.monitored_task_id = (
            default_process_instance_fixture.id
        )
        workflow_instance_fixture.add_task(default_process_instance_fixture)

        default_monitoring_instance_fixture.process_monitored_task = CoroutineMock()
        await default_monitoring_instance_fixture.execute(
            runtime_parameters={}, workflow_instance=workflow_instance_fixture
        )
        assert default_monitoring_instance_fixture.process_monitored_task.called

    @pytest.fixture()
    def default_monitored_process_template_instance(self):
        return MonitoredProcessTemplateDAGInstance(CoroutineMock())

    @pytest.mark.asyncio
    async def test_monitored_process_template_instance(
        self, default_monitored_process_template_instance, workflow_instance_fixture
    ):
        workflow_instance_fixture.runtime_parameters = {COMPLETE_BY_KEY: 414}
        dagger.service.services.Dagger.app._store_trigger_instance = CoroutineMock()
        default_monitored_process_template_instance.get_monitoring_task_type = (
            MagicMock(return_value=MagicMock())
        )
        await default_monitored_process_template_instance.execute(
            runtime_parameters=workflow_instance_fixture.runtime_parameters,
            workflow_instance=workflow_instance_fixture,
        )
        assert dagger.service.services.Dagger.app._store_trigger_instance.called is True

    @pytest.mark.asyncio
    async def test_monitored_process_template_instance_not_called(
        self, default_monitored_process_template_instance, workflow_instance_fixture
    ):
        default_monitored_process_template_instance.runtime_parameters = {}
        dagger.service.services.Dagger.app._store_task_instance = CoroutineMock()
        workflow_instance_fixture.runtime_parameters = {COMPLETE_BY_KEY: 414}
        default_monitored_process_template_instance.monitoring_task_id = uuid1()
        await default_monitored_process_template_instance.execute(
            workflow_instance_fixture.runtime_parameters,
            workflow_instance=workflow_instance_fixture,
        )
        assert dagger.service.services.Dagger.app._store_task_instance.called is False

    @pytest.mark.asyncio
    async def test_monitored_process_template_instance_on_complete(
        self, default_monitored_process_template_instance
    ):
        default_monitored_process_template_instance.monitoring_task_id = uuid.uuid1()
        mt = MagicMock()
        mt.on_complete = CoroutineMock()
        dagger.service.services.Dagger.app.get_instance = CoroutineMock(return_value=mt)
        assert mt.on_complete.calleds


class TestDefaultMonitoringClass(DefaultMonitoringTask):
    async def process_monitored_task(
        self, monitored_task: ITask, workflow_instance: ITask
    ) -> None:
        pass


class TestKafkaAgent:
    @pytest.fixture()
    def workflow_instance_fixture(self):
        return DefaultTemplateDAGInstance(uuid.uuid1())

    @pytest.mark.asyncio
    async def test_kafka_agent(self, workflow_instance_fixture):
        app = MagicMock()
        topic = MagicMock()
        topic.get_topic_name = MagicMock(return_value="topic")
        task = KafkaListenerTask(uuid1())
        task._topic = MagicMock()
        task._topic.get_topic_name = MagicMock(return_value="topic")
        task.topic = "topic"
        listener = KafkaListenerTask(uuid1())
        workflow_instance_fixture.add_task(listener)
        workflow_instance_fixture.add_task(task)
        agent = KafkaAgent(app=app, topic=topic, task=listener)

        # Non-executing task
        task.on_message = CoroutineMock(return_value=True)
        task.on_complete = CoroutineMock()
        task.status = TaskStatus(
            code=TaskStatusEnum.NOT_STARTED.name, value=TaskStatusEnum.NOT_STARTED.value
        )
        task.allow_skip_to = True
        root_template_instance = CoroutineMock()
        skipped_task = KafkaListenerTask(uuid1())
        workflow_instance_fixture.add_task(skipped_task)
        skipped_task.on_complete = CoroutineMock()
        skipped_task.status.code = TaskStatusEnum.EXECUTING.name
        workflow_instance_fixture.get_remaining_tasks = CoroutineMock(
            return_value=[skipped_task, task]
        )
        dagger.service.services.Dagger.app.get_root_template_instance = CoroutineMock(
            return_value=root_template_instance
        )
        generator_mock = MagicMock()
        generator_mock.__aiter__.return_value = [(workflow_instance_fixture, task)]
        agent.app._get_tasks_by_correlatable_key.return_value = generator_mock
        listener.get_correlatable_keys_from_payload = CoroutineMock(
            return_value=[("id", "v1"), ("id2", "v2")]
        )
        stream = asynctest.MagicMock()
        stream.__aiter__.return_value = [range(1)]
        await agent.process_event(stream)
        assert task.on_message.called
        assert task.on_complete.called
        assert skipped_task.on_complete.called
        assert agent.app._get_tasks_by_correlatable_key.call_count == 2

        # Executing task but on_message returned False, assert on_complete not called
        task.on_message = CoroutineMock(return_value=False)
        task.on_complete = CoroutineMock()
        task.status.code = TaskStatusEnum.EXECUTING.name
        await agent.process_event(stream)
        assert task.on_message.called
        assert not task.on_complete.called

        # Skipped task with allow_skip_to as true, should process on_message and on_complete
        task.on_message = CoroutineMock(return_value=True)
        task.on_complete = CoroutineMock()
        task.status.code = TaskStatusEnum.SKIPPED.name
        task.allow_skip_to = True
        await agent.process_event(stream)
        assert task.on_message.called
        assert task.on_complete.called

        # Skipped task with allow_skip_to as false, should not process on_message or on_complete
        task.on_message = CoroutineMock(return_value=True)
        task.on_complete = CoroutineMock()
        task.status.code = TaskStatusEnum.SKIPPED.name
        task.allow_skip_to = False
        await agent.process_event(stream)
        assert not task.on_message.called
        assert not task.on_complete.called

        # test completed
        task.on_message = CoroutineMock(return_value=True)
        task.on_complete = CoroutineMock()
        task.start = CoroutineMock()
        task.status.code = TaskStatusEnum.COMPLETED.name
        task.allow_skip_to = False
        task.reprocess_on_message = False
        await agent.process_event(stream)
        assert task.start.called

        # test exactly once
        listener.match_only_one = True

        task1 = KafkaListenerTask(uuid1())
        workflow_instance_fixture.add_task(task1)
        task1._topic = MagicMock()
        task1._topic.get_topic_name = MagicMock(return_value="topic")
        task1.on_message = CoroutineMock(return_value=True)
        task1.on_complete = CoroutineMock()
        task1.status.code = TaskStatusEnum.EXECUTING.name
        task1.topic = "topic"

        task2 = KafkaListenerTask(uuid1())
        workflow_instance_fixture.add_task(task2)
        task2._topic = MagicMock()
        task2.topic = "topic"
        task2._topic.get_topic_name = MagicMock(return_value="topic")
        task2.on_message = CoroutineMock(return_value=True)
        task2.on_complete = CoroutineMock()
        task2.status.code = TaskStatusEnum.EXECUTING.name

        generator_mock2 = MagicMock()
        generator_mock2.__aiter__.return_value = [
            (workflow_instance_fixture, task1),
            (workflow_instance_fixture, task2),
        ]
        agent.app._get_tasks_by_correlatable_key.return_value = generator_mock2

        stream = asynctest.MagicMock()
        stream.__aiter__.return_value = range(1)
        await agent.process_event(stream)
        assert task1.on_message.called
        assert task1.on_complete.called
        assert not task2.on_message.called
