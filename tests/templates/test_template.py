import time
import uuid

import pytest
from asynctest import CoroutineMock, MagicMock

from dagger.exceptions.exceptions import InvalidTaskType, InvalidTriggerTimeForTask
from dagger.tasks.task import (
    DecisionTask,
    DefaultTemplateDAGInstance,
    ExecutorTask,
    IntervalTask,
    KafkaCommandTask,
    KafkaListenerTask,
    ParallelCompositeTask,
    TaskOperator,
    TriggerTask,
)
from dagger.templates.template import (
    DecisionTaskTemplateBuilder,
    DefaultIntervalTaskTemplate,
    DefaultKafkaTaskTemplate,
    DefaultTaskTemplate,
    DefaultTaskTemplateBuilder,
    DefaultTriggerTaskTemplate,
    IntervalTaskTemplateBuilder,
    KafkaCommandTaskTemplateBuilder,
    KafkaListenerTaskTemplateBuilder,
    ParallelCompositeTaskTemplate,
    TriggerTaskTemplateBuilder,
)


class TestDefaultKafkaCommandTaskTemplate:
    @pytest.mark.asyncio
    async def test_template_create(self):
        app = MagicMock()
        topic = MagicMock()
        next_template = DefaultTaskTemplate(
            app=app,
            type=KafkaListenerTask,
            name="task",
            task_dag_template=list(),
            allow_skip_to=False,
        )
        next_template_list = list()
        next_template_list.append(next_template)
        current_template = DefaultKafkaTaskTemplate(
            app=app,
            type=KafkaCommandTask,
            name="kafka_task",
            topic=topic,
            task_dag_templates=next_template_list,
            allow_skip_to=False,
        )
        args = dict()
        args["containerid"] = 123
        parent_id = uuid.uuid1()
        current_id = uuid.uuid1()
        task = await current_template.create_instance(
            id=current_id,
            parent_id=parent_id,
            parent_name="trigger",
            partition_key_lookup="containerid",
            **args,
        )
        assert task.id == current_id
        assert len(task.next_dags) == 1
        assert task.parent_id == parent_id


class TestParallelCompositeTaskTemplate:
    @pytest.fixture()
    def workflow_instance_fixture(self):
        return DefaultTemplateDAGInstance(uuid.uuid1())

    @pytest.mark.asyncio
    async def test_template_create(self, workflow_instance_fixture):
        app = MagicMock()
        next_template = MagicMock()
        next_template.create_instance = CoroutineMock()
        next_template_list = list()
        next_template_list.append(next_template)
        parallel_task_1 = MagicMock()
        parallel_task_1.create_instance = CoroutineMock()
        parallel_task_2 = MagicMock()
        parallel_task_2.create_instance = CoroutineMock()
        parallel_task_templates = list()
        parallel_task_templates.append(parallel_task_1)
        parallel_task_templates.append(parallel_task_2)
        current_template = ParallelCompositeTaskTemplate(
            app=app,
            type=ParallelCompositeTask,
            name="parallel_composite_task",
            task_dag_templates=next_template_list,
            child_task_templates=parallel_task_templates,
            parallel_operator_type=TaskOperator.JOIN_ALL,
            allow_skip_to=False,
        )
        args = dict()
        args["containerid"] = 123
        parent_id = uuid.uuid1()
        current_id = uuid.uuid1()
        task = await current_template.create_instance(
            id=current_id,
            parent_id=parent_id,
            parent_name="trigger",
            partition_key_lookup="containerid",
            workflow_instance=workflow_instance_fixture,
            **args,
        )
        assert task.id == current_id
        assert parallel_task_2.create_instance.called
        assert parallel_task_1.create_instance.called
        assert next_template.create_instance.called
        assert len(task.next_dags) == 1
        assert task.parent_id == parent_id


class TestDefaultTriggerTaskTemplate:
    @pytest.fixture()
    def workflow_instance_fixture(self):
        return DefaultTemplateDAGInstance(uuid.uuid1())

    @pytest.mark.asyncio
    async def test_template_create(self, workflow_instance_fixture):
        app = MagicMock()
        args = dict()
        workflow_instance_fixture.runtime_parameters = {}
        args["containerid"] = 123
        parent_id = uuid.uuid1()
        app._store_trigger_instance = CoroutineMock()
        current_id = uuid.uuid1()
        fail_template = DefaultTriggerTaskTemplate(
            app=app,
            type=TriggerTask,
            name="trigger",
            time_to_execute_key=None,
            task_dag_templates=list(),
            allow_skip_to=False,
        )
        with pytest.raises(InvalidTriggerTimeForTask):
            await fail_template.create_instance(
                id=current_id,
                parent_id=parent_id,
                parent_name="trigger_process",
                partition_key_lookup="containerid",
                workflow_instance=workflow_instance_fixture,
                **args,
            )
        fail_template = DefaultTriggerTaskTemplate(
            app=app,
            type=TriggerTask,
            name="trigger",
            time_to_execute_key="cpt",
            task_dag_templates=list(),
            allow_skip_to=False,
        )
        with pytest.raises(InvalidTriggerTimeForTask):
            await fail_template.create_instance(
                id=current_id,
                parent_id=parent_id,
                parent_name="trigger_process",
                partition_key_lookup="containerid",
                workflow_instance=workflow_instance_fixture,
                **args,
            )
        tte = int(time.time())
        args["cpt"] = tte
        current_template = DefaultTriggerTaskTemplate(
            app=app,
            type=TriggerTask,
            name="trigger",
            time_to_execute_key="cpt",
            task_dag_templates=list(),
            allow_skip_to=False,
        )
        workflow_instance_fixture.runtime_parameters["cpt"] = tte
        task = await current_template.create_instance(
            id=current_id,
            parent_id=parent_id,
            parent_name="trigger_process",
            partition_key_lookup="containerid",
            workflow_instance=workflow_instance_fixture,
            **args,
        )
        assert task.id == current_id
        assert len(task.next_dags) == 0
        assert task.parent_id == parent_id
        assert task.time_to_execute == tte


class TestDefaultIntervalTaskTemplate:
    @pytest.fixture()
    def workflow_instance_fixture(self):
        return DefaultTemplateDAGInstance(uuid.uuid1())

    @pytest.mark.asyncio
    async def test_template_create(self, workflow_instance_fixture):
        app = MagicMock()
        app.tasks_topic.send = CoroutineMock()
        app._store_trigger_instance = CoroutineMock()
        args = dict()
        args["containerid"] = 123
        parent_id = uuid.uuid1()
        current_id = uuid.uuid1()
        workflow_instance_fixture.runtime_parameters = {}
        tte = workflow_instance_fixture.runtime_parameters["start"] = int(time.time())
        fail_template = DefaultIntervalTaskTemplate(
            app=app,
            type=IntervalTask,
            name="trigger",
            time_to_execute_key="start",
            time_to_force_complete_key="cpt",
            interval_execute_period_key="interval",
            task_dag_templates=list(),
            allow_skip_to=False,
        )
        with pytest.raises(InvalidTriggerTimeForTask):
            await fail_template.create_instance(
                id=current_id,
                parent_id=parent_id,
                parent_name="trigger_process",
                partition_key_lookup="containerid",
                workflow_instance=workflow_instance_fixture,
                **args,
            )
        args["cpt"] = tte
        workflow_instance_fixture.runtime_parameters["cpt"] = tte
        fail_template = DefaultIntervalTaskTemplate(
            app=app,
            type=IntervalTask,
            name="trigger",
            time_to_execute_key="start",
            time_to_force_complete_key="cpt",
            interval_execute_period_key="interval",
            task_dag_templates=list(),
            allow_skip_to=False,
        )
        with pytest.raises(InvalidTriggerTimeForTask):
            await fail_template.create_instance(
                id=current_id,
                parent_id=parent_id,
                parent_name="trigger_process",
                partition_key_lookup="containerid",
                workflow_instance=workflow_instance_fixture,
                **args,
            )
        args["start"] = int(time.time())
        args["interval"] = 10
        workflow_instance_fixture.runtime_parameters["interval"] = 10
        current_template = DefaultIntervalTaskTemplate(
            app=app,
            type=IntervalTask,
            name="trigger",
            time_to_execute_key="start",
            time_to_force_complete_key="cpt",
            interval_execute_period_key="interval",
            task_dag_templates=list(),
            allow_skip_to=False,
        )
        task = await current_template.create_instance(
            id=current_id,
            parent_id=parent_id,
            parent_name="trigger_process",
            partition_key_lookup="containerid",
            workflow_instance=workflow_instance_fixture,
            **args,
        )
        assert task.id == current_id
        assert len(task.next_dags) == 0
        assert task.parent_id == parent_id
        assert task.time_to_execute == tte
        current_template = DefaultIntervalTaskTemplate(
            app=app,
            type=IntervalTask,
            name="trigger",
            time_to_execute_key=None,
            time_to_force_complete_key="cpt",
            interval_execute_period_key="interval",
            task_dag_templates=list(),
            allow_skip_to=False,
        )
        task = await current_template.create_instance(
            id=current_id,
            parent_id=parent_id,
            parent_name="trigger_process",
            partition_key_lookup="containerid",
            workflow_instance=workflow_instance_fixture,
            **args,
        )
        assert task.id == current_id
        assert len(task.next_dags) == 0
        assert task.parent_id == parent_id
        assert task.time_to_execute is None


class TestDefaultTaskTemplateBuilder:
    @pytest.mark.asyncio
    async def test_template_builder(self):
        app = MagicMock()
        builder = DefaultTaskTemplateBuilder(app)
        with pytest.raises(InvalidTaskType):
            builder.set_type(KafkaCommandTask)
        builder.set_type(ExecutorTask)
        next_template = MagicMock()
        default_template = builder.set_next(task_template=next_template).build()
        assert default_template._type == ExecutorTask
        assert len(default_template.next_task_dag) == 1


class TestKafkaCommandTaskTemplateBuilder:
    @pytest.mark.asyncio
    async def test_kafka_command_template_builder(self):
        app = MagicMock()
        topic = MagicMock()
        builder = KafkaCommandTaskTemplateBuilder(app)
        with pytest.raises(InvalidTaskType):
            builder.set_type(ExecutorTask)
        builder.set_type(KafkaCommandTask)
        builder.set_topic(topic)
        next_template = MagicMock()
        command_template = builder.set_next(task_template=next_template).build()
        assert command_template._type == KafkaCommandTask
        assert len(command_template.next_task_dag) == 1


class TestDecisionTaskTemplateBuilder:
    @pytest.mark.asyncio
    async def test_decision_template_builder(self):
        app = MagicMock()
        builder = DecisionTaskTemplateBuilder(app)
        with pytest.raises(InvalidTaskType):
            builder.set_type(ExecutorTask)
        builder.set_type(DecisionTask)
        next_template = MagicMock()
        decision_template = builder.set_next(task_template=next_template).build()
        assert decision_template._type == DecisionTask
        assert len(decision_template.next_task_dag) == 1


class TestTriggerTaskTemplateBuilder:
    @pytest.mark.asyncio
    async def test_trigger_template_builder(self):
        app = MagicMock()
        builder = TriggerTaskTemplateBuilder(app)
        with pytest.raises(InvalidTaskType):
            builder.set_type(ExecutorTask)
        builder.set_type(TriggerTask)
        next_template = MagicMock()
        trigger_template = (
            builder.set_next(task_template=next_template)
            .set_time_to_execute_lookup_key("cpt")
            .build()
        )
        assert trigger_template._type == TriggerTask
        assert len(trigger_template.next_task_dag) == 1


class TestIntervalTaskTemplateBuilder:
    @pytest.mark.asyncio
    async def test_interval_template_builder(self):
        app = MagicMock()
        builder = IntervalTaskTemplateBuilder(app)
        with pytest.raises(InvalidTaskType):
            builder.set_type(ExecutorTask)
        builder.set_type(IntervalTask)
        next_template = MagicMock()
        builder.set_time_to_execute_lookup_key("cpt")
        builder.set_time_to_force_complete_lookup_key("start")
        builder.set_interval_execute_period_lookup_key("interval")
        builder.set_next(task_template=next_template)
        interval_template = builder.build()
        assert interval_template._type == IntervalTask
        assert len(interval_template.next_task_dag) == 1


class TestKafkaListenerTaskTemplateBuilder:
    @pytest.mark.asyncio
    async def test_kafka_listener_template_builder(self):
        app = MagicMock()
        app.faust_app.agent = MagicMock()
        topic = MagicMock()
        builder = KafkaListenerTaskTemplateBuilder(app)
        with pytest.raises(InvalidTaskType):
            builder.set_type(ExecutorTask)
        builder.set_type(KafkaListenerTask)
        builder.set_topic(topic)
        builder.set_concurrency(concurrency=2)
        next_template = MagicMock()
        listener_template = builder.set_next(task_template=next_template).build()
        assert listener_template._type == KafkaListenerTask
        assert len(listener_template.next_task_dag) == 1
        assert app.faust_app.agent.called
