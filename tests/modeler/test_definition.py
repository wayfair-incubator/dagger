import uuid

import pytest
from asynctest import CoroutineMock, MagicMock

from dagger.modeler.definition import (
    DefaultTemplateBuilder,
    DynamicProcessTemplateDAG,
    DynamicProcessTemplateDagBuilder,
    ParallelCompositeProcessTemplateDAG,
    ParallelCompositeProcessTemplateDagBuilder,
    ProcessTemplateDAG,
    ProcessTemplateDagBuilder,
    TemplateDAG,
)
from dagger.tasks.task import (
    DefaultProcessTemplateDAGInstance,
    DefaultTemplateDAGInstance,
    ParallelCompositeTask,
    TaskOperator,
    TaskStatusEnum,
    TaskType,
)
from dagger.templates.template import (
    ParallelCompositeTaskTemplate,
    ParallelCompositeTaskTemplateBuilder,
)


class TestDefaultTemplateBuilder:
    @pytest.fixture()
    def template_fixture(self):
        return DefaultTemplateBuilder(MagicMock())

    @pytest.mark.asyncio
    async def test_builder(self, template_fixture):
        template_fixture.set_type(DefaultTemplateDAGInstance)
        template_fixture.set_name("TEST")
        mock_process_template_dag = MagicMock()
        template_fixture.set_root(mock_process_template_dag)
        instance = template_fixture.build()
        assert instance.name == "TEST"
        assert instance.root_process_dag == mock_process_template_dag
        assert instance.template_type == DefaultTemplateDAGInstance


class TestProcessTemplateDagBuilder:
    @pytest.fixture()
    def template_fixture(self):
        return ProcessTemplateDagBuilder(MagicMock())

    @pytest.mark.asyncio
    async def test_builder(self, template_fixture):
        template_fixture.set_type(ProcessTemplateDAG)
        template_fixture.set_max_run_duration(300)
        template_fixture.set_name("TEST")
        mock_process_template_dag = MagicMock()
        next_process_dag = MagicMock()
        template_fixture.set_root_task(mock_process_template_dag)
        template_fixture.set_next_process(next_process_dag)
        instance = template_fixture.build()
        assert instance.name == "TEST"
        assert instance.root_task_dag == mock_process_template_dag
        assert instance.max_run_duration == 300
        assert instance.process_type == ProcessTemplateDAG
        assert len(instance.next_process_dag) == 1
        assert instance.next_process_dag[0] == next_process_dag


class TestProcessTemplateDAG:
    @pytest.fixture()
    def template_fixture(self):
        app = MagicMock()
        app._store_process_instance = CoroutineMock()
        app.tasks_topic.send = CoroutineMock()
        next_template_dag = MagicMock()
        next_template_dag_return_instance = MagicMock()
        next_template_dag_return_instance.id = "next_id"
        next_template_dag.create_instance = CoroutineMock(
            return_value=next_template_dag_return_instance
        )
        list = [next_template_dag]
        root_task_dag = MagicMock()
        root_task_dag_create_instance = MagicMock()
        root_task_dag_create_instance.id = "root_id"
        root_task_dag.create_instance = CoroutineMock(
            return_value=root_task_dag_create_instance
        )
        template = ProcessTemplateDAG(
            next_process_dag=list,
            app=app,
            name="TESTP1",
            process_type=DefaultProcessTemplateDAGInstance,
            root_task_dag=root_task_dag,
            max_run_duration=0,
        )
        return template

    @pytest.mark.asyncio
    async def test_template_create(self, template_fixture):

        instance = await template_fixture.create_instance(
            uuid.uuid1(),
            uuid.uuid1(),
            parent_name="template",
            partition_key_lookup="warehouse_id",
            warehouse_id="05",
            container="c1",
        )
        assert instance.process_name == "TESTP1"
        assert instance.task_type == TaskType.SUB_DAG.name
        assert instance.root_dag == "root_id"
        assert instance.next_dags[0] == "next_id"
        assert instance.status.code == TaskStatusEnum.NOT_STARTED.name
        assert instance.task_type == TaskType.SUB_DAG.name


class TestDynamicProcessTemplateDAG:
    @pytest.fixture()
    async def dynamic_template_fixture(self):
        app = MagicMock()
        app._store_process_instance = CoroutineMock()
        app.tasks_topic.send = CoroutineMock()
        terminal_template_dag = MagicMock()
        next_template_dag_return_instance = MagicMock()
        next_template_dag_return_instance.id = "D"
        terminal_template_dag.create_instance = CoroutineMock(
            return_value=next_template_dag_return_instance
        )
        list = [terminal_template_dag]

        root_task_dag = MagicMock()
        root_task_dag_create_instance = MagicMock()
        root_task_dag_create_instance.id = "root_id"
        root_task_dag.create_instance = CoroutineMock(
            return_value=root_task_dag_create_instance
        )
        template = DynamicProcessTemplateDAG(
            next_process_dag=list, app=app, name="dynamic", max_run_duration=0
        )
        return template

    @pytest.fixture()
    def template_fixture(self, dynamic_template_fixture):
        app = MagicMock()
        app._store_process_instance = CoroutineMock()
        process_dag = MagicMock()
        process_dag.id = "p1"
        app.tasks_topic.send = CoroutineMock()
        template = TemplateDAG(
            dag=dynamic_template_fixture,
            app=app,
            name="TEST1",
            template_type=DefaultTemplateDAGInstance,
        )
        return template

    @pytest.mark.asyncio
    async def test_template_create(
        self, dynamic_template_fixture, template_fixture: TemplateDAG
    ):
        process_A_template_dag_builder = MagicMock()
        process_A_template_dag_builder.set_next_process = MagicMock(
            return_value=process_A_template_dag_builder
        )
        process_A_template_dag = MagicMock()
        process_A_dag_instance = MagicMock()
        process_A_dag_instance.id = "A"
        process_A_template_dag.create_instance = CoroutineMock(
            return_value=process_A_dag_instance
        )
        process_A_template_dag_builder.build = MagicMock(
            return_value=process_A_template_dag
        )

        process_B_template_dag_builder = MagicMock()
        process_B_template_dag_builder.set_next_process = MagicMock(
            return_value=process_B_template_dag_builder
        )
        process_B_template_dag = MagicMock()
        process_B_dag_instance = MagicMock()
        process_B_dag_instance.id = "B"
        process_B_template_dag.create_instance = CoroutineMock(
            return_value=process_B_dag_instance
        )
        process_B_template_dag_builder.build = MagicMock(
            return_value=process_B_template_dag
        )

        process_C_template_dag_builder = MagicMock()
        process_C_template_dag_builder.set_next_process = MagicMock(
            return_value=process_C_template_dag_builder
        )
        process_C_template_dag = MagicMock()
        process_C_dag_instance = MagicMock()
        process_C_dag_instance.id = "C"
        process_C_template_dag.create_instance = CoroutineMock(
            return_value=process_C_dag_instance
        )
        process_C_template_dag_builder.build = MagicMock(
            return_value=process_C_template_dag
        )
        template_fixture.set_dynamic_builders_for_process_template(
            name="dynamic",
            process_template_builders=[
                process_A_template_dag_builder,
                process_B_template_dag_builder,
                process_C_template_dag_builder,
            ],
        )
        instance = await dynamic_template_fixture.create_instance(
            uuid.uuid1(),
            uuid.uuid1(),
            parent_name="template",
            partition_key_lookup="warehouse_id",
            warehouse_id="05",
            container="c1",
        )
        assert instance.id == process_A_dag_instance.id

    @pytest.mark.asyncio
    async def test_template_create_terminal(
        self, dynamic_template_fixture, template_fixture
    ):
        template_fixture.set_dynamic_builders_for_process_template(
            name="dynamic", process_template_builders=[]
        )
        instance = await dynamic_template_fixture.create_instance(
            uuid.uuid1(),
            uuid.uuid1(),
            parent_name="template",
            partition_key_lookup="warehouse_id",
            warehouse_id="05",
            container="c1",
        )
        assert instance.id == "D"


class TestParallelCompositeProcessTemplateDAG:
    @pytest.fixture()
    def dynamic_parallel_composite_fixture(self):
        app = MagicMock()
        app._store_process_instance = CoroutineMock()
        app.tasks_topic.send = CoroutineMock()
        next_process_template = MagicMock()
        next_template_dag_return_instance = MagicMock()
        next_template_dag_return_instance.id = "np"
        next_process_template.create_instance = CoroutineMock(
            return_value=next_template_dag_return_instance
        )

        child_1_task = MagicMock()
        child_1_task.id = "c1"
        child_2_task = MagicMock()
        child_2_task.id = "c2"
        child_1_template = MagicMock()
        child_2_template = MagicMock()
        child_1_template.create_instance = CoroutineMock(return_value=child_1_task)
        child_2_template.create_instance = CoroutineMock(return_value=child_2_task)

        template = ParallelCompositeProcessTemplateDAG(
            next_process_dag=[next_process_template],
            app=app,
            name="parallel",
            process_type=ParallelCompositeTask,
            child_process_task_templates=[child_1_template, child_2_template],
            parallel_operator_type=TaskOperator.JOIN_ALL,
        )
        return template

    @pytest.mark.asyncio
    async def test_template_create(
        self, dynamic_parallel_composite_fixture: ParallelCompositeProcessTemplateDAG
    ):

        instance = await dynamic_parallel_composite_fixture.create_instance(
            uuid.uuid1(),
            uuid.uuid1(),
            parent_name="template",
            partition_key_lookup="warehouse_id",
            warehouse_id="05",
            container="c1",
        )
        assert instance.next_dags == ["np"]
        assert instance.parallel_child_task_list == ["c1", "c2"]


class TestDynamicProcessTemplateDagBuilder:
    @pytest.mark.asyncio
    async def test_dynamic_builder(self):
        next_pd = MagicMock()
        builder = DynamicProcessTemplateDagBuilder(MagicMock())
        builder.set_next_process(next_pd)
        builder.set_name("dynamic")
        builder.set_max_run_duration(10)
        template: DynamicProcessTemplateDAG = builder.build()
        template.set_dynamic_process_builders([])
        assert template.next_process_dag == [next_pd]
        assert template.name == "dynamic"
        with pytest.raises(NotImplementedError):
            builder.set_type(MagicMock())
        with pytest.raises(NotImplementedError):
            builder.set_root_task(MagicMock())


class TestParallelCompositeProcessTemplateDagBuilder:
    @pytest.mark.asyncio
    async def test_parallel_composite_process_builder(self):
        next_pd = MagicMock()
        builder = ParallelCompositeProcessTemplateDagBuilder(MagicMock())
        builder.set_next_process(next_pd)
        builder.set_name("parallel")
        parallel_task_template = MagicMock()
        builder.set_parallel_process_templates(parallel_task_template)
        builder.set_type(ParallelCompositeTask)
        builder.set_parallel_operator_type(TaskOperator.JOIN_ALL)
        template: ParallelCompositeProcessTemplateDAG = builder.build()
        assert template.next_process_dag == [next_pd]
        assert template.name == "parallel"
        with pytest.raises(NotImplementedError):
            builder.set_max_run_duration(MagicMock())
        with pytest.raises(NotImplementedError):
            builder.set_root_task(MagicMock())


class TestParallelCompositeTemplateDagBuilder:
    @pytest.mark.asyncio
    async def test_parallel_composite_dynamic_builder(self):
        next_pd = MagicMock()
        task_template = MagicMock()
        no_of_parallel_tasks = 2

        builder: ParallelCompositeTaskTemplateBuilder = (
            ParallelCompositeTaskTemplateBuilder(
                MagicMock(), no_of_parallel_tasks, task_template
            )
        )
        builder.set_next(next_pd)
        builder.set_name("parallel")
        parallel_task_template = MagicMock()
        builder.add_parallel_task(parallel_task_template)
        builder.set_type(ParallelCompositeTask)
        builder.set_task_operator(TaskOperator.JOIN_ALL)
        template: ParallelCompositeTaskTemplate = builder.build()
        assert template.next_task_dag == [next_pd]
        assert template.name == "parallel"
        assert template.child_task_templates == [
            task_template,
            task_template,
            parallel_task_template,
        ]
        assert template.parallel_operator_type == TaskOperator.JOIN_ALL


class TestTemplateDAG:
    @pytest.fixture()
    def template_fixture(self):
        app = MagicMock()
        app._store_process_instance = CoroutineMock()
        process_dag_template = MagicMock()
        process_dag = MagicMock()
        process_dag.id = "p1"
        process_dag_template.create_instance = CoroutineMock(return_value=process_dag)
        app.tasks_topic.send = CoroutineMock()
        template = TemplateDAG(
            dag=process_dag_template,
            app=app,
            name="TEST1",
            template_type=DefaultTemplateDAGInstance,
        )
        return template

    @pytest.mark.asyncio
    async def test_template_create(self, template_fixture: TemplateDAG):
        instance = await template_fixture.create_instance(
            uuid.uuid1(),
            partition_key_lookup="warehouse_id",
            warehouse_id="05",
            container="c1",
        )
        assert instance.template_name == "TEST1"
        assert instance.task_type == TaskType.ROOT.name
        assert instance.root_dag == "p1"
        assert instance.status.code == TaskStatusEnum.NOT_STARTED.name
