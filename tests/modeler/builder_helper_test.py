from unittest.mock import MagicMock

import pytest

from dagger.modeler.builder_helper import DAGBuilderHelper
from dagger.service.services import Dagger
from dagger.tasks.task import KafkaCommandTask, KafkaListenerTask, SystemTimerTask


class TestDAGBuilderHelper:
    @pytest.fixture()
    def builder_fixture(self):
        return DAGBuilderHelper(MagicMock())

    @pytest.mark.asyncio
    async def test_build_and_link_tasks(self, builder_fixture):

        builder1 = MagicMock()
        builder2 = MagicMock()
        mock_task_template2 = MagicMock()
        builder2.build = MagicMock(return_value=mock_task_template2)
        mock_task_template1 = MagicMock()
        builder1.build = MagicMock(return_value=mock_task_template1)
        tasks_builder_list = [builder1, builder2]
        assert (
            builder_fixture.build_and_link_tasks(tasks_builder_list=tasks_builder_list)
            == mock_task_template1
        )

    @pytest.mark.asyncio
    async def test_generic_process_builder(self, builder_fixture):
        root_task = MagicMock()
        assert (
            builder_fixture.generic_process_builder(
                process_name="test", root_task=root_task
            )
            is not None
        )

    @pytest.mark.asyncio
    async def test_generic_dynamic_process_builder(self, builder_fixture):
        assert builder_fixture.generic_dynamic_process_builder(name="test") is not None

    @pytest.mark.asyncio
    async def test_generic_executor_task_builder(self, builder_fixture):
        assert (
            builder_fixture.generic_executor_task_builder(
                task_type=SystemTimerTask, name="test"
            )
            is not None
        )

    @pytest.mark.asyncio
    async def test_generic_command_task_builder(self, builder_fixture):
        Dagger.app = MagicMock()
        Dagger.app.topics = {"test_topic": MagicMock()}
        assert (
            builder_fixture.generic_command_task_builder(
                topic="test_topic", task_type=KafkaCommandTask, process_name="test"
            )
            is not None
        )

    @pytest.mark.asyncio
    async def test_generic_listener_task_builder(self, builder_fixture):
        Dagger.app = MagicMock()
        Dagger.app.topics = {"test_topic": MagicMock()}
        assert (
            builder_fixture.generic_listener_task_builder(
                topic="test_topic", task_type=KafkaListenerTask, process_name="test"
            )
            is not None
        )

    @pytest.mark.asyncio
    async def test_build_and_link_processes(self, builder_fixture):
        builder1 = MagicMock()
        builder2 = MagicMock()
        mock_process_task_template2 = MagicMock()
        builder2.build = MagicMock(return_value=mock_process_task_template2)
        mock_process_task_template1 = MagicMock()
        builder1.build = MagicMock(return_value=mock_process_task_template1)
        tasks_builder_list = [builder1, builder2]
        assert (
            builder_fixture.build_and_link_processes(
                process_builder_list=tasks_builder_list
            )
            == mock_process_task_template1
        )

    @pytest.mark.asyncio
    async def test_generic_template(self, builder_fixture):
        Dagger.app = MagicMock()
        Dagger.app.topics = {"test_topic": MagicMock()}
        assert (
            builder_fixture.generic_template(
                template_name="test", root_process=MagicMock()
            )
            is not None
        )
