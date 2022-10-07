from unittest.mock import MagicMock

import asynctest
import pytest

from dagger.exceptions.exceptions import TaskInvalidState
from dagger.executor.executor import SerialExecutorStrategy
from dagger.tasks.task import TaskStatusEnum


class TestSerialExecutorStrategy:
    @pytest.fixture()
    def task_fixture(self):
        app = MagicMock()
        app.tasks_topic.send = asynctest.CoroutineMock()
        return SerialExecutorStrategy(app)

    @pytest.mark.asyncio
    async def test_serial_execution_not_started(self, task_fixture):
        task = MagicMock()
        task.start = asynctest.CoroutineMock()
        task.status.code = TaskStatusEnum.NOT_STARTED.name
        await task_fixture.submit(task)
        task_fixture.app.tasks_topic.send.assert_called()

    @pytest.mark.asyncio
    async def test_serial_execution_completed(self, task_fixture):
        task = MagicMock()
        task.start = asynctest.CoroutineMock()
        task.on_complete = asynctest.CoroutineMock()
        task.status.code = TaskStatusEnum.COMPLETED.name
        with pytest.raises(TaskInvalidState):
            await task_fixture.submit(task)
