import abc

from mode import Service

from dagger.exceptions.exceptions import TaskInvalidState
from dagger.tasks.task import ITemplateDAGInstance, TaskStatus, TaskStatusEnum


class ExecutorStrategy:
    @abc.abstractmethod
    async def submit(
        self, task_template: ITemplateDAGInstance, *, repartition: bool = True
    ) -> None:  # pragma: no cover
        ...


class SerialExecutorStrategy(ExecutorStrategy):
    app: Service

    def __init__(self, app: Service) -> None:
        super().__init__()
        self.app = app

    async def submit(
        self, task_template: ITemplateDAGInstance, *, repartition: bool = True
    ) -> None:
        if task_template.status.code != TaskStatusEnum.COMPLETED.name:
            task_template.status = TaskStatus(
                code=TaskStatusEnum.SUBMITTED.name, value=TaskStatusEnum.SUBMITTED.value
            )
            if repartition:
                key_value = (
                    task_template.runtime_parameters.get(
                        task_template.partition_key_lookup, None
                    )
                    if task_template
                    and task_template.runtime_parameters
                    and task_template.partition_key_lookup
                    else None
                )
                await self.app.tasks_topic.send(  # type: ignore
                    key=key_value,
                    value=task_template,
                )
            else:
                await self.app._store_and_create_task(task_template)  # type: ignore
        else:
            await task_template.on_complete(workflow_instance=task_template)
            raise TaskInvalidState(f"Task in invalid state {task_template.get_id()}")
