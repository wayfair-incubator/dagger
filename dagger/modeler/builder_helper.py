import logging
from typing import List, Optional, Type, Union

from faust.types.codecs import CodecArg
from faust.types.models import ModelArg

import dagger.service.services
from dagger.modeler.definition import (  # type: ignore
    DefaultTemplateBuilder,
    DynamicProcessTemplateDagBuilder,
    ProcessTemplateDagBuilder,
)
from dagger.service.services import Dagger
from dagger.tasks.task import (  # type: ignore
    DefaultProcessTemplateDAGInstance,
    DefaultTemplateDAGInstance,
    ITask,
    ITemplateDAGInstance,
)
from dagger.templates.template import (  # type: ignore
    DefaultTaskTemplateBuilder,
    IProcessTemplateDAG,
    IProcessTemplateDAGBuilder,
    IProcessTemplateDAGInstance,
    ITemplateDAG,
    KafkaCommandTaskTemplateBuilder,
    KafkaListenerTaskTemplateBuilder,
    TaskTemplate,
    TaskTemplateBuilder,
)

logger = logging.getLogger(__name__)


class DAGBuilderHelper:
    """Helper class to Model and connect Task Definition(s) for DAG's"""

    app: Dagger

    def __init__(self, dagger_app: Dagger) -> None:
        """
        init method
        :param dagger_app: The Dagger app
        """
        self.app = dagger_app

    def build_and_link_tasks(
        self, tasks_builder_list: List[TaskTemplateBuilder]
    ) -> TaskTemplate:
        """
        Link the tasks definition's together to define the order of tasks to execute
        :param tasks_builder_list: The list of TaskTemplatesBuilders to link togehter defining the chain of tasks
        :return: An instance of the TaskTemplate
        """
        next_task = tasks_builder_list[-1].build()
        if len(tasks_builder_list) > 1:
            for task_builder in reversed(tasks_builder_list[:-1]):
                if next_task:
                    task_builder.set_next(next_task)
                    next_task = task_builder.build()
        return next_task

    def generic_process_builder(
        self,
        *,
        process_name: str,
        root_task: TaskTemplate,
        process_type: Type[
            IProcessTemplateDAGInstance
        ] = DefaultProcessTemplateDAGInstance,
        max_run_duration: int = 0,
    ) -> ProcessTemplateDagBuilder:
        """
        Helper function to build a Process Definition
        :param process_name: the Name of the process example 'ORDERS'
        :param root_task: The first task to execute in this process
        :param process_type: The class type of the Process
        :param max_run_duration: the timeout on the process
        :return: The ProcessTemplateDagBuilder
        """
        process_builder = ProcessTemplateDagBuilder(self.app)
        process_builder.set_name(process_name)
        process_builder.set_root_task(root_task)
        process_builder.set_type(process_type)
        process_builder.set_max_run_duration(max_run_duration)
        return process_builder

    def generic_dynamic_process_builder(
        self, *, name: str, max_run_duration: int = 0
    ) -> DynamicProcessTemplateDagBuilder:
        """
        Helper function to build a dynamic process, to be determined at runtime based on the runtime parameters
        :param name: The name of the dynamic process
        :param max_run_duration: The timeout on the process
        :return: The DynamicProcessTemplateDagBuilder
        """
        process_builder = DynamicProcessTemplateDagBuilder(self.app)
        process_builder.set_name(name)
        process_builder.set_max_run_duration(max_run_duration)
        return process_builder

    def generic_executor_task_builder(
        self, *, task_type: Type[ITask], name: str, allow_skip_to: bool = False
    ) -> DefaultTaskTemplateBuilder:
        """
        Helper Function to define an executor task
        :param task_type: The type of the Task
        :param name: the Name of the Task
        :param allow_skip_to: If true, the execution of the DAG can skip execution to this task out of order
        :return: The DefaultTaskTemplateBuilder instance
        """
        executor_builder = DefaultTaskTemplateBuilder(self.app)
        executor_builder.set_name(f"{name}_executor_task")
        executor_builder.set_type(task_type)
        executor_builder.set_allow_skip_to(allow_skip_to)
        return executor_builder

    def generic_command_task_builder(
        self,
        *,
        topic: str,
        task_type: Type[ITask],
        process_name: str,
        key_type: Optional[ModelArg] = None,
        value_type: Optional[ModelArg] = None,
        key_serializer: CodecArg = None,
        value_serializer: CodecArg = None,
    ) -> KafkaCommandTaskTemplateBuilder:
        """
        Helper function to define the KafkCommandTask definition
        :param topic: The name of the topic to write the command to
        :param task_type: The type of KafkaCommandTask
        :param process_name: the name of the process it belongs to
        :return: the instance of KafkaCommandTaskTemplateBuilder
        """
        command_topic = dagger.service.services.Dagger.create_topic(
            topic,
            key_type=key_type,
            value_type=value_type,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
        )
        command_task_builder = KafkaCommandTaskTemplateBuilder(self.app)
        command_task_builder.set_topic(command_topic)  # type: ignore
        command_task_builder.set_type(task_type)
        command_task_builder.set_name(f"{process_name}_command_task")
        return command_task_builder

    def generic_listener_task_builder(
        self,
        *,
        topic: str,
        task_type: Type[ITask],
        process_name: str,
        allow_skip_to: bool = False,
        concurrency: int = 1,
        reprocess_on_message: bool = False,
        key_type: Optional[ModelArg] = None,
        value_type: Optional[ModelArg] = None,
        key_serializer: CodecArg = None,
        value_serializer: CodecArg = None,
    ) -> KafkaListenerTaskTemplateBuilder:
        """
        Helper function to define a KafkaListenerTaskTemplateBuilder
        :param topic: the topic name the listener listens to
        :param task_type: The type of KafkaListenerTask
        :param process_name: the name of the parent process this task belongs tp
        :param allow_skip_to: If true, the execution of the DAG can skip execution to this task out of order
        :param concurrency: Check [Concurrency](https://faust-streaming.github.io/faust/userguide/agents.html#concurrency)
        :param reprocess_on_message: Re-executes the task when the message is received irrespective of the state of the task
        :return: The instance of KafkaListenerTaskTemplateBuilder
        """
        listener_topic = dagger.service.services.Dagger.create_topic(
            topic,
            key_type=key_type,
            value_type=value_type,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
        )
        listener_task_builder = KafkaListenerTaskTemplateBuilder(self.app)
        listener_task_builder.set_topic(listener_topic)  # type: ignore
        listener_task_builder.set_concurrency(concurrency=concurrency)
        listener_task_builder.set_type(task_type)
        listener_task_builder.set_name(f"{process_name}_listener_task")
        listener_task_builder.set_allow_skip_to(allow_skip_to)
        listener_task_builder.set_reprocess_on_message(reprocess_on_message)
        return listener_task_builder

    def build_and_link_processes(
        self,
        process_builder_list: List[
            Union[ProcessTemplateDagBuilder, IProcessTemplateDAGBuilder]
        ],
    ) -> IProcessTemplateDAG:
        """
        Helper function to link processes together in a DAG definition
        :param process_builder_list: The list of process definitions to link together
        :return: The instance of IProcessTemplateDAG
        """
        next_process = process_builder_list[-1].build()
        if len(process_builder_list) > 1:
            for process_builder in reversed(process_builder_list[:-1]):
                if next_process:
                    process_builder.set_next_process(next_process)
                    next_process = process_builder.build()
        return next_process

    def generic_template(
        self,
        *,
        template_name: str,
        root_process: IProcessTemplateDAG,
        template_type: Type[ITemplateDAGInstance] = DefaultTemplateDAGInstance,
    ) -> ITemplateDAG:
        """
        Helper function to define the Workflow Template
        :param template_name: The name of the workflow
        :param root_process: The first process definition to execute in the workflow
        :param template_type: The tye of ITemplateDAGInstance
        :return: The instance of ITemplateDAG
        """
        default_template_builder = DefaultTemplateBuilder(self.app)
        default_template_builder.set_name(template_name)
        default_template_builder.set_type(template_type)
        default_template_builder.set_root(root_process)
        return default_template_builder.build()
