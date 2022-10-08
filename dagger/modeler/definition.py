from __future__ import annotations

import logging
import random
import time
import uuid
from typing import Any, List, Optional, Type
from uuid import UUID, uuid1

from dagger.service.services import Dagger
from dagger.tasks.task import (KT, VT, IProcessTemplateDAGInstance, ITask,
                               ITemplateDAGInstance, ParallelCompositeTask,
                               TaskOperator, TaskStatus, TaskStatusEnum,
                               TaskType)
from dagger.templates.template import (IDynamicProcessTemplateDAG,
                                       IProcessTemplateDAG,
                                       IProcessTemplateDAGBuilder,
                                       ITemplateDAG, ITemplateDAGBuilder,
                                       TaskTemplate)
from dagger.utils.utils import DAGIDGenerator

logger = logging.getLogger(__name__)


class DefaultTemplateBuilder(ITemplateDAGBuilder):
    """Default implementation of template builder"""

    __name: str
    root_process_dag: IProcessTemplateDAG
    __template_type: Type[IProcessTemplateDAGInstance]

    def __init__(self, app: Dagger) -> None:
        super().__init__(app)

    def set_type(
        self, template_type: Type[ITemplateDAGInstance]
    ) -> ITemplateDAGBuilder:
        self.__template_type = template_type
        return self

    def set_root(self, template: IProcessTemplateDAG) -> ITemplateDAGBuilder:
        self.root_process_dag = template
        return self

    def build(self) -> ITemplateDAG:
        return TemplateDAG(
            dag=self.root_process_dag,
            name=self.__name,
            app=self.app,  # type: ignore
            template_type=self.__template_type,
        )

    def set_name(self, name: str) -> ITemplateDAGBuilder:
        self.__name = name
        return self


class ProcessTemplateDAG(IProcessTemplateDAG):
    def __init__(
        self,
        next_process_dag: List[IProcessTemplateDAG],
        app: Dagger,
        name: str,
        process_type: Type[ITask[KT, VT]],
        root_task_dag: TaskTemplate,
        max_run_duration: int,
    ) -> None:
        super().__init__(
            next_process_dag=next_process_dag,
            app=app,
            name=name,
            process_type=process_type,
            root_task_dag=root_task_dag,
            max_run_duration=max_run_duration,
        )

    async def create_instance(
        self,
        id: UUID,
        parent_id: UUID,
        parent_name: str,
        partition_key_lookup: str,
        *,
        repartition: bool = True,
        seed: random.Random = None,
        workflow_instance: ITemplateDAGInstance = None,
        **kwargs: Any,
    ) -> IProcessTemplateDAGInstance[KT, VT]:
        # create the instance in the graph store
        process_instance = self.process_type(
            id=id, process_name=self.name, parent_id=parent_id
        )
        process_instance.task_type = TaskType.SUB_DAG.name
        process_instance.max_run_duration = self.max_run_duration
        process_instance.time_created = int(time.time())
        process_instance.status = TaskStatus(
            code=TaskStatusEnum.NOT_STARTED.name, value=TaskStatusEnum.NOT_STARTED.value
        )
        process_instance.task_name = f"{parent_name}.{self.name}"
        process_instance.next_dags = list()
        dag_id = DAGIDGenerator.generate_dag_id_from_seed(seed) if seed else uuid1()
        if workflow_instance:
            workflow_instance.add_task(task=process_instance)
        task = await self.root_task_dag.create_instance(
            id=dag_id,
            parent_id=process_instance.id,
            parent_name=process_instance.task_name,
            partition_key_lookup=partition_key_lookup,
            repartition=repartition,
            seed=seed,
            workflow_instance=workflow_instance,
            **kwargs,
        )
        process_instance.root_dag = task.id
        for process_task_template in self.next_process_dag:
            dag_id = DAGIDGenerator.generate_dag_id_from_seed(seed) if seed else uuid1()
            next_process_instance = await process_task_template.create_instance(
                id=dag_id,
                parent_id=parent_id,
                parent_name=parent_name,
                partition_key_lookup=partition_key_lookup,
                repartition=repartition,
                seed=seed,
                workflow_instance=workflow_instance,
                **kwargs,
            )
            process_instance.next_dags.append(next_process_instance.id)
        return process_instance

    def set_dynamic_process_builders(
        self, process_template_builders: List[ProcessTemplateDagBuilder]
    ) -> None:  # pragma: no cover
        raise NotImplementedError("ProcessTemplateDAG does not implement this method")

    def set_parallel_process_template_dags(
        self, parallel_process_templates: List[IProcessTemplateDAG]
    ) -> None:  # pragma: no cover
        raise NotImplementedError("ProcessTemplateDAG does not implement this method")


class ParallelCompositeProcessTemplateDAG(ProcessTemplateDAG):
    child_process_task_templates: List[IProcessTemplateDAG]
    parallel_operator_type: TaskOperator

    def __init__(
        self,
        next_process_dag: List[IProcessTemplateDAG],
        app: Dagger,
        name: str,
        process_type: Type[ParallelCompositeTask[KT, VT]],
        child_process_task_templates: List[IProcessTemplateDAG],
        parallel_operator_type: TaskOperator,
    ) -> None:
        super().__init__(
            next_process_dag=next_process_dag,
            app=app,
            name=name,
            process_type=process_type,
            root_task_dag=None,
            max_run_duration=0,
        )
        self.child_process_task_templates = child_process_task_templates
        self.parallel_operator_type = parallel_operator_type

    def set_parallel_process_template_dags(
        self, parallel_process_templates: List[IProcessTemplateDAG]
    ) -> None:  # pragma: no cover
        """Sets child_process_task_templates

        Args:
            parallel_process_templates (List[IProcessTemplateDAG]): List of parallel process templates
        """
        self.child_process_task_templates = parallel_process_templates

    async def create_instance(
        self,
        id: UUID,
        parent_id: UUID,
        parent_name: str,
        partition_key_lookup: str,
        *,
        repartition: bool = True,
        seed: random.Random = None,
        workflow_instance: ITemplateDAGInstance = None,
        **kwargs: Any,
    ) -> ParallelCompositeTask[KT, VT]:
        # create the instance in the graph store
        kwargs = {} if workflow_instance else kwargs
        process_instance = self.process_type(
            id=id, process_name=self.name, parent_id=parent_id
        )
        process_instance.task_type = TaskType.PARALLEL_COMPOSITE.name
        process_instance.time_created = int(time.time())
        process_instance.status = TaskStatus(
            code=TaskStatusEnum.NOT_STARTED.name, value=TaskStatusEnum.NOT_STARTED.value
        )
        process_instance.task_name = f"{parent_name}.{self.name}"
        process_instance.next_dags = list()
        process_instance.parallel_child_task_list = list()

        process_instance.root_dag = None
        if workflow_instance:
            workflow_instance.add_task(task=process_instance)
        for next_task_template in self.child_process_task_templates:
            dag_id = DAGIDGenerator.generate_dag_id_from_seed(seed) if seed else uuid1()
            next_task_instance = await next_task_template.create_instance(
                id=dag_id,
                parent_id=process_instance.id,
                parent_name=process_instance.task_name,
                partition_key_lookup=partition_key_lookup,
                repartition=repartition,
                seed=seed,
                workflow_instance=workflow_instance,
                **kwargs,
            )
            process_instance.parallel_child_task_list.append(next_task_instance.id)
        for process_task_template in self.next_process_dag:
            dag_id = DAGIDGenerator.generate_dag_id_from_seed(seed) if seed else uuid1()
            next_process_instance = await process_task_template.create_instance(
                id=dag_id,
                parent_id=parent_id,
                parent_name=parent_name,
                partition_key_lookup=partition_key_lookup,
                repartition=repartition,
                seed=seed,
                workflow_instance=workflow_instance,
                **kwargs,
            )
            process_instance.next_dags.append(next_process_instance.id)
        return process_instance


class DynamicProcessTemplateDAG(IDynamicProcessTemplateDAG):
    _dynamic_process_builders: List[ProcessTemplateDagBuilder] = []

    def __init__(
        self,
        next_process_dag: List[IProcessTemplateDAG],
        app: Dagger,
        name: str,
        max_run_duration: int,
    ) -> None:
        super().__init__(
            next_process_dags=next_process_dag,
            app=app,
            name=name,
            max_run_duration=max_run_duration,
        )

    def build_and_link_processes(
        self,
        process_builder_list: List[ProcessTemplateDagBuilder],
        dag_templates: List[IProcessTemplateDAG],
    ) -> List[IProcessTemplateDAG]:
        for dag_template in dag_templates:
            process_builder_list[-1].set_next_process(dag_template)
        next_process = process_builder_list[-1].build()
        if len(process_builder_list) > 1:
            for process_builder in reversed(process_builder_list[:-1]):
                if next_process:
                    process_builder.set_next_process(next_process)
                    next_process = process_builder.build()
        return [next_process]

    async def create_instance(
        self,
        id: UUID,
        parent_id: UUID,
        parent_name: str,
        partition_key_lookup: str,
        *,
        repartition: bool = True,
        seed: random.Random = None,
        workflow_instance: ITemplateDAGInstance = None,
        **kwargs: Any,
    ) -> IProcessTemplateDAGInstance[KT, VT]:

        head_process: List[IProcessTemplateDAG] = None
        if len(self._dynamic_process_builders) > 0:
            head_process = self.build_and_link_processes(
                self._dynamic_process_builders, self.next_process_dag
            )
        else:
            head_process = self.next_process_dag
        if head_process and len(head_process) > 0:
            return await head_process[0].create_instance(
                id=id,
                parent_id=parent_id,
                parent_name=parent_name,
                partition_key_lookup=partition_key_lookup,
                repartition=repartition,
                seed=seed,
                workflow_instance=workflow_instance,
                **kwargs,
            )
        return None


class TemplateDAG(ITemplateDAG):
    def get_given_process(self, process_name: str) -> Optional[IProcessTemplateDAG]:
        """
        Looks up for a specific process template within a DAG

        Args:
            process_name [str]: Name of the process

        Returns:
            [IProcessTemplateDAG]: Process Template if found, else None
        """
        #   get the root dag
        process_template = self.root_process_dag
        found = False
        while process_template and not found:
            #  if root dag is the one for which we want  to set multiple processes
            if process_template.name == process_name:
                return process_template
            #  else fetch next process dag's template and repeat
            process_template_list = process_template.next_process_dag
            if process_template_list:
                process_template = process_template_list[0]
            else:
                process_template = None
        if not found:
            logger.warning(f"Could not find process template {process_name}")
        return None

    def set_dynamic_builders_for_process_template(
        self, name: str, process_template_builders: List[ProcessTemplateDagBuilder]
    ):

        process_template = self.get_given_process(process_name=name)
        if process_template:
            process_template.set_dynamic_process_builders(process_template_builders)

    def set_parallel_process_template_dags_for_a_composite_process(
        self, name: str, parallel_process_templates: List[IProcessTemplateDAG]
    ):
        """Sets new process builders within a given process in a DAG

        Args:
            name (str): Name of the process with in which given process builders must reside
            parallel_process_templates (List[IProcessTemplateDAG]): List of process template builders
        """
        composite_process = self.get_given_process(process_name=name)
        if composite_process:
            composite_process.set_parallel_process_template_dags(
                parallel_process_templates
            )

    def set_given_num_of_parallel_processes_for_a_composite_process(
        self,
        no_of_processes: int,
        composite_process_name: str,
        parallel_template_builder: ProcessTemplateDagBuilder,
    ):
        """This method creates and sets 'N' number of new parralel processes for a given process in a DAG

        Args:
            no_of_processes (int): Number of parallel processes required
            composite_process_name (str):  Name of the process with in which the parallel processes must reside
            parallel_template_builder (ProcessTemplateDagBuilder): A process template builder which needs to be cloned 'N' times and executed parallely
        """

        parallel_process_templates = list()
        while no_of_processes > 0:
            parallel_process_templates.append(parallel_template_builder.build())
            no_of_processes -= 1

        composite_process = self.get_given_process(process_name=composite_process_name)
        if composite_process:
            composite_process.set_parallel_process_template_dags(
                parallel_process_templates
            )

    def __init__(
        self,
        dag: IProcessTemplateDAG,
        name: str,
        app: Dagger,
        template_type: Type[ITemplateDAGInstance[KT, VT]],
    ) -> None:
        super().__init__(dag, name, app, template_type)

    async def create_instance(
        self,
        id: UUID,
        partition_key_lookup: str,
        *,
        repartition: bool = True,
        seed: random.Random = None,
        **kwargs,
    ) -> ITemplateDAGInstance[KT, VT]:
        template_instance = self.template_type(
            id=id,
            template_name=self.name,
            parent_id=None,
            partition_key_lookup=partition_key_lookup,
            runtime_parameters=kwargs,
        )
        template_instance.next_dags = list()
        template_instance.tasks = dict()
        dag_id: UUID = (
            DAGIDGenerator.generate_dag_id_from_seed(seed) if seed else uuid.uuid1()
        )
        kwargs = {} if template_instance else kwargs
        process_instance = await self.root_process_dag.create_instance(
            id=dag_id,
            parent_id=template_instance.id,
            parent_name=template_instance.template_name,
            partition_key_lookup=partition_key_lookup,
            repartition=repartition,
            seed=seed,
            workflow_instance=template_instance,
            **kwargs,
        )
        template_instance.time_created = int(time.time())
        template_instance.status = TaskStatus(
            code=TaskStatusEnum.NOT_STARTED.name, value=TaskStatusEnum.NOT_STARTED.value
        )
        template_instance.root_dag = process_instance.id
        template_instance.add_task(task=process_instance)
        if repartition:
            await self.app.tasks_topic.send(key=template_instance.runtime_parameters[partition_key_lookup], value=template_instance)  # type: ignore
        else:
            await self.app._store_and_create_task(template_instance)  # type: ignore
        return template_instance


class ProcessTemplateDagBuilder(IProcessTemplateDAGBuilder):
    """Default implementation of process builder"""

    __name: str
    __process_type: Type[ITask]
    __max_run_duration: int = 0

    def __init__(self, app: Dagger) -> None:
        super().__init__(app)

    def set_root_task(self, task: TaskTemplate) -> IProcessTemplateDAGBuilder:
        self.root_task_dag = task
        return self

    def set_next_process(
        self, task_template: IProcessTemplateDAG
    ) -> IProcessTemplateDAGBuilder:
        self.next_process_dag.append(task_template)
        return self

    def set_name(self, process_name: str) -> IProcessTemplateDAGBuilder:
        self.__name = process_name
        return self

    def set_type(self, process_type: Type[ITask]) -> IProcessTemplateDAGBuilder:
        self.__process_type = process_type
        return self

    def set_max_run_duration(self, max_run_duration: int) -> IProcessTemplateDAGBuilder:
        self.__max_run_duration = max_run_duration
        return self

    def build(self) -> IProcessTemplateDAG:
        return ProcessTemplateDAG(
            next_process_dag=self.next_process_dag,
            app=self.app,  # type: ignore
            name=self.__name,
            process_type=self.__process_type,
            root_task_dag=self.root_task_dag,
            max_run_duration=self.__max_run_duration,
        )


class DynamicProcessTemplateDagBuilder(IProcessTemplateDAGBuilder):
    """Skeleton builder class used to build a dynamic process object(s)."""

    __max_run_duration: int = 0
    __name: str

    def __init__(self, app: Dagger) -> None:
        super().__init__(app)

    def set_root_task(self, task: TaskTemplate) -> IProcessTemplateDAGBuilder:
        """Not implemented.

        Raises:
            NotImplementedError: Not implemented.
        """
        raise NotImplementedError(
            "DynamicProcessTemplateDagBuilder does not implement this method"
        )

    def set_next_process(self, task: IProcessTemplateDAG) -> IProcessTemplateDAGBuilder:
        self.next_process_dag.append(task)
        return self

    def set_name(self, process_name: str) -> IProcessTemplateDAGBuilder:
        self.__name = process_name
        return self

    def set_type(self, process_type: Type[ITask]) -> IProcessTemplateDAGBuilder:
        """Not implemented.

        Raises:
            NotImplementedError: Not implemented.
        """
        raise NotImplementedError(
            "DynamicProcessTemplateDagBuilder does not implement this method"
        )

    def set_max_run_duration(self, max_run_duration: int) -> IProcessTemplateDAGBuilder:
        self.__max_run_duration = max_run_duration
        return self

    def build(self) -> IProcessTemplateDAG:
        return DynamicProcessTemplateDAG(
            self.next_process_dag, self.app, self.__name, self.__max_run_duration  # type: ignore
        )


class ParallelCompositeProcessTemplateDagBuilder(IProcessTemplateDAGBuilder):
    """Skeleton builder class used to build a parallel process object(s)."""

    child_process_task_templates: List[IProcessTemplateDAG]
    parallel_operator_type: TaskOperator
    __name: str
    __process_type: Type[ITask]

    def __init__(self, app: Dagger) -> None:
        super().__init__(app)
        self.parallel_operator_type = TaskOperator.JOIN_ALL
        self.child_process_task_templates = list()

    def set_parallel_operator_type(
        self, operator_type: TaskOperator
    ) -> IProcessTemplateDAGBuilder:
        self.parallel_operator_type = operator_type
        return self

    def set_root_task(self, task: TaskTemplate) -> IProcessTemplateDAGBuilder:
        """Not implemented.

        Raises:
            NotImplementedError: Not implemented.
        """
        raise NotImplementedError(
            "ParallelCompositeProcessTemplateDagBuilder does not implement this method"
        )

    def set_next_process(self, task: IProcessTemplateDAG) -> IProcessTemplateDAGBuilder:
        self.next_process_dag.append(task)
        return self

    def set_parallel_process_templates(
        self, task: IProcessTemplateDAG
    ) -> IProcessTemplateDAGBuilder:
        self.child_process_task_templates.append(task)
        return self

    def set_name(self, process_name: str) -> IProcessTemplateDAGBuilder:
        self.__name = process_name
        return self

    def set_type(self, process_type: Type[ITask]) -> IProcessTemplateDAGBuilder:
        self.__process_type = process_type
        return self

    def set_max_run_duration(self, max_run_duration: int) -> IProcessTemplateDAGBuilder:
        """Not implemented.

        Raises:
            NotImplementedError: Not implemented.
        """
        raise NotImplementedError(
            "ParallelCompositeProcessTemplateDagBuilder does not implement this method"
        )

    def build(self) -> IProcessTemplateDAG:
        return ParallelCompositeProcessTemplateDAG(
            next_process_dag=self.next_process_dag,
            app=self.app,  # type: ignore
            name=self.__name,
            process_type=self.__process_type,
            child_process_task_templates=self.child_process_task_templates,
            parallel_operator_type=self.parallel_operator_type,
        )
