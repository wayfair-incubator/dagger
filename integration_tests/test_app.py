from __future__ import annotations

import json
import logging
import os
import random
import sys
import time
import uuid
from typing import Any, Dict, List, Optional, Type
from uuid import UUID

import aerospike
import aiohttp
from faust import Topic
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic

from dagger.modeler.builder_helper import DAGBuilderHelper
from dagger.modeler.definition import (
    DefaultTemplateBuilder,
    DynamicProcessTemplateDagBuilder,
    ParallelCompositeProcessTemplateDagBuilder,
    ProcessTemplateDagBuilder,
)
from dagger.service.engineconfig import AerospikeConfig
from dagger.service.services import Dagger
from dagger.tasks.task import (
    KT,
    VT,
    DecisionTask,
    DefaultMonitoringTask,
    DefaultProcessTemplateDAGInstance,
    DefaultTemplateDAGInstance,
    ExecutorTask,
    IntervalTask,
    ITask,
    ITemplateDAGInstance,
    KafkaCommandTask,
    KafkaListenerTask,
    MonitoredProcessTemplateDAGInstance,
    MonitoringTask,
    ParallelCompositeTask,
    TaskLookupKey,
    TaskOperator,
    TaskStatus,
    TriggerTask,
)
from dagger.templates.template import (
    DecisionTaskTemplateBuilder,
    DefaultTaskTemplateBuilder,
    IntervalTaskTemplateBuilder,
    IProcessTemplateDAG,
    ITemplateDAG,
    KafkaCommandTaskTemplateBuilder,
    KafkaListenerTaskTemplateBuilder,
    ParallelCompositeTaskTemplate,
    ParallelCompositeTaskTemplateBuilder,
    TriggerTaskTemplateBuilder,
    IProcessTemplateDAGBuilder,
)

KAFKA_ADMIN_CLIENT_URL = "kafka:29092"
logger = logging.getLogger(__name__)

TOTAL_ORDERS = 4
orders_left = TOTAL_ORDERS
running_task_ids = []
tagged_list_ids = []
config = {
    "hosts": [(os.environ.get("AEROSPIKE_HOST"), 3000)],
    "policies": {"key": aerospike.POLICY_KEY_SEND},
}

ORDERS_TOPIC: str = "orders_topic"

try:
    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_ADMIN_CLIENT_URL, client_id="test"
    )
    topic_list = []
    topic_list.append(
        NewTopic(name="PAYMENT_LISTENER", num_partitions=1, replication_factor=1)
    )
    topic_list.append(
        NewTopic(name=ORDERS_TOPIC, num_partitions=1, replication_factor=1)
    )
    topic_list.append(
        NewTopic(name="dagger_task_topic", num_partitions=1, replication_factor=1)
    )
    topic_list.append(
        NewTopic(name="PAYMENT_COMMAND", num_partitions=1, replication_factor=1)
    )
    topic_list.append(
        NewTopic(name="FULFILLMENT_COMMAND", num_partitions=1, replication_factor=1)
    )
    topic_list.append(
        NewTopic(name="FULFILLMENT_LISTENER", num_partitions=1, replication_factor=1)
    )
    topic_list.append(
        NewTopic(name="NOOP_LISTENER_2", num_partitions=1, replication_factor=1)
    )
    topic_list.append(
        NewTopic(name="NOOP_LISTENER_1", num_partitions=1, replication_factor=1)
    )
    topic_list.append(
        NewTopic(name="simple_topic", num_partitions=1, replication_factor=1)
    )
    topic_list.append(
        NewTopic(name="simple_topic_stop", num_partitions=1, replication_factor=1)
    )
    topic_list.append(
        NewTopic(name="SIMPLE_LISTENER", num_partitions=1, replication_factor=1)
    )
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
except Exception:
    pass


aerospike_config = AerospikeConfig(
    HOSTS=[(os.environ.get("AEROSPIKE_HOST"), 3000)],
    POLICIES={"key": aerospike.POLICY_KEY_SEND},
    NAMESPACE="test",
    USERNAME="",
    PASSWORD="",
    TTL=-1,
    KWARGS=None,
)

workflow_engine = Dagger(
    broker=KAFKA_ADMIN_CLIENT_URL,
    datadir="/tmp/data/",
    store="aerospike://",
    consumer_auto_offset_reset="latest",
    task_update_topic="task_update_topic",
    trigger_interval=5,
    aerospike_config=aerospike_config,
    enable_changelog=False,
    web_port=6066,
    serializer="raw",
)
workflow_engine.tables_cleared = False
orders_topic = workflow_engine.faust_app.topic(ORDERS_TOPIC, value_type=str)
simple_topic = workflow_engine.faust_app.topic("simple_topic", value_type=str)
simple_topic_stop = workflow_engine.faust_app.topic("simple_topic_stop", value_type=str)

simple_listener = workflow_engine.faust_app.topic("SIMPLE_LISTENER", value_type=str)

templates: List[ITemplateDAGInstance] = list()

logger = logging.getLogger(__name__)

out_hdlr = logging.StreamHandler(sys.stdout)
out_hdlr.setFormatter(
    logging.Formatter("%(asctime)s %(levelname)s %(module)s - %(message)s")
)
logger.addHandler(out_hdlr)
logger.setLevel(logging.INFO)
dagger_logger = logging.getLogger("dagger")
dagger_logger.addHandler(out_hdlr)
dagger_logger.setLevel(logging.INFO)
faust_logger = logging.getLogger("faust")
faust_logger.addHandler(out_hdlr)
faust_logger.setLevel(logging.INFO)


class Noopexecutor(ExecutorTask[str, str]):
    def get_status(self) -> TaskStatus:
        return self.status  # pragma: no cover

    async def execute(
        self, runtime_parameters: Dict[str, VT], workflow_instance: ITask = None
    ) -> bool:
        logger.info("Executing executor")


class OrderPreProcess1Executor(ExecutorTask[str, str]):
    def get_status(self) -> TaskStatus:
        return self.status  # pragma: no cover

    async def execute(
        self, runtime_parameters: Dict[str, str], workflow_instance: ITask = None
    ) -> bool:
        logger.info("Executing executor")


class ShippingExecutor(ExecutorTask[str, str]):
    def get_status(self) -> TaskStatus:
        return self.status  # pragma: no cover

    async def execute(
        self, runtime_parameters: Dict[str, str], workflow_instance: ITask = None
    ) -> bool:
        logger.info("Executing executor")


class PaymentKafkaCommandTask(KafkaCommandTask[str, str]):
    async def execute(
        self,
        runtime_parameters: Dict[str, str],
        workflow_instance: ITemplateDAGInstance,
    ) -> None:
        logger.info(f"Sending Payment Command to topic {self.topic}")


class OrderCommandTask(KafkaCommandTask[str, str]):
    async def execute(
        self,
        runtime_parameters: Dict[str, str],
        workflow_instance: ITemplateDAGInstance,
    ) -> None:
        payload = {
            "order_id": runtime_parameters["order_id"],
            "customer_id": runtime_parameters["customer_id"],
            "pizza_type": runtime_parameters["pizza_type"],
        }
        topic: Topic = await workflow_engine.topics[self.topic].send(
            value=json.dumps(payload)
        )


class PaymentDecisionTask(DecisionTask[str, str]):
    async def evaluate(self, **kwargs: Any) -> Optional[UUID]:
        for task_id in self.next_dags:
            return task_id


class FulfillmentKafkaCommandTask(KafkaCommandTask[str, str]):
    async def execute(
        self, runtime_parameters: Dict[str, VT], workflow_instance: ITask = None
    ) -> None:
        logger.info(f"Sending Command to topic {self.topic}")


class DeliveryCommandTask(ExecutorTask[str, str]):
    async def execute(
        self, runtime_parameters: Dict[str, VT], workflow_instance: ITask = None
    ) -> None:
        payload = {
            "order_id": runtime_parameters["order_id"],
            "customer_id": runtime_parameters["customer_id"],
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url="http://www.deliverysvc.com", json=payload):
                pass


class FulfillmentTriggerTask(TriggerTask[str, str]):
    async def execute(
        self, runtime_parameters: Dict[str, VT], workflow_instance: ITask
    ) -> None:
        logger.info(
            f"Executing fulfillment trigger task to topic {self.time_to_execute}"
        )


class FulfillmentIntervalTask(IntervalTask[str, str]):
    async def interval_execute(self, runtime_parameters: Dict[str, VT]) -> bool:
        # parent_process = await self.get_parent_node()
        logger.info(f"Executing interval task {runtime_parameters.get('dagId', None)}")
        if "count" not in runtime_parameters:
            runtime_parameters["count"] = 1
        else:
            runtime_parameters["count"] += 1

        if runtime_parameters["count"] == 2:
            return True
        else:
            logger.info(f"Executing interval task to topic {self.time_to_execute}")
            return False

    async def execute(
        self, runtime_parameters: Dict[str, VT], workflow_instance: ITask = None
    ) -> None:
        logger.info(f"Executing trigger task to topic {self.time_to_execute}")


class PaymentKafkaListenerTask(KafkaListenerTask[str, str]):

    correlatable_key = "order_id"

    async def stop(
        self, runtime_parameters: Dict[str, str], workflow_instance: ITask = None
    ) -> None:  # pragma: no cover
        """Stops the ITask.
        :param runtime_parameters: The runtime parameters of the task
        :param workflow_instance: The workflow object
        """
        pass

    def get_status(self) -> TaskStatus:
        return self.status

    async def on_message(
        self, runtime_parameters: Dict[str, str], *args: Any, **kwargs: Any
    ) -> bool:
        logger.info(f"Done {self.id}")
        new_params = json.loads(*args)
        runtime_parameters["box_id"] = new_params.get("box_id", None)
        return True

    def get_correlatable_key(self, payload: Any) -> TaskLookupKey:
        tpayload = json.loads(payload)
        logger.info(f"payload {tpayload}")
        return self.correlatable_key, tpayload[self.correlatable_key]


class PizzaWaitForReadyListener(KafkaListenerTask[str, str]):
    correlatable_key = "order_id"

    async def get_correlatable_keys_from_payload(
        self, payload: Any
    ) -> List[TaskLookupKey]:
        tpayload = json.loads(payload)
        key = tpayload[self.correlatable_key]
        return [(self.correlatable_key, key)]

    async def on_message(
        self, runtime_parameters: Dict[str, VT], *args: Any, **kwargs: Any
    ) -> bool:
        logger.info(f"Pizza Order is Ready")
        return True


def pizza_ordering_process(process_name: str = "Order") -> IProcessTemplateDAGBuilder:
    dag_builder = DAGBuilderHelper(dagger_app=workflow_engine)
    root_task = dag_builder.build_and_link_tasks(
        [
            dag_builder.generic_command_task_builder(
                topic="pizza_order_topic",
                task_type=OrderCommandTask,
                process_name=process_name,
            ),
            dag_builder.generic_listener_task_builder(
                topic="PizzaWaitForReadyListener",
                task_type=PizzaWaitForReadyListener,
                process_name=process_name,
            ),
        ]
    )
    return dag_builder.generic_process_builder(
        process_name=process_name, root_task=root_task
    )


def pizza_delivery_process(
    process_name: str = "Delivery",
) -> IProcessTemplateDAGBuilder:
    dag_builder = DAGBuilderHelper(dagger_app=workflow_engine)
    root_task = dag_builder.build_and_link_tasks(
        [
            dag_builder.generic_executor_task_builder(
                task_type=DeliveryCommandTask,
                name=process_name,
            )
        ]
    )
    return dag_builder.generic_process_builder(
        process_name=process_name, root_task=root_task
    )


class SimpleKafkaListenerTask(KafkaListenerTask[str, str]):

    correlatable_key = "simple_id"

    async def stop(
        self, runtime_parameters: Dict[str, str], workflow_instance: ITask = None
    ) -> None:  # pragma: no cover
        """Stops the ITask.
        :param runtime_parameters: The runtime parameters of the task
        :param workflow_instance: The workflow object
        """
        print("Stop called")

    def get_status(self) -> TaskStatus:
        return self.status

    async def on_message(
        self, runtime_parameters: Dict[str, VT], *args: Any, **kwargs: Any
    ) -> bool:
        logger.info(f"Done {self.id}")
        return True

    async def get_correlatable_keys_from_payload(
        self, payload: Any
    ) -> List[TaskLookupKey]:  # pragma: no cover
        tpayload = json.loads(payload)
        keys = tpayload[self.correlatable_key]
        logger.info(f"correletable keys {keys}")
        return [(self.correlatable_key, key) for key in keys]

    def get_correlatable_key(self, payload: Any) -> TaskLookupKey:
        tpayload = json.loads(payload)
        return self.correlatable_key, tpayload[self.correlatable_key]


class NoopKafkaListenerTask(KafkaListenerTask[str, str]):
    correlatable_key = "order_id"

    async def stop(
        self, runtime_parameters: Dict[str, str], workflow_instance: ITask = None
    ) -> None:  # pragma: no cover
        """Stops the ITask.
        :param runtime_parameters: The runtime parameters of the task
        :param workflow_instance: The workflow object
        """
        pass

    def get_status(self) -> TaskStatus:
        return self.status

    async def on_message(
        self, runtime_parameters: Dict[str, VT], *args: Any, **kwargs: Any
    ) -> bool:
        return True

    def get_correlatable_key(self, payload: Any) -> TaskLookupKey:
        tpayload = json.loads(payload)
        logger.info(f"Nooplistner {payload}")
        return self.correlatable_key, tpayload[self.correlatable_key]


class FulfillmentKafkaListenerTask(KafkaListenerTask[str, str]):
    correlatable_key = "box_id"

    async def stop(
        self, runtime_parameters: Dict[str, str], workflow_instance: ITask = None
    ) -> None:  # pragma: no cover
        """Stops the ITask.
        :param runtime_parameters: The runtime parameters of the task
        :param workflow_instance: The workflow object
        """
        pass

    def get_status(self) -> TaskStatus:
        return self.status

    async def on_message(
        self, runtime_parameters: Dict[str, VT], *args: Any, **kwargs: Any
    ) -> bool:
        logger.info(f"Done {self.id}")
        return True

    def get_correlatable_key(self, payload: Any) -> TaskLookupKey:
        tpayload = json.loads(payload)
        return self.correlatable_key, tpayload[self.correlatable_key]


class TestMonitoringTask(DefaultMonitoringTask):
    async def process_monitored_task(
        self, monitored_task: ITask, workflow_instance: ITask
    ) -> None:
        tagged_list_ids.append(self.monitored_task_id)
        logger.info(f"Tagging late process {len(tagged_list_ids)}")
        with open("test.txt", "w") as f:
            f.write(str(len(tagged_list_ids)))


class FulfillmentMonitoringProcessDAGInstance(MonitoredProcessTemplateDAGInstance):
    async def execute(
        self, runtime_parameters: Dict[str, VT], workflow_instance: ITask
    ) -> None:
        logger.info(f"Executing FulfillmentMonitoringProcessDAGInstance")
        await super().execute(
            runtime_parameters=runtime_parameters, workflow_instance=workflow_instance
        )

    def get_monitoring_task_type(self) -> Type[MonitoringTask]:
        return TestMonitoringTask


order_preprocess1_executor_task_template_builder: DefaultTaskTemplateBuilder = (
    DefaultTaskTemplateBuilder(Dagger.app)
)
order_preprocess1_executor_task_template_builder.set_name("order_preprocess1_executor")
order_preprocess1_executor_task_template_builder.set_type(OrderPreProcess1Executor)
order_preprocess1_executor_task_template = (
    order_preprocess1_executor_task_template_builder.build()
)


shipping_executor_task_template_builder: DefaultTaskTemplateBuilder = (
    DefaultTaskTemplateBuilder(Dagger.app)
)
shipping_executor_task_template_builder.set_name("shipping_executor")
shipping_executor_task_template_builder.set_type(ShippingExecutor)
shipping_executor_task_template = shipping_executor_task_template_builder.build()


@Dagger.register_process_template("SimpleProcess")
def register_simple_process(process_name: str) -> IProcessTemplateDAG:
    simple_topic: Topic = Dagger.create_topic(
        "SIMPLE_LISTENER", key_type=bytes, value_type=bytes
    )
    simple_listener_task_template_builder = KafkaListenerTaskTemplateBuilder(Dagger.app)
    simple_listener_task_template_builder.set_topic(simple_topic)
    simple_listener_task_template_builder.set_type(SimpleKafkaListenerTask)
    simple_listener_task_template_builder.set_name("SIMPLE_LISTENER_TASK")
    simple_listener_task_template = simple_listener_task_template_builder.build()

    simple_process_builder = ProcessTemplateDagBuilder(Dagger.app)
    simple_process_builder.set_name(process_name)
    simple_process_builder.set_root_task(simple_listener_task_template)
    simple_process_builder.set_type(DefaultProcessTemplateDAGInstance)
    return simple_process_builder.build()


@Dagger.register_template("PizzaWorkflow")
def register_pizza_workflow(template_name: str) -> ITemplateDAG:
    dag_builder_helper = DAGBuilderHelper(workflow_engine)
    order_process = dag_builder_helper.build_and_link_processes(
        [
            pizza_ordering_process(process_name="Order"),
            pizza_delivery_process(process_name="Delivery"),
        ]
    )
    return dag_builder_helper.generic_template(
        template_name=template_name, root_process=order_process
    )


@Dagger.register_template("OrderWorkflow")
def test_order_template(template_name: str) -> ITemplateDAG:
    payment_status_topic: Topic = Dagger.create_topic(
        "PAYMENT_LISTENER", key_type=bytes, value_type=bytes
    )
    payment_listener_task_template_builder = KafkaListenerTaskTemplateBuilder(
        Dagger.app
    )
    payment_listener_task_template_builder.set_topic(payment_status_topic)
    payment_listener_task_template_builder.set_type(PaymentKafkaListenerTask)
    payment_listener_task_template_builder.set_name("PAYMENT_LISTENER_TASK")
    payment_listener_task_template = payment_listener_task_template_builder.build()
    payment_decision_task_template_builder = DecisionTaskTemplateBuilder(Dagger.app)
    payment_decision_task_template_builder.set_type(PaymentDecisionTask)
    payment_decision_task_template_builder.set_name("PAYMENT_DECISION_TASK")
    payment_decision_task_template_builder.set_next(payment_listener_task_template)
    payment_decision_task_template = payment_decision_task_template_builder.build()
    payment_command_topic = Dagger.create_topic(
        "PAYMENT_COMMAND", key_type=bytes, value_type=str
    )
    payment_command_task_template_builder = KafkaCommandTaskTemplateBuilder(Dagger.app)
    payment_command_task_template_builder.set_topic(payment_command_topic)
    payment_command_task_template_builder.set_type(PaymentKafkaCommandTask)
    payment_command_task_template_builder.set_name("PAYMENT_COMMAND_TASK")
    payment_command_task_template_builder.set_next(payment_decision_task_template)
    payment_command_task_template = payment_command_task_template_builder.build()
    payment_process_builder = ProcessTemplateDagBuilder(Dagger.app)
    payment_process_builder.set_name("PAYMENT")
    payment_process_builder.set_root_task(payment_command_task_template)
    payment_process_builder.set_type(DefaultProcessTemplateDAGInstance)
    template_builder = DefaultTemplateBuilder(Dagger.app)
    template_builder.set_name(template_name)
    template_builder.set_type(DefaultTemplateDAGInstance)

    dynamic_template_builder = DynamicProcessTemplateDagBuilder(Dagger.app)
    dynamic_template_builder.set_name("ORDER_PREPROCESSING")

    fulfillment_topic: Topic = Dagger.create_topic(
        "FULFILLMENT_LISTENER", key_type=bytes, value_type=bytes
    )

    fulfillment_trigger_task_template_builder = TriggerTaskTemplateBuilder(Dagger.app)
    fulfillment_trigger_task_template_builder.set_time_to_execute_lookup_key("DEADLINE")
    fulfillment_trigger_task_template_builder.set_type(FulfillmentTriggerTask)
    fulfillment_trigger_task_template_builder.set_name("FULFILLMENT_TRIGGER_TASK")
    fulfillment_trigger_task_template = (
        fulfillment_trigger_task_template_builder.build()
    )

    fulfillment_listener_task_template_builder = KafkaListenerTaskTemplateBuilder(
        Dagger.app
    )
    fulfillment_listener_task_template_builder.set_topic(fulfillment_topic)
    fulfillment_listener_task_template_builder.set_type(FulfillmentKafkaListenerTask)
    fulfillment_listener_task_template_builder.set_name("FULFILLMENT_LISTENER_TASK")
    fulfillment_listener_task_template_builder.set_next(
        fulfillment_trigger_task_template
    )
    fulfillment_listener_task_template = (
        fulfillment_listener_task_template_builder.build()
    )

    fulfillment_interval_task_template_builder = IntervalTaskTemplateBuilder(Dagger.app)
    fulfillment_interval_task_template_builder.set_time_to_force_complete_lookup_key(
        "DEADLINE"
    )
    fulfillment_interval_task_template_builder.set_interval_execute_period_lookup_key(
        "interval"
    )
    fulfillment_interval_task_template_builder.set_next(
        fulfillment_listener_task_template
    )
    fulfillment_interval_task_template_builder.set_type(FulfillmentIntervalTask)
    fulfillment_interval_task_template_builder.set_name("FULFILLMENT_INTERVAL_TASK")
    fulfillment_interval_task = fulfillment_interval_task_template_builder.build()

    fulfillment_command_topic = Dagger.create_topic(
        "FULFILLMENT_COMMAND", key_type=bytes, value_type=str
    )
    fulfillment_command_task_template_builder = KafkaCommandTaskTemplateBuilder(
        Dagger.app
    )
    fulfillment_command_task_template_builder.set_topic(fulfillment_command_topic)
    fulfillment_command_task_template_builder.set_next(fulfillment_interval_task)
    fulfillment_command_task_template_builder.set_type(FulfillmentKafkaCommandTask)
    fulfillment_command_task_template_builder.set_name("FULFILLMENT_COMMAND_TASK")
    fulfillment_command_task_template = (
        fulfillment_command_task_template_builder.build()
    )

    fulfillment_process_builder = ProcessTemplateDagBuilder(Dagger.app)
    fulfillment_process_builder.set_name("FULFILLMENT")
    fulfillment_process_builder.set_root_task(fulfillment_command_task_template)
    fulfillment_process_builder.set_type(FulfillmentMonitoringProcessDAGInstance)

    no_op_topic_1: Topic = Dagger.create_topic(
        "NOOP_LISTENER_1", key_type=bytes, value_type=bytes
    )
    no_op_listener_task_template_builder_1 = KafkaListenerTaskTemplateBuilder(
        Dagger.app
    )
    no_op_listener_task_template_builder_1.set_topic(no_op_topic_1)
    no_op_listener_task_template_builder_1.set_type(NoopKafkaListenerTask)
    no_op_listener_task_template_builder_1.set_name("NO_OP_1_LISTENER_TASK")
    no_op_listener_task_template_1 = no_op_listener_task_template_builder_1.build()

    no_op_executor_task_template_builder: DefaultTaskTemplateBuilder = (
        DefaultTaskTemplateBuilder(Dagger.app)
    )
    no_op_executor_task_template_builder.set_name("noop_executor")
    no_op_executor_task_template_builder.set_type(Noopexecutor)
    no_op_executor_task_template = no_op_executor_task_template_builder.build()

    parallel_composite_task_template_builder: ParallelCompositeTaskTemplateBuilder = (
        ParallelCompositeTaskTemplateBuilder(
            Dagger.app, 2, no_op_executor_task_template
        )
    )
    parallel_composite_task_template_builder.set_name("parallelTasks")

    parallel_composite_process_builder: ParallelCompositeProcessTemplateDagBuilder = (
        ParallelCompositeProcessTemplateDagBuilder(Dagger.app)
    )
    parallel_composite_process_builder.set_name("parallelProcess")
    parallel_composite_process_builder.set_parallel_operator_type(TaskOperator.JOIN_ALL)
    parallel_composite_process_builder.set_type(ParallelCompositeTask)

    dummy_executor_process = ProcessTemplateDagBuilder(Dagger.app)
    dummy_executor_process.set_name("DummyExecutor")
    dummy_executor_process.set_root_task(no_op_executor_task_template)
    dummy_executor_process.set_type(DefaultProcessTemplateDAGInstance)
    dummy_executor_process_template = dummy_executor_process.build()

    no_op_topic_2: Topic = Dagger.create_topic(
        "NOOP_LISTENER_2", key_type=bytes, value_type=bytes
    )
    no_op_listener_task_template_builder_2 = KafkaListenerTaskTemplateBuilder(
        Dagger.app
    )
    no_op_listener_task_template_builder_2.set_topic(no_op_topic_2)
    no_op_listener_task_template_builder_2.set_type(NoopKafkaListenerTask)
    no_op_listener_task_template_builder_2.set_name("NO_OP_2_LISTENER_TASK")
    no_op_listener_task_template_2 = no_op_listener_task_template_builder_2.build()
    dummy_listener_process = ProcessTemplateDagBuilder(Dagger.app)
    dummy_listener_process.set_name("DummyListener")
    dummy_listener_process.set_root_task(no_op_listener_task_template_2)
    dummy_listener_process.set_type(DefaultProcessTemplateDAGInstance)
    dummy_listener_process_template = dummy_listener_process.build()
    parallel_composite_process_builder.set_parallel_process_templates(
        dummy_listener_process_template
    )
    parallel_composite_process_builder.set_parallel_process_templates(
        dummy_executor_process_template
    )

    parallel_composite_task_template_builder.add_parallel_task(
        no_op_executor_task_template
    )
    parallel_composite_task_template_builder.add_parallel_task(
        no_op_listener_task_template_1
    )

    parallel_composite_task_template_builder.set_type(ParallelCompositeTask)
    parallel_composite_task_template_builder.set_task_operator(TaskOperator.ATLEAST_ONE)

    parallel_composite_task_template: ParallelCompositeTaskTemplate = (
        parallel_composite_task_template_builder.build()
    )

    parallel_composite_process_builder_2: ParallelCompositeProcessTemplateDagBuilder = (
        ParallelCompositeProcessTemplateDagBuilder(Dagger.app)
    )
    parallel_composite_process_builder_2.set_name("ShippingCompositeProcess")
    parallel_composite_process_builder_2.set_parallel_operator_type(
        TaskOperator.JOIN_ALL
    )
    parallel_composite_process_builder_2.set_type(ParallelCompositeTask)
    parallel_composite_process_builder_2.set_next_process(
        parallel_composite_task_template
    )
    parallel_composite_process_builder_2_template = (
        parallel_composite_process_builder_2.build()
    )
    parallel_composite_process_builder.set_next_process(
        parallel_composite_process_builder_2_template
    )

    parallel_composite_process_template = parallel_composite_process_builder.build()

    fulfillment_process_builder.set_next_process(parallel_composite_process_template)
    fulfillment_template = fulfillment_process_builder.build()

    dynamic_template_builder.set_next_process(fulfillment_template)
    dynamic_template = dynamic_template_builder.build()
    payment_process_builder.set_next_process(dynamic_template)
    payment_template = payment_process_builder.build()
    template_builder.set_root(payment_template)
    return template_builder.build()


async def create_and_submit_pizza_delivery_workflow(
    order_id: str, customer_id: str, pizza_type: int
):
    pizza_workflow_template = workflow_engine.template_dags["PizzaWorkflow"]
    pizza_workflow_instance = await pizza_workflow_template.create_instance(
        uuid.uuid1(),
        repartition=False,  # Create this instance on the current worker
        submit_task=True,
        order_id=order_id,
        customer_id=customer_id,
        pizza_type=pizza_type,
    )


@workflow_engine.faust_app.agent(simple_topic_stop)
async def simple_data_stream_stop(stream):
    async for value in stream:

        instance = await workflow_engine.get_instance(running_task_ids[-1])
        await instance.stop(
            runtime_parameters=instance.runtime_parameters, workflow_instance=instance
        )


@workflow_engine.faust_app.agent(simple_topic)
async def simple_data_stream(stream):
    async for value in stream:
        rd = random.Random()
        rd.seed(time.time())
        simple_process = workflow_engine.process_templates["SimpleProcess"]
        template_builder = DefaultTemplateBuilder(Dagger.app)
        template_builder.set_name("OnTheFlyWorkflow")
        template_builder.set_type(DefaultTemplateDAGInstance)
        template_builder.set_root(simple_process)
        simple_template = template_builder.build()
        value = json.loads(value)
        dag_id = uuid.UUID(int=rd.getrandbits(128))
        # create dag
        logger.info(f"Creating Simple Workflow Dag {value}")
        instance = await simple_template.create_instance(
            dag_id,
            partition_key_lookup="order_id",
            simple_id=value,
            DEADLINE=int(time.time()) + 2,
            interval=1,
            order_id="ID00" + format(value),
            complete_by_time=120000,
            repartition=False,
            seed=rd,
            submit_task=True,
        )
        templates.append(instance)
        running_task_ids.append(instance.id)


@workflow_engine.faust_app.agent(orders_topic)
async def orders_stream(stream):
    async for value in stream:
        if not workflow_engine.tables_cleared:
            for table in workflow_engine.faust_app.tables.values():
                try:
                    for key in [key for key in table]:
                        table.pop(key, None)
                except Exception:
                    table.clear()
            workflow_engine.tables_cleared = True
        template = workflow_engine.template_dags["OrderWorkflow"]
        order_preprocess1_process_builder = ProcessTemplateDagBuilder(Dagger.app)
        order_preprocess1_process_builder.set_name("ORDER_PREPROCESS_1")
        order_preprocess1_process_builder.set_root_task(
            order_preprocess1_executor_task_template
        )
        order_preprocess1_process_builder.set_type(DefaultProcessTemplateDAGInstance)

        order_preprocess2_process_builder = ProcessTemplateDagBuilder(Dagger.app)
        order_preprocess2_process_builder.set_name("ORDER_PREPROCESS_2")
        order_preprocess2_process_builder.set_root_task(
            order_preprocess1_executor_task_template
        )
        order_preprocess2_process_builder.set_type(DefaultProcessTemplateDAGInstance)
        template.set_dynamic_builders_for_process_template(
            "ORDER_PREPROCESSING",
            [order_preprocess1_process_builder, order_preprocess2_process_builder],
        )

        shipping_process_builder = ProcessTemplateDagBuilder(Dagger.app)
        shipping_process_builder.set_name("SHIPPING")
        shipping_process_builder.set_root_task(shipping_executor_task_template)
        shipping_process_builder.set_type(DefaultProcessTemplateDAGInstance)

        # create 4 parallel SHIPPING processes
        template.set_given_num_of_parallel_processes_for_a_composite_process(
            4, "ShippingCompositeProcess", shipping_process_builder
        )
        rd = random.Random()
        rd.seed(time.time())
        dag_id = uuid.UUID(int=rd.getrandbits(128))
        instance = await template.create_instance(
            dag_id,
            partition_key_lookup="order_id",
            box_id="box_ID00" + format(value),
            DEADLINE=int(time.time()) + 2,
            interval=10,
            order_id="ID00" + format(value),
            complete_by_time=120000,
            repartition=False,
            seed=rd,
            dagId=dag_id,
        )
        templates.append(instance)
        running_task_ids.append(instance.id)
        await workflow_engine.submit(instance, repartition=False)


workflow_engine.main()
