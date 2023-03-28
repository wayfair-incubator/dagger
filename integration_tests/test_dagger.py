from __future__ import annotations

import json
import logging
import time
import uuid
from functools import wraps

import jsonpickle
import pytest
import requests
from kafka import KafkaConsumer, KafkaProducer

from dagger.tasks.task import ITask, ITemplateDAGInstance, TaskStatusEnum

KAFKA_ADMIN_CLIENT_URL = "kafka:29092"
DAGGER_URL = "http://dagger_test_app:6066"
logger = logging.getLogger(__name__)

TOTAL_ORDERS = 4
SIMPLE_IDS = ["simpleid1", "simpleid2"]

orders_left = TOTAL_ORDERS
running_task_ids = []
tagged_list_ids = []

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_ADMIN_CLIENT_URL],
    api_version=(3, 6, 3),
    security_protocol="PLAINTEXT",
)


def retry(exceptions, tries=5, delay=1):
    def wrapper_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            f_tries, f_delay = tries, delay
            while f_tries > 1:
                try:
                    return f(*args, **kwargs)
                except exceptions:
                    time.sleep(f_delay)
                    f_tries -= 1
            return f(*args, **kwargs)

        return f_retry

    return wrapper_retry


@pytest.fixture(scope="function")
def workflow_consumer():
    workflow_consumer = KafkaConsumer(
        "task_update_topic",
        bootstrap_servers=KAFKA_ADMIN_CLIENT_URL,
        group_id=f"workflow_{uuid.uuid1()}",
        consumer_timeout_ms=20000,
        auto_offset_reset="earliest",
    )
    return workflow_consumer


@retry(AssertionError, tries=30, delay=1)
def test_clean_consumer(workflow_consumer):
    messages = []
    for message in workflow_consumer:
        messages.append(message)
    assert len(messages) == 0


@retry(AssertionError, tries=30, delay=1)
def test_dagger_initialized():
    response = requests.get(DAGGER_URL + "/service/ready")
    engine_response = json.loads(response.content)["is_ready"]
    assert engine_response is True


def test_dagger_start():
    for i in range(0, TOTAL_ORDERS):
        future = producer.send(
            topic="orders_topic", value=json.dumps(i).encode("utf-8")
        )
        # Block for 'synchronous' sends
        future.get(timeout=10)
    producer.flush()


@retry(AssertionError, tries=30, delay=1)
def test_instances_created():
    response = requests.get(DAGGER_URL + "/tasks/instances")
    assert response.status_code == requests.codes.ok
    engine_response = json.loads(response.content)
    assert len(engine_response) > 0
    for instance in engine_response:
        assert instance["status"]["code"] == "EXECUTING"


@retry(AssertionError, tries=30, delay=1)
def test_instances_executing():
    response = requests.get(DAGGER_URL + "/tasks/instances")
    assert response.status_code == requests.codes.ok
    engine_response = json.loads(response.content)
    assert len(engine_response) > 0
    for instance in engine_response:
        if instance["task_type"] == "ROOT":
            assert instance["status"]["code"] == "EXECUTING"


def test_dagger_update_listeners():
    for i in range(0, TOTAL_ORDERS):
        payload = dict()
        payload["box_id"] = "ID00" + format(i)
        payload["order_id"] = "ID00" + format(i)
        producer.send(
            topic="PAYMENT_LISTENER", value=json.dumps(payload).encode("utf-8")
        ).add_callback(lambda _: None)
    producer.flush()


@retry(AssertionError, tries=30, delay=1)
def test_interval_tasks():

    response = requests.get(DAGGER_URL + "/tasks/instances")
    assert response.status_code == requests.codes.ok
    engine_response = json.loads(response.content)
    for instance in engine_response:
        count = instance["runtime_parameters"].get("count", None)
        assert count is not None
        assert instance["runtime_parameters"]["count"] == 2
        assert instance["status"]["code"] == "EXECUTING"


def test_send_fulfillment_ack():

    for i in range(0, TOTAL_ORDERS):
        payload = dict()
        payload["order_id"] = "ID00" + format(i)
        payload["box_id"] = "ID00" + format(i)
        producer.send(
            topic="FULFILLMENT_LISTENER", value=json.dumps(payload).encode("utf-8")
        ).add_callback(lambda _: None)
    producer.flush()


@retry(AssertionError, tries=30, delay=1)
def test_fulfillment_executing():
    response = requests.get(DAGGER_URL + "/tasks/instances")
    assert response.status_code == requests.codes.ok
    engine_response = json.loads(response.content)
    for instance in engine_response:
        assert instance["status"]["code"] == "EXECUTING"


@retry(AssertionError, tries=30, delay=1)
def test_parallel_instances_executing():
    response = requests.get(DAGGER_URL + "/tasks/instances")
    assert response.status_code == requests.codes.ok
    engine_response = json.loads(response.content)
    assert len(engine_response) > 0
    for instance in engine_response:
        if instance["task_type"] == "ROOT":
            assert instance["status"]["code"] == "EXECUTING"


def test_send_parallel_process_ack():
    time.sleep(10)
    for i in range(0, TOTAL_ORDERS):
        payload = dict()
        payload["order_id"] = "ID00" + format(i)
        payload["box_id"] = "ID00" + format(i)
        producer.send(
            topic="NOOP_LISTENER_2", value=json.dumps(payload).encode("utf-8")
        ).add_callback(lambda _: None)
    producer.flush()


@retry(AssertionError, tries=30, delay=1)
def test_all_completed():

    response = requests.get(DAGGER_URL + "/tasks/instances")
    assert response.status_code == requests.codes.ok
    engine_response = json.loads(response.content)
    assert len(engine_response) > 0
    for instance in engine_response:
        assert instance["status"]["code"] == "COMPLETED"


def test_dagger_one_to_many_create():
    producer = KafkaProducer(bootstrap_servers=[KAFKA_ADMIN_CLIENT_URL])
    for i in SIMPLE_IDS:
        future = producer.send(
            topic="simple_topic", value=json.dumps(i).encode("utf-8")
        )
        # Block for 'synchronous' sends
        future.get(timeout=10)
    producer.flush()


@retry(AssertionError, tries=30, delay=1)
def test_on_the_fly_workflow_instances_created():
    response = requests.get(DAGGER_URL + "/tasks/instances")
    assert response.status_code == requests.codes.ok
    engine_response = json.loads(response.content)
    executing_counter = 0
    for instance in engine_response:
        if instance["status"]["code"] == "EXECUTING":
            executing_counter += 1
    assert executing_counter == len(SIMPLE_IDS)


def test_on_the_fly_workflow_listeners():
    payload = dict()
    payload["simple_id"] = ["simpleid1", "simpleid2"]
    producer.send(
        topic="SIMPLE_LISTENER", value=json.dumps(payload).encode("utf-8")
    ).add_callback(lambda _: None)
    producer.flush()


@retry(AssertionError, tries=30, delay=1)
def test_all_simple_completed():

    response = requests.get(DAGGER_URL + "/tasks/instances")
    assert response.status_code == requests.codes.ok
    engine_response = json.loads(response.content)
    assert len(engine_response) > 0
    for instance in engine_response:
        assert instance["status"]["code"] == "COMPLETED"


def test_workflow_update(workflow_consumer):
    value = None
    task_map = {}
    workflow_count = 0
    executing_count = 0
    completed_count = 0
    task_list_count = 0

    for message in workflow_consumer:
        value = message.value
        try:
            value: ITemplateDAGInstance = ITemplateDAGInstance.from_data(
                jsonpickle.decode(message.value)
            )
        except Exception as ex:
            logger.error(f"error as {ex}")
            continue

        current_list = task_map.get(value.id, None)
        if current_list is None:
            current_list = list()
        current_list.append(value)
        task_map[value.id] = current_list

    for key, task_list in task_map.items():
        workflow_count += 1
        for task in task_list:
            if task.status.code == TaskStatusEnum.EXECUTING.name:
                executing_count += 1
            if task.status.code == TaskStatusEnum.COMPLETED.name:
                completed_count += 1
                assert task.update_count > 1
            task_list_count += 1
    assert task_list_count > 1
    assert executing_count > 1
    assert completed_count >= 1
    assert workflow_count > 1


SIMPLE_ID_TO_DELETE = "SIMPLE_ID_3"


def test_create_new_simple():
    producer = KafkaProducer(bootstrap_servers=[KAFKA_ADMIN_CLIENT_URL])
    future = producer.send(
        topic="simple_topic", value=json.dumps(SIMPLE_ID_TO_DELETE).encode("utf-8")
    )
    # Block for 'synchronous' sends
    future.get(timeout=10)
    producer.flush()


@retry(AssertionError, tries=30, delay=1)
def test_new_simple_workflow_instances_created():
    response = requests.get(DAGGER_URL + "/tasks/instances")
    assert response.status_code == requests.codes.ok
    engine_response = json.loads(response.content)
    executing_counter = 0
    for instance in engine_response:
        if instance["status"]["code"] == "EXECUTING":
            executing_counter += 1
    assert executing_counter == 1


def test_stop_new_simple():
    producer = KafkaProducer(bootstrap_servers=[KAFKA_ADMIN_CLIENT_URL])
    future = producer.send(
        topic="simple_topic_stop", value=json.dumps(SIMPLE_ID_TO_DELETE).encode("utf-8")
    )
    # Block for 'synchronous' sends
    future.get(timeout=10)
    producer.flush()


@retry(AssertionError, tries=30, delay=1)
def test_new_simple_workflow_instances_stopped():
    response = requests.get(DAGGER_URL + "/tasks/instances")
    assert response.status_code == requests.codes.ok
    engine_response = json.loads(response.content)
    executing_counter = 0
    for instance in engine_response:
        if instance["status"]["code"] == "STOPPED":
            executing_counter += 1
    assert executing_counter == 1
