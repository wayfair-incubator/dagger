import opentracing
import pytest
from asynctest import MagicMock
from faust.utils.tracing import set_current_span

from dagger.tracing.utils import TracingSensor


class TestTracerSensor:
    @pytest.fixture()
    def tracer_fixture(self):
        opentracing.tracer = MagicMock()
        return TracingSensor()

    @pytest.fixture()
    def topic_fixture(self):
        tp = MagicMock()
        tp.topic = "TEST_TOPIC"
        tp.partition = 1
        return tp

    @pytest.fixture()
    def event_fixture(self):
        event = MagicMock()
        event.message = MagicMock()
        event.message.stream_meta = None
        event.message.span = MagicMock()
        event.message.topic = "TEST"
        return event

    @pytest.mark.asyncio
    async def test_kafka_tracer(self, tracer_fixture: TracingSensor):
        assert tracer_fixture.kafka_tracer is not None

    @pytest.mark.asyncio
    async def test_app_tracer(self, tracer_fixture: TracingSensor):
        assert tracer_fixture.app_tracer is not None

    @pytest.mark.asyncio
    async def test_on_message_in_with_context(
        self, tracer_fixture: TracingSensor, topic_fixture
    ):
        tracer_fixture.app_tracer.extract = MagicMock()
        tracer_fixture.app_tracer.start_span = MagicMock()
        message = MagicMock()
        message.headers = [("key", "value")]
        tracer_fixture.on_message_in(topic_fixture, 10, message)
        assert tracer_fixture.app_tracer.start_span.called
        assert tracer_fixture.app_tracer.extract.called

    @pytest.mark.asyncio
    async def test_on_message_without_context(
        self, tracer_fixture: TracingSensor, topic_fixture
    ):
        tracer_fixture.app_tracer.extract = MagicMock()
        tracer_fixture.app_tracer.start_span = MagicMock()
        message = MagicMock()
        message.headers = []
        tracer_fixture.on_message_in(topic_fixture, 10, message)
        assert tracer_fixture.app_tracer.start_span.called
        assert not tracer_fixture.app_tracer.extract.called

    @pytest.mark.asyncio
    async def test_on_stream_event_in(
        self, tracer_fixture: TracingSensor, event_fixture
    ):
        stream = MagicMock()
        opentracing.start_child_span = MagicMock()
        tracer_fixture.on_stream_event_in(MagicMock(), 10, stream, event_fixture)
        assert opentracing.start_child_span.called

    @pytest.mark.asyncio
    async def test_on_stream_event_out(
        self, tracer_fixture: TracingSensor, topic_fixture, event_fixture
    ):
        stream = MagicMock()
        span = MagicMock()
        span.finish = MagicMock()
        spans = MagicMock()
        spans.pop = MagicMock(return_value=span)
        event_fixture.message.stream_meta = {"stream_spans": spans}
        tracer_fixture.on_stream_event_out(topic_fixture, 10, stream, event_fixture)
        assert span.finish.called

    @pytest.mark.asyncio
    async def test_on_message_out(self, tracer_fixture: TracingSensor, topic_fixture):
        message = MagicMock()
        message.span = MagicMock()
        message.span.finish = MagicMock()
        tracer_fixture.on_message_out(topic_fixture, 10, message)
        assert message.span.finish.called

    @pytest.mark.asyncio
    async def test_on_send_initiated(self, tracer_fixture: TracingSensor):
        set_current_span(MagicMock())
        opentracing.start_child_span = MagicMock(return_value=MagicMock())
        assert (
            tracer_fixture.on_send_initiated(MagicMock(), "test", MagicMock(), 10, 10)
            is not None
        )

    @pytest.mark.asyncio
    async def test_on_send_completed(self, tracer_fixture: TracingSensor):
        state = MagicMock()
        span = MagicMock()
        span.finish = MagicMock()
        state.get = MagicMock(return_value=span)
        tracer_fixture.on_send_completed(MagicMock(), state, MagicMock())
        assert span.finish.called

    @pytest.mark.asyncio
    async def test_on_send_error(self, tracer_fixture: TracingSensor):
        state = MagicMock()
        span = MagicMock()
        span.finish = MagicMock()
        state.get = MagicMock(return_value=span)
        tracer_fixture.on_send_error(MagicMock(), MagicMock(), state)
        assert span.finish.called
