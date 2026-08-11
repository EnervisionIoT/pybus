import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

from confluent_kafka import KafkaError
from confluent_kafka.aio import AIOConsumer

from pybus.container.application import Application
from pybus.container.consumer import run_event_consumer
from pybus.container.transaction import TransactionContext
from pybus.domain.events import DomainEvent


# Named distinctly from tests.conftest.DummyEvent: TypeRegistryMixin._registry
# is a single dict shared by class name across the whole process, so a
# same-named class here would clobber conftest's registry entry for
# "DummyEvent" and break unrelated deserialization tests elsewhere in the
# suite (e.g. test_sqlalchemy.py's isinstance(event, DummyEvent) check).
class ConsumerDummyEvent(DomainEvent):
    value: str = "x"


def _fake_message(value: bytes | None = None, error: KafkaError | None = None):
    msg = MagicMock()
    msg.error.return_value = error
    msg.value.return_value = value
    return msg


async def test_run_event_consumer_subscribes_to_the_given_topic():
    consumer = MagicMock(spec=AIOConsumer)
    consumer.subscribe = AsyncMock()
    consumer.poll = AsyncMock(return_value=None)
    consumer.close = AsyncMock()
    application = MagicMock(spec=Application)
    stop_event = asyncio.Event()
    stop_event.set()

    await run_event_consumer(application, consumer, topic="domain_events", stop_event=stop_event)

    consumer.subscribe.assert_awaited_once_with(["domain_events"])


async def test_run_event_consumer_defaults_to_the_domain_events_topic():
    consumer = MagicMock(spec=AIOConsumer)
    consumer.subscribe = AsyncMock()
    consumer.poll = AsyncMock(return_value=None)
    consumer.close = AsyncMock()
    application = MagicMock(spec=Application)
    stop_event = asyncio.Event()
    stop_event.set()

    await run_event_consumer(application, consumer, stop_event=stop_event)

    consumer.subscribe.assert_awaited_once_with([TransactionContext.DOMAIN_EVENTS_TOPIC])


async def test_run_event_consumer_deserializes_and_executes_then_commits():
    event = ConsumerDummyEvent(
        aggregate_id=__import__("uuid").uuid4(), aggregate_type="Dummy", value="hi"
    )
    payload = json.dumps(event.model_dump(mode="json")).encode("utf-8")
    msg = _fake_message(value=payload)

    consumer = MagicMock(spec=AIOConsumer)
    consumer.subscribe = AsyncMock()
    consumer.close = AsyncMock()
    consumer.commit = AsyncMock()

    calls = {"n": 0}

    async def fake_poll(timeout):
        calls["n"] += 1
        if calls["n"] == 1:
            return msg
        stop_event.set()
        return None

    consumer.poll = fake_poll
    application = MagicMock(spec=Application)
    application.execute = AsyncMock()
    stop_event = asyncio.Event()

    await run_event_consumer(application, consumer, stop_event=stop_event, poll_timeout=0.01)

    application.execute.assert_awaited_once()
    executed_event = application.execute.call_args.args[0]
    assert isinstance(executed_event, ConsumerDummyEvent)
    assert executed_event.value == "hi"
    consumer.commit.assert_awaited_once_with(message=msg, asynchronous=False)


async def test_run_event_consumer_skips_none_polls_without_erroring():
    consumer = MagicMock(spec=AIOConsumer)
    consumer.subscribe = AsyncMock()
    consumer.close = AsyncMock()

    calls = {"n": 0}

    async def fake_poll(timeout):
        calls["n"] += 1
        if calls["n"] >= 3:
            stop_event.set()
        return None

    consumer.poll = fake_poll
    application = MagicMock(spec=Application)
    application.execute = AsyncMock()
    stop_event = asyncio.Event()

    await run_event_consumer(application, consumer, stop_event=stop_event, poll_timeout=0.01)

    application.execute.assert_not_awaited()


async def test_run_event_consumer_skips_messages_with_a_kafka_error():
    error = MagicMock(spec=KafkaError)
    msg = _fake_message(error=error)

    consumer = MagicMock(spec=AIOConsumer)
    consumer.subscribe = AsyncMock()
    consumer.close = AsyncMock()
    consumer.commit = AsyncMock()

    calls = {"n": 0}

    async def fake_poll(timeout):
        calls["n"] += 1
        if calls["n"] == 1:
            return msg
        stop_event.set()
        return None

    consumer.poll = fake_poll
    application = MagicMock(spec=Application)
    application.execute = AsyncMock()
    stop_event = asyncio.Event()

    await run_event_consumer(application, consumer, stop_event=stop_event, poll_timeout=0.01)

    application.execute.assert_not_awaited()
    consumer.commit.assert_not_awaited()


async def test_run_event_consumer_logs_and_continues_when_a_handler_raises(caplog):
    event = ConsumerDummyEvent(
        aggregate_id=__import__("uuid").uuid4(), aggregate_type="Dummy", value="hi"
    )
    payload = json.dumps(event.model_dump(mode="json")).encode("utf-8")
    msg = _fake_message(value=payload)

    consumer = MagicMock(spec=AIOConsumer)
    consumer.subscribe = AsyncMock()
    consumer.close = AsyncMock()
    consumer.commit = AsyncMock()

    calls = {"n": 0}

    async def fake_poll(timeout):
        calls["n"] += 1
        if calls["n"] == 1:
            return msg
        stop_event.set()
        return None

    consumer.poll = fake_poll
    application = MagicMock(spec=Application)
    application.execute = AsyncMock(side_effect=ValueError("handler blew up"))
    stop_event = asyncio.Event()

    with caplog.at_level("ERROR"):
        await run_event_consumer(application, consumer, stop_event=stop_event, poll_timeout=0.01)

    assert "handler blew up" in caplog.text
    # A message whose handler raised is still committed -- v1 has no
    # dead-letter queue or retry policy (see the design spec's Risks
    # section); the alternative (never commit) would wedge the consumer on
    # the same poisoned message forever.
    consumer.commit.assert_awaited_once_with(message=msg, asynchronous=False)


async def test_run_event_consumer_closes_the_consumer_on_exit():
    consumer = MagicMock(spec=AIOConsumer)
    consumer.subscribe = AsyncMock()
    consumer.poll = AsyncMock(return_value=None)
    consumer.close = AsyncMock()
    application = MagicMock(spec=Application)
    stop_event = asyncio.Event()
    stop_event.set()

    await run_event_consumer(application, consumer, stop_event=stop_event)

    consumer.close.assert_awaited_once()
