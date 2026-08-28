import asyncio
import json
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest
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


async def test_run_event_consumer_retries_a_handler_that_fails_once(caplog):
    """A transient failure costs a backoff, not the message."""
    event = ConsumerDummyEvent(aggregate_id=uuid.uuid4(), aggregate_type="Dummy", value="hi")
    msg = _fake_message(value=json.dumps(event.model_dump(mode="json")).encode("utf-8"))

    consumer = MagicMock(spec=AIOConsumer)
    consumer.subscribe = AsyncMock()
    consumer.close = AsyncMock()
    consumer.commit = AsyncMock()

    polls = {"n": 0}

    async def fake_poll(timeout):
        polls["n"] += 1
        if polls["n"] == 1:
            return msg
        stop_event.set()
        return None

    consumer.poll = fake_poll
    application = MagicMock(spec=Application)
    application.execute = AsyncMock(side_effect=[ValueError("blip"), None])
    stop_event = asyncio.Event()

    await run_event_consumer(
        application, consumer, stop_event=stop_event, poll_timeout=0.01, retry_backoff=0
    )

    assert application.execute.await_count == 2
    consumer.commit.assert_awaited_once_with(message=msg, asynchronous=False)


async def test_run_event_consumer_does_not_commit_a_message_it_could_not_handle(caplog):
    """The regression guard for silent data loss.

    This loop used to log the failure and commit anyway, so one transient
    database error meant the message was never seen again. Kafka offsets are
    monotonic -- committing a later message would implicitly commit this one
    -- so the only alternative to losing it is to stop.
    """
    event = ConsumerDummyEvent(aggregate_id=uuid.uuid4(), aggregate_type="Dummy", value="hi")
    msg = _fake_message(value=json.dumps(event.model_dump(mode="json")).encode("utf-8"))

    consumer = MagicMock(spec=AIOConsumer)
    consumer.subscribe = AsyncMock()
    consumer.close = AsyncMock()
    consumer.commit = AsyncMock()

    polls = {"n": 0}

    async def fake_poll(timeout):
        # Bounded on purpose. An unbounded poll would let a regression here
        # hang the suite instead of failing it, and a hanging test says
        # nothing.
        polls["n"] += 1
        if polls["n"] == 1:
            return msg
        stop_event.set()
        return None

    consumer.poll = fake_poll
    application = MagicMock(spec=Application)
    application.execute = AsyncMock(side_effect=ValueError("handler blew up"))
    stop_event = asyncio.Event()

    with pytest.raises(ValueError, match="handler blew up"):
        await run_event_consumer(
            application,
            consumer,
            stop_event=stop_event,
            poll_timeout=0.01,
            max_attempts=3,
            retry_backoff=0,
        )

    assert application.execute.await_count == 3
    consumer.commit.assert_not_awaited()
    # Still tidied up, so the offset stays where it was.
    consumer.close.assert_awaited_once()


async def test_run_event_consumer_discards_a_message_it_cannot_parse(caplog):
    """Poison, and the one case where committing past it is right: another
    attempt reads the same bytes and fails the same way. One malformed
    message must not wedge the loop."""
    broken = _fake_message(value=b"{not json at all")
    good_event = ConsumerDummyEvent(aggregate_id=uuid.uuid4(), aggregate_type="Dummy", value="hi")
    good = _fake_message(value=json.dumps(good_event.model_dump(mode="json")).encode("utf-8"))

    consumer = MagicMock(spec=AIOConsumer)
    consumer.subscribe = AsyncMock()
    consumer.close = AsyncMock()
    consumer.commit = AsyncMock()

    polls = {"n": 0}

    async def fake_poll(timeout):
        polls["n"] += 1
        if polls["n"] == 1:
            return broken
        if polls["n"] == 2:
            return good
        stop_event.set()
        return None

    consumer.poll = fake_poll
    application = MagicMock(spec=Application)
    application.execute = AsyncMock()
    stop_event = asyncio.Event()

    with caplog.at_level("ERROR"):
        await run_event_consumer(application, consumer, stop_event=stop_event, poll_timeout=0.01)

    assert "unreadable" in caplog.text
    # Both committed: the broken one to get past it, the good one on success.
    assert consumer.commit.await_count == 2
    # And the loop carried on to the message behind it.
    application.execute.assert_awaited_once()


async def test_run_event_consumer_stops_promptly_when_asked_to_mid_retry():
    """A shutdown must not have to sit through the backoff, and must not be
    reported as a failure -- the offset is uncommitted either way, so the
    message comes back."""
    event = ConsumerDummyEvent(aggregate_id=uuid.uuid4(), aggregate_type="Dummy", value="hi")
    msg = _fake_message(value=json.dumps(event.model_dump(mode="json")).encode("utf-8"))

    consumer = MagicMock(spec=AIOConsumer)
    consumer.subscribe = AsyncMock()
    consumer.close = AsyncMock()
    consumer.commit = AsyncMock()

    stop_event = asyncio.Event()
    polls = {"n": 0}

    async def fake_poll(timeout):
        polls["n"] += 1
        if polls["n"] == 1:
            return msg
        stop_event.set()
        return None

    consumer.poll = fake_poll

    async def fail_then_ask_to_stop(*_args, **_kwargs):
        stop_event.set()
        raise ValueError("blip")

    application = MagicMock(spec=Application)
    application.execute = AsyncMock(side_effect=fail_then_ask_to_stop)

    # An hour of backoff that must never be waited on.
    await run_event_consumer(
        application,
        consumer,
        stop_event=stop_event,
        poll_timeout=0.01,
        max_attempts=5,
        retry_backoff=3600,
    )

    consumer.commit.assert_not_awaited()
    consumer.close.assert_awaited_once()


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


async def test_run_event_consumer_dispatches_inside_the_events_tenant_context():
    """Without this a handler behind a row-level security policy has no
    context to write into: the insert is refused, the select comes back
    empty, and nothing is raised to say why."""
    tenant_id = uuid.uuid4()
    event = ConsumerDummyEvent(
        aggregate_id=uuid.uuid4(), aggregate_type="Dummy", value="hi", tenant_id=tenant_id
    )
    msg = _fake_message(value=json.dumps(event.model_dump(mode="json")).encode("utf-8"))

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

    assert application.execute.call_args.kwargs["tenant_id"] == tenant_id


async def test_run_event_consumer_passes_no_tenant_when_the_event_carries_none():
    """A service with no tenants is the normal case for this, and `execute`
    already treats None as "do not set a context"."""
    event = ConsumerDummyEvent(aggregate_id=uuid.uuid4(), aggregate_type="Dummy", value="hi")
    msg = _fake_message(value=json.dumps(event.model_dump(mode="json")).encode("utf-8"))

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

    assert application.execute.call_args.kwargs["tenant_id"] is None
