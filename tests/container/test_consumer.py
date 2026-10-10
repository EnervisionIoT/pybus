import asyncio
import json
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from confluent_kafka import KafkaError
from confluent_kafka.aio import AIOConsumer
from pydantic import BaseModel, ValidationError
from sqlalchemy.exc import IntegrityError

from pybus.container import consumer as consumer_module
from pybus.container.application import Application
from pybus.container.consumer import run_event_consumer, run_event_consumer_for
from pybus.container.dead_letters import DeadLetter
from pybus.container.transaction import TransactionContext
from pybus.domain.events import DomainEvent


# Named distinctly from tests.conftest.DummyEvent: TypeRegistryMixin._registry
# is a single dict shared by class name across the whole process, so a
# same-named class here would clobber conftest's registry entry for
# "DummyEvent" and break unrelated deserialization tests elsewhere in the
# suite (e.g. test_sqlalchemy.py's isinstance(event, DummyEvent) check).
class ConsumerDummyEvent(DomainEvent):
    value: str = "x"


ENGINE = MagicMock(name="engine")


@pytest.fixture(autouse=True)
def table(monkeypatch):
    """Stands in for `dead_letters`: what the consumer would have written,
    and -- in `log` -- the order writes and commits happened in."""
    state = SimpleNamespace(recorded=[], log=[], installed=AsyncMock())

    async def record(engine, schema, letter):
        state.log.append("record")
        state.recorded.append(letter)

    state.record = AsyncMock(side_effect=record)
    monkeypatch.setattr(consumer_module, "require_dead_letters_installed", state.installed)
    monkeypatch.setattr(consumer_module, "record_dead_letter", state.record)
    return state


def raw_message(
    value: bytes | None, *, offset: int = 7, error: KafkaError | None = None
) -> MagicMock:
    msg = MagicMock()
    msg.error.return_value = error
    msg.value.return_value = value
    msg.topic.return_value = "domain_events"
    msg.partition.return_value = 0
    msg.offset.return_value = offset
    msg.key.return_value = b"aggregate"
    return msg


def event_message(*, offset: int = 7, **fields) -> tuple[ConsumerDummyEvent, MagicMock]:
    event = ConsumerDummyEvent(aggregate_id=uuid.uuid4(), aggregate_type="Dummy", **fields)
    return event, raw_message(event.model_dump_json().encode(), offset=offset)


def kafka(*messages, stop_event: asyncio.Event, log: list[str] | None = None) -> MagicMock:
    """Hands over `messages` one poll at a time, then sets `stop_event`.

    Bounded on purpose: an unbounded poll would let a regression here hang
    the suite instead of failing it, and a hanging test says nothing."""
    consumer = MagicMock(spec=AIOConsumer)
    consumer.subscribe = AsyncMock()
    consumer.close = AsyncMock()

    def commit(**_):
        if log is not None:
            log.append("commit")

    consumer.commit = AsyncMock(side_effect=commit)
    queue = list(messages)

    async def poll(_timeout):
        if queue:
            return queue.pop(0)
        stop_event.set()
        return None

    consumer.poll = poll
    return consumer


def application_executing(side_effect=None) -> MagicMock:
    application = MagicMock(spec=Application)
    application.execute = AsyncMock(side_effect=side_effect)
    return application


async def consume(application, consumer, stop_event, **overrides):
    options = {
        "engine": ENGINE,
        "schema": "svc",
        "stop_event": stop_event,
        "poll_timeout": 0.01,
        "retry_backoff": (0.0, 0.0),
        "record_backoff": 0.0,
    }
    options.update(overrides)
    await asyncio.wait_for(run_event_consumer(application, consumer, **options), timeout=5)


# --- the ordinary path --------------------------------------------------------


async def test_subscribes_to_the_given_topic():
    stop = asyncio.Event()
    stop.set()
    consumer = kafka(stop_event=stop)

    await consume(application_executing(), consumer, stop, topic="elsewhere")

    consumer.subscribe.assert_awaited_once_with(["elsewhere"])


async def test_defaults_to_the_domain_events_topic():
    stop = asyncio.Event()
    stop.set()
    consumer = kafka(stop_event=stop)

    await consume(application_executing(), consumer, stop)

    consumer.subscribe.assert_awaited_once_with([TransactionContext.DOMAIN_EVENTS_TOPIC])


async def test_deserializes_executes_and_commits_and_writes_no_dead_letter(table):
    stop = asyncio.Event()
    _, msg = event_message(value="hi")
    consumer = kafka(msg, stop_event=stop)
    application = application_executing()

    await consume(application, consumer, stop)

    executed = application.execute.call_args.args[0]
    assert isinstance(executed, ConsumerDummyEvent)
    assert executed.value == "hi"
    consumer.commit.assert_awaited_once_with(message=msg, asynchronous=False)
    assert table.recorded == []


async def test_an_empty_poll_is_skipped_without_erroring():
    stop = asyncio.Event()
    application = application_executing()

    await consume(application, kafka(stop_event=stop), stop)

    application.execute.assert_not_awaited()


async def test_a_message_carrying_a_kafka_error_is_skipped(table):
    stop = asyncio.Event()
    msg = raw_message(None, error=MagicMock(spec=KafkaError))
    consumer = kafka(msg, stop_event=stop)
    application = application_executing()

    await consume(application, consumer, stop)

    application.execute.assert_not_awaited()
    consumer.commit.assert_not_awaited()
    assert table.recorded == []


async def test_dispatches_inside_the_events_tenant_context():
    """Without this a handler behind a row-level security policy has no
    context to write into: the insert is refused, the select comes back
    empty, and nothing is raised to say why."""
    stop = asyncio.Event()
    tenant_id = uuid.uuid4()
    _, msg = event_message(tenant_id=tenant_id)
    application = application_executing()

    await consume(application, kafka(msg, stop_event=stop), stop)

    assert application.execute.call_args.kwargs["tenant_id"] == tenant_id


async def test_passes_no_tenant_when_the_event_carries_none():
    """A service with no tenants is the normal case for this, and `execute`
    already treats None as "do not set a context"."""
    stop = asyncio.Event()
    _, msg = event_message()
    application = application_executing()

    await consume(application, kafka(msg, stop_event=stop), stop)

    assert application.execute.call_args.kwargs["tenant_id"] is None


async def test_closes_the_consumer_on_exit():
    stop = asyncio.Event()
    stop.set()
    consumer = kafka(stop_event=stop)

    await consume(application_executing(), consumer, stop)

    consumer.close.assert_awaited_once()


async def test_a_handler_that_succeeds_and_asks_to_stop_is_committed():
    """A shutdown asked for from inside a handler is an orderly exit: the
    message in hand is finished and committed, and the consumer closed."""
    stop = asyncio.Event()
    _, msg = event_message()
    consumer = kafka(msg, stop_event=stop)

    async def succeed_then_ask_to_stop(*_args, **_kwargs):
        stop.set()

    await consume(application_executing(succeed_then_ask_to_stop), consumer, stop)

    consumer.commit.assert_awaited_once_with(message=msg, asynchronous=False)
    consumer.close.assert_awaited_once()


# --- retries ------------------------------------------------------------------


async def test_a_handler_that_fails_then_succeeds_is_retried_in_place(table):
    stop = asyncio.Event()
    _, msg = event_message()
    consumer = kafka(msg, stop_event=stop)
    application = application_executing([ValueError("blip"), ValueError("blip"), None])

    await consume(application, consumer, stop)

    assert application.execute.await_count == 3
    assert table.recorded == []
    consumer.commit.assert_awaited_once_with(message=msg, asynchronous=False)


async def test_retries_wait_out_the_backoff_between_attempts(monkeypatch):
    sleeps: list[float] = []

    async def sleep(timeout, *_events):
        sleeps.append(timeout)

    monkeypatch.setattr(consumer_module, "interruptible_sleep", sleep)
    stop = asyncio.Event()
    _, msg = event_message()

    await asyncio.wait_for(
        run_event_consumer(
            application_executing(ValueError("blip")),
            kafka(msg, stop_event=stop),
            engine=ENGINE,
            schema="svc",
            stop_event=stop,
            poll_timeout=0.01,
        ),
        timeout=5,
    )

    assert sleeps == [1.0, 2.0]


# --- dead letters -------------------------------------------------------------


async def test_a_handler_that_keeps_failing_is_kept_before_its_offset_is_committed(table):
    stop = asyncio.Event()
    event, msg = event_message(offset=41)
    consumer = kafka(msg, stop_event=stop, log=table.log)
    application = application_executing(ValueError("handler blew up"))

    await consume(application, consumer, stop)

    assert application.execute.await_count == 3
    assert table.recorded == [
        DeadLetter(
            topic="domain_events",
            partition=0,
            offset=41,
            key=b"aggregate",
            value=msg.value.return_value,
            message_type="ConsumerDummyEvent",
            event_id=event.id,
            error="ValueError: handler blew up",
            attempts=3,
        )
    ]
    assert table.log == ["record", "commit"]
    assert table.record.call_args.args[:2] == (ENGINE, "svc")


async def test_the_loop_carries_on_past_a_dead_letter(table):
    stop = asyncio.Event()
    _, failing = event_message(offset=1)
    _, good = event_message(offset=2)
    consumer = kafka(failing, good, stop_event=stop)
    application = application_executing([ValueError("a"), ValueError("a"), ValueError("a"), None])

    await consume(application, consumer, stop)

    assert [letter.offset for letter in table.recorded] == [1]
    assert application.execute.await_count == 4
    assert consumer.commit.await_count == 2


async def test_an_unreadable_message_is_a_dead_letter_at_once(table):
    """Not retried: the same bytes are read the same way every time."""
    stop = asyncio.Event()
    broken = raw_message(b"{not json at all", offset=1)
    _, good = event_message(offset=2)
    consumer = kafka(broken, good, stop_event=stop)
    application = application_executing()

    await consume(application, consumer, stop)

    [letter] = table.recorded
    assert (letter.offset, letter.attempts, letter.message_type, letter.event_id) == (
        1,
        1,
        None,
        None,
    )
    assert letter.value == b"{not json at all"
    assert letter.error.startswith("JSONDecodeError: ")
    application.execute.assert_awaited_once()
    assert consumer.commit.await_count == 2


async def test_a_message_nested_too_deep_to_read_is_a_dead_letter_at_once(table):
    """json.loads raises RecursionError, not a ValueError, on a deep enough
    payload; one hostile message must not take the consumer down."""
    stop = asyncio.Event()
    consumer = kafka(raw_message(b"[" * 100_000), stop_event=stop)

    await consume(application_executing(), consumer, stop)

    [letter] = table.recorded
    assert (letter.attempts, letter.message_type, letter.event_id) == (1, None, None)
    assert letter.error.startswith("RecursionError")
    consumer.commit.assert_awaited_once()


@pytest.mark.parametrize(
    "value",
    [b"[1, 2]", b"{}", b'{"message_type": "ConsumerDummyEvent", "id": "not-a-uuid"}'],
    ids=["a-list", "no-message-type", "not-an-event"],
)
async def test_valid_json_that_is_not_an_event_is_a_dead_letter_at_once(table, value):
    stop = asyncio.Event()
    consumer = kafka(raw_message(value), stop_event=stop)
    application = application_executing()

    await consume(application, consumer, stop)

    [letter] = table.recorded
    assert letter.attempts == 1
    application.execute.assert_not_awaited()
    consumer.commit.assert_awaited_once()


async def test_a_message_with_no_value_is_kept_as_an_empty_one(table):
    """`value` is NOT NULL; a None written there would fail the record on
    every try and hold the consumer in its back-off for good."""
    stop = asyncio.Event()
    consumer = kafka(raw_message(None), stop_event=stop)

    await consume(application_executing(), consumer, stop)

    [letter] = table.recorded
    assert (letter.value, letter.attempts) == (b"", 1)
    consumer.commit.assert_awaited_once()


async def test_neither_the_log_nor_the_table_ever_holds_the_value(table, caplog):
    stop = asyncio.Event()
    _, handler_fails = event_message(offset=1, value="secret-token-XYZ")
    # Fails validation, and a ValidationError quotes the input it refused.
    unreadable = raw_message(
        json.dumps(
            {
                "message_type": "ConsumerDummyEvent",
                "aggregate_id": "secret-token-XYZ",
                "aggregate_type": "Dummy",
            }
        ).encode(),
        offset=2,
    )

    with caplog.at_level("DEBUG"):
        await consume(
            application_executing(ValueError("blip")),
            kafka(handler_fails, unreadable, stop_event=stop),
            stop,
        )

    assert len(table.recorded) == 2
    assert "secret-token-XYZ" not in caplog.text
    assert all("secret-token-XYZ" not in letter.error for letter in table.recorded)


def _validation_error_quoting_the_secret() -> Exception:
    class Strict(BaseModel):
        count: int

    try:
        Strict.model_validate({"count": "secret-token-XYZ"})
    except ValidationError as error:
        return error
    raise AssertionError("unreachable")


def _statement_error_quoting_the_secret() -> Exception:
    return IntegrityError(
        "INSERT INTO x VALUES (%(v)s)", {"v": "secret-token-XYZ"}, Exception("duplicate key")
    )


@pytest.mark.parametrize("via", ["cause", "context"])
@pytest.mark.parametrize(
    "inner",
    [_validation_error_quoting_the_secret, _statement_error_quoting_the_secret],
    ids=["validation-error", "statement-error"],
)
async def test_a_wrapped_error_does_not_bring_the_value_back_through_its_chain(caplog, inner, via):
    """The traceback formatter prints __cause__ and __context__, so an
    exception that wraps a ValidationError or StatementError would log the
    input that error quotes."""

    async def wrap(*_args, **_kwargs):
        if via == "cause":
            raise RuntimeError("wrapped") from inner()
        try:
            raise inner()
        except Exception:  # noqa: BLE001 -- the point is to wrap whatever came
            raise RuntimeError("wrapped")  # implicit __context__

    stop = asyncio.Event()
    _, msg = event_message()

    with caplog.at_level("DEBUG"):
        await consume(application_executing(wrap), kafka(msg, stop_event=stop), stop)

    assert "secret-token-XYZ" not in caplog.text


# --- a record that will not land, and shutdowns ---------------------------------


async def test_a_failed_record_is_retried_and_nothing_is_committed_until_it_lands(table):
    stop = asyncio.Event()
    tries = {"n": 0}

    async def flaky(engine, schema, letter):
        tries["n"] += 1
        table.log.append("record")
        if tries["n"] < 3:
            raise ConnectionError("database gone")

    table.record.side_effect = flaky
    _, msg = event_message()
    consumer = kafka(msg, stop_event=stop, log=table.log)

    await consume(application_executing(ValueError("blip")), consumer, stop)

    assert table.log == ["record", "record", "record", "commit"]


async def test_a_shutdown_while_the_record_keeps_failing_leaves_the_message_uncommitted(table):
    table.record.side_effect = ConnectionError("database gone")
    stop = asyncio.Event()
    _, msg = event_message()
    consumer = kafka(msg, stop_event=stop)

    task = asyncio.create_task(
        run_event_consumer(
            application_executing(ValueError("blip")),
            consumer,
            engine=ENGINE,
            schema="svc",
            stop_event=stop,
            poll_timeout=0.01,
            retry_backoff=(0.0, 0.0),
            record_backoff=60,
        )
    )
    await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(task, timeout=2)

    consumer.commit.assert_not_awaited()
    consumer.close.assert_awaited_once()


async def test_a_shutdown_during_the_retry_backoff_leaves_the_message_uncommitted(table):
    stop = asyncio.Event()
    _, msg = event_message()
    consumer = kafka(msg, stop_event=stop)

    async def fail_then_ask_to_stop(*_args, **_kwargs):
        stop.set()
        raise ValueError("blip")

    application = application_executing(fail_then_ask_to_stop)

    await consume(application, consumer, stop, retry_backoff=(60.0, 60.0))

    application.execute.assert_awaited_once()
    assert table.recorded == []
    consumer.commit.assert_not_awaited()
    consumer.close.assert_awaited_once()


# --- refusing to start ----------------------------------------------------------


async def test_refuses_to_start_without_the_dead_letters_migration(table):
    table.installed.side_effect = RuntimeError("svc.dead_letter_record does not exist")
    stop = asyncio.Event()
    consumer = kafka(stop_event=stop)

    with pytest.raises(RuntimeError, match="dead_letter_record"):
        await consume(application_executing(), consumer, stop)

    assert table.installed.call_args.args == (ENGINE, "svc")
    consumer.subscribe.assert_not_awaited()
    consumer.close.assert_awaited_once()


@pytest.mark.parametrize("schema", ["Iam", "iam; DROP TABLE x", ""])
async def test_refuses_a_schema_it_would_have_to_quote(schema):
    stop = asyncio.Event()
    consumer = kafka(stop_event=stop)

    with pytest.raises(ValueError, match="POSTGRES_SCHEMA"):
        await consume(application_executing(), consumer, stop, schema=schema)

    consumer.subscribe.assert_not_awaited()
    consumer.close.assert_awaited_once()


async def test_refuses_fewer_than_one_attempt():
    stop = asyncio.Event()
    consumer = kafka(stop_event=stop)

    with pytest.raises(ValueError, match="max_attempts"):
        await consume(application_executing(), consumer, stop, max_attempts=0)
    consumer.close.assert_awaited_once()


# --- run_event_consumer_for -----------------------------------------------------


async def test_run_event_consumer_for_wires_the_containers_own_parts(monkeypatch):
    run = AsyncMock()
    monkeypatch.setattr(consumer_module, "run_event_consumer", run)
    container = MagicMock()
    container.config.return_value.POSTGRES_SCHEMA = "iam"
    stop = asyncio.Event()

    await run_event_consumer_for(container, stop)

    run.assert_awaited_once_with(
        container.application.return_value,
        container.kafka_consumer.return_value,
        engine=container.engine.return_value,
        schema="iam",
        stop_event=stop,
    )
