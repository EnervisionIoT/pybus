import asyncio
import json
import logging
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest
from confluent_kafka.aio import AIOProducer

from pybus.container import outbox
from pybus.container.outbox import (
    publish_rows,
    run_outbox_relay,
    run_outbox_relay_for,
    wire_value,
)
from pybus.container.transaction import TransactionContext
from tests.conftest import DummyEvent


def row_for(event: DummyEvent, correlation_id: uuid.UUID) -> dict:
    """The row `save_domain_events` writes for `event`, as the relay reads
    it back -- envelope in columns, the rest in `payload`, plus the column
    the relay itself owns."""
    return {
        "id": event.id,
        "tenant_id": event.tenant_id,
        "correlation_id": correlation_id,
        "aggregate_id": event.aggregate_id,
        "aggregate_type": event.aggregate_type,
        "message_type": event.message_type,
        "occurred_on": event.occurred_on,
        "version": event.version,
        "created_by_id": event.created_by_id,
        "payload": event.payload,
        "published_at": None,
    }


def acknowledged(error: Exception | None = None) -> asyncio.Future:
    future = asyncio.get_running_loop().create_future()
    if error is None:
        future.set_result(None)
    else:
        future.set_exception(error)
    return future


def producer_acknowledging(*errors: Exception | None) -> AsyncMock:
    """A producer whose n-th `produce` is acknowledged, or refused with
    `errors[n]`. With no arguments every message is acknowledged."""
    producer = AsyncMock(spec=AIOProducer)
    outcomes = iter(errors)
    producer.produce.side_effect = lambda **_: acknowledged(next(outcomes, None))
    return producer


# --- wire_value ---------------------------------------------------------------


def expected_wire(event: DummyEvent, correlation_id: uuid.UUID) -> dict:
    event.correlation_id = correlation_id
    return json.loads(event.model_dump_json())


def test_wire_value_matches_model_dump_json():
    """Consumers deserialize what `publish_event` used to send. The relay
    rebuilds it from the row, and has to rebuild it exactly."""
    correlation_id = uuid.uuid4()
    event = DummyEvent(
        aggregate_id=uuid.uuid4(),
        aggregate_type="DummyThing",
        version=3,
        created_by_id=uuid.uuid4(),
        tenant_id=uuid.uuid4(),
        payload_value="hello",
    )

    assert json.loads(wire_value(row_for(event, correlation_id))) == expected_wire(
        event, correlation_id
    )


def test_wire_value_matches_model_dump_json_with_nulls():
    correlation_id = uuid.uuid4()
    event = DummyEvent(aggregate_id=uuid.uuid4(), aggregate_type="DummyThing")

    assert json.loads(wire_value(row_for(event, correlation_id))) == expected_wire(
        event, correlation_id
    )


def test_wire_value_matches_model_dump_json_with_non_ascii_payload():
    correlation_id = uuid.uuid4()
    event = DummyEvent(
        aggregate_id=uuid.uuid4(), aggregate_type="DummyThing", payload_value="台北市信義區"
    )

    assert json.loads(wire_value(row_for(event, correlation_id))) == expected_wire(
        event, correlation_id
    )


def test_wire_value_leaves_out_the_relays_own_column():
    row = row_for(DummyEvent(aggregate_id=uuid.uuid4(), aggregate_type="X"), uuid.uuid4())

    assert "published_at" not in json.loads(wire_value(row))


# --- publish_rows -------------------------------------------------------------


async def test_publish_rows_produces_each_row_keyed_on_its_aggregate():
    rows = [
        row_for(DummyEvent(aggregate_id=uuid.uuid4(), aggregate_type="X"), uuid.uuid4())
        for _ in range(2)
    ]
    producer = producer_acknowledging()

    delivered = await publish_rows(producer, rows)

    assert delivered == [row["id"] for row in rows]
    for call, row in zip(producer.produce.call_args_list, rows, strict=True):
        assert call.kwargs["topic"] == TransactionContext.DOMAIN_EVENTS_TOPIC
        assert call.kwargs["key"] == str(row["aggregate_id"]).encode("utf-8")
        assert call.kwargs["value"] == wire_value(row)
    producer.flush.assert_awaited_once()


async def test_publish_rows_returns_only_what_the_broker_acknowledged():
    rows = [
        row_for(DummyEvent(aggregate_id=uuid.uuid4(), aggregate_type="X"), uuid.uuid4())
        for _ in range(3)
    ]
    producer = producer_acknowledging(None, RuntimeError("refused"), None)

    delivered = await publish_rows(producer, rows)

    assert delivered == [rows[0]["id"], rows[2]["id"]]


async def test_a_delivery_still_pending_after_the_grace_is_not_marked(monkeypatch):
    monkeypatch.setattr(outbox, "DELIVERY_GRACE_SECONDS", 0.01)
    row = row_for(DummyEvent(aggregate_id=uuid.uuid4(), aggregate_type="X"), uuid.uuid4())
    pending = asyncio.get_running_loop().create_future()
    producer = AsyncMock(spec=AIOProducer)
    producer.produce.side_effect = lambda **_: pending

    assert await publish_rows(producer, [row]) == []

    # A refusal arriving after the round has moved on must be retrieved by
    # the relay, not reported by asyncio as never retrieved.
    pending.set_exception(RuntimeError("late refusal"))
    await asyncio.sleep(0)
    assert pending.done()


async def test_publish_rows_with_nothing_to_send_touches_no_producer():
    producer = AsyncMock(spec=AIOProducer)

    assert await publish_rows(producer, []) == []
    producer.produce.assert_not_called()
    producer.flush.assert_not_called()


# --- run_outbox_relay ---------------------------------------------------------


def rounds(*counts: int | Exception, then_stop: asyncio.Event) -> AsyncMock:
    """A relay round reporting `counts` in turn; sets `then_stop` on the last."""
    remaining = list(counts)

    async def round_() -> int:
        outcome = remaining.pop(0)
        if not remaining:
            then_stop.set()
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    return AsyncMock(side_effect=round_)


async def test_a_full_batch_is_followed_at_once_by_another_round():
    stop = asyncio.Event()
    relay_round = rounds(100, 100, 3, then_stop=stop)

    await asyncio.wait_for(
        run_outbox_relay(
            relay_round,
            AsyncMock(spec=AIOProducer),
            asyncio.Event(),
            stop,
            batch_size=100,
            idle_interval=60,
        ),
        timeout=2,
    )

    assert relay_round.await_count == 3


async def test_a_wake_during_a_round_earns_another_round_at_once():
    stop = asyncio.Event()
    wakeup = asyncio.Event()
    calls = 0

    async def relay_round() -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            wakeup.set()  # a transaction commits while this round runs
        else:
            stop.set()
        return 0

    await asyncio.wait_for(
        run_outbox_relay(relay_round, AsyncMock(spec=AIOProducer), wakeup, stop, idle_interval=60),
        timeout=2,
    )

    assert calls == 2


async def test_a_wake_while_idle_starts_a_round():
    stop = asyncio.Event()
    wakeup = asyncio.Event()
    calls = 0

    async def relay_round() -> int:
        nonlocal calls
        calls += 1
        if calls == 2:
            stop.set()
        return 0

    task = asyncio.create_task(
        run_outbox_relay(relay_round, AsyncMock(spec=AIOProducer), wakeup, stop, idle_interval=60)
    )
    await asyncio.sleep(0.05)
    wakeup.set()
    await asyncio.wait_for(task, timeout=2)

    assert calls == 2


async def test_a_failing_round_is_logged_and_the_relay_carries_on(caplog):
    stop = asyncio.Event()
    relay_round = rounds(RuntimeError("database gone"), 0, then_stop=stop)

    with caplog.at_level(logging.ERROR, logger="pybus.container.outbox"):
        await asyncio.wait_for(
            run_outbox_relay(
                relay_round,
                AsyncMock(spec=AIOProducer),
                asyncio.Event(),
                stop,
                error_backoff=0.01,
            ),
            timeout=2,
        )

    assert relay_round.await_count == 2
    assert "database gone" in caplog.text


async def test_a_failing_round_backs_off_instead_of_spinning():
    stop = asyncio.Event()
    wakeup = asyncio.Event()
    relay_round = AsyncMock(side_effect=RuntimeError("broker down"))

    task = asyncio.create_task(
        run_outbox_relay(
            relay_round,
            AsyncMock(spec=AIOProducer),
            wakeup,
            stop,
            error_backoff=60,
            idle_interval=60,
        )
    )
    await asyncio.sleep(0.05)
    wakeup.set()  # a commit during an outage must not cut the back-off short
    await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(task, timeout=2)

    assert relay_round.await_count == 1


async def test_stop_while_idle_returns_promptly():
    stop = asyncio.Event()
    task = asyncio.create_task(
        run_outbox_relay(
            AsyncMock(return_value=0),
            AsyncMock(spec=AIOProducer),
            asyncio.Event(),
            stop,
            idle_interval=60,
        )
    )
    await asyncio.sleep(0.05)
    stop.set()

    await asyncio.wait_for(task, timeout=2)


async def test_stop_flushes_with_a_timeout():
    stop = asyncio.Event()
    producer = AsyncMock(spec=AIOProducer)

    await asyncio.wait_for(
        run_outbox_relay(
            rounds(0, then_stop=stop), producer, asyncio.Event(), stop, flush_timeout=7.0
        ),
        timeout=2,
    )

    producer.flush.assert_awaited_once_with(7.0)


async def test_a_failing_shutdown_flush_does_not_raise():
    stop = asyncio.Event()
    producer = AsyncMock(spec=AIOProducer)
    producer.flush.side_effect = RuntimeError("broker down")

    await asyncio.wait_for(
        run_outbox_relay(rounds(0, then_stop=stop), producer, asyncio.Event(), stop),
        timeout=2,
    )


# --- run_outbox_relay_for -----------------------------------------------------


@pytest.mark.parametrize("schema", ["Iam", "iam; DROP TABLE x", "iam.events", ""])
async def test_run_outbox_relay_for_refuses_a_schema_it_would_have_to_quote(schema):
    """The schema is interpolated into SQL. A name that is not a plain
    lowercase identifier stops the service at start, not every round."""
    container = MagicMock()
    container.config.return_value.POSTGRES_SCHEMA = schema

    with pytest.raises(ValueError, match="POSTGRES_SCHEMA"):
        await run_outbox_relay_for(container, asyncio.Event())
