"""The outbox relay: what moves committed `domain_events` rows onto Kafka.

Until this existed, the transaction's on-exit hook produced each event right
after its commit and dropped the delivery future. "Published" meant "handed
to librdkafka": a process that died after the commit, a broker that never
acknowledged, or a produce that raised -- logged and nothing else -- left
the row in the table and the topic without it, and nothing ever looked
again.

Now the row is the only thing a transaction writes, and this module is the
only thing that produces. A row is marked `published_at` only after the
broker has acknowledged it, inside the transaction that claimed it, so a
crash anywhere in between leaves it unmarked and the next round sends it
again. That is at-least-once, which every consumer already has to tolerate.
Order is best effort (`ORDER BY occurred_on` in the claim), not a promise.

It runs in each service's own process, against that service's own schema,
beside the gRPC server -- not as one relay over every schema, which would be
a single point of failure for the platform's whole event flow and would
need one credential able to read every service's events. The service's app
role reaches the rows only through two SECURITY DEFINER functions each
service's migration defines. See
docs/superpowers/specs/2026-10-08-outbox-relay-design.md in the platform
repository.
"""

import asyncio
import logging
import re
import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from functools import partial
from typing import TYPE_CHECKING, Any

from confluent_kafka.aio import AIOProducer
from pydantic_core import to_json
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from .transaction import TransactionContext

if TYPE_CHECKING:
    from .application import ApplicationContainer

logger = logging.getLogger(__name__)

BATCH_SIZE = 100
FLUSH_TIMEOUT_SECONDS = 10.0
DELIVERY_GRACE_SECONDS = 1.0
IDLE_INTERVAL_SECONDS = 5.0
ERROR_BACKOFF_SECONDS = 5.0

# What `DomainEvent.payload` leaves out, and the row keeps as columns
# instead. The wire format is both, flattened back into one object.
ENVELOPE_COLUMNS = (
    "id",
    "correlation_id",
    "aggregate_id",
    "aggregate_type",
    "message_type",
    "occurred_on",
    "version",
    "created_by_id",
    "tenant_id",
)

_PLAIN_IDENTIFIER = re.compile(r"[a-z_][a-z0-9_]*")


def wire_value(row: Mapping[str, Any]) -> bytes:
    """The message value for `row`: what `event.model_dump_json()` gave.

    Rebuilt from the columns and the payload, not by deserializing through
    the event class. That would need the class to still exist under the
    name the row recorded; a renamed or deleted event would then fail on
    every round and never leave the table. `to_json` is pydantic's own
    serializer, so a UUID or a naive datetime comes out exactly as it did.
    """
    envelope = {column: row[column] for column in ENVELOPE_COLUMNS}
    return to_json({**row["payload"], **envelope})


def _retrieve_outcome(future: asyncio.Future[Any]) -> None:
    # A refusal that lands after the round has moved on would otherwise be
    # reported by asyncio as "exception was never retrieved". The row is
    # already unmarked; there is nothing else to do with it.
    if not future.cancelled():
        future.exception()


async def publish_rows(
    producer: AIOProducer,
    rows: Sequence[Mapping[str, Any]],
    *,
    flush_timeout: float = FLUSH_TIMEOUT_SECONDS,
) -> list[uuid.UUID]:
    """Produce every row and return the ids the broker acknowledged.

    A row missing from the result is left unpublished and sent again by a
    later round -- whether the broker refused it or simply had not answered
    yet. Sending a message twice is the accepted cost; marking one that
    never arrived is the failure this module exists to remove.
    """
    if not rows:
        return []

    futures: list[asyncio.Future[Any]] = [
        await producer.produce(
            topic=TransactionContext.DOMAIN_EVENTS_TOPIC,
            key=str(row["aggregate_id"]).encode("utf-8"),
            value=wire_value(row),
        )
        for row in rows
    ]
    await producer.flush(flush_timeout)
    # Delivery reports arrive on librdkafka's thread and reach these futures
    # through the event loop, so a flush that has returned can leave some
    # not yet resolved. Without the wait, those rows would be sent again on
    # every round however healthy the broker was.
    await asyncio.wait(futures, timeout=DELIVERY_GRACE_SECONDS)

    delivered: list[uuid.UUID] = []
    for row, future in zip(rows, futures, strict=True):
        if not future.done():
            future.add_done_callback(_retrieve_outcome)
        elif future.cancelled() or future.exception() is not None:
            logger.warning(
                "Broker did not acknowledge %s id=%s; it stays unpublished",
                row["message_type"],
                row["id"],
            )
        else:
            delivered.append(row["id"])
    return delivered


def _plain_schema(schema: str) -> str:
    # Interpolated into SQL: a schema name cannot be a bind parameter.
    if not _PLAIN_IDENTIFIER.fullmatch(schema):
        raise ValueError(
            f"POSTGRES_SCHEMA {schema!r} is not a plain lowercase identifier; "
            "the outbox relay will not interpolate it into SQL"
        )
    return schema


async def relay_once(
    engine: AsyncEngine,
    producer: AIOProducer,
    schema: str,
    *,
    batch_size: int = BATCH_SIZE,
    flush_timeout: float = FLUSH_TIMEOUT_SECONDS,
) -> int:
    """One round: claim up to `batch_size` rows, send them, mark the ones
    the broker acknowledged. Returns how many were claimed, so the caller
    knows whether a full batch suggests more are waiting.

    Claim, produce and mark share one transaction because the claim's row
    locks are what keep another replica off these rows; they are released
    at commit, or at rollback when anything here raises, and either way an
    unmarked row is claimable again. The transaction is held open for the
    flush, which `flush_timeout` and the producer's `message.timeout.ms`
    bound together.
    """
    schema = _plain_schema(schema)
    async with AsyncSession(engine) as session, session.begin():
        result = await session.execute(
            text(f"SELECT * FROM {schema}.outbox_claim(:batch)"), {"batch": batch_size}
        )
        # Plain dicts: `list` is invariant, so a list of RowMapping is not a
        # Sequence[Mapping[str, Any]] to mypy.
        rows: list[Mapping[str, Any]] = [dict(row) for row in result.mappings()]
        delivered = await publish_rows(producer, rows, flush_timeout=flush_timeout)
        if delivered:
            await session.execute(
                text(f"SELECT {schema}.outbox_mark_published(CAST(:ids AS uuid[]))"),
                {"ids": delivered},
            )
    return len(rows)


async def _sleep(timeout: float, *events: asyncio.Event) -> None:
    waiters = [asyncio.ensure_future(event.wait()) for event in events]
    try:
        await asyncio.wait(waiters, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for waiter in waiters:
            waiter.cancel()


async def run_outbox_relay(
    relay_round: Callable[[], Awaitable[int]],
    producer: AIOProducer,
    wakeup: asyncio.Event,
    stop_event: asyncio.Event,
    *,
    batch_size: int = BATCH_SIZE,
    idle_interval: float = IDLE_INTERVAL_SECONDS,
    error_backoff: float = ERROR_BACKOFF_SECONDS,
    flush_timeout: float = FLUSH_TIMEOUT_SECONDS,
) -> None:
    """Run rounds until `stop_event` is set. Never raises.

    It shares an `asyncio.gather` with the gRPC server, so an exception out
    of here would stop the service. A round that fails -- database gone,
    broker gone -- is logged and retried after `error_backoff`, its rows
    still unpublished. The first round runs at once, which is what sends
    whatever an earlier process committed and never got out.

    Between rounds it waits for `wakeup` (set by every transaction that
    committed an event) or `idle_interval`, whichever comes first: the wake
    is latency, the interval is what picks up rows another replica wrote. A
    failed round waits out the back-off whatever wakes it -- a commit during
    an outage should not turn the back-off into a retry loop.
    """
    try:
        while not stop_event.is_set():
            # Cleared before the round, not after: a commit that lands while
            # the round runs sets it again and earns another round at once.
            wakeup.clear()
            try:
                while await relay_round() >= batch_size:
                    if stop_event.is_set():
                        break
            except Exception:
                logger.exception("Outbox relay round failed; its rows stay unpublished")
                await _sleep(error_backoff, stop_event)
                continue
            await _sleep(idle_interval, stop_event, wakeup)
    finally:
        try:
            await producer.flush(flush_timeout)
        except Exception:
            logger.exception("Flushing the producer at shutdown failed")


async def run_outbox_relay_for(
    container: "ApplicationContainer", stop_event: asyncio.Event
) -> None:
    """`run_outbox_relay` wired from a service's container: its engine, its
    producer, its schema, and the wake its application sets on commit. One
    line in each service's `asyncio.gather`."""
    schema = _plain_schema(container.config().POSTGRES_SCHEMA)
    producer = container.kafka_producer()
    await run_outbox_relay(
        partial(relay_once, container.engine(), producer, schema),
        producer,
        container.application().outbox_wakeup,
        stop_event,
    )


__all__ = [
    "BATCH_SIZE",
    "DELIVERY_GRACE_SECONDS",
    "ENVELOPE_COLUMNS",
    "ERROR_BACKOFF_SECONDS",
    "FLUSH_TIMEOUT_SECONDS",
    "IDLE_INTERVAL_SECONDS",
    "publish_rows",
    "relay_once",
    "run_outbox_relay",
    "run_outbox_relay_for",
    "wire_value",
]
