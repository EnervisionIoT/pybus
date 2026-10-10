"""The Kafka consumer each service runs beside its gRPC server.

Each message is dispatched through `application.execute()`, the entrypoint
a command or query goes through, and its offset is committed only
afterwards (`enable.auto.commit: False` -- see
`ApplicationContainer.kafka_consumer`): at-least-once.

A handler that raises is tried again in place, `max_attempts` times in all,
so a deadlock or a dropped connection is absorbed where it happened. What
still fails -- and anything that cannot be read at all, which another
attempt would only read the same way -- is written to the service's
`dead_letters` table, and only then committed. Until that write lands
nothing is committed: the consumer backs off and writes again, and a
shutdown in the meantime leaves the message for Kafka to deliver again. A
message is processed, or kept for a person; it is never dropped. See
`pybus.container.dead_letters` and
docs/superpowers/specs/2026-10-10-dead-letters-design.md in the platform
repository.

Every failure is retried, not only the kinds known to be transient. A list
of transient errors is right only until somebody meets one it does not
name, and that mistake would turn a passing blip into a dead letter a
person has to replay by hand. The accepted cost is a permanent failure
waiting out the retries -- about three seconds by default -- with its
partition, and with one consumer per service the whole service's event
flow, waiting behind it.

Nothing here logs a message's value: it may carry an invitation token.
"""

import asyncio
import json
import logging
from collections.abc import Sequence
from typing import Any

from confluent_kafka.aio import AIOConsumer
from pydantic import ValidationError
from sqlalchemy.exc import StatementError
from sqlalchemy.ext.asyncio import AsyncEngine

from pybus.domain.events import DomainEvent

from .application import Application, ApplicationContainer
from .dead_letters import (
    DeadLetter,
    describe_error,
    peek_envelope,
    record_dead_letter,
    require_dead_letters_installed,
)
from .loops import interruptible_sleep, plain_schema
from .transaction import TransactionContext

logger = logging.getLogger(__name__)

MAX_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS: tuple[float, ...] = (1.0, 2.0)
RECORD_BACKOFF_SECONDS = 5.0


class _Interrupted(Exception):
    """A shutdown landed while a message was still being retried."""


def _position(msg: Any) -> str:
    return f"{msg.topic()}[{msg.partition()}]@{msg.offset()}"


def _backoff(schedule: Sequence[float], attempt: int) -> float:
    """The wait after `attempt` failed; the last value repeats for any later one."""
    if not schedule:
        return 0.0
    return schedule[min(attempt, len(schedule)) - 1]


def _log_failure(message: str, *args: object, error: Exception) -> None:
    """Log at ERROR, with the traceback unless the error quotes its input.

    A pydantic ValidationError's message quotes the input it refused, and a
    SQLAlchemy StatementError's carries the statement's parameters; for a
    message being consumed, either can be an invitation token. Those two
    are logged only as `describe_error` restates them. The traceback is
    the accepted loss.
    """
    exc_info = None if isinstance(error, (ValidationError, StatementError)) else error
    logger.error(message, *args, describe_error(error), exc_info=exc_info)


async def _process(
    application: Application,
    msg: Any,
    *,
    max_attempts: int,
    retry_backoff: Sequence[float],
    stop_event: asyncio.Event,
) -> DeadLetter | None:
    """Run one message; the dead letter to keep if it did not go through.

    Raises `_Interrupted` when a shutdown lands in a retry's back-off: the
    message is neither processed nor kept, and is left for Kafka to deliver
    again.
    """
    # A message with no value is kept as an empty one: the column is NOT
    # NULL, and a None there would fail the write on every try.
    value: bytes = msg.value() or b""
    message_type, event_id = peek_envelope(value)
    described = f"{_position(msg)} {message_type or '?'} id={event_id or '?'}"

    def dead_letter(error: Exception, attempts: int) -> DeadLetter:
        return DeadLetter(
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
            key=msg.key(),
            value=value,
            message_type=message_type,
            event_id=event_id,
            error=describe_error(error),
            attempts=attempts,
        )

    try:
        event = DomainEvent.deserialize(json.loads(value))
    except Exception as error:  # noqa: BLE001 -- any failure to read is the same verdict
        _log_failure("Cannot read %s; keeping it as a dead letter: %s", described, error=error)
        return dead_letter(error, 1)

    for attempt in range(1, max_attempts + 1):
        try:
            # The tenant rides on the envelope, so a handler behind a
            # row-level security policy has the context it needs to write
            # anything at all. Without it every insert is refused and every
            # select comes back empty, with nothing raised to say why.
            # `None` for a service that has no tenants, which is what
            # `execute` already expects.
            await application.execute(event, tenant_id=event.tenant_id)
            return None
        except Exception as error:  # noqa: BLE001 -- every handler failure is retried, see the docstring
            if attempt == max_attempts:
                _log_failure(
                    "Handler failed %d times on %s; keeping it as a dead letter: %s",
                    attempt,
                    described,
                    error=error,
                )
                return dead_letter(error, attempt)
            logger.warning(
                "Handler failed on %s (attempt %d of %d), retrying: %s",
                described,
                attempt,
                max_attempts,
                describe_error(error),
            )
        await interruptible_sleep(_backoff(retry_backoff, attempt), stop_event)
        if stop_event.is_set():
            raise _Interrupted
    raise AssertionError("unreachable: the last attempt returns")


async def _record(
    engine: AsyncEngine,
    schema: str,
    letter: DeadLetter,
    *,
    backoff: float,
    stop_event: asyncio.Event,
) -> bool:
    """Write `letter`, again and again until it lands; False if a shutdown
    came first.

    Until it lands the offset is not committed and the consumer goes no
    further than this message. A database that is down is what every
    handler is waiting on anyway, so this waits for it too, rather than
    dropping the one message it could not keep.
    """
    while True:
        try:
            await record_dead_letter(engine, schema, letter)
            return True
        except Exception as error:  # noqa: BLE001 -- whatever stops the write, it is tried again
            logger.error(
                "Could not write the dead letter for %s[%d]@%d; not committing it, "
                "trying again in %.0fs: %s",
                letter.topic,
                letter.partition,
                letter.offset,
                backoff,
                describe_error(error),
            )
        await interruptible_sleep(backoff, stop_event)
        if stop_event.is_set():
            return False


async def run_event_consumer(
    application: Application,
    consumer: AIOConsumer,
    *,
    engine: AsyncEngine,
    schema: str,
    topic: str = TransactionContext.DOMAIN_EVENTS_TOPIC,
    poll_timeout: float = 1.0,
    max_attempts: int = MAX_ATTEMPTS,
    retry_backoff: Sequence[float] = RETRY_BACKOFF_SECONDS,
    record_backoff: float = RECORD_BACKOFF_SECONDS,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Poll `topic` until `stop_event` is set; see the module docstring.

    `engine` and `schema` are required: a consumer with nowhere to keep a
    failure is the one that used to drop it, and should not be possible to
    build. Refuses to start when the service's dead_letters migration has
    not run, rather than discovering it at the first failure.
    """
    if max_attempts < 1:
        raise ValueError(f"max_attempts must be at least 1, not {max_attempts}")
    schema = plain_schema(schema)
    stop_event = stop_event or asyncio.Event()

    try:
        await require_dead_letters_installed(engine, schema)
        await consumer.subscribe([topic])
        while not stop_event.is_set():
            msg = await consumer.poll(poll_timeout)
            if msg is None:
                continue
            if msg.error() is not None:
                logger.error("Kafka consumer error on topic %s: %s", topic, msg.error())
                continue

            try:
                letter = await _process(
                    application,
                    msg,
                    max_attempts=max_attempts,
                    retry_backoff=retry_backoff,
                    stop_event=stop_event,
                )
            except _Interrupted:
                logger.info(
                    "Stopping with %s uncommitted; it will be delivered again", _position(msg)
                )
                return
            if letter is not None and not await _record(
                engine, schema, letter, backoff=record_backoff, stop_event=stop_event
            ):
                logger.warning(
                    "Stopping before the dead letter for %s was written; "
                    "it will be delivered again",
                    _position(msg),
                )
                return

            await consumer.commit(message=msg, asynchronous=False)
    finally:
        await consumer.close()


async def run_event_consumer_for(
    container: ApplicationContainer, stop_event: asyncio.Event
) -> None:
    """`run_event_consumer` wired from a service's container: its
    application, its consumer, and the engine and schema its dead letters
    live in. One line in each consuming service's `asyncio.gather`, the
    same shape as `run_outbox_relay_for`."""
    await run_event_consumer(
        container.application(),
        container.kafka_consumer(),
        engine=container.engine(),
        schema=container.config().POSTGRES_SCHEMA,
        stop_event=stop_event,
    )


__all__ = [
    "MAX_ATTEMPTS",
    "RECORD_BACKOFF_SECONDS",
    "RETRY_BACKOFF_SECONDS",
    "run_event_consumer",
    "run_event_consumer_for",
]
