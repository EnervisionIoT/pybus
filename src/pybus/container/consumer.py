import asyncio
import json
import logging

from confluent_kafka.aio import AIOConsumer

from pybus.domain.events import DomainEvent

from .application import Application
from .transaction import TransactionContext

logger = logging.getLogger(__name__)

DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_RETRY_BACKOFF = 1.0


async def run_event_consumer(
    application: Application,
    consumer: AIOConsumer,
    topic: str = TransactionContext.DOMAIN_EVENTS_TOPIC,
    poll_timeout: float = 1.0,
    stop_event: asyncio.Event | None = None,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    retry_backoff: float = DEFAULT_RETRY_BACKOFF,
) -> None:
    """Poll `topic` and dispatch each message to `application.execute()`, the
    same entrypoint an in-process command or query goes through. Each one runs
    inside its own tenant context, taken from the event's envelope.

    Offsets are committed only after a message has been handled
    (`enable.auto.commit: False` -- see `ApplicationContainer.kafka_consumer`),
    which is what makes delivery at-least-once. Handlers must therefore be
    idempotent.

    **Two kinds of failure, handled differently**, because conflating them is
    how this loop used to lose data:

    - A message that will not *parse* is poison. Retrying cannot help, and no
      later message is implicated, so it is logged, committed and skipped.
      One malformed message cannot wedge the loop.
    - A message that parsed but whose *handler* raised may simply have caught
      something temporary. It is retried in place, and if it still fails the
      offset is left uncommitted and the exception is raised.

    Giving up by raising rather than by skipping is the important half. Kafka
    offsets are monotonic: committing a later message implicitly commits an
    earlier one, so "carry on without committing this" is not available --
    the choice is between stopping and losing the message. When a database is
    down every message fails, and skipping would discard the entire backlog
    silently. Stopping is visible and costs nothing: the uncommitted offset is
    redelivered on the next start.

    The residual is that a well-formed message whose handler fails every time
    will stop this loop. That is an outage, but a loud one with the event's id
    in the log, which is the better half of the trade.

    Raising rather than shutting down on its own leaves the policy to the
    caller. A service that runs this beside its server under one
    `asyncio.gather` will exit and be restarted, which is what a database
    outage wants; returning quietly instead would leave a process still
    serving requests with a dead consumer.
    """
    stop_event = stop_event or asyncio.Event()

    await consumer.subscribe([topic])
    try:
        while not stop_event.is_set():
            msg = await consumer.poll(poll_timeout)
            if msg is None:
                continue
            if msg.error() is not None:
                logger.error("Kafka consumer error on topic %s: %s", topic, msg.error())
                continue

            try:
                event = DomainEvent.deserialize(json.loads(msg.value()))
            except Exception:
                # Poison. Another attempt reads the same bytes and fails the
                # same way, so committing is the only way past it.
                logger.exception(
                    "Discarding an unreadable message from %s [%s] at offset %s",
                    topic,
                    msg.partition(),
                    msg.offset(),
                )
                await consumer.commit(message=msg, asynchronous=False)
                continue

            failure = await _dispatch(application, event, stop_event, max_attempts, retry_backoff)
            if failure is not None:
                if stop_event.is_set():
                    # Asked to shut down part-way through the retries. The
                    # offset is uncommitted, so this comes back on the next
                    # start -- that is an orderly exit, not a failure.
                    return
                logger.error(
                    "Giving up on %s (id=%s) from %s [%s] at offset %s after %s attempts. "
                    "The offset is not committed; it will be redelivered on the next start.",
                    event.message_type,
                    event.id,
                    topic,
                    msg.partition(),
                    msg.offset(),
                    max_attempts,
                )
                raise failure

            await consumer.commit(message=msg, asynchronous=False)
    finally:
        await consumer.close()


async def _dispatch(
    application: Application,
    event: DomainEvent,
    stop_event: asyncio.Event,
    max_attempts: int,
    retry_backoff: float,
) -> Exception | None:
    """Run the handlers, retrying in place. Returns the last failure, or None.

    In place rather than by seeking: the message is already in hand, so a
    transient failure costs nothing but the backoff, and nothing has to touch
    the consumer's position.

    Returns the exception rather than raising so the caller can put the
    topic, partition and offset into the log before it goes up -- this
    function has none of those.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            await application.execute(event, tenant_id=event.tenant_id)
        except Exception as error:
            logger.exception(
                "Attempt %s of %s failed for %s (id=%s)",
                attempt,
                max_attempts,
                event.message_type,
                event.id,
            )
            if attempt == max_attempts:
                return error
            # Waiting on the stop event rather than sleeping, so a shutdown
            # does not have to sit through the backoff.
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=retry_backoff)
            except TimeoutError:
                pass
            if stop_event.is_set():
                return error
        else:
            return None
    return None
