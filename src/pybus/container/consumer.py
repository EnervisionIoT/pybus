import asyncio
import json
import logging

from confluent_kafka.aio import AIOConsumer

from pybus.domain.events import DomainEvent

from .application import Application
from .transaction import TransactionContext

logger = logging.getLogger(__name__)


async def run_event_consumer(
    application: Application,
    consumer: AIOConsumer,
    topic: str = TransactionContext.DOMAIN_EVENTS_TOPIC,
    poll_timeout: float = 1.0,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Polls `topic` and dispatches each message to `application.execute()`,
    the same entrypoint an in-process command/query goes through. Offsets
    are committed only after a message has been through `execute()`
    (`enable.auto.commit: False` on the consumer config -- see
    `ApplicationContainer.kafka_consumer`), giving at-least-once delivery.
    A single message's exception is logged, not raised -- one poisoned
    message must not wedge the whole consumer loop (see the notify design
    spec's Risks section: there is no dead-letter queue in this round)."""
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
                await application.execute(event)
            except Exception:
                logger.exception("Failed to process message from topic %s", topic)

            await consumer.commit(message=msg, asynchronous=False)
    finally:
        await consumer.close()
