"""Dead letters: a message the consumer could not process, kept for a person.

The consumer retries a failing handler in place; what still fails -- or
could not be read at all, which another attempt would only read the same
way -- is written to the service's own `dead_letters` table before the
offset is committed. A message is processed, or it is here. A person lists,
replays or discards the rows through the service's `<svc>-dead-letters`.
See docs/superpowers/specs/2026-10-10-dead-letters-design.md in the platform
repository.

The app role has no privilege on the table. Five SECURITY DEFINER
functions, created by each service's migration, are all it can do there: a
dead letter crosses tenants -- a failed message may have no tenant, or the
wrong one may be why it failed -- so row security has nothing to key on.

Nothing here logs or prints a message's value: it may carry an invitation
token. `describe_error` is the one place an exception becomes text, for the
log and for the `error` column alike.
"""

import json
import logging
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal

from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.exc import StatementError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from pybus.domain.events import DomainEvent

from .loops import plain_schema

if TYPE_CHECKING:
    from .application import Application

logger = logging.getLogger(__name__)

RECORD_SIGNATURE = (
    "dead_letter_record(text, integer, bigint, bytea, bytea, text, uuid, text, integer)"
)


@dataclass(frozen=True)
class DeadLetter:
    """A message as the consumer gives up on it."""

    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes
    message_type: str | None
    event_id: uuid.UUID | None
    error: str
    attempts: int


@dataclass(frozen=True)
class DeadLetterSummary:
    """A row as `list` shows it. There is no field for the message itself,
    so nothing built from one can print it."""

    id: uuid.UUID
    topic: str
    partition: int
    offset: int
    message_type: str | None
    event_id: uuid.UUID | None
    error: str
    attempts: int
    first_failed_at: datetime
    last_attempted_at: datetime


@dataclass(frozen=True)
class ReplayOutcome:
    id: uuid.UUID
    status: Literal["replayed", "failed", "unavailable"]
    error: str | None = None


def describe_error(error: BaseException) -> str:
    """The error as it is stored and logged: its class and its message, and
    never the input that caused it.

    Two kinds quote their input in their own message, and the input here is
    a message off the topic: a pydantic `ValidationError` names the value it
    refused, and a SQLAlchemy `StatementError` appends the statement's
    parameters. The first is restated from its error list without the
    input; the second is reduced to the driver's error it wraps.
    """
    if isinstance(error, StatementError) and error.orig is not None:
        return f"{type(error).__name__}: {describe_error(error.orig)}"
    if isinstance(error, ValidationError):
        details = "; ".join(
            f"{'.'.join(str(part) for part in detail['loc']) or '<root>'}: {detail['msg']}"
            for detail in error.errors(
                include_url=False, include_context=False, include_input=False
            )
        )
        return f"ValidationError: {details}"
    return f"{type(error).__name__}: {error}"


def peek_envelope(value: bytes) -> tuple[str | None, uuid.UUID | None]:
    """`message_type` and the event id, as far as the bytes say; `None` for
    whichever they do not. For `list` and the log, which have nothing else
    to tell one dead letter from another by."""
    try:
        data = json.loads(value)
    except ValueError:
        return None, None
    if not isinstance(data, dict):
        return None, None
    message_type = data.get("message_type")
    try:
        event_id: uuid.UUID | None = uuid.UUID(str(data["id"]))
    except (KeyError, ValueError):
        event_id = None
    return (message_type if isinstance(message_type, str) else None), event_id


async def require_dead_letters_installed(engine: AsyncEngine, schema: str) -> None:
    """Refuse to start a consumer that has nowhere to keep a failure.

    Without this, a service whose migration has not run consumes normally
    until its first failure and then sits in the record back-off for good,
    with a log line every few seconds as the only sign.
    """
    schema = plain_schema(schema)
    async with AsyncSession(engine) as session:
        installed = await session.scalar(
            text("SELECT to_regprocedure(:signature) IS NOT NULL"),
            {"signature": f"{schema}.{RECORD_SIGNATURE}"},
        )
    if not installed:
        raise RuntimeError(
            f"{schema}.dead_letter_record does not exist: this service's dead_letters "
            "migration has not been applied. Run `alembic upgrade head` before starting "
            "the consumer."
        )


async def record_dead_letter(engine: AsyncEngine, schema: str, letter: DeadLetter) -> None:
    """Write `letter` in a transaction of its own.

    Not through `Application`: this is not a handler, and it has no tenant.
    Opened the way the outbox relay opens its rounds. A second write of the
    same position is a no-op in the function, which is what makes a crash
    between this and the offset commit harmless.
    """
    schema = plain_schema(schema)
    async with AsyncSession(engine) as session, session.begin():
        await session.execute(
            text(
                f"SELECT {schema}.dead_letter_record("
                "CAST(:topic AS text), CAST(:partition AS integer), CAST(:offset AS bigint), "
                "CAST(:key AS bytea), CAST(:value AS bytea), CAST(:message_type AS text), "
                "CAST(:event_id AS uuid), CAST(:error AS text), CAST(:attempts AS integer))"
            ),
            {
                "topic": letter.topic,
                "partition": letter.partition,
                "offset": letter.offset,
                "key": letter.key,
                "value": letter.value,
                "message_type": letter.message_type,
                "event_id": letter.event_id,
                "error": letter.error,
                "attempts": letter.attempts,
            },
        )


def _summary(row: Mapping[Any, Any]) -> DeadLetterSummary:
    return DeadLetterSummary(
        id=row["id"],
        topic=row["topic"],
        partition=row["partition"],
        offset=row["offset"],
        message_type=row["message_type"],
        event_id=row["event_id"],
        error=row["error"],
        attempts=row["attempts"],
        first_failed_at=row["first_failed_at"],
        last_attempted_at=row["last_attempted_at"],
    )


async def list_dead_letters(engine: AsyncEngine, schema: str) -> list[DeadLetterSummary]:
    """Every row still waiting, oldest failure first."""
    schema = plain_schema(schema)
    async with AsyncSession(engine) as session:
        result = await session.execute(text(f"SELECT * FROM {schema}.dead_letters_list()"))
        return [_summary(row) for row in result.mappings()]


async def replay_dead_letters(
    application: "Application",
    engine: AsyncEngine,
    schema: str,
    ids: Sequence[uuid.UUID],
) -> list[ReplayOutcome]:
    """Run each row's message through `application` once more, in order,
    carrying on past a failure: one row that is still broken must not keep
    a person from the rest."""
    schema = plain_schema(schema)
    return [
        await _replay_one(application, engine, schema, dead_letter_id) for dead_letter_id in ids
    ]


async def _replay_one(
    application: "Application", engine: AsyncEngine, schema: str, dead_letter_id: uuid.UUID
) -> ReplayOutcome:
    """One attempt, no retry: the consumer has already spent the retries,
    and a person is waiting for the answer.

    Take, run, then resolve or record the failure, all inside the take's
    transaction, so the row stays locked against a second replayer until
    the outcome is written. The handler commits in a transaction of its
    own; a crash between that commit and this one leaves the row to be
    replayed again, which the handler's idempotence absorbs. Events the
    handler raises reach the topic through the service's own outbox relay,
    within its idle interval -- nothing here has to send them.
    """
    async with AsyncSession(engine) as session, session.begin():
        taken = (
            await session.execute(
                text(f"SELECT value FROM {schema}.dead_letter_take(:id)"), {"id": dead_letter_id}
            )
        ).first()
        if taken is None:
            return ReplayOutcome(dead_letter_id, "unavailable")
        try:
            event = DomainEvent.deserialize(json.loads(taken[0]))
            await application.execute(event, tenant_id=event.tenant_id)
        except Exception as error:  # noqa: BLE001 -- reported to a person, nothing to raise to
            described = describe_error(error)
            logger.warning("Replaying dead letter %s failed: %s", dead_letter_id, described)
            await session.execute(
                text(f"SELECT {schema}.dead_letter_failed_again(:id, :error)"),
                {"id": dead_letter_id, "error": described},
            )
            return ReplayOutcome(dead_letter_id, "failed", described)
        await session.execute(
            text(f"SELECT {schema}.dead_letter_resolve(:id)"), {"id": dead_letter_id}
        )
        return ReplayOutcome(dead_letter_id, "replayed")


async def discard_dead_letters(
    engine: AsyncEngine, schema: str, ids: Sequence[uuid.UUID]
) -> list[uuid.UUID]:
    """Delete without replaying -- for what no longer needs doing, such as an
    invitation long expired. Returns the ids that were there to delete."""
    schema = plain_schema(schema)
    discarded: list[uuid.UUID] = []
    async with AsyncSession(engine) as session, session.begin():
        for dead_letter_id in ids:
            if await session.scalar(
                text(f"SELECT {schema}.dead_letter_resolve(:id)"), {"id": dead_letter_id}
            ):
                discarded.append(dead_letter_id)
    return discarded


__all__ = [
    "RECORD_SIGNATURE",
    "DeadLetter",
    "DeadLetterSummary",
    "ReplayOutcome",
    "describe_error",
    "discard_dead_letters",
    "list_dead_letters",
    "peek_envelope",
    "record_dead_letter",
    "replay_dead_letters",
    "require_dead_letters_installed",
]
