import dataclasses
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Self
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel, ValidationError
from sqlalchemy.exc import IntegrityError

from pybus.container import dead_letters
from pybus.container.application import Application
from pybus.container.dead_letters import (
    DeadLetter,
    DeadLetterSummary,
    ReplayOutcome,
    describe_error,
    discard_dead_letters,
    list_dead_letters,
    peek_envelope,
    record_dead_letter,
    replay_dead_letters,
    require_dead_letters_installed,
)
from pybus.domain.events import DomainEvent


# Named distinctly: TypeRegistryMixin keeps one registry for the whole
# process, keyed by class name, so a name another test module uses would
# clobber its entry.
class DeadLetterProbe(DomainEvent):
    note: str = "x"


FIRST = uuid.UUID(int=1)
SECOND = uuid.UUID(int=2)
NOW = datetime(2026, 10, 10, 12, 0, tzinfo=UTC)
FUNCTIONS = (
    "to_regprocedure",
    "dead_letter_record",
    "dead_letters_list",
    "dead_letter_take",
    "dead_letter_resolve",
    "dead_letter_failed_again",
)
LETTER = DeadLetter(
    topic="domain_events",
    partition=2,
    offset=41,
    key=b"k",
    value=b"{}",
    message_type="DeadLetterProbe",
    event_id=uuid.UUID(int=9),
    error="ValueError: boom",
    attempts=3,
)


class FakeSession:
    """Stands in for `AsyncSession(engine)`. Each statement is answered by
    the first entry of `answers` whose key appears in its SQL; an answer
    that is callable is called for each statement, so a sequence can be
    played out one call at a time."""

    def __init__(self, answers: dict[str, Any]) -> None:
        self.answers = answers
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def __call__(self, _engine: object) -> Self:
        return self

    def begin(self) -> Self:
        return self

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    def _answer(self, statement: object, parameters: dict[str, Any] | None) -> Any:
        sql = str(statement)
        self.calls.append((sql, parameters or {}))
        for name, answer in self.answers.items():
            if name in sql:
                return answer() if callable(answer) else answer
        return None

    async def execute(self, statement: object, parameters: dict[str, Any] | None = None):
        rows = self._answer(statement, parameters) or []
        result = MagicMock()
        result.first.return_value = rows[0] if rows else None
        result.mappings.return_value = rows
        return result

    async def scalar(self, statement: object, parameters: dict[str, Any] | None = None):
        return self._answer(statement, parameters)


def called(session: FakeSession) -> list[str]:
    return [next(name for name in FUNCTIONS if name in sql) for sql, _ in session.calls]


@pytest.fixture
def session_answering(monkeypatch) -> Callable[..., FakeSession]:
    def install(**answers: Any) -> FakeSession:
        session = FakeSession(answers)
        monkeypatch.setattr(dead_letters, "AsyncSession", session)
        return session

    return install


def probe(**fields: Any) -> tuple[DeadLetterProbe, bytes]:
    event = DeadLetterProbe(aggregate_id=uuid.uuid4(), aggregate_type="Probe", **fields)
    return event, event.model_dump_json().encode()


def application_running(execute: AsyncMock) -> MagicMock:
    application = MagicMock(spec=Application)
    application.execute = execute
    return application


# --- describe_error -----------------------------------------------------------


class Strict(BaseModel):
    count: int


def test_a_validation_error_is_described_without_its_input():
    """Its own message quotes the input it refused, and the input here is a
    message off the topic -- a UserInvited would put its token in the log."""
    with pytest.raises(ValidationError) as caught:
        Strict.model_validate({"count": "secret-token-XYZ"})

    described = describe_error(caught.value)

    assert described.startswith("ValidationError: count: ")
    assert "secret-token-XYZ" not in described


def test_a_database_error_is_described_without_its_parameters():
    """SQLAlchemy's own message carries the statement's parameters."""
    error = IntegrityError(
        "INSERT INTO x VALUES (%(v)s)", {"v": "secret-token-XYZ"}, Exception("duplicate key")
    )

    assert describe_error(error) == "IntegrityError: Exception: duplicate key"


def test_any_other_error_is_its_class_and_message():
    assert describe_error(KeyError("message_type")) == "KeyError: 'message_type'"


# --- peek_envelope ------------------------------------------------------------


def test_peek_reads_the_type_and_id_off_an_event():
    event, value = probe()

    assert peek_envelope(value) == ("DeadLetterProbe", event.id)


@pytest.mark.parametrize(
    "value",
    [b"{not json", b"", b"[1, 2]", b"\xff\xfe", b'{"message_type": 7, "id": "not-a-uuid"}'],
)
def test_peek_gives_none_where_the_bytes_do_not_say(value):
    assert peek_envelope(value) == (None, None)


# --- the start-up check and record --------------------------------------------


async def test_a_missing_record_function_refuses_to_start(session_answering):
    session = session_answering(to_regprocedure=False)

    with pytest.raises(RuntimeError, match="dead_letters migration"):
        await require_dead_letters_installed(MagicMock(), "iam")

    [(_, parameters)] = session.calls
    assert parameters == {
        "signature": "iam.dead_letter_record(text, integer, bigint, bytea, bytea, text, uuid, "
        "text, integer)"
    }


async def test_an_installed_record_function_passes(session_answering):
    session_answering(to_regprocedure=True)

    await require_dead_letters_installed(MagicMock(), "iam")


async def test_record_hands_every_field_to_the_function(session_answering):
    session = session_answering()

    await record_dead_letter(MagicMock(), "iam", LETTER)

    [(sql, parameters)] = session.calls
    assert "iam.dead_letter_record(" in sql
    assert parameters == {
        "topic": "domain_events",
        "partition": 2,
        "offset": 41,
        "key": b"k",
        "value": b"{}",
        "message_type": "DeadLetterProbe",
        "event_id": uuid.UUID(int=9),
        "error": "ValueError: boom",
        "attempts": 3,
    }


async def test_record_refuses_a_schema_it_would_have_to_quote():
    with pytest.raises(ValueError, match="POSTGRES_SCHEMA"):
        await record_dead_letter(MagicMock(), "Iam", LETTER)


# --- list ---------------------------------------------------------------------


async def test_list_reads_the_summaries(session_answering):
    row = {
        "id": FIRST,
        "topic": "domain_events",
        "partition": 0,
        "offset": 3,
        "message_type": "DeadLetterProbe",
        "event_id": None,
        "error": "ValueError: boom",
        "attempts": 3,
        "first_failed_at": NOW,
        "last_attempted_at": NOW,
    }
    session_answering(dead_letters_list=[row])

    assert await list_dead_letters(MagicMock(), "iam") == [DeadLetterSummary(**row)]


def test_a_summary_has_no_room_for_the_message():
    fields = {field.name for field in dataclasses.fields(DeadLetterSummary)}

    assert not fields & {"key", "value"}


# --- replay -------------------------------------------------------------------


async def test_a_replay_that_succeeds_resolves_the_row(session_answering):
    tenant = uuid.uuid4()
    _, value = probe(note="again", tenant_id=tenant)
    session = session_answering(dead_letter_take=[(value,)])
    execute = AsyncMock()

    [outcome] = await replay_dead_letters(application_running(execute), MagicMock(), "iam", [FIRST])

    assert outcome == ReplayOutcome(FIRST, "replayed")
    replayed = execute.call_args.args[0]
    assert isinstance(replayed, DeadLetterProbe)
    assert replayed.note == "again"
    assert execute.call_args.kwargs["tenant_id"] == tenant
    assert called(session) == ["dead_letter_take", "dead_letter_resolve"]


async def test_a_replay_that_fails_records_why_and_keeps_the_row(session_answering):
    _, value = probe()
    session = session_answering(dead_letter_take=[(value,)])
    execute = AsyncMock(side_effect=ValueError("still broken"))

    [outcome] = await replay_dead_letters(application_running(execute), MagicMock(), "iam", [FIRST])

    assert outcome == ReplayOutcome(FIRST, "failed", "ValueError: still broken")
    # One attempt: a person is waiting for the answer, and the consumer has
    # already spent the retries.
    execute.assert_awaited_once()
    assert called(session) == ["dead_letter_take", "dead_letter_failed_again"]
    assert session.calls[-1][1] == {"id": FIRST, "error": "ValueError: still broken"}


async def test_a_replay_of_bytes_that_cannot_be_read_fails_without_running_anything(
    session_answering,
):
    session = session_answering(dead_letter_take=[(b"{not json",)])
    execute = AsyncMock()

    [outcome] = await replay_dead_letters(application_running(execute), MagicMock(), "iam", [FIRST])

    assert outcome.status == "failed"
    assert outcome.error is not None and outcome.error.startswith("JSONDecodeError: ")
    execute.assert_not_awaited()
    assert called(session) == ["dead_letter_take", "dead_letter_failed_again"]


async def test_a_row_someone_else_holds_or_nobody_has_is_unavailable(session_answering):
    session = session_answering(dead_letter_take=[])
    execute = AsyncMock()

    [outcome] = await replay_dead_letters(application_running(execute), MagicMock(), "iam", [FIRST])

    assert outcome == ReplayOutcome(FIRST, "unavailable")
    execute.assert_not_awaited()
    assert called(session) == ["dead_letter_take"]


async def test_a_replay_carries_on_past_a_failure(session_answering):
    _, value = probe()
    session_answering(dead_letter_take=[(value,)])
    execute = AsyncMock(side_effect=[ValueError("first"), None])

    outcomes = await replay_dead_letters(
        application_running(execute), MagicMock(), "iam", [FIRST, SECOND]
    )

    assert [(outcome.id, outcome.status) for outcome in outcomes] == [
        (FIRST, "failed"),
        (SECOND, "replayed"),
    ]


# --- discard ------------------------------------------------------------------


async def test_discard_returns_only_what_was_there_to_delete(session_answering):
    resolved = iter([True, False])
    session_answering(dead_letter_resolve=lambda: next(resolved))

    assert await discard_dead_letters(MagicMock(), "iam", [FIRST, SECOND]) == [FIRST]
