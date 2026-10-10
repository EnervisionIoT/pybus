import dataclasses
import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from pybus.container import dead_letters
from pybus.container.dead_letters import DeadLetterSummary, ReplayOutcome, dead_letters_main

FIRST = uuid.UUID(int=1)
SECOND = uuid.UUID(int=2)
THIRD = uuid.UUID(int=3)
NOW = datetime(2026, 10, 10, 12, 0, tzinfo=UTC)
SUMMARY = DeadLetterSummary(
    id=FIRST,
    topic="domain_events",
    partition=0,
    offset=3,
    message_type="UserInvited",
    event_id=uuid.UUID(int=9),
    error="ValueError: first line\nsecond line",
    attempts=3,
    first_failed_at=NOW,
    last_attempted_at=NOW,
)


def container_double() -> MagicMock:
    container = MagicMock()
    container.config.return_value.POSTGRES_SCHEMA = "iam"
    container.engine.return_value.dispose = AsyncMock()
    return container


def run_cli(argv: list[str], container: MagicMock, **kwargs) -> int:
    with pytest.raises(SystemExit) as exited:
        dead_letters_main(lambda: container, argv=argv, **kwargs)
    return exited.value.code


def test_list_prints_one_line_per_letter(monkeypatch, capsys):
    monkeypatch.setattr(dead_letters, "list_dead_letters", AsyncMock(return_value=[SUMMARY]))
    container = container_double()

    assert run_cli(["list"], container) == 0

    out = capsys.readouterr().out
    assert str(FIRST) in out
    assert "UserInvited" in out
    assert "attempts=3" in out
    assert "ValueError: first line" in out
    assert "second line" not in out
    container.engine.return_value.dispose.assert_awaited_once()


def test_list_with_nothing_waiting_says_so(monkeypatch, capsys):
    monkeypatch.setattr(dead_letters, "list_dead_letters", AsyncMock(return_value=[]))

    assert run_cli(["list"], container_double()) == 0
    assert capsys.readouterr().out.strip() == "No dead letters."


def test_replay_reports_each_outcome_and_fails_if_any_did_not_replay(monkeypatch, capsys):
    replay = AsyncMock(
        return_value=[
            ReplayOutcome(FIRST, "replayed"),
            ReplayOutcome(SECOND, "failed", "ValueError: no"),
            ReplayOutcome(THIRD, "unavailable"),
        ]
    )
    monkeypatch.setattr(dead_letters, "replay_dead_letters", replay)
    container = container_double()

    assert run_cli(["replay", str(FIRST), str(SECOND), str(THIRD)], container) == 1

    application, _engine, schema, ids = replay.call_args.args
    assert application is container.application.return_value
    assert (schema, ids) == ("iam", [FIRST, SECOND, THIRD])
    out = capsys.readouterr().out.splitlines()
    assert out == [
        f"{FIRST}  replayed",
        f"{SECOND}  failed: ValueError: no",
        f"{THIRD}  not found, or being replayed by someone else",
    ]


def test_replay_all_replays_what_list_shows_oldest_first(monkeypatch):
    listed = [SUMMARY, dataclasses.replace(SUMMARY, id=SECOND)]
    monkeypatch.setattr(dead_letters, "list_dead_letters", AsyncMock(return_value=listed))
    replay = AsyncMock(
        return_value=[ReplayOutcome(FIRST, "replayed"), ReplayOutcome(SECOND, "replayed")]
    )
    monkeypatch.setattr(dead_letters, "replay_dead_letters", replay)

    assert run_cli(["replay", "--all"], container_double()) == 0
    assert replay.call_args.args[3] == [FIRST, SECOND]


def test_replay_all_with_nothing_waiting_replays_nothing(monkeypatch, capsys):
    monkeypatch.setattr(dead_letters, "list_dead_letters", AsyncMock(return_value=[]))
    replay = AsyncMock()
    monkeypatch.setattr(dead_letters, "replay_dead_letters", replay)

    assert run_cli(["replay", "--all"], container_double()) == 0
    replay.assert_not_awaited()
    assert capsys.readouterr().out.strip() == "No dead letters."


@pytest.mark.parametrize("argv", [["replay"], ["replay", "--all", str(FIRST)]])
def test_replay_takes_ids_or_all_but_not_both_and_not_neither(argv):
    with pytest.raises(SystemExit) as exited:
        dead_letters_main(lambda: pytest.fail("nothing should be built"), argv=argv)

    assert exited.value.code == 2


def test_an_id_that_is_not_a_uuid_is_a_usage_error():
    with pytest.raises(SystemExit) as exited:
        dead_letters_main(lambda: pytest.fail("nothing should be built"), argv=["discard", "nope"])

    assert exited.value.code == 2


def test_discard_reports_what_was_not_there(monkeypatch, capsys):
    discard = AsyncMock(return_value=[FIRST])
    monkeypatch.setattr(dead_letters, "discard_dead_letters", discard)

    assert run_cli(["discard", str(FIRST), str(SECOND)], container_double()) == 1

    assert discard.call_args.args[1:] == ("iam", [FIRST, SECOND])
    assert capsys.readouterr().out.splitlines() == [
        f"{FIRST}  discarded",
        f"{SECOND}  not found",
    ]


def test_on_exit_runs_and_the_engine_is_disposed_even_when_a_command_fails(monkeypatch):
    monkeypatch.setattr(
        dead_letters, "list_dead_letters", AsyncMock(side_effect=RuntimeError("db down"))
    )
    on_exit = AsyncMock()
    container = container_double()

    with pytest.raises(RuntimeError, match="db down"):
        dead_letters_main(lambda: container, argv=["list"], on_exit=on_exit)

    on_exit.assert_awaited_once()
    container.engine.return_value.dispose.assert_awaited_once()
