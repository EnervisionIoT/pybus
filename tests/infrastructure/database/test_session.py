import uuid
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from pybus.infrastructure.database.session import DataBaseSession
from pybus.infrastructure.database.sqlalchemy import SqlAlchemySession


@pytest.fixture
def engine():
    return create_async_engine("sqlite+aiosqlite:///:memory:")


def test_sqlalchemy_session_satisfies_database_session_protocol(engine):
    session = SqlAlchemySession(engine)
    assert isinstance(session, DataBaseSession)
    assert hasattr(session, "set_tenant_context")
    assert hasattr(session, "set_platform_context")


def test_connection_property_returns_the_underlying_async_session(engine):
    session = SqlAlchemySession(engine)
    assert session.connection is session._session


async def test_commit_delegates_to_underlying_async_session(engine):
    session = SqlAlchemySession(engine)
    session._session.commit = AsyncMock()

    await session.commit()

    session._session.commit.assert_awaited_once()


async def test_rollback_delegates_to_underlying_async_session(engine):
    session = SqlAlchemySession(engine)
    session._session.rollback = AsyncMock()

    await session.rollback()

    session._session.rollback.assert_awaited_once()


async def test_close_delegates_to_underlying_async_session(engine):
    session = SqlAlchemySession(engine)
    session._session.close = AsyncMock()

    await session.close()

    session._session.close.assert_awaited_once()


async def test_aenter_returns_self(engine):
    session = SqlAlchemySession(engine)
    result = await session.__aenter__()
    assert result is session


async def test_aexit_commits_on_success_and_always_closes(engine):
    session = SqlAlchemySession(engine)
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.close = AsyncMock()

    await session.__aexit__(None, None, None)

    session.commit.assert_awaited_once()
    session.rollback.assert_not_called()
    session.close.assert_awaited_once()


async def test_aexit_rolls_back_on_exception_and_always_closes(engine):
    session = SqlAlchemySession(engine)
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.close = AsyncMock()

    await session.__aexit__(ValueError, ValueError("boom"), None)

    session.rollback.assert_awaited_once()
    session.commit.assert_not_called()
    session.close.assert_awaited_once()


async def test_set_tenant_context_delegates_to_underlying_async_session(engine):
    # `tenant_id` is interpolated directly into the statement text rather
    # than passed as a bind parameter: Postgres's `SET LOCAL` grammar
    # doesn't accept a parameter for the value (confirmed against a real
    # connection -- `SET LOCAL app.tenant_id = $1` is a syntax error).
    # Safe here because the parameter type is `uuid.UUID`, whose `str()`
    # can only ever produce the canonical hex-and-hyphen form.
    session = SqlAlchemySession(engine)
    session._session.execute = AsyncMock()
    tenant_id = uuid.uuid4()

    await session.set_tenant_context(tenant_id)

    session._session.execute.assert_awaited_once()
    args, _kwargs = session._session.execute.call_args
    statement = args[0]
    assert statement.text == f"SET LOCAL app.tenant_id = '{tenant_id}'"
    assert len(args) == 1


async def test_set_platform_context_delegates_to_underlying_async_session(engine):
    session = SqlAlchemySession(engine)
    session._session.execute = AsyncMock()

    await session.set_platform_context()

    session._session.execute.assert_awaited_once()
    args, _kwargs = session._session.execute.call_args
    statement = args[0]
    assert statement.text == "SET LOCAL app.is_platform = 'true'"
    assert len(args) == 1
