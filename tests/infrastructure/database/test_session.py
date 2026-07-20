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
