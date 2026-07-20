from typing import override

from sqlalchemy import DDL, Connection, event
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.sql.schema import SchemaItem

from ..session import DataBaseSession
from .base import Base


@event.listens_for(Base.metadata, "before_create")
def before_create(target: SchemaItem, connection: Connection, **kw: object):
    if connection.dialect.name == "postgresql":
        pg_trgm_ddl = DDL("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        connection.execute(pg_trgm_ddl)


class SqlAlchemySession(DataBaseSession):
    def __init__(self, engine: AsyncEngine) -> None:
        self._engine: AsyncEngine = engine
        self._session: AsyncSession = AsyncSession(self._engine, expire_on_commit=False)

    @property
    @override
    def connection(self) -> AsyncSession:
        return self._session

    @override
    async def commit(self) -> None:
        await self._session.commit()

    @override
    async def rollback(self) -> None:
        await self._session.rollback()

    @override
    async def close(self) -> None:
        await self._session.close()
