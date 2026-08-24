import uuid
from typing import override

from sqlalchemy import DDL, Connection, event, text
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

    @override
    async def set_tenant_context(self, tenant_id: uuid.UUID):
        # Postgres's `SET`/`SET LOCAL` grammar does not accept a bind
        # parameter for the value (confirmed against a real connection --
        # it raises a syntax error at the resulting `$1` placeholder), so
        # this can't be `text("SET LOCAL app.tenant_id = :tenant_id")`
        # with `{"tenant_id": ...}` like a normal query. Interpolating
        # `tenant_id` directly is safe here specifically because the
        # parameter type is `uuid.UUID`, not `str` -- `str(tenant_id)` on
        # a real UUID instance can only ever produce the canonical
        # 36-character hex-and-hyphen form, which contains no characters
        # that could break out of the surrounding SQL.
        await self._session.execute(text(f"SET LOCAL app.tenant_id = '{tenant_id}'"))

    @override
    async def set_platform_context(self):
        await self._session.execute(text("SET LOCAL app.is_platform = 'true'"))
