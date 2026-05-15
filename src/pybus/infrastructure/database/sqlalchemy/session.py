from typing import override

from sqlalchemy import DDL, Connection, Engine, event
from sqlalchemy.orm import Session
from sqlalchemy.sql.schema import SchemaItem

from ..session import DataBaseSession
from .base import Base


@event.listens_for(Base.metadata, "before_create")
def before_create(target: SchemaItem, connection: Connection, **kw: object):
    if connection.dialect.name == "postgresql":
        pg_trgm_ddl = DDL("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        connection.execute(pg_trgm_ddl)


class SqlAlchemySession(DataBaseSession):
    def __init__(self, engine: Engine) -> None:
        self._engine: Engine = engine
        self._session: Session = Session(self._engine, expire_on_commit=False)

    @property
    @override
    def connection(self) -> Session:
        return self._session

    @override
    def commit(self) -> None:
        self._session.commit()

    @override
    def rollback(self) -> None:
        self._session.rollback()

    @override
    def close(self) -> None:
        self._session.close()
