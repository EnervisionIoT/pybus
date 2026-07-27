import inflection
from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, declared_attr

from pybus.container.config import ApplicationSettings


class Base(AsyncAttrs, DeclarativeBase):
    # Read once at import time, before any mapped class body runs -- a
    # Table's schema is fixed when the Table is constructed (i.e. at class
    # definition time), which happens well before the DI container (and its
    # lazily-resolved `config` provider) exists. Every service that shares
    # this Postgres instance sets POSTGRES_SCHEMA to its own schema name so
    # its tables (and the shared DomainEvent table) land there instead of
    # always defaulting to "public".
    metadata = MetaData(schema=ApplicationSettings().POSTGRES_SCHEMA)

    @declared_attr.directive
    def __tablename__(cls) -> str:
        return inflection.tableize(cls.__name__)
