import inflection
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, declared_attr


class Base(AsyncAttrs, DeclarativeBase):
    @declared_attr.directive
    def __tablename__(cls) -> str:
        return inflection.tableize(cls.__name__)
