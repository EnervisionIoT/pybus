from sqlalchemy.orm import Mapped, mapped_column

from pybus.container.config import ApplicationSettings
from pybus.infrastructure.database.sqlalchemy import Base


class BlogPost(Base):
    id: Mapped[int] = mapped_column(primary_key=True)


def test_tablename_is_derived_via_inflection_tableize():
    assert BlogPost.__tablename__ == "blog_posts"


def test_table_schema_is_taken_from_application_settings():
    # POSTGRES_SCHEMA drives Base.metadata.schema, so every mapped class's
    # Table lands in that schema rather than always defaulting to "public"
    # -- this is what lets each service that shares one Postgres instance
    # keep its own tables in its own namespace.
    assert BlogPost.__table__.schema == ApplicationSettings().POSTGRES_SCHEMA
