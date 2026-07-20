from sqlalchemy.orm import Mapped, mapped_column

from pybus.infrastructure.database.sqlalchemy import Base


class BlogPost(Base):
    id: Mapped[int] = mapped_column(primary_key=True)


def test_tablename_is_derived_via_inflection_tableize():
    assert BlogPost.__tablename__ == "blog_posts"
