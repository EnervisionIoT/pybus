from datetime import datetime

from sqlalchemy import DateTime
from sqlalchemy.orm import Mapped, mapped_column


class SoftDeleteMixin:
    # `timezone=True`, so this is `timestamptz` and the value carries an
    # offset. Without it SQLAlchemy emits `timestamp without time zone` and
    # the repository's `datetime.now()` writes the server's local wall clock
    # with nothing recording which clock that was -- readable, comparable,
    # and wrong by whatever the deploying machine's offset happened to be.
    # It also loses an hour, or repeats one, across a DST boundary.
    #
    # Cheap to get right here and expensive later: the first migration that
    # creates this column freezes the type, and changing it afterwards is an
    # ALTER on a table already holding values whose intended zone nobody
    # recorded.
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)
