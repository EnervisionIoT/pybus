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
    # Free to state now: no service uses this mixin yet, so no migration has
    # ever created the column. Once one has, changing it is an ALTER on a
    # table holding values whose intended zone nobody recorded.
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)
