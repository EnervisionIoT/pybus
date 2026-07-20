from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from ...database.sqlalchemy import Base


class DomainEvent(Base):
    id: Mapped[UUID] = mapped_column(primary_key=True)
    correlation_id: Mapped[UUID] = mapped_column(nullable=False)
    aggregate_id: Mapped[UUID] = mapped_column(nullable=False)
    aggregate_type: Mapped[str] = mapped_column(nullable=False)
    message_type: Mapped[str] = mapped_column(nullable=False)
    occurred_on: Mapped[datetime] = mapped_column(nullable=False)
    version: Mapped[int] = mapped_column(nullable=False)
    created_by_id: Mapped[UUID] = mapped_column(nullable=False)
    payload: Mapped[Any] = mapped_column(JSONB, nullable=False)
