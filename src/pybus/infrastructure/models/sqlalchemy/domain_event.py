from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from ...database.sqlalchemy import Base


class DomainEvent(Base):
    id: Mapped[UUID] = mapped_column(primary_key=True)
    # The tenant context the writing transaction ran in -- NOT, in general,
    # the tenant the event is about. Where a service's aggregates can act on
    # each other those differ, and the subject is recoverable from the row
    # anyway: `aggregate_id` when the aggregate is itself the tenant,
    # `payload` otherwise. This column exists so a row-level security policy
    # has something to key on; it is not the answer to "who is this about".
    #
    # Nullable because pybus is shared with services that establish no tenant
    # context at all, whose rows are legitimately NULL. A service behind RLS
    # declares it NOT NULL in its own migration.
    tenant_id: Mapped[UUID | None] = mapped_column(nullable=True, index=True)
    correlation_id: Mapped[UUID] = mapped_column(nullable=False)
    aggregate_id: Mapped[UUID] = mapped_column(nullable=False)
    aggregate_type: Mapped[str] = mapped_column(nullable=False)
    message_type: Mapped[str] = mapped_column(nullable=False)
    occurred_on: Mapped[datetime] = mapped_column(nullable=False)
    version: Mapped[int | None] = mapped_column(nullable=True)
    created_by_id: Mapped[UUID | None] = mapped_column(nullable=True)
    payload: Mapped[Any] = mapped_column(JSONB, nullable=False)
