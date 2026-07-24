import uuid
from datetime import datetime
from typing import Any

from pydantic import Field

from .mixins import TypeRegistryMixin


class DomainEvent(TypeRegistryMixin):
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    correlation_id: uuid.UUID | None = Field(default=None, description="執行 ID")
    aggregate_id: uuid.UUID = Field(description="聚合根 ID")
    aggregate_type: str = Field(description="聚合根類型")
    occurred_on: datetime = Field(default_factory=datetime.now, description="事件發生時間")
    version: int | None = Field(default=None, description="事件版本")
    created_by_id: uuid.UUID | None = Field(default=None, description="創建者 ID")

    @property
    def payload(self) -> dict[str, Any]:
        return self.model_dump(
            mode="json",
            exclude={
                "id",
                "correlation_id",
                "aggregate_id",
                "aggregate_type",
                "message_type",
                "occurred_on",
                "version",
                "created_by_id",
            }
        )
