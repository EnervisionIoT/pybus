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
    # 事件所屬的 tenant。信封欄位而非 payload 的一部分，因為消費端要靠它
    # 建立 tenant context 才能寫入受 RLS 保護的表 —— 那是路由資訊，不是
    # 事件內容。未設定時由 `save_domain_events` 從交易的 tenant context
    # 補上，與 `created_by_id` 同一機制。需要它必填的服務可以在子類別
    # 把型別收窄成 `uuid.UUID`。
    tenant_id: uuid.UUID | None = Field(default=None, description="所屬 tenant ID")

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
                "tenant_id",
            },
        )
