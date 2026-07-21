import uuid
from types import TracebackType
from typing import Any, Protocol, Self, runtime_checkable


@runtime_checkable
class DataBaseSession(Protocol):
    @property
    def connection(self) -> Any: ...

    async def commit(self) -> None: ...

    async def rollback(self) -> None: ...

    async def close(self) -> None: ...

    async def set_tenant_context(self, tenant_id: uuid.UUID) -> None: ...

    async def set_platform_context(self) -> None: ...

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if exc_type is None:
            await self.commit()
        else:
            await self.rollback()
        await self.close()
