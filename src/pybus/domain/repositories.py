import uuid
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, overload

from .entities import AggregateRoot

if TYPE_CHECKING:
    from .events import DomainEvent


class GenericRepository[TEntity: AggregateRoot](ABC):
    """An interface for a generic repository"""

    @abstractmethod
    async def get_by_id(self, entity_id: uuid.UUID, skip_filter: bool = False) -> TEntity | None:
        raise NotImplementedError()

    @abstractmethod
    async def get_by_ids(
        self, entity_ids: list[uuid.UUID], skip_filter: bool = False
    ) -> list[TEntity]:
        raise NotImplementedError()

    @overload
    async def get_all(
        self, page: None = None, size: None = None, skip_filter: bool = False
    ) -> list[TEntity]: ...

    @overload
    async def get_all(
        self, page: int = 1, size: int = 10, skip_filter: bool = False
    ) -> tuple[int, list[TEntity]]: ...

    @abstractmethod
    async def get_all(
        self, page: int | None = None, size: int | None = None, skip_filter: bool = False
    ) -> list[TEntity] | tuple[int, list[TEntity]]:
        raise NotImplementedError()

    @abstractmethod
    async def get_event_history(self, entity_id: uuid.UUID) -> list["DomainEvent"]:
        raise NotImplementedError()

    @abstractmethod
    async def add(self, entity: TEntity) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def persist(self, entity: TEntity) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def persist_all(self) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def collect_events(self) -> list["DomainEvent"]:
        raise NotImplementedError()

    @abstractmethod
    async def remove(self, entity: TEntity) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def restore(self, entity: TEntity) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def save_domain_events(self) -> list["DomainEvent"]:
        raise NotImplementedError()
