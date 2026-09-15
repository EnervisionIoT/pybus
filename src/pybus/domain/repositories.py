import uuid
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, overload

from .entities import AggregateRoot

if TYPE_CHECKING:
    from .events import DomainEvent


class GenericRepository[TEntity: AggregateRoot](ABC):
    """An interface for a generic repository.

    `include_deleted` appears on every read and means what it says: rows a
    soft delete has retired are left out unless it is asked for. It only
    ever does anything for an `orm_model` mixing in `SoftDeleteMixin`; for
    every other model there is nothing to exclude and the flag is inert.

    It was once called `skip_filter` and was inverted -- the filter applied
    only when the flag was *set*, so the default returned deleted rows. That
    was invisible for as long as it lasted, because no model mixed the mixin
    in and the branch never ran; the first service to adopt soft deletion
    would have inherited it as "deleted rows keep appearing everywhere".
    """

    @abstractmethod
    async def get_by_id(
        self, entity_id: uuid.UUID, include_deleted: bool = False
    ) -> TEntity | None:
        raise NotImplementedError()

    @abstractmethod
    async def get_by_ids(
        self, entity_ids: list[uuid.UUID], include_deleted: bool = False
    ) -> list[TEntity]:
        raise NotImplementedError()

    @overload
    async def get_all(
        self, page: None = None, size: None = None, include_deleted: bool = False
    ) -> list[TEntity]: ...

    @overload
    async def get_all(
        self, page: int = 1, size: int = 10, include_deleted: bool = False
    ) -> tuple[int, list[TEntity]]: ...

    @abstractmethod
    async def get_all(
        self, page: int | None = None, size: int | None = None, include_deleted: bool = False
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
