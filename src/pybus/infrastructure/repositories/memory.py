import uuid
from typing import overload, override

from pybus.domain.entities import AggregateRoot
from pybus.domain.events import DomainEvent
from pybus.domain.repositories import GenericRepository


class InMemoryRepository(GenericRepository[AggregateRoot]):
    def __init__(self) -> None:
        self.objects: dict[uuid.UUID, AggregateRoot] = {}
        self._removed_events: list[DomainEvent] = []

    @override
    async def get_by_id(
        self, entity_id: uuid.UUID, include_deleted: bool = False
    ) -> AggregateRoot | None:
        return self.objects.get(entity_id, None)

    @override
    async def get_by_ids(
        self, entity_ids: list[uuid.UUID], include_deleted: bool = False
    ) -> list[AggregateRoot]:
        return [self.objects[entity_id] for entity_id in entity_ids if entity_id in self.objects]

    @overload
    async def get_all(
        self, page: None = None, size: None = None, include_deleted: bool = False
    ) -> list[AggregateRoot]: ...

    @overload
    async def get_all(
        self, page: int = 1, size: int = 10, include_deleted: bool = False
    ) -> tuple[int, list[AggregateRoot]]: ...

    @override
    async def get_all(
        self, page: int | None = None, size: int | None = None, include_deleted: bool = False
    ) -> list[AggregateRoot] | tuple[int, list[AggregateRoot]]:
        items = list(self.objects.values())
        if page is not None and size is not None:
            start = (page - 1) * size
            end = start + size
            return (len(items), items[start:end])

        return items

    @override
    async def get_event_history(self, entity_id: uuid.UUID) -> list[DomainEvent]:
        entity = self.objects.get(entity_id, None)
        if entity is not None:
            return [event for event in entity.collect_events()]
        return []

    @override
    async def add(self, entity: AggregateRoot):
        self.objects[entity.id] = entity

    @override
    async def persist(self, entity: AggregateRoot): ...

    @override
    async def persist_all(self): ...

    @override
    async def collect_events(self) -> list[DomainEvent]:
        live = [event for entity in self.objects.values() for event in entity.collect_events()]
        removed, self._removed_events = self._removed_events, []
        return live + removed

    @override
    async def remove(self, entity: AggregateRoot):
        # Drained before the entity is dropped, matching the SQLAlchemy
        # repository: an aggregate announcing its own deletion must not lose
        # the announcement by being deleted. See the note there.
        self._removed_events.extend(entity.collect_events())
        del self.objects[entity.id]

    @override
    async def restore(self, entity: AggregateRoot):
        self.objects[entity.id] = entity

    @override
    async def save_domain_events(self) -> list[DomainEvent]:
        # Delegates rather than repeating the walk: the two used to say the
        # same thing in two ways, and the one that had to learn about
        # removed aggregates' events was never going to be both.
        return await self.collect_events()
