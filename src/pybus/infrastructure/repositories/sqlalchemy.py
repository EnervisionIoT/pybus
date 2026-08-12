import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from typing import overload, override

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Select

from pybus.domain.entities import AggregateRoot
from pybus.domain.events import DomainEvent
from pybus.domain.exceptions import EntityNotFoundException, SoftDeleteException
from pybus.domain.repositories import GenericRepository

from ..database.sqlalchemy import Base, SoftDeleteMixin
from ..models.sqlalchemy import DomainEvent as DomainEventModel


class Removed:
    @override
    def __repr__(self):
        return "<Removed entity>"

    @override
    def __str__(self):
        return "<Removed entity>"


REMOVED = Removed()


class SqlAlchemyGenericRepository[TEntity: AggregateRoot, TModel: Base](
    GenericRepository[TEntity], ABC
):
    orm_model: type[TModel]

    @property
    def _default_stmt(self) -> Select[tuple[TModel]]:
        return select(self.orm_model)

    def __init__(self, session: AsyncSession, correlation_id: uuid.UUID):
        self._session: AsyncSession = session
        self._correlation_id: uuid.UUID = correlation_id
        self._identity_map: dict[uuid.UUID, TEntity | Removed] = dict()
        self._added_ids: set[uuid.UUID] = set()

    async def _paginate(
        self, stmt: Select[tuple[TModel]], page: int = 1, size: int = 10
    ) -> tuple[int, list[TModel]]:
        total_stmt = select(func.count()).select_from(stmt.subquery())
        items_stmt = stmt.offset((page - 1) * size).limit(size)

        total = await self._session.scalar(total_stmt) or 0
        items = list((await self._session.scalars(items_stmt)).unique().all())
        return total, items

    @override
    async def get_by_id(self, entity_id: uuid.UUID, skip_filter: bool = False) -> TEntity | None:
        stmt = self._default_stmt.where(getattr(self.orm_model, "id") == entity_id)
        if skip_filter and issubclass(self.orm_model, SoftDeleteMixin):
            stmt = stmt.where(self.orm_model.deleted_at.is_(None))
        instance = await self._session.scalar(stmt)
        return await self._get_entity(instance) if instance else None

    @override
    async def get_by_ids(
        self, entity_ids: list[uuid.UUID], skip_filter: bool = False
    ) -> list[TEntity]:
        stmt = self._default_stmt.where(getattr(self.orm_model, "id").in_(entity_ids))
        if skip_filter and issubclass(self.orm_model, SoftDeleteMixin):
            stmt = stmt.where(self.orm_model.deleted_at.is_(None))
        instances = (await self._session.scalars(stmt)).all()
        return [await self._get_entity(instance) for instance in instances]

    @overload
    async def get_all(
        self, page: None = None, size: None = None, skip_filter: bool = False
    ) -> list[TEntity]: ...

    @overload
    async def get_all(
        self, page: int = 1, size: int = 10, skip_filter: bool = False
    ) -> tuple[int, list[TEntity]]: ...

    @override
    async def get_all(
        self, page: int | None = None, size: int | None = None, skip_filter: bool = False
    ) -> list[TEntity] | tuple[int, list[TEntity]]:
        stmt = self._default_stmt
        if skip_filter and issubclass(self.orm_model, SoftDeleteMixin):
            stmt = stmt.where(self.orm_model.deleted_at.is_(None))
        if page is not None and size is not None:
            total, instances = await self._paginate(stmt, page or 1, size or 10)
            return total, [await self._get_entity(instance) for instance in instances]

        instances = (await self._session.scalars(stmt)).all()
        return [await self._get_entity(instance) for instance in instances]

    @override
    async def get_event_history(self, entity_id: uuid.UUID) -> list[DomainEvent]:
        stmt = select(DomainEventModel).where(
            DomainEventModel.aggregate_id == entity_id, DomainEventModel.version > 0
        )
        instances = (await self._session.scalars(stmt)).all()
        return [
            DomainEvent.deserialize(
                {
                    "id": instance.id,
                    "correlation_id": instance.correlation_id,
                    "aggregate_id": instance.aggregate_id,
                    "aggregate_type": instance.aggregate_type,
                    "message_type": instance.message_type,
                    "occurred_on": instance.occurred_on,
                    "version": instance.version,
                    "created_by_id": instance.created_by_id,
                    **instance.payload,
                },
            )
            for instance in instances
        ]

    @override
    async def add(self, entity: TEntity):
        self._identity_map[entity.id] = entity
        self._added_ids.add(entity.id)
        instance = await self._entity_to_model(entity)
        self._session.add(instance)

    @override
    async def persist(self, entity: TEntity):
        if entity.id not in self._identity_map:
            raise EntityNotFoundException(
                repository_name=self.__class__.__name__, entity_id=entity.id
            )

        if self._identity_map[entity.id] is REMOVED:
            raise EntityNotFoundException(
                repository_name=self.__class__.__name__, entity_id=entity.id
            )

        instance = await self._entity_to_model(entity)
        await self._session.merge(instance)

    @override
    async def persist_all(self):
        for entity_id, entity in self._identity_map.items():
            if not isinstance(entity, Removed) and entity_id not in self._added_ids:
                await self.persist(entity)
        self._added_ids.clear()

    @override
    async def collect_events(self) -> list[DomainEvent]:
        return [
            event
            for entity in self._identity_map.values()
            if not isinstance(entity, Removed)
            for event in entity.collect_events()
        ]

    @override
    async def remove(self, entity: TEntity):
        if entity.id in self._identity_map:
            if self._identity_map[entity.id] is REMOVED:
                raise EntityNotFoundException(
                    repository_name=self.__class__.__name__, entity_id=entity.id
                )
            self._identity_map[entity.id] = REMOVED

        instance = await self._session.get(self.orm_model, entity.id)
        if not instance:
            raise EntityNotFoundException(
                repository_name=self.__class__.__name__, entity_id=entity.id
            )

        if isinstance(instance, SoftDeleteMixin):
            instance.deleted_at = datetime.now()
            await self._session.merge(instance)
        else:
            await self._session.delete(instance)

    @override
    async def restore(self, entity: TEntity):
        if entity.id not in self._identity_map or self._identity_map[entity.id] is not REMOVED:
            raise EntityNotFoundException(
                repository_name=self.__class__.__name__, entity_id=entity.id
            )

        instance = await self._entity_to_model(entity)
        if not isinstance(instance, SoftDeleteMixin):
            raise SoftDeleteException(repository_name=self.__class__.__name__, entity_id=entity.id)

        instance.deleted_at = None
        instance = await self._session.merge(instance)
        self._identity_map[entity.id] = await self._model_to_entity(instance)

    @override
    async def save_domain_events(self) -> list[DomainEvent]:
        events = await self.collect_events()
        # `created_by_id` is stashed here by `Application.execute` (see
        # `pybus.container.application`) onto this same `AsyncSession`'s
        # `.info` dict -- reading it back here (rather than threading it in
        # as a parameter) means a future event that sets its own
        # `created_by_id` explicitly is never overwritten by this fallback.
        created_by_id = self._session.info.get("created_by_id")
        for domain_event in events:
            if domain_event.created_by_id is None and created_by_id is not None:
                domain_event.created_by_id = created_by_id
        self._session.add_all(
            [
                DomainEventModel(
                    id=domain_event.id,
                    correlation_id=self._correlation_id,
                    aggregate_id=domain_event.aggregate_id,
                    aggregate_type=domain_event.aggregate_type,
                    message_type=domain_event.message_type,
                    occurred_on=domain_event.occurred_on,
                    version=domain_event.version,
                    created_by_id=domain_event.created_by_id,
                    payload=domain_event.payload,
                )
                for domain_event in events
            ]
        )
        return events

    async def _get_entity(self, instance: TModel) -> TEntity:
        entity = await self._model_to_entity(instance)
        if entity.id in self._identity_map:
            cached_entity = self._identity_map[entity.id]
            if isinstance(cached_entity, Removed):
                raise EntityNotFoundException(
                    repository_name=self.__class__.__name__, entity_id=entity.id
                )
            return cached_entity

        self._identity_map[entity.id] = entity
        return entity

    @abstractmethod
    async def _entity_to_model(self, entity: TEntity) -> TModel:
        raise NotImplementedError()

    @abstractmethod
    async def _model_to_entity(self, model: TModel) -> TEntity:
        raise NotImplementedError()
