import uuid
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import Table
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import Mapped, mapped_column

from pybus.container.config import ApplicationSettings
from pybus.domain.entities import AggregateRoot
from pybus.domain.exceptions import EntityNotFoundException, SoftDeleteException
from pybus.infrastructure.database.sqlalchemy import Base, SoftDeleteMixin
from pybus.infrastructure.models.sqlalchemy import DomainEvent as DomainEventModel
from pybus.infrastructure.repositories.sqlalchemy import REMOVED, SqlAlchemyGenericRepository

from tests.conftest import DummyEvent, DummyThing, make_dummy_event


def test_removed_sentinel_repr_and_str():
    assert repr(REMOVED) == "<Removed entity>"
    assert str(REMOVED) == "<Removed entity>"


class WidgetEntity(AggregateRoot):
    name: str


class WidgetModel(Base, SoftDeleteMixin):
    id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()


class PlainWidgetModel(Base):
    id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()


class WidgetRepository(SqlAlchemyGenericRepository[WidgetEntity, WidgetModel]):
    orm_model = WidgetModel

    async def _entity_to_model(self, entity: WidgetEntity) -> WidgetModel:
        return WidgetModel(id=entity.id, name=entity.name)

    async def _model_to_entity(self, model: WidgetModel) -> WidgetEntity:
        return WidgetEntity(id=model.id, name=model.name)


class PlainWidgetRepository(SqlAlchemyGenericRepository[WidgetEntity, PlainWidgetModel]):
    orm_model = PlainWidgetModel

    async def _entity_to_model(self, entity: WidgetEntity) -> PlainWidgetModel:
        return PlainWidgetModel(id=entity.id, name=entity.name)

    async def _model_to_entity(self, model: PlainWidgetModel) -> WidgetEntity:
        return WidgetEntity(id=model.id, name=model.name)


class ThingRepository(SqlAlchemyGenericRepository[DummyThing, WidgetModel]):
    """Repository over DummyThing, used only to unit-test save_domain_events /
    get_event_history against a mocked AsyncSession — DomainEventModel.payload
    is a postgresql JSONB column and cannot be exercised against real SQLite."""

    orm_model = WidgetModel

    async def _entity_to_model(self, entity: DummyThing) -> WidgetModel:
        return WidgetModel(id=entity.id, name=entity.name)

    async def _model_to_entity(self, model: WidgetModel) -> DummyThing:
        return DummyThing(id=model.id, name=model.name)


@pytest.fixture
async def session():
    # Only create tables for our own test models — Base.metadata also holds
    # DomainEvent (postgresql JSONB), which SQLite cannot compile DDL for.
    # schema_translate_map remaps Base.metadata.schema ("public") to no
    # schema at all for this connection -- SQLite has no equivalent of a
    # Postgres schema/namespace, so a schema-qualified table name (e.g.
    # `public.widget_models`) fails outright ("unknown database public").
    engine = create_async_engine("sqlite+aiosqlite:///:memory:").execution_options(
        schema_translate_map={ApplicationSettings().POSTGRES_SCHEMA: None}
    )
    async with engine.begin() as conn:
        await conn.run_sync(
            Base.metadata.create_all,
            # DeclarativeBase.__table__ is typed as the broader FromClause
            # (a mapped class could in principle map to a join/subquery) --
            # both of these map directly to a single Table.
            tables=[cast(Table, WidgetModel.__table__), cast(Table, PlainWidgetModel.__table__)],
        )

    async_session = AsyncSession(engine, expire_on_commit=False)
    try:
        yield async_session
    finally:
        await async_session.close()
        await engine.dispose()


@pytest.fixture
def repo(session: AsyncSession) -> WidgetRepository:
    return WidgetRepository(session, correlation_id=uuid.uuid4())


@pytest.fixture
def plain_repo(session: AsyncSession) -> PlainWidgetRepository:
    return PlainWidgetRepository(session, correlation_id=uuid.uuid4())


async def test_add_then_get_by_id_returns_the_same_cached_instance(repo: WidgetRepository):
    entity = WidgetEntity(name="foo")
    await repo.add(entity)

    result = await repo.get_by_id(entity.id)

    assert result is entity


async def test_get_by_id_returns_none_when_missing(repo: WidgetRepository):
    assert await repo.get_by_id(uuid.uuid4()) is None


async def test_get_by_id_reads_back_from_a_fresh_repository_after_commit(
    session: AsyncSession, repo: WidgetRepository
):
    entity = WidgetEntity(name="foo")
    await repo.add(entity)
    await session.commit()

    other_repo = WidgetRepository(session, correlation_id=uuid.uuid4())
    result = await other_repo.get_by_id(entity.id)

    assert result is not None
    assert result.id == entity.id
    assert result.name == "foo"


async def test_get_by_ids_filters_out_missing_entities(
    repo: WidgetRepository, session: AsyncSession
):
    a = WidgetEntity(name="a")
    await repo.add(a)
    await session.commit()

    result = await repo.get_by_ids([a.id, uuid.uuid4()])

    assert [e.id for e in result] == [a.id]


async def test_get_by_ids_with_skip_filter_excludes_soft_deleted_rows(
    repo: WidgetRepository, session: AsyncSession
):
    a, b = WidgetEntity(name="a"), WidgetEntity(name="b")
    await repo.add(a)
    await repo.add(b)
    await session.commit()
    await repo.remove(a)
    await session.commit()

    other_repo = WidgetRepository(session, correlation_id=uuid.uuid4())
    result = await other_repo.get_by_ids([a.id, b.id], skip_filter=True)

    assert [e.id for e in result] == [b.id]


async def test_get_all_without_pagination_returns_list(
    repo: WidgetRepository, session: AsyncSession
):
    a, b = WidgetEntity(name="a"), WidgetEntity(name="b")
    await repo.add(a)
    await repo.add(b)
    await session.commit()

    result = await repo.get_all()

    assert {e.id for e in result} == {a.id, b.id}


async def test_get_all_with_pagination_returns_total_and_page(
    repo: WidgetRepository, session: AsyncSession
):
    for i in range(5):
        await repo.add(WidgetEntity(name=f"w{i}"))
    await session.commit()

    total, page = await repo.get_all(page=1, size=2)

    assert total == 5
    assert len(page) == 2


async def test_get_all_with_skip_filter_excludes_soft_deleted_rows(
    repo: WidgetRepository, session: AsyncSession
):
    a, b = WidgetEntity(name="a"), WidgetEntity(name="b")
    await repo.add(a)
    await repo.add(b)
    await session.commit()
    await repo.remove(a)
    await session.commit()

    other_repo = WidgetRepository(session, correlation_id=uuid.uuid4())
    result = await other_repo.get_all(skip_filter=True)

    assert [e.id for e in result] == [b.id]


async def test_persist_updates_an_existing_entity(repo: WidgetRepository, session: AsyncSession):
    entity = WidgetEntity(name="foo")
    await repo.add(entity)
    await session.commit()

    entity.name = "bar"
    await repo.persist(entity)
    await session.commit()

    other_repo = WidgetRepository(session, correlation_id=uuid.uuid4())
    result = await other_repo.get_by_id(entity.id)
    assert result is not None
    assert result.name == "bar"


async def test_persist_raises_entity_not_found_when_never_added(repo: WidgetRepository):
    entity = WidgetEntity(name="foo")

    with pytest.raises(EntityNotFoundException):
        await repo.persist(entity)


async def test_persist_raises_entity_not_found_after_removal(
    repo: WidgetRepository, session: AsyncSession
):
    entity = WidgetEntity(name="foo")
    await repo.add(entity)
    await session.commit()
    await repo.remove(entity)

    with pytest.raises(EntityNotFoundException):
        await repo.persist(entity)


async def test_get_by_id_raises_entity_not_found_for_a_removed_entity_in_the_same_repository(
    repo: WidgetRepository, session: AsyncSession
):
    """Regression coverage for _get_entity: once an entity has been removed
    within this repository's identity map, re-fetching it by id (without
    skip_filter, so the soft-deleted row is still physically returned by the
    query) must raise instead of silently returning the stale entity."""
    entity = WidgetEntity(name="foo")
    await repo.add(entity)
    await session.commit()
    await repo.remove(entity)

    with pytest.raises(EntityNotFoundException):
        await repo.get_by_id(entity.id)


async def test_persist_all_persists_tracked_entities_fetched_in_the_same_unit_of_work(
    repo: WidgetRepository, session: AsyncSession
):
    entity = WidgetEntity(name="foo")
    await repo.add(entity)
    await session.commit()

    # persist_all() only re-persists entities that were fetched (not freshly added)
    # in this repository instance's identity map — a newly-added entity is skipped
    # (see SqlAlchemyGenericRepository.persist_all's `_added_ids` check), so we
    # exercise this via a fresh repository that loads the entity via get_by_id.
    fetch_repo = WidgetRepository(session, correlation_id=uuid.uuid4())
    fetched = await fetch_repo.get_by_id(entity.id)
    assert fetched is not None
    fetched.name = "changed"

    await fetch_repo.persist_all()
    await session.commit()

    other_repo = WidgetRepository(session, correlation_id=uuid.uuid4())
    result = await other_repo.get_by_id(entity.id)
    assert result is not None
    assert result.name == "changed"


async def test_persist_all_skips_entities_added_in_the_same_unit_of_work(
    repo: WidgetRepository, session: AsyncSession
):
    entity = WidgetEntity(name="foo")
    await repo.add(entity)
    await session.commit()

    entity.name = "changed"
    await repo.persist_all()  # should be a no-op for `entity` since it's still in _added_ids
    await session.commit()

    other_repo = WidgetRepository(session, correlation_id=uuid.uuid4())
    result = await other_repo.get_by_id(entity.id)
    assert result is not None
    assert result.name == "foo"


async def test_remove_soft_deletes_when_model_supports_it(
    repo: WidgetRepository, session: AsyncSession
):
    entity = WidgetEntity(name="foo")
    await repo.add(entity)
    await session.commit()

    await repo.remove(entity)
    await session.commit()

    other_repo = WidgetRepository(session, correlation_id=uuid.uuid4())
    # skip_filter=True adds the "deleted_at IS NULL" filter, excluding soft-deleted rows.
    assert await other_repo.get_by_id(entity.id, skip_filter=True) is None
    # skip_filter=False (default) does not filter, so the soft-deleted row is still visible.
    still_visible = await WidgetRepository(session, correlation_id=uuid.uuid4()).get_by_id(
        entity.id
    )
    assert still_visible is not None


async def test_remove_hard_deletes_when_model_has_no_soft_delete_mixin(
    plain_repo: PlainWidgetRepository, session: AsyncSession
):
    entity = WidgetEntity(name="foo")
    await plain_repo.add(entity)
    await session.commit()

    await plain_repo.remove(entity)
    await session.commit()

    other_repo = PlainWidgetRepository(session, correlation_id=uuid.uuid4())
    assert await other_repo.get_by_id(entity.id) is None


async def test_remove_raises_entity_not_found_when_entity_unknown(repo: WidgetRepository):
    with pytest.raises(EntityNotFoundException):
        await repo.remove(WidgetEntity(name="ghost"))


async def test_remove_twice_raises_entity_not_found(repo: WidgetRepository, session: AsyncSession):
    entity = WidgetEntity(name="foo")
    await repo.add(entity)
    await session.commit()
    await repo.remove(entity)

    with pytest.raises(EntityNotFoundException):
        await repo.remove(entity)


async def test_restore_without_prior_removal_raises_entity_not_found(repo: WidgetRepository):
    entity = WidgetEntity(name="foo")

    with pytest.raises(EntityNotFoundException):
        await repo.restore(entity)


async def test_restore_raises_soft_delete_exception_when_model_lacks_soft_delete(
    plain_repo: PlainWidgetRepository, session: AsyncSession
):
    entity = WidgetEntity(name="foo")
    await plain_repo.add(entity)
    await session.commit()
    await plain_repo.remove(entity)

    with pytest.raises(SoftDeleteException):
        await plain_repo.restore(entity)


async def test_restore_undoes_a_soft_delete(repo: WidgetRepository, session: AsyncSession):
    entity = WidgetEntity(name="foo")
    await repo.add(entity)
    await session.commit()
    await repo.remove(entity)
    await session.commit()

    await repo.restore(entity)
    await session.commit()

    other_repo = WidgetRepository(session, correlation_id=uuid.uuid4())
    assert await other_repo.get_by_id(entity.id, skip_filter=True) is not None


async def test_save_domain_events_persists_via_session_add_all_and_returns_them():
    session = MagicMock()
    session.add_all = MagicMock()
    correlation_id = uuid.uuid4()
    repo = ThingRepository(session, correlation_id=correlation_id)

    thing = DummyThing()
    event = make_dummy_event(aggregate_id=thing.id, version=1)
    thing.register_event(event)
    await repo.add(thing)

    saved_events = await repo.save_domain_events()

    assert saved_events == [event]
    session.add_all.assert_called_once()
    (persisted_models,), _ = session.add_all.call_args
    assert len(persisted_models) == 1
    model = persisted_models[0]
    assert isinstance(model, DomainEventModel)
    assert model.id == event.id
    assert model.correlation_id == correlation_id
    assert model.aggregate_id == thing.id
    assert model.message_type == "DummyEvent"
    assert model.payload == event.payload


async def test_save_domain_events_returns_empty_when_no_events_registered():
    session = MagicMock()
    session.add_all = MagicMock()
    repo = ThingRepository(session, correlation_id=uuid.uuid4())
    await repo.add(DummyThing())

    saved_events = await repo.save_domain_events()

    assert saved_events == []
    session.add_all.assert_called_once_with([])


async def test_get_event_history_deserializes_rows_into_domain_events():
    aggregate_id = uuid.uuid4()
    stored = DomainEventModel(
        id=uuid.uuid4(),
        correlation_id=uuid.uuid4(),
        aggregate_id=aggregate_id,
        aggregate_type="DummyThing",
        message_type="DummyEvent",
        occurred_on=make_dummy_event(aggregate_id=aggregate_id).occurred_on,
        version=1,
        created_by_id=uuid.uuid4(),
        payload={"payload_value": "hello"},
    )

    scalars_result = MagicMock()
    scalars_result.all = MagicMock(return_value=[stored])
    session = MagicMock()
    session.scalars = AsyncMock(return_value=scalars_result)
    repo = ThingRepository(session, correlation_id=uuid.uuid4())

    history = await repo.get_event_history(aggregate_id)

    assert len(history) == 1
    event = history[0]
    assert isinstance(event, DummyEvent)
    assert event.id == stored.id
    assert event.aggregate_id == aggregate_id
    assert event.version == 1
    assert event.payload_value == "hello"


async def test_get_event_history_returns_empty_list_when_no_rows():
    scalars_result = MagicMock()
    scalars_result.all = MagicMock(return_value=[])
    session = MagicMock()
    session.scalars = AsyncMock(return_value=scalars_result)
    repo = ThingRepository(session, correlation_id=uuid.uuid4())

    history = await repo.get_event_history(uuid.uuid4())

    assert history == []
