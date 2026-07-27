import pytest

from pybus.infrastructure.repositories.memory import InMemoryRepository

from tests.conftest import DummyThing, make_dummy_event


@pytest.fixture
def repo() -> InMemoryRepository:
    return InMemoryRepository()


async def test_add_and_get_by_id(repo: InMemoryRepository):
    entity = DummyThing()
    await repo.add(entity)

    result = await repo.get_by_id(entity.id)

    assert result is entity


async def test_get_by_id_returns_none_when_missing(repo: InMemoryRepository):
    result = await repo.get_by_id(DummyThing().id)
    assert result is None


async def test_get_by_ids_filters_out_missing_entities(repo: InMemoryRepository):
    a, b = DummyThing(), DummyThing()
    await repo.add(a)

    result = await repo.get_by_ids([a.id, b.id])

    assert result == [a]


async def test_get_all_without_pagination_returns_list(repo: InMemoryRepository):
    a, b = DummyThing(), DummyThing()
    await repo.add(a)
    await repo.add(b)

    result = await repo.get_all()

    assert set(e.id for e in result) == {a.id, b.id}


async def test_get_all_with_pagination_returns_total_and_page(repo: InMemoryRepository):
    entities = [DummyThing() for _ in range(5)]
    for entity in entities:
        await repo.add(entity)

    total, page = await repo.get_all(page=1, size=2)

    assert total == 5
    assert len(page) == 2


async def test_remove_deletes_entity(repo: InMemoryRepository):
    entity = DummyThing()
    await repo.add(entity)

    await repo.remove(entity)

    assert await repo.get_by_id(entity.id) is None


async def test_restore_re_adds_entity(repo: InMemoryRepository):
    entity = DummyThing()
    await repo.add(entity)
    await repo.remove(entity)

    await repo.restore(entity)

    assert await repo.get_by_id(entity.id) is entity


async def test_get_event_history_returns_events_for_aggregate_root(repo: InMemoryRepository):
    thing = DummyThing()
    event = make_dummy_event(aggregate_id=thing.id)
    thing.register_event(event)
    await repo.add(thing)

    history = await repo.get_event_history(thing.id)

    assert history == [event]


async def test_get_event_history_returns_empty_for_aggregate_root_with_no_events(
    repo: InMemoryRepository,
):
    thing = DummyThing()
    await repo.add(thing)

    history = await repo.get_event_history(thing.id)

    assert history == []


async def test_get_event_history_returns_empty_when_entity_missing(repo: InMemoryRepository):
    history = await repo.get_event_history(DummyThing().id)
    assert history == []


async def test_collect_events_gathers_from_all_aggregate_roots(repo: InMemoryRepository):
    thing1, thing2 = DummyThing(), DummyThing()
    event1 = make_dummy_event(aggregate_id=thing1.id)
    event2 = make_dummy_event(aggregate_id=thing2.id)
    thing1.register_event(event1)
    thing2.register_event(event2)
    await repo.add(thing1)
    await repo.add(thing2)

    events = await repo.collect_events()

    assert set(e.id for e in events) == {event1.id, event2.id}


async def test_save_domain_events_gathers_from_all_aggregate_roots(repo: InMemoryRepository):
    thing = DummyThing()
    event = make_dummy_event(aggregate_id=thing.id)
    thing.register_event(event)
    await repo.add(thing)

    events = await repo.save_domain_events()

    assert events == [event]
    # Subsequent collection should be empty since events were already drained.
    assert await repo.collect_events() == []
