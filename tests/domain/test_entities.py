from pybus.domain.entities import Entity

from tests.conftest import DummyCounter, DummyThing, make_dummy_event


def test_entity_generates_unique_id_by_default():
    a = Entity()
    b = Entity()
    assert a.id != b.id


def test_aggregate_root_register_and_collect_events():
    thing = DummyThing()
    event = make_dummy_event(aggregate_id=thing.id)

    thing.register_event(event)
    collected = thing.collect_events()

    assert collected == [event]


def test_collect_events_empties_the_pending_list():
    thing = DummyThing()
    thing.register_event(make_dummy_event(aggregate_id=thing.id))

    thing.collect_events()
    second_collect = thing.collect_events()

    assert second_collect == []


def test_event_sourced_apply_uses_event_version_when_present():
    counter = DummyCounter()
    event = make_dummy_event(aggregate_id=counter.id, version=5)

    counter.apply(event)

    assert counter._version == 5
    assert counter.count == 1


def test_event_sourced_apply_increments_version_when_absent():
    counter = DummyCounter()

    counter.apply(make_dummy_event(aggregate_id=counter.id, version=None))
    counter.apply(make_dummy_event(aggregate_id=counter.id, version=None))

    assert counter._version == 2
    assert counter.count == 2


def test_event_sourced_rebuild_replays_all_events():
    aggregate_id = DummyThing().id
    events = [
        make_dummy_event(aggregate_id=aggregate_id, version=1),
        make_dummy_event(aggregate_id=aggregate_id, version=2),
    ]

    counter = DummyCounter.rebuild(events)

    assert counter.count == 2
    assert counter._version == 2
