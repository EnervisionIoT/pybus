import uuid
from typing import override

import pytest

from pybus.domain.entities import AggregateRoot, EventSourced
from pybus.domain.events import DomainEvent


class DummyEvent(DomainEvent):
    payload_value: str = "default"


class DummyThing(AggregateRoot):
    """Concrete AggregateRoot for tests that just need register/collect events."""

    name: str = "thing"


class DummyCounter(EventSourced):
    """Concrete EventSourced aggregate: applies DummyEvent by incrementing a counter."""

    count: int = 0

    @override
    def _apply(self, event: DomainEvent):
        if isinstance(event, DummyEvent):
            self.count += 1


def make_dummy_event(
    aggregate_id: uuid.UUID | None = None, version: int | None = None
) -> DummyEvent:
    return DummyEvent(
        aggregate_id=aggregate_id or uuid.uuid4(),
        aggregate_type="DummyThing",
        version=version,
    )


@pytest.fixture
def dummy_event_factory():
    return make_dummy_event
