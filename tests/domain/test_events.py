import uuid

from tests.conftest import DummyEvent


def test_payload_excludes_bookkeeping_fields():
    event = DummyEvent(
        aggregate_id=uuid.uuid4(),
        aggregate_type="DummyThing",
        payload_value="hello",
    )

    payload = event.payload

    assert payload == {"payload_value": "hello"}
    for field in (
        "id",
        "correlation_id",
        "aggregate_id",
        "aggregate_type",
        "message_type",
        "occurred_on",
        "version",
        "created_by_id",
    ):
        assert field not in payload
