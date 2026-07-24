import json
import uuid

from pybus.domain.events import DomainEvent
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


class DummyEventWithUuidField(DomainEvent):
    related_id: uuid.UUID


def test_payload_serializes_uuid_fields_to_json_safe_strings():
    # Regression guard: domain_events.payload is a Postgres JSONB column
    # persisted via stdlib json.dumps (no custom json_serializer configured
    # anywhere), which cannot encode a raw uuid.UUID -- TypeError: Object of
    # type UUID is not JSON serializable. Most iam domain events carry a UUID
    # field (RefreshTokenIssued.user_id, RoleAssignmentGranted.role_id, ...),
    # so payload must recursively coerce UUIDs (and other non-JSON-primitive
    # types) to JSON-safe values, not just plain model_dump() them.
    related_id = uuid.uuid4()
    event = DummyEventWithUuidField(
        aggregate_id=uuid.uuid4(),
        aggregate_type="DummyThing",
        related_id=related_id,
    )

    payload = event.payload

    assert payload == {"related_id": str(related_id)}
    # Must genuinely be JSON-serializable, not merely equal to a string.
    assert json.dumps(payload) == json.dumps({"related_id": str(related_id)})
