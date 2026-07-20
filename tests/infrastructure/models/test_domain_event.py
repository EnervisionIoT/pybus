from pybus.infrastructure.models.sqlalchemy import DomainEvent


def test_domain_event_model_has_expected_columns():
    columns = {column.name for column in DomainEvent.__table__.columns}
    assert columns == {
        "id",
        "correlation_id",
        "aggregate_id",
        "aggregate_type",
        "message_type",
        "occurred_on",
        "version",
        "created_by_id",
        "payload",
    }


def test_domain_event_model_id_is_primary_key():
    assert DomainEvent.__table__.columns["id"].primary_key is True


def test_domain_event_model_tablename_is_pluralized():
    assert DomainEvent.__tablename__ == "domain_events"
