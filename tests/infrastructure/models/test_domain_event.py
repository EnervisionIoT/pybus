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


def test_domain_event_model_version_and_created_by_id_are_nullable():
    # Regression guard: pybus.domain.events.DomainEvent declares version and
    # created_by_id as genuinely optional (`int | None`, `UUID | None`,
    # default=None), and nothing in this codebase currently ever sets them
    # when constructing a domain event. The ORM model must match that
    # optional semantics or persisting any such event violates a NOT NULL
    # constraint. correlation_id is different -- save_domain_events() always
    # populates it from the repository's own correlation_id -- so it stays
    # NOT NULL.
    assert DomainEvent.__table__.columns["version"].nullable is True
    assert DomainEvent.__table__.columns["created_by_id"].nullable is True
    assert DomainEvent.__table__.columns["correlation_id"].nullable is False
