import uuid

from pybus.domain.exceptions import (
    BusinessRuleValidationException,
    DomainException,
    EntityNotFoundException,
    SoftDeleteException,
)
from pybus.domain.rules import BusinessRule


class BrokenRule(BusinessRule):
    _message: str = "widget is broken"

    def is_broken(self) -> bool:
        return True


def test_domain_exception_stores_message():
    exc = DomainException("something went wrong")
    assert exc.message == "something went wrong"
    assert str(exc) == "something went wrong"


def test_business_rule_validation_exception_uses_rule_message():
    rule = BrokenRule()
    exc = BusinessRuleValidationException(rule)

    assert exc.rule is rule
    assert exc.message == "Business rule violated: widget is broken"


def test_business_rule_validation_exception_str_delegates_to_rule():
    rule = BrokenRule()
    exc = BusinessRuleValidationException(rule)

    assert str(exc) == str(rule)


def test_entity_not_found_exception_message_includes_repository_and_kwargs():
    entity_id = uuid.uuid4()
    exc = EntityNotFoundException("WidgetRepository", entity_id=entity_id)

    assert "WidgetRepository" in exc.message
    assert str(entity_id) in exc.message


def test_soft_delete_exception_message_includes_repository_name():
    exc = SoftDeleteException("WidgetRepository", entity_id=uuid.uuid4())

    assert "WidgetRepository" in exc.message
    assert "soft delete" in exc.message
