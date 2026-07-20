import pytest

from pybus.domain.rules import BusinessRule


def test_business_rule_cannot_be_instantiated_directly():
    with pytest.raises(TypeError):
        BusinessRule()  # type: ignore[abstract]


def test_get_message_returns_default_when_not_overridden():
    class UnbrokenRule(BusinessRule):
        def is_broken(self) -> bool:
            return False

    assert UnbrokenRule().get_message() == "Business rule is broken"


def test_get_message_returns_overridden_message():
    class NamedRule(BusinessRule):
        _message: str = "custom message"

        def is_broken(self) -> bool:
            return True

    assert NamedRule().get_message() == "custom message"
