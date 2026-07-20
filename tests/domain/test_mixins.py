import pytest

from pybus.domain.exceptions import BusinessRuleValidationException
from pybus.domain.mixins import BusinessRuleValidationMixin, TypeRegistryMixin
from pybus.domain.rules import BusinessRule


class AlwaysBrokenRule(BusinessRule):
    _message: str = "always broken"

    def is_broken(self) -> bool:
        return True


class NeverBrokenRule(BusinessRule):
    def is_broken(self) -> bool:
        return False


class Validator(BusinessRuleValidationMixin):
    pass


def test_check_rule_raises_when_broken():
    validator = Validator()
    with pytest.raises(BusinessRuleValidationException) as exc_info:
        validator.check_rule(AlwaysBrokenRule())
    assert exc_info.value.rule.get_message() == "always broken"


def test_check_rule_does_not_raise_when_not_broken():
    validator = Validator()
    validator.check_rule(NeverBrokenRule())


class Widget(TypeRegistryMixin):
    name: str = "widget"


class Gadget(TypeRegistryMixin):
    name: str = "gadget"


class SpecialWidget(Widget):
    pass


def test_message_type_computed_field_is_class_name():
    assert Widget().message_type == "Widget"
    assert SpecialWidget().message_type == "SpecialWidget"


def test_registry_is_isolated_per_direct_subclass_of_type_registry_mixin():
    assert "Widget" in Widget._registry
    assert "SpecialWidget" in Widget._registry
    assert "Gadget" not in Widget._registry
    assert "Widget" not in Gadget._registry


def test_deserialize_dispatches_to_registered_subclass():
    data = {"message_type": "SpecialWidget", "name": "foo"}
    instance = Widget.deserialize(data)
    assert isinstance(instance, SpecialWidget)
    assert instance.name == "foo"


def test_deserialize_falls_back_to_calling_class_when_unregistered():
    data = {"message_type": "Unknown", "name": "bar"}
    instance = Widget.deserialize(data)
    assert isinstance(instance, Widget)
    assert type(instance) is Widget
    assert instance.name == "bar"
