import pytest

from pybus.domain.exceptions import BusinessRuleValidationException
from pybus.domain.rules import BusinessRule
from pybus.domain.services import DomainService


class BrokenRule(BusinessRule):
    def is_broken(self) -> bool:
        return True


class PricingService(DomainService):
    def validate(self):
        self.check_rule(BrokenRule())


def test_domain_service_inherits_business_rule_validation():
    with pytest.raises(BusinessRuleValidationException):
        PricingService().validate()
