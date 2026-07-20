from typing import final, override

from .rules import BusinessRule


class DomainException(Exception):
    message: str

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


@final
class BusinessRuleValidationException(DomainException):
    rule: BusinessRule

    def __init__(self, rule: BusinessRule):
        self.rule = rule
        self.message = f"Business rule violated: {self.rule.get_message()}"
        super().__init__(self.message)

    @override
    def __str__(self):
        return str(self.rule)


@final
class EntityNotFoundException(DomainException):
    def __init__(self, repository_name: str, **kwargs: object):
        self.message = f"Entity {kwargs} not found in {repository_name}"
        super().__init__(self.message)


@final
class SoftDeleteException(DomainException):
    def __init__(self, repository_name: str, **kwargs: object):
        self.message = f"The entity's ORM model does not support soft delete in {repository_name}"
        super().__init__(self.message)
