from typing import TYPE_CHECKING, Any, ClassVar, Self

from pydantic import BaseModel, computed_field

from .exceptions import BusinessRuleValidationException

if TYPE_CHECKING:
    from .rules import BusinessRule


class BusinessRuleValidationMixin:
    def check_rule(self, rule: "BusinessRule"):
        if rule.is_broken():
            raise BusinessRuleValidationException(rule)


class TypeRegistryMixin(BaseModel):
    _registry: ClassVar[dict[str, type[Self]]] = {}

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if TypeRegistryMixin in cls.__bases__:
            cls._registry = {}
        cls._registry[cls.__name__] = cls

    @computed_field
    @property
    def message_type(self) -> str:
        return self.__class__.__name__

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        target_cls = cls._registry.get(data["message_type"], cls)
        return target_cls.model_validate(data)
