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
    # Not `type[Self]`: the registry is keyed by class name and holds every
    # subclass that ever registered itself, which is exactly the set
    # `deserialize` has to be able to return one of. `Self` narrowed each
    # entry to the class doing the lookup and made `__init_subclass__`'s own
    # registration -- the only thing that ever writes here -- a type error.
    _registry: ClassVar[dict[str, type["TypeRegistryMixin"]]] = {}

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
        # Ours, not mypy's: the class is picked by a string in the payload,
        # so nothing static can show the registry entry is a `cls`. `Self`
        # is still the annotation callers need -- every call site is a root
        # (`DomainEvent.deserialize`), and a root's registry holds only its
        # own subclasses. What this hides is calling it on a leaf with a
        # payload naming a sibling, which was unsound before mypy saw it.
        return target_cls.model_validate(data)  # type: ignore[return-value]
