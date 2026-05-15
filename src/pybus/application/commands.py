import uuid
from typing import Any, ClassVar

from pydantic import BaseModel, Field, computed_field


class Command[TResult: Any](BaseModel):
    _registry: ClassVar[dict[str, type["Command[Any]"]]] = {}

    id: uuid.UUID = Field(default_factory=uuid.uuid4)

    @computed_field
    @property
    def command_type(self) -> str:
        return self.__class__.__name__

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls._registry[cls.__name__] = cls

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "Command[TResult]":
        target_cls = cls._registry.get(data["command_type"], cls)
        return target_cls.model_validate(data)
