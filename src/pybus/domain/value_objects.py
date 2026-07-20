import io
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, computed_field, model_validator


class ValueObject(BaseModel):
    _registry: ClassVar[dict[str, type["ValueObject"]]] = {}

    @computed_field
    @property
    def value_type(self) -> str:
        return self.__class__.__name__

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls._registry[cls.__name__] = cls

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "ValueObject":
        target_cls = cls._registry.get(data["value_type"], cls)
        return target_cls.model_validate(data)

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)


class FileObject(ValueObject):
    filename: str
    content_type: str
    size: int
    stream: io.IOBase

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    @model_validator(mode="after")
    def compute_and_validate_size(self) -> "FileObject":
        self.stream.seek(0, 2)
        size = self.stream.tell()
        self.stream.seek(0)
        if size > 2 * 1024 * 1024:
            raise ValueError("File size exceeds the maximum limit of 2MB")
        object.__setattr__(self, "size", size)
        return self

    def to_bytes(self) -> bytes:
        self.stream.seek(0)
        return self.stream.read()
