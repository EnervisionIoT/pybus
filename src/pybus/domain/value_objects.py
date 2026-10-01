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
    def compute_size(self) -> "FileObject":
        """`size` is measured from the stream, never taken from the caller.

        This used to refuse anything over 2MB as well. That was a policy, and
        a value object is the wrong owner for one: the first service to use
        `Storage` for real (utility, storing photographed bills) needs 15MB,
        and every caller already knows its own limit better than a framework
        constant does. Enforce size where the upload arrives.
        """
        self.stream.seek(0, 2)
        size = self.stream.tell()
        self.stream.seek(0)
        object.__setattr__(self, "size", size)
        return self

    def to_bytes(self) -> bytes:
        self.stream.seek(0)
        return self.stream.read()
