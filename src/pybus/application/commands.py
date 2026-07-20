import uuid
from typing import Any

from pydantic import Field

from ..domain.mixins import TypeRegistryMixin


class Command[TResult: Any = None](TypeRegistryMixin):
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
