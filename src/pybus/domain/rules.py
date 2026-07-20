from abc import ABC, abstractmethod

from pydantic import BaseModel


class BusinessRule(BaseModel, ABC):
    _message: str = "Business rule is broken"

    def get_message(self) -> str:
        return self._message

    @abstractmethod
    def is_broken(self) -> bool:
        raise NotImplementedError()
