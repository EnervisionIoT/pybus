from abc import ABC, abstractmethod


class Cache(ABC):
    @abstractmethod
    def get(self, key: str) -> str | None:
        raise NotImplementedError()

    @abstractmethod
    def set_value(self, key: str, value: str, expire: int | None = None, nx: bool = False) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def ttl(self, key: str) -> int:
        raise NotImplementedError()

    @abstractmethod
    def get_set(self, key: str) -> set[str]:
        raise NotImplementedError()

    @abstractmethod
    def add_to_set(self, key: str, *values: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def increment(self, key: str) -> int:
        raise NotImplementedError()

    @abstractmethod
    def expire(self, key: str, expire: int) -> None:
        raise NotImplementedError()

    @abstractmethod
    def delete(self, key: str) -> None:
        raise NotImplementedError()
