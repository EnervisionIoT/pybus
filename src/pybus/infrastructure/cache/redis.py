from typing import cast, override

from redis import Redis

from pybus.application.interfaces import Cache


class RedisCache(Cache):
    def __init__(self, host: str, port: int, db: int, password: str | None = None) -> None:
        self._client: Redis = Redis(
            host=host, port=port, db=db, password=password, decode_responses=True
        )

    @override
    def get(self, key: str) -> str | None:
        result = self._client.get(key)
        return result if isinstance(result, str) else None

    @override
    def set_value(self, key: str, value: str, expire: int | None = None, nx: bool = False) -> bool:
        return cast(bool, self._client.set(key, value, ex=expire, nx=nx))

    @override
    def ttl(self, key: str) -> int:
        result = self._client.ttl(key)
        return result if isinstance(result, int) else -2

    @override
    def get_set(self, key: str) -> set[str]:
        result = cast(set[str], self._client.smembers(key))  # type: ignore
        return result

    @override
    def add_to_set(self, key: str, *values: str) -> None:
        self._client.sadd(key, *values)

    @override
    def increment(self, key: str) -> int:
        return cast(int, self._client.incr(key))

    @override
    def expire(self, key: str, expire: int) -> None:
        self._client.expire(key, expire)

    @override
    def delete(self, key: str) -> None:
        self._client.delete(key)

    def flushall(self) -> None:
        self._client.flushall()  # type: ignore
