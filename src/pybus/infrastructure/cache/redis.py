from typing import cast, override

from redis import Redis

from pybus.domain.interfaces import Cache


class RedisCache(Cache):
    def __init__(self, host: str, port: int, db: int, password: str | None = None) -> None:
        self._client: Redis = Redis(
            host=host, port=port, db=db, password=password, decode_responses=True
        )

    @override
    def get(self, key: str) -> str | None:
        with self._client as client:
            result = client.get(key)
            return result if isinstance(result, str) else None

    @override
    def set_value(self, key: str, value: str, expire: int | None = None, nx: bool = False) -> bool:
        with self._client as client:
            return cast(bool, client.set(key, value, ex=expire, nx=nx))

    @override
    def ttl(self, key: str) -> int:
        with self._client as client:
            result = client.ttl(key)
            return result if isinstance(result, int) else -2

    @override
    def get_set(self, key: str) -> set[str]:
        with self._client as client:
            result = cast(set[str], client.smembers(key))  # type: ignore
            return result

    @override
    def add_to_set(self, key: str, *values: str) -> None:
        with self._client as client:
            client.sadd(key, *values)

    @override
    def increment(self, key: str) -> int:
        with self._client as client:
            return cast(int, client.incr(key))

    @override
    def expire(self, key: str, expire: int) -> None:
        with self._client as client:
            client.expire(key, expire)

    @override
    def delete(self, key: str) -> None:
        with self._client as client:
            client.delete(key)

    def flushall(self) -> None:
        with self._client as client:
            client.flushall()  # type: ignore
