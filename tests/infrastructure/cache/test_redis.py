from unittest.mock import MagicMock, patch

import pytest

from pybus.infrastructure.cache.redis import RedisCache


@pytest.fixture
def mock_client():
    with patch("pybus.infrastructure.cache.redis.Redis") as mock_redis_cls:
        client = MagicMock()
        mock_redis_cls.return_value = client
        yield client


@pytest.fixture
def cache(mock_client) -> RedisCache:
    return RedisCache(host="localhost", port=6379, db=0, password="secret")


def test_constructor_builds_redis_client_with_given_parameters(mock_client):
    with patch("pybus.infrastructure.cache.redis.Redis") as mock_redis_cls:
        mock_redis_cls.return_value = MagicMock()
        RedisCache(host="myhost", port=1234, db=2, password="pw")
        mock_redis_cls.assert_called_once_with(
            host="myhost", port=1234, db=2, password="pw", decode_responses=True
        )


def test_get_returns_string_value(cache: RedisCache, mock_client: MagicMock):
    mock_client.get.return_value = "value"
    assert cache.get("key") == "value"
    mock_client.get.assert_called_once_with("key")


def test_get_returns_none_when_result_is_not_a_string(cache: RedisCache, mock_client: MagicMock):
    mock_client.get.return_value = None
    assert cache.get("key") is None


def test_set_value_forwards_arguments_and_returns_bool(cache: RedisCache, mock_client: MagicMock):
    mock_client.set.return_value = True
    result = cache.set_value("key", "value", expire=60, nx=True)
    assert result is True
    mock_client.set.assert_called_once_with("key", "value", ex=60, nx=True)


def test_ttl_returns_int_result(cache: RedisCache, mock_client: MagicMock):
    mock_client.ttl.return_value = 42
    assert cache.ttl("key") == 42


def test_ttl_returns_negative_two_fallback_when_not_int(cache: RedisCache, mock_client: MagicMock):
    mock_client.ttl.return_value = None
    assert cache.ttl("key") == -2


def test_get_set_returns_client_result(cache: RedisCache, mock_client: MagicMock):
    mock_client.smembers.return_value = {"a", "b"}
    assert cache.get_set("key") == {"a", "b"}
    mock_client.smembers.assert_called_once_with("key")


def test_add_to_set_forwards_values(cache: RedisCache, mock_client: MagicMock):
    cache.add_to_set("key", "a", "b")
    mock_client.sadd.assert_called_once_with("key", "a", "b")


def test_increment_returns_int_result(cache: RedisCache, mock_client: MagicMock):
    mock_client.incr.return_value = 5
    assert cache.increment("key") == 5
    mock_client.incr.assert_called_once_with("key")


def test_expire_forwards_to_client(cache: RedisCache, mock_client: MagicMock):
    cache.expire("key", 30)
    mock_client.expire.assert_called_once_with("key", 30)


def test_delete_forwards_to_client(cache: RedisCache, mock_client: MagicMock):
    cache.delete("key")
    mock_client.delete.assert_called_once_with("key")


def test_flushall_forwards_to_client(cache: RedisCache, mock_client: MagicMock):
    cache.flushall()
    mock_client.flushall.assert_called_once_with()
