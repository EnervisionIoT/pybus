import asyncio

import pytest

from pybus.container.loops import interruptible_sleep, plain_schema


@pytest.mark.parametrize("schema", ["iam", "notify_v2", "_x"])
def test_a_plain_schema_is_returned_unchanged(schema):
    assert plain_schema(schema) == schema


@pytest.mark.parametrize("schema", ["Iam", "iam; DROP TABLE x", "iam.events", "", "1iam"])
def test_a_schema_that_would_need_quoting_is_refused(schema):
    with pytest.raises(ValueError, match="POSTGRES_SCHEMA"):
        plain_schema(schema)


async def test_the_sleep_ends_as_soon_as_an_event_is_set():
    stop = asyncio.Event()
    asyncio.get_running_loop().call_later(0.05, stop.set)

    await asyncio.wait_for(interruptible_sleep(60, stop), timeout=2)


async def test_the_sleep_ends_at_its_timeout_when_no_event_is_set():
    await asyncio.wait_for(interruptible_sleep(0.01, asyncio.Event()), timeout=2)
