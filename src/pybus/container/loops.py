"""What the outbox relay and the event consumer share.

Both interpolate the service's schema into SQL, which a bind parameter
cannot carry, and both wait in a way a shutdown has to be able to cut short.
"""

import asyncio
import re

_PLAIN_IDENTIFIER = re.compile(r"[a-z_][a-z0-9_]*")


def plain_schema(schema: str) -> str:
    """`schema`, if it can go into SQL unquoted; refused otherwise, at start
    rather than on the first statement that needs it."""
    if not _PLAIN_IDENTIFIER.fullmatch(schema):
        raise ValueError(
            f"POSTGRES_SCHEMA {schema!r} is not a plain lowercase identifier; "
            "pybus will not interpolate it into SQL"
        )
    return schema


async def interruptible_sleep(timeout: float, *events: asyncio.Event) -> None:
    """Wait `timeout` seconds, or until any of `events` is set."""
    waiters = [asyncio.ensure_future(event.wait()) for event in events]
    try:
        await asyncio.wait(waiters, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for waiter in waiters:
            waiter.cancel()


__all__ = ["interruptible_sleep", "plain_schema"]
