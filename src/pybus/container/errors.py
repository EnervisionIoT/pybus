"""An exception as text, without the input that caused it.

The one place an exception becomes text for a log line or for the
`dead_letters.error` column. A consumed message may carry an invitation
token, and three kinds of error quote their input in their own message:
a pydantic `ValidationError` names the value it refused, a SQLAlchemy
`StatementError` appends the statement's parameters, and a psycopg error's
`str()` is the server's whole message, DETAIL line included -- `Key
(token)=(...) already exists`, or `Failing row contains (...)`, a whole row.

Its own module, rather than `dead_letters`, because `transaction` needs it
too and `dead_letters` already reaches `transaction` through `application`.
"""

from typing import Any

from pydantic import ValidationError
from sqlalchemy.exc import StatementError

MAX_DESCRIBED_LENGTH = 2000


def _storable(text: str) -> str:
    """Postgres `text` refuses NUL and the driver cannot encode a lone surrogate,
    so a dead letter carrying either could never be written and the consumer
    would stop on it for good, again after every restart."""
    return text.replace("\x00", "").encode("utf-8", "replace").decode("utf-8")


def storable_name(text: str) -> str:
    """`_storable`, on one line: for a name read off the message, such as its
    `message_type`, which is logged as it is and could otherwise start a
    forged log line of its own. An error message keeps its lines; it was
    written by code, not by whoever produced the message."""
    return _storable(text).replace("\r", " ").replace("\n", " ")


def describe_error(error: BaseException) -> str:
    """The error as it is stored and logged: its class and its message, and
    never the input that caused it.

    Capped, because the column and the log line both take it whole and
    nothing here controls how long an error's message is.
    """
    described = _storable(_describe(error))
    if len(described) <= MAX_DESCRIBED_LENGTH:
        return described
    return described[: MAX_DESCRIBED_LENGTH - 3] + "..."


def _driver_diag(error: BaseException) -> Any | None:
    """A psycopg error's `diag`, or None for anything else. Duck-typed, so
    pybus does not have to depend on psycopg to recognise its errors."""
    diag = getattr(error, "diag", None)
    return diag if isinstance(getattr(diag, "message_primary", None), str) else None


def _describe(error: BaseException) -> str:
    if isinstance(error, StatementError) and error.orig is not None:
        return f"{type(error).__name__}: {_describe(error.orig)}"
    if isinstance(error, ValidationError):
        details = "; ".join(
            f"{'.'.join(str(part) for part in detail['loc']) or '<root>'}: {detail['msg']}"
            for detail in error.errors(
                include_url=False, include_context=False, include_input=False
            )
        )
        return f"ValidationError: {details}"
    diag = _driver_diag(error)
    if diag is not None:
        codes = ", ".join(
            f"{label}={value}"
            for label, value in (
                ("sqlstate", getattr(diag, "sqlstate", None)),
                ("constraint", getattr(diag, "constraint_name", None)),
            )
            if isinstance(value, str) and value
        )
        return f"{type(error).__name__}: {diag.message_primary}" + (f" [{codes}]" if codes else "")
    return f"{type(error).__name__}: {error}"


def quotes_input(error: BaseException) -> bool:
    """Whether a traceback of `error` would print its input.

    The traceback formatter follows the whole `__cause__`/`__context__`
    chain, so a handler that catches an IntegrityError and raises a domain
    error would put the parameters back into the log through the link
    underneath.
    """
    seen: set[int] = set()
    pending: list[BaseException | None] = [error]
    while pending:
        link = pending.pop()
        if link is None or id(link) in seen:
            continue
        seen.add(id(link))
        if isinstance(link, (ValidationError, StatementError)) or _driver_diag(link) is not None:
            return True
        pending.extend((link.__cause__, link.__context__))
    return False


__all__ = ["MAX_DESCRIBED_LENGTH", "describe_error", "quotes_input", "storable_name"]
