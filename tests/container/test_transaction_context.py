import logging
import uuid
from collections.abc import Awaitable, Callable
from unittest.mock import AsyncMock, MagicMock

import pytest
from confluent_kafka.aio import AIOProducer
from dependency_injector import containers, providers

from pybus.application.commands import Command
from pybus.application.common.pagination import PaginationQuery
from pybus.application.queries import Query
from pybus.container.transaction import DependencyProvider, TransactionContext
from pybus.domain.events import DomainEvent

from tests.conftest import make_dummy_event


class DoThing(Command[str]):
    value: str = "x"


class GetThing(Query[str]):
    pass


class SomeService:
    pass


def make_context(container: containers.Container | None = None) -> TransactionContext:
    if container is None:

        class EmptyContainer(containers.DeclarativeContainer):
            pass

        container = EmptyContainer()
    return TransactionContext(DependencyProvider(container))


def no_handlers(message):
    return iter([])


async def test_aenter_calls_on_enter_hook_and_returns_self():
    ctx = make_context()
    on_enter = AsyncMock()
    ctx.configure(handlers_iterator=no_handlers, on_enter_transaction_context=on_enter)

    result = await ctx.__aenter__()

    assert result is ctx
    on_enter.assert_awaited_once_with(ctx)


async def test_aenter_is_noop_without_hook():
    ctx = make_context()
    ctx.configure(handlers_iterator=no_handlers)

    result = await ctx.__aenter__()

    assert result is ctx


async def test_aexit_calls_on_exit_hook_with_exception_info():
    ctx = make_context()
    on_exit = AsyncMock()
    ctx.configure(handlers_iterator=no_handlers, on_exit_transaction_context=on_exit)
    exc = ValueError("boom")

    await ctx.__aexit__(ValueError, exc, None)

    on_exit.assert_awaited_once_with(ctx, exc)


async def test_aexit_is_noop_without_hook():
    ctx = make_context()
    ctx.configure(handlers_iterator=no_handlers)

    await ctx.__aexit__(None, None, None)  # must not raise


async def test_resolve_parameters_injects_message_for_command_annotated_param():
    ctx = make_context()
    ctx.configure(handlers_iterator=no_handlers)

    async def handler(command: DoThing) -> None: ...

    command = DoThing(value="hi")
    params = await ctx._resolve_parameters(handler, command)

    assert params == {"command": command}


async def test_resolve_parameters_injects_pagination_for_union_annotated_param():
    ctx = make_context()
    ctx.configure(handlers_iterator=no_handlers)

    async def handler(query: GetThing, pagination: PaginationQuery | None) -> None: ...

    query = GetThing()
    pagination = PaginationQuery(page=2, size=5)
    params = await ctx._resolve_parameters(handler, query, pagination)

    assert params["query"] is query
    assert params["pagination"] is pagination


async def test_resolve_parameters_skips_parameters_without_annotation():
    ctx = make_context()
    ctx.configure(handlers_iterator=no_handlers)

    async def handler(command: DoThing, untyped) -> None: ...

    params = await ctx._resolve_parameters(handler, DoThing())

    assert "untyped" not in params


async def test_resolve_parameters_resolves_dependency_by_type_annotation():
    class C(containers.DeclarativeContainer):
        service = providers.Factory(SomeService)

    ctx = make_context(C())
    ctx.configure(handlers_iterator=no_handlers)

    async def handler(command: DoThing, service: SomeService) -> None: ...

    params = await ctx._resolve_parameters(handler, DoThing())

    assert isinstance(params["service"], SomeService)


async def test_resolve_parameters_resolves_dependency_by_parameter_name_for_non_class_annotation():
    class C(containers.DeclarativeContainer):
        pass

    ctx = make_context(C())
    ctx.configure(handlers_iterator=no_handlers)
    ctx.set_dependency("publish_event", "the-callback")

    async def handler(command: DoThing, publish_event: Callable[..., Awaitable[None]]) -> None: ...

    params = await ctx._resolve_parameters(handler, DoThing())

    assert params["publish_event"] == "the-callback"


async def test_call_applies_middlewares_in_reverse_registration_order():
    ctx = make_context()
    order: list[str] = []

    async def handler(command: DoThing) -> str:
        order.append("handler")
        return "result"

    async def middleware_a(context: TransactionContext, call_next):
        order.append("a-before")
        result = await call_next()
        order.append("a-after")
        return result

    async def middleware_b(context: TransactionContext, call_next):
        order.append("b-before")
        result = await call_next()
        order.append("b-after")
        return result

    ctx.configure(handlers_iterator=no_handlers, middlewares=[middleware_a, middleware_b])

    result = await ctx.call(handler, DoThing())

    assert result == "result"
    assert order == ["b-before", "a-before", "handler", "a-after", "b-after"]


def container_with_logger(logger: logging.Logger) -> containers.Container:
    class C(containers.DeclarativeContainer):
        app_logger = providers.Dependency(instance_of=logging.Logger)

    return C(app_logger=logger)


async def test_execute_command_calls_the_first_handler_and_returns_its_result():
    ctx = make_context()

    async def handler(command: DoThing) -> str:
        return f"handled-{command.value}"

    ctx.configure(handlers_iterator=lambda message: iter([handler]))

    result = await ctx.execute_command(DoThing(value="x"))

    assert result == "handled-x"


async def test_execute_command_raises_when_handlers_iterator_not_configured():
    ctx = make_context()

    with pytest.raises(RuntimeError, match="not configured"):
        await ctx.execute_command(DoThing())


async def test_execute_command_raises_when_no_handler_found():
    logger = MagicMock(spec=logging.Logger)
    ctx = make_context(container_with_logger(logger))
    ctx.configure(handlers_iterator=no_handlers)

    with pytest.raises(Exception, match="No handler found"):
        await ctx.execute_command(DoThing())


async def test_execute_command_logs_and_reraises_handler_exception():
    logger = MagicMock(spec=logging.Logger)
    ctx = make_context(container_with_logger(logger))

    async def failing_handler(command: DoThing) -> None:
        raise ValueError("handler blew up")

    ctx.configure(handlers_iterator=lambda message: iter([failing_handler]))

    with pytest.raises(ValueError, match="handler blew up"):
        await ctx.execute_command(DoThing())

    logger.error.assert_called_once()


async def test_execute_query_calls_the_first_handler_and_returns_its_result():
    ctx = make_context()

    async def handler(query: GetThing) -> str:
        return "query-result"

    ctx.configure(handlers_iterator=lambda message: iter([handler]))

    result = await ctx.execute_query(GetThing())

    assert result == "query-result"


async def test_execute_query_raises_when_no_handler_found():
    logger = MagicMock(spec=logging.Logger)
    ctx = make_context(container_with_logger(logger))
    ctx.configure(handlers_iterator=no_handlers)

    with pytest.raises(Exception, match="No handler found"):
        await ctx.execute_query(GetThing())


async def test_execute_query_raises_when_handlers_iterator_not_configured():
    ctx = make_context()

    with pytest.raises(RuntimeError, match="not configured"):
        await ctx.execute_query(GetThing())


async def test_execute_query_logs_and_reraises_handler_exception():
    logger = MagicMock(spec=logging.Logger)
    ctx = make_context(container_with_logger(logger))

    async def failing_handler(query: GetThing) -> None:
        raise ValueError("query handler blew up")

    ctx.configure(handlers_iterator=lambda message: iter([failing_handler]))

    with pytest.raises(ValueError, match="query handler blew up"):
        await ctx.execute_query(GetThing())

    logger.error.assert_called_once()


async def test_execute_event_raises_when_handlers_iterator_not_configured():
    ctx = make_context()

    with pytest.raises(RuntimeError, match="not configured"):
        await ctx.execute_event(make_dummy_event())


async def test_execute_event_invokes_all_matching_handlers():
    calls: list[str] = []

    async def handler(event: DomainEvent) -> None:
        calls.append("called")

    ctx = make_context()
    ctx.configure(handlers_iterator=lambda message: iter([handler]))

    await ctx.execute_event(make_dummy_event())

    assert calls == ["called"]


async def test_execute_event_logs_and_reraises_handler_exception():
    logger = MagicMock(spec=logging.Logger)
    ctx = make_context(container_with_logger(logger))

    async def failing_handler(event: DomainEvent) -> None:
        raise ValueError("event handler blew up")

    ctx.configure(handlers_iterator=lambda message: iter([failing_handler]))

    with pytest.raises(ValueError, match="event handler blew up"):
        await ctx.execute_event(make_dummy_event())

    logger.error.assert_called_once()


async def test_publish_event_produces_to_kafka_with_correlation_id_set():
    correlation_id = uuid.uuid4()
    fake_producer = MagicMock(spec=AIOProducer)
    fake_producer.produce = AsyncMock()

    class C(containers.DeclarativeContainer):
        correlation_id_dep = providers.Dependency(instance_of=uuid.UUID)
        kafka_producer_dep = providers.Dependency(instance_of=AIOProducer)

    container = C(correlation_id_dep=correlation_id, kafka_producer_dep=fake_producer)
    ctx = make_context(container)

    event = make_dummy_event()
    await ctx.publish_event(event)

    assert event.correlation_id == correlation_id
    fake_producer.produce.assert_awaited_once()
    _, kwargs = fake_producer.produce.call_args
    assert kwargs["topic"] == TransactionContext.DOMAIN_EVENTS_TOPIC
    assert kwargs["key"] == str(event.aggregate_id).encode("utf-8")
