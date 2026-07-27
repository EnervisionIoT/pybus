import logging
from functools import partial
from unittest.mock import AsyncMock, MagicMock

import pytest
from confluent_kafka.aio import AIOProducer
from dependency_injector import providers

from pybus.application import ApplicationModule
from pybus.application.commands import Command
from pybus.application.queries import Query
from pybus.container.application import Application, ApplicationContainer, create_application
from pybus.container.config import ApplicationSettings
from pybus.container.transaction import TransactionContainer, TransactionContext
from pybus.domain.repositories import GenericRepository
from pybus.infrastructure.database.session import DataBaseSession
from tests.conftest import make_dummy_event


class DoThing(Command[str]):
    pass


class GetThing(Query[str]):
    pass


def build_application() -> Application:
    return create_application(name="test-app", container=MagicMock(), modules=[])


def test_create_application_registers_lifecycle_hooks_and_one_middleware():
    app = build_application()

    assert app._on_enter_transaction_context is not None
    assert app._on_exit_transaction_context is not None
    assert len(app._transaction_middleware) == 1


def test_create_application_includes_given_modules():
    sub_module = ApplicationModule(name="sub")

    app = create_application(name="test-app", container=MagicMock(), modules=[sub_module])

    assert sub_module in app._sub_modules


async def test_on_enter_hook_sets_publish_event_as_a_string_dependency():
    app = build_application()
    fake_context = MagicMock()
    fake_context.set_dependency = MagicMock()
    fake_context.publish_event = AsyncMock()

    await app._on_enter_transaction_context(fake_context)

    fake_context.set_dependency.assert_called_once_with("publish_event", fake_context.publish_event)


async def test_on_exit_hook_commits_and_closes_when_no_exception():
    app = build_application()
    session = AsyncMock()
    fake_context = MagicMock()
    fake_context.get_dependency = MagicMock(return_value=session)

    await app._on_exit_transaction_context(fake_context, None)

    session.commit.assert_awaited_once()
    session.rollback.assert_not_called()
    session.close.assert_awaited_once()


async def test_on_exit_hook_rolls_back_and_closes_on_exception():
    app = build_application()
    session = AsyncMock()
    fake_context = MagicMock()
    fake_context.get_dependency = MagicMock(return_value=session)

    await app._on_exit_transaction_context(fake_context, ValueError("boom"))

    session.rollback.assert_awaited_once()
    session.commit.assert_not_called()
    session.close.assert_awaited_once()


async def test_event_collector_middleware_publishes_domain_events_from_repository_kwargs():
    app = build_application()
    middleware = app._transaction_middleware[0]

    fake_repo = MagicMock(spec=GenericRepository)
    event = make_dummy_event()
    fake_repo.save_domain_events = AsyncMock(return_value=[event])

    async def handler(repo) -> str:
        return "handler-result"

    call_next = partial(handler, repo=fake_repo)

    fake_context = MagicMock()
    fake_context.publish_event = AsyncMock()

    result = await middleware(fake_context, call_next)

    assert result == "handler-result"
    fake_repo.save_domain_events.assert_awaited_once()
    fake_context.publish_event.assert_awaited_once_with(event)


async def test_event_collector_middleware_ignores_non_repository_kwargs():
    app = build_application()
    middleware = app._transaction_middleware[0]

    async def handler(command) -> str:
        return "handler-result"

    call_next = partial(handler, command=DoThing())

    fake_context = MagicMock()
    fake_context.publish_event = AsyncMock()

    result = await middleware(fake_context, call_next)

    assert result == "handler-result"
    fake_context.publish_event.assert_not_called()


class FakeTransactionContext:
    def __init__(self):
        self.execute_command = AsyncMock(return_value="cmd-result")
        self.execute_query = AsyncMock(return_value="query-result")
        self.execute_event = AsyncMock(return_value=None)
        self.entered = False
        self.exited = False

    async def __aenter__(self):
        self.entered = True
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.exited = True


async def test_execute_dispatches_command(monkeypatch: pytest.MonkeyPatch):
    app = build_application()
    fake_ctx = FakeTransactionContext()
    monkeypatch.setattr(app, "transaction_context", lambda: fake_ctx)

    command = DoThing()
    result = await app.execute(command)

    assert result == "cmd-result"
    fake_ctx.execute_command.assert_awaited_once_with(command)
    assert fake_ctx.entered and fake_ctx.exited


async def test_execute_dispatches_query(monkeypatch: pytest.MonkeyPatch):
    app = build_application()
    fake_ctx = FakeTransactionContext()
    monkeypatch.setattr(app, "transaction_context", lambda: fake_ctx)

    query = GetThing()
    result = await app.execute(query)

    assert result == "query-result"
    fake_ctx.execute_query.assert_awaited_once_with(query, None)


async def test_execute_dispatches_domain_event(monkeypatch: pytest.MonkeyPatch):
    app = build_application()
    fake_ctx = FakeTransactionContext()
    monkeypatch.setattr(app, "transaction_context", lambda: fake_ctx)

    event = make_dummy_event()
    result = await app.execute(event)

    assert result is None
    fake_ctx.execute_event.assert_awaited_once_with(event)


def test_on_enter_transaction_context_decorator_stores_and_returns_func():
    app = Application(name="test", container=MagicMock())

    async def hook(ctx): ...

    result = app.on_enter_transaction_context(hook)

    assert result is hook
    assert app._on_enter_transaction_context is hook


def test_transaction_middleware_decorator_appends_and_returns_func():
    app = Application(name="test", container=MagicMock())

    async def mw(ctx, call_next): ...

    result = app.transaction_middleware(mw)

    assert result is mw
    assert mw in app._transaction_middleware


def test_transaction_context_builds_context_wired_with_application_hooks():
    app = Application(name="test", container=MagicMock())

    async def on_enter(ctx): ...

    app.on_enter_transaction_context(on_enter)

    ctx = app.transaction_context()

    assert isinstance(ctx, TransactionContext)
    assert ctx._handlers_iterator == app.get_handlers
    assert ctx._on_enter_transaction_context is on_enter


def test_application_container_self_wiring():
    """Test that ApplicationContainer.application() resolves with reference to the container."""
    from pybus.container.application import ApplicationContainer
    from pybus.container.config import ApplicationSettings

    # Create a real container with minimal setup
    container = ApplicationContainer()
    container.config.override(ApplicationSettings())

    # Mock external dependencies that are hard to set up
    container.kafka_producer.override(MagicMock())
    container.session.override(MagicMock())
    container.logger.override(MagicMock())

    # Resolve the application and verify container binding
    app = container.application()

    assert app._container is container


def test_application_container_self_provider_binding():
    """Test that __self__ is bound as a Self provider for wiring consistency."""
    from pybus.container.application import ApplicationContainer
    from dependency_injector import providers

    # Verify __self__ is defined as a Self provider for wiring consistency
    assert isinstance(ApplicationContainer.__self__, providers.Self)


def test_transaction_container_builds_real_instance_wired_with_injected_dependencies():
    """`transaction_container()` must genuinely construct a `TransactionContainer`
    with the container's kafka_producer/session/logger injected into it -- not
    silently return the bare `TransactionContainer` class untouched."""
    container = ApplicationContainer()
    container.config.override(ApplicationSettings())

    kafka_producer = MagicMock(spec=AIOProducer)
    session = MagicMock(spec=DataBaseSession)
    logger = MagicMock(spec=logging.Logger)
    container.kafka_producer.override(kafka_producer)
    container.session.override(session)
    container.logger.override(logger)

    built = container.transaction_container()

    # The old bug returned the bare class itself (Object._provide ignores
    # args/kwargs), so guard against that regression explicitly.
    assert built is not TransactionContainer
    assert not isinstance(built, type)

    # `TransactionContainer()` instances are dependency_injector
    # `DynamicContainer`s (declarative containers don't support isinstance
    # checks against themselves), so verify it's a real, correctly wired
    # container by its provider surface and resolved values instead.
    assert set(built.providers) == set(TransactionContainer.providers)
    assert built.kafka_producer() is kafka_producer
    assert built.session() is session
    assert built.logger() is logger


class _MarkerTransactionContainer(TransactionContainer):
    """Throwaway subclass used to prove `transaction_container()` builds
    whatever class `transaction_cls` currently points to, not always the
    base `TransactionContainer` -- the entire reason for the `.provided`
    indirection this bug fix replaces."""

    marker: providers.Provider[str] = providers.Object("subclass-marker")


def test_transaction_container_respects_transaction_cls_override_to_subclass():
    container = ApplicationContainer()
    container.config.override(ApplicationSettings())
    container.kafka_producer.override(MagicMock(spec=AIOProducer))
    container.session.override(MagicMock(spec=DataBaseSession))
    container.logger.override(MagicMock(spec=logging.Logger))

    # `transaction_container`'s `cls=transaction_cls` kwarg is auto-resolved
    # from the `transaction_cls` provider *at call time*, so overriding it
    # (e.g. what a real subclass/deployment override would do) must make
    # `transaction_container()` build the overridden class, not the base
    # `TransactionContainer` it was declared with.
    container.transaction_cls.override(providers.Object(_MarkerTransactionContainer))

    built = container.transaction_container()

    assert "marker" in built.providers
    assert built.marker() == "subclass-marker"


def test_dependency_accepts_subclass():
    """Sanity test: providers.Dependency(instance_of=ApplicationSettings) accepts
    a subclass instance via isinstance checks. This validates the documented
    "alias, don't redeclare" escape hatch — you can safely alias the base
    container's config provider in a subclass because isinstance allows
    subclass instances."""
    from dependency_injector import containers

    # Create a trivial ApplicationSettings subclass
    class CustomSettings(ApplicationSettings):
        pass

    # Create a simple container with a Dependency provider
    class TestContainer(containers.DeclarativeContainer):
        config: providers.Provider[ApplicationSettings] = providers.Dependency(
            instance_of=ApplicationSettings
        )

    # Instantiate and override with a subclass instance
    container = TestContainer()
    custom_instance = CustomSettings()
    container.config.override(custom_instance)

    # The provider should resolve to the subclass instance without error
    resolved = container.config()

    # Verify it resolves to the instance we passed
    assert resolved is custom_instance
