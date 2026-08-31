import logging
import os
import uuid
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


async def test_on_enter_hook_binds_publish_event_to_the_buffer():
    """The injectable dependency is `enqueue_event`, not `publish_event`.

    A handler that injected this and produced directly would produce from
    inside the transaction -- the exact ordering the exit hook exists to
    prevent. Binding it to the buffer means it cannot.
    """
    app = build_application()
    fake_context = MagicMock()
    fake_context.set_dependency = MagicMock()
    fake_context.enqueue_event = AsyncMock()

    assert app._on_enter_transaction_context is not None
    await app._on_enter_transaction_context(fake_context)

    fake_context.set_dependency.assert_called_once_with("publish_event", fake_context.enqueue_event)


async def test_on_exit_hook_commits_and_closes_when_no_exception():
    app = build_application()
    session = AsyncMock()
    fake_context = MagicMock()
    fake_context.get_dependency = MagicMock(return_value=session)

    assert app._on_exit_transaction_context is not None
    await app._on_exit_transaction_context(fake_context, None)

    session.commit.assert_awaited_once()
    session.rollback.assert_not_called()
    session.close.assert_awaited_once()


async def test_on_exit_hook_rolls_back_and_closes_on_exception():
    app = build_application()
    session = AsyncMock()
    fake_context = MagicMock()
    fake_context.get_dependency = MagicMock(return_value=session)

    assert app._on_exit_transaction_context is not None
    await app._on_exit_transaction_context(fake_context, ValueError("boom"))

    session.rollback.assert_awaited_once()
    session.commit.assert_not_called()
    session.close.assert_awaited_once()


async def test_event_collector_middleware_buffers_instead_of_publishing():
    """The regression guard for publish-before-commit.

    The middleware runs inside the transaction, so anything it produces can
    still be rolled back out from under. It writes the outbox rows and hands
    the events to the buffer; the exit hook publishes them once the commit
    has returned.
    """
    app = build_application()
    middleware = app._transaction_middleware[0]

    fake_repo = MagicMock(spec=GenericRepository)
    event = make_dummy_event()
    fake_repo.save_domain_events = AsyncMock(return_value=[event])

    async def handler(repo) -> str:
        return "handler-result"

    call_next = partial(handler, repo=fake_repo)

    fake_context = MagicMock()
    fake_context.enqueue_event = AsyncMock()
    fake_context.publish_event = AsyncMock()

    result = await middleware(fake_context, call_next)

    assert result == "handler-result"
    fake_repo.save_domain_events.assert_awaited_once()
    fake_context.enqueue_event.assert_awaited_once_with(event)
    fake_context.publish_event.assert_not_awaited()


async def test_event_collector_middleware_ignores_non_repository_kwargs():
    app = build_application()
    middleware = app._transaction_middleware[0]

    async def handler(command) -> str:
        return "handler-result"

    call_next = partial(handler, command=DoThing())

    fake_context = MagicMock()
    fake_context.enqueue_event = AsyncMock()

    result = await middleware(fake_context, call_next)

    assert result == "handler-result"
    fake_context.enqueue_event.assert_not_called()


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


class _FakeSession:
    def __init__(self):
        self.info: dict = {}


class _FakeDataBaseSession:
    def __init__(self):
        self.connection = _FakeSession()


async def test_execute_stashes_created_by_id_on_the_session_when_given(
    monkeypatch: pytest.MonkeyPatch,
):
    app = build_application()
    fake_ctx = FakeTransactionContext()
    fake_db_session = _FakeDataBaseSession()
    fake_ctx.get_dependency = MagicMock(return_value=fake_db_session)
    monkeypatch.setattr(app, "transaction_context", lambda: fake_ctx)

    created_by_id = uuid.uuid4()
    await app.execute(DoThing(), created_by_id=created_by_id)

    fake_ctx.get_dependency.assert_called_once_with(DataBaseSession)
    assert fake_db_session.connection.info["created_by_id"] == created_by_id


async def test_execute_does_not_touch_the_session_when_created_by_id_is_omitted(
    monkeypatch: pytest.MonkeyPatch,
):
    app = build_application()
    fake_ctx = FakeTransactionContext()
    fake_ctx.get_dependency = MagicMock()
    monkeypatch.setattr(app, "transaction_context", lambda: fake_ctx)

    await app.execute(DoThing())

    fake_ctx.get_dependency.assert_not_called()


async def test_execute_sets_tenant_context_when_tenant_id_given(monkeypatch: pytest.MonkeyPatch):
    app = build_application()
    fake_ctx = FakeTransactionContext()
    fake_db_session = AsyncMock()
    fake_ctx.get_dependency = MagicMock(return_value=fake_db_session)
    monkeypatch.setattr(app, "transaction_context", lambda: fake_ctx)

    tenant_id = uuid.uuid4()
    await app.execute(DoThing(), tenant_id=tenant_id)

    fake_db_session.set_tenant_context.assert_awaited_once_with(tenant_id)


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
    from dependency_injector import providers

    from pybus.container.application import ApplicationContainer

    # Verify __self__ is defined as a Self provider for wiring consistency
    assert isinstance(ApplicationContainer.__self__, providers.Self)


async def test_kafka_producer_resolves_to_a_real_instance_without_raising():
    """Regression test: kafka_producer used to call
    `AIOProducer(bootstrap_servers=...)`, but AIOProducer's real constructor
    takes a single `producer_conf` dict (the librdkafka config format), not a
    `bootstrap_servers` kwarg -- every real (unmocked) resolution crashed with
    `TypeError: AIOProducer.__init__() got an unexpected keyword argument
    'bootstrap_servers'`. No other test caught this because every other test
    overrides kafka_producer with a MagicMock; this one resolves it for real."""
    container = ApplicationContainer()
    container.config.override(ApplicationSettings())

    producer = container.kafka_producer()
    try:
        assert isinstance(producer, AIOProducer)
    finally:
        await producer.close()


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


def test_application_container_logger_resolves_to_a_real_named_log_file(tmp_path, monkeypatch):
    """Regression: `logger`'s `log_relative_path` used to be built with an
    f-string over the still-unresolved `config.provided.APPLICATION_NAME`
    provider object, stringifying its repr (e.g.
    `logs/<dependency_injector.providers.AttributeGetter() at 0x...>.log`)
    instead of the actual application name -- illegal on Windows (`<`/`>`)
    and wrong everywhere else. Every other test overrides `container.logger`
    with a Mock, so this bug had zero coverage; this test resolves the REAL
    provider, unmocked, to prove the path is actually correct."""
    from pybus.infrastructure.logging import LoggerFactory

    monkeypatch.chdir(tmp_path)
    original_configured = LoggerFactory._configured
    original_logger = LoggerFactory._logger
    LoggerFactory._configured = False
    LoggerFactory._logger = None
    try:
        container = ApplicationContainer()
        container.config.override(ApplicationSettings(APPLICATION_NAME="myapp"))

        logger = container.logger()

        assert isinstance(logger, logging.Logger)
        assert LoggerFactory.log_filename == os.path.join(str(tmp_path), "logs/myapp.log")
    finally:
        LoggerFactory._configured = original_configured
        LoggerFactory._logger = original_logger


async def test_kafka_consumer_provider_resolves_to_an_aio_consumer():
    from confluent_kafka.aio import AIOConsumer

    settings = ApplicationSettings(KAFKA_BOOTSTRAP_SERVERS="localhost:9092")
    container = ApplicationContainer()
    container.config.override(settings)

    consumer = container.kafka_consumer()

    try:
        assert isinstance(consumer, AIOConsumer)
    finally:
        await consumer.close()


def test_kafka_consumer_group_id_setting_has_a_default():
    settings = ApplicationSettings()
    assert settings.KAFKA_CONSUMER_GROUP_ID == "pybus"


def test_the_consumer_starts_from_the_beginning_not_the_end():
    """librdkafka's own default is `latest`, and it loses events silently.

    A group with no committed offset -- a first start, a renamed group, a
    restart after Kafka's offset retention expired -- would begin at the end
    of the partition and skip everything already there. No error, no log
    line, and `kafka-consumer-groups --describe` reports no lag, because as
    far as the group is concerned there is nothing behind it.

    This was found by starting the platform in containers for the first
    time: idp committed a `UserInvited` row, produced it, and iam never
    built the membership. Asserted on the configuration rather than through
    a broker because the value is the whole of the fix.
    """
    settings = ApplicationSettings(KAFKA_BOOTSTRAP_SERVERS="localhost:9092")
    container = ApplicationContainer()
    container.config.override(settings)

    conf = container.kafka_consumer.kwargs["consumer_conf"]()

    assert conf["auto.offset.reset"] == "earliest"
    # Together these are what make delivery at-least-once: the offset moves
    # only after a message has been handled, and a group that has never
    # committed one starts from the beginning rather than skipping ahead.
    assert conf["enable.auto.commit"] is False


class OrderRecorder:
    """Records commit/rollback/close/publish in the order they happen.

    The regression being guarded is purely about *order*: every call the old
    code made, the new code also makes. Only a recorder spanning both the
    session and the publish path can tell the two apart.
    """

    def __init__(self, events=(), commit_error=None, publish_error=None):
        self.calls: list[str] = []
        self._events = list(events)
        self._commit_error = commit_error
        self._publish_error = publish_error
        self.logger = MagicMock(spec=logging.Logger)

        self.session = AsyncMock()
        self.session.commit.side_effect = self._commit
        self.session.rollback.side_effect = lambda: self.calls.append("rollback")
        self.session.close.side_effect = lambda: self.calls.append("close")

    def _commit(self):
        self.calls.append("commit")
        if self._commit_error:
            raise self._commit_error

    def get_dependency(self, identifier):
        return self.logger if identifier is logging.Logger else self.session

    def take_pending_events(self):
        self.calls.append("take")
        return list(self._events)

    async def publish_event(self, message):
        self.calls.append(f"publish:{message.message_type}")
        if self._publish_error:
            raise self._publish_error


async def test_on_exit_hook_publishes_only_after_the_commit_returns():
    """The whole point of the change: nothing reaches Kafka until the write
    is durable."""
    app = build_application()
    recorder = OrderRecorder(events=[make_dummy_event()])

    assert app._on_exit_transaction_context is not None
    await app._on_exit_transaction_context(recorder, None)

    assert recorder.calls == ["commit", "close", "take", "publish:DummyEvent"]


async def test_on_exit_hook_publishes_nothing_when_the_commit_fails():
    """The bug, stated directly.

    Before this change the produce had already happened by the time the
    commit ran, so a failing commit left the broker holding an event whose
    row does not exist.
    """
    app = build_application()
    recorder = OrderRecorder(events=[make_dummy_event()], commit_error=RuntimeError("boom"))

    assert app._on_exit_transaction_context is not None
    with pytest.raises(RuntimeError):
        await app._on_exit_transaction_context(recorder, None)

    assert "close" in recorder.calls
    assert not [call for call in recorder.calls if call.startswith("publish")]


async def test_on_exit_hook_publishes_nothing_when_the_transaction_rolled_back():
    app = build_application()
    recorder = OrderRecorder(events=[make_dummy_event()])

    assert app._on_exit_transaction_context is not None
    await app._on_exit_transaction_context(recorder, ValueError("handler blew up"))

    assert recorder.calls == ["rollback", "close"]


async def test_on_exit_hook_logs_and_continues_when_a_produce_fails():
    """A produce failure after the commit cannot undo the write, so raising
    would report a committed command as failed and invite a retry that
    writes it twice."""
    app = build_application()
    recorder = OrderRecorder(
        events=[make_dummy_event(), make_dummy_event()],
        publish_error=RuntimeError("broker down"),
    )

    assert app._on_exit_transaction_context is not None
    await app._on_exit_transaction_context(recorder, None)

    assert recorder.calls.count("publish:DummyEvent") == 2
    assert recorder.logger.exception.call_count == 2
