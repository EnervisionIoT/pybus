import uuid
from typing import Any, Protocol, runtime_checkable

import pytest
from dependency_injector import containers, providers

from pybus.container.transaction import DependencyProvider, TransactionContainer
from pybus.infrastructure.database.session import DataBaseSession


@runtime_checkable
class NonMethodProtocol(Protocol):
    """A runtime_checkable Protocol with a non-method member (a property),
    same shape as DataBaseSession. issubclass() against this as the target
    (second argument) unconditionally raises TypeError in CPython's typing
    module, regardless of the candidate (first argument) -- even when the
    candidate is a genuine subclass."""

    @property
    def connection(self) -> Any: ...


class RealSessionImpl(NonMethodProtocol):
    """A concrete class that genuinely (nominally) subclasses
    NonMethodProtocol, used to prove the MRO fallback is necessary even for
    true subclass relationships -- issubclass() can't verify it either."""

    @property
    def connection(self) -> Any:
        return None


class Widget:
    pass


class Gadget:
    pass


class SpecialWidget(Widget):
    pass


def test_resolve_provider_by_type_finds_factory_provider():
    class C(containers.DeclarativeContainer):
        widget = providers.Factory(Widget)

    container = C()
    dp = DependencyProvider(container)
    assert dp.resolve_provider_by_type(Widget) is container.widget


def test_resolve_provider_by_type_finds_singleton_provider():
    class C(containers.DeclarativeContainer):
        widget = providers.Singleton(Widget)

    container = C()
    dp = DependencyProvider(container)
    assert dp.resolve_provider_by_type(Widget) is container.widget


def test_resolve_provider_by_type_finds_dependency_provider():
    class C(containers.DeclarativeContainer):
        widget = providers.Dependency(instance_of=Widget)

    container = C()
    dp = DependencyProvider(container)
    assert dp.resolve_provider_by_type(Widget) is container.widget


def test_resolve_provider_by_type_matches_subclasses():
    class C(containers.DeclarativeContainer):
        widget = providers.Factory(SpecialWidget)

    container = C()
    dp = DependencyProvider(container)
    assert dp.resolve_provider_by_type(Widget) is container.widget


def test_resolve_provider_by_type_raises_when_no_match():
    class C(containers.DeclarativeContainer):
        widget = providers.Factory(Widget)

    dp = DependencyProvider(C())
    with pytest.raises(ValueError, match="Cannot resolve"):
        dp.resolve_provider_by_type(Gadget)


def test_resolve_provider_by_type_raises_when_multiple_match():
    class C(containers.DeclarativeContainer):
        widget = providers.Factory(Widget)
        widget2 = providers.Singleton(Widget)

    dp = DependencyProvider(C())
    with pytest.raises(ValueError, match="Cannot uniquely resolve"):
        dp.resolve_provider_by_type(Widget)


def test_resolve_provider_by_type_skips_callable_singletons_without_crashing():
    """Regression test: TransactionContainer.correlation_id is a
    Singleton(uuid.uuid4) — a callable, not a class. Scanning it used to
    raise a raw TypeError from issubclass(); it must now be safely skipped
    and reported as an ordinary "cannot resolve" failure instead."""

    class C(containers.DeclarativeContainer):
        correlation_id = providers.Singleton(uuid.uuid4)

    dp = DependencyProvider(C())
    with pytest.raises(ValueError, match="Cannot resolve"):
        dp.resolve_provider_by_type(uuid.UUID)


def test_resolve_provider_by_type_handles_dependency_provider_with_protocol_target():
    """Regression test: DataBaseSession is a @runtime_checkable Protocol with
    a non-method member (the `connection` property). CPython's typing module
    unconditionally raises TypeError when such a Protocol is the *second*
    argument to issubclass(), regardless of the first argument. The
    Dependency branch of inspect_provider calls
    issubclass(provider.instance_of, cls) with cls=DataBaseSession, so it
    used to crash outright instead of matching. This is unrelated to
    test_resolve_provider_by_type_skips_callable_singletons_without_crashing,
    which guards against a non-class `.cls` (a callable) on Factory/Singleton
    providers -- a completely different failure mode."""

    class C(containers.DeclarativeContainer):
        session = providers.Dependency(instance_of=DataBaseSession)

    container = C()
    dp = DependencyProvider(container)
    assert dp.resolve_provider_by_type(DataBaseSession) is container.session


def test_resolve_provider_by_type_handles_factory_provider_with_protocol_target():
    """Regression test: a Factory/Singleton whose .cls genuinely subclasses
    a non-method-member Protocol like DataBaseSession also used to crash,
    because inspect_provider calls issubclass(provider.cls, cls) with
    cls=DataBaseSession -- and issubclass() raises TypeError whenever such a
    Protocol is the target (second argument), even for a real subclass. This
    is unrelated to
    test_resolve_provider_by_type_skips_callable_singletons_without_crashing,
    which guards against a non-class `.cls` (a callable), not a Protocol
    target."""

    class C(containers.DeclarativeContainer):
        session = providers.Factory(RealSessionImpl)

    container = C()
    dp = DependencyProvider(container)
    assert dp.resolve_provider_by_type(NonMethodProtocol) is container.session


def test_register_dependency_by_string_then_get_dependency_by_string():
    class C(containers.DeclarativeContainer):
        pass

    dp = DependencyProvider(C())
    widget = Widget()

    dp.register_dependency("widget", widget)

    assert dp.get_dependency("widget") is widget


def test_get_dependency_by_type_calls_the_resolved_provider():
    class C(containers.DeclarativeContainer):
        widget = providers.Factory(Widget)

    dp = DependencyProvider(C())

    result = dp.get_dependency(Widget)

    assert isinstance(result, Widget)


def test_register_dependency_by_type_is_not_discoverable_via_get_dependency_by_type():
    """Regression test documenting current behavior: register_dependency()
    always stores the value behind a providers.Object, but get_dependency()
    for a *type* identifier goes through resolve_provider_by_type(), which
    only inspects Factory/Singleton/Dependency providers — so a
    type-registered dependency is only retrievable via its string name."""

    class C(containers.DeclarativeContainer):
        pass

    dp = DependencyProvider(C())
    dp.register_dependency(Widget, Widget())

    with pytest.raises(ValueError, match="Cannot resolve"):
        dp.get_dependency(Widget)

    assert isinstance(dp.get_dependency("Widget"), Widget)


def test_transaction_container_declares_expected_dependencies():
    assert isinstance(TransactionContainer.correlation_id, providers.Singleton)
    assert isinstance(TransactionContainer.kafka_producer, providers.Dependency)
    assert isinstance(TransactionContainer.session, providers.Dependency)
    assert isinstance(TransactionContainer.logger, providers.Dependency)
