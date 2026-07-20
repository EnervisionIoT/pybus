import uuid

import pytest
from dependency_injector import containers, providers

from pybus.container.transaction import DependencyProvider, TransactionContainer


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
