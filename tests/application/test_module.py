from pybus.application import ApplicationModule
from pybus.application.commands import Command


class DoThing(Command[None]):
    pass


class DoOtherThing(Command[None]):
    pass


def test_register_handler_returns_the_function_unchanged():
    module = ApplicationModule("test")

    @module.register_handler(DoThing)
    async def handler(command: DoThing) -> None: ...

    assert handler.__name__ == "handler"


def test_get_handlers_yields_registered_handler_for_matching_message_type():
    module = ApplicationModule("test")

    async def handler(command: DoThing) -> None: ...

    module.register_handler(DoThing)(handler)

    handlers = list(module.get_handlers(DoThing()))

    assert handlers == [handler]


def test_get_handlers_yields_nothing_for_unregistered_message_type():
    module = ApplicationModule("test")

    handlers = list(module.get_handlers(DoThing()))

    assert handlers == []


def test_get_handlers_does_not_yield_handlers_for_a_different_message_type():
    module = ApplicationModule("test")

    async def handler(command: DoThing) -> None: ...

    module.register_handler(DoThing)(handler)

    handlers = list(module.get_handlers(DoOtherThing()))

    assert handlers == []


def test_included_submodule_handlers_are_yielded_too():
    parent = ApplicationModule("parent")
    child = ApplicationModule("child")

    async def handler(command: DoThing) -> None: ...

    child.register_handler(DoThing)(handler)
    parent.include_module(child)

    handlers = list(parent.get_handlers(DoThing()))

    assert handlers == [handler]
