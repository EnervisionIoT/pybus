from collections import defaultdict
from collections.abc import AsyncGenerator, Awaitable, Callable, Generator
from typing import Any, overload

from pybus.domain.events import DomainEvent

from .commands import Command
from .queries import Query


class ApplicationModule:
    def __init__(self, name: str):
        self.name: str = name
        self._handlers: dict[
            type[Command[Any] | DomainEvent | Query[Any]],
            set[Callable[..., Awaitable[None] | Awaitable[Any]]],
        ] = defaultdict(set)
        self._sub_modules: set[ApplicationModule] = set()
        self._dependencies: dict[
            type[Any], Callable[..., Awaitable[Any] | AsyncGenerator[Any]]
        ] = {}

    def include_module(self, module: "ApplicationModule"):
        self._sub_modules.add(module)

    def register_handler[TResult](
        self, message_type: type[Command[TResult] | DomainEvent | Query[TResult]]
    ):
        def decorator(
            func: Callable[..., Awaitable[None] | Awaitable[TResult]],
        ):
            self._handlers[message_type].add(func)
            return func

        return decorator

    @overload
    def _iterate_handlers[TResult](
        self, message: Command[TResult]
    ) -> Generator[Callable[..., Awaitable[None]]]: ...

    @overload
    def _iterate_handlers[TResult](
        self, message: Query[TResult]
    ) -> Generator[Callable[..., Awaitable[TResult]]]: ...

    @overload
    def _iterate_handlers(
        self, message: DomainEvent
    ) -> Generator[Callable[..., Awaitable[None]]]: ...

    def _iterate_handlers[TResult](
        self, message: Command[TResult] | Query[TResult] | DomainEvent
    ) -> Generator[Callable[..., Awaitable[None]] | Callable[..., Awaitable[TResult]]]:
        if type(message) in self._handlers:
            yield from self._handlers[type(message)]

        for sub_module in self._sub_modules:
            yield from sub_module._iterate_handlers(message)

    def get_handlers[TResult](
        self, message: Command[TResult] | DomainEvent | Query[TResult]
    ) -> Generator[Callable[..., Awaitable[None]] | Callable[..., Awaitable[TResult]]]:
        return self._iterate_handlers(message)


__all__ = ["ApplicationModule", "Command", "Query"]
