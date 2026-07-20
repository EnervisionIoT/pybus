import inspect
import uuid
from collections.abc import Awaitable, Callable, Generator
from functools import partial
from logging import Logger
from types import TracebackType, UnionType
from typing import Any, final, get_args, get_origin, overload
from uuid import UUID

from confluent_kafka.aio import AIOProducer
from dependency_injector import containers, providers

from pybus.application.commands import Command
from pybus.application.common.pagination import PaginationQuery
from pybus.application.queries import Query
from pybus.domain.events import DomainEvent
from pybus.infrastructure.database.session import DataBaseSession


class TransactionContainer(containers.DeclarativeContainer):
    correlation_id: providers.Provider[UUID] = providers.Singleton(uuid.uuid4)
    kafka_producer: providers.Provider[AIOProducer] = providers.Dependency(instance_of=AIOProducer)
    session: providers.Provider[DataBaseSession] = providers.Dependency(instance_of=DataBaseSession)
    logger: providers.Provider[Logger] = providers.Dependency(instance_of=Logger)


class DependencyProvider:
    def __init__(self, container: containers.Container):
        self._container: containers.Container = container

    def resolve_provider_by_type(self, cls: type) -> providers.Provider[Any]:
        def inspect_provider(provider: providers.Provider[Any]) -> bool:
            if isinstance(provider, (providers.Factory, providers.Singleton)):
                return inspect.isclass(provider.cls) and issubclass(provider.cls, cls)
            elif isinstance(provider, providers.Dependency):
                return issubclass(provider.instance_of, cls)

            return False

        matching_providers = inspect.getmembers(self._container, inspect_provider)
        if matching_providers:
            if len(matching_providers) > 1:
                raise ValueError(
                    f"Cannot uniquely resolve {cls}. Found {len(matching_providers)} matching resources."
                )
            return matching_providers[0][1]
        raise ValueError(f"Cannot resolve {cls}")

    def register_dependency[TDependency](
        self, identifier: type[TDependency] | str, value: TDependency
    ) -> None:
        if isinstance(identifier, str):
            setattr(self._container, identifier, providers.Object(value))
        else:
            setattr(self._container, identifier.__name__, providers.Object(value))

    def get_dependency[TDependency](self, identifier: type[TDependency] | str) -> TDependency:
        if isinstance(identifier, str):
            provider = getattr(self._container, identifier)
        else:
            provider = self.resolve_provider_by_type(identifier)

        return provider()


@final
class TransactionContext:
    DOMAIN_EVENTS_TOPIC = "domain_events"

    def __init__(self, dependency_provider: DependencyProvider):
        self._dependency_provider: DependencyProvider = dependency_provider

        self._on_enter_transaction_context: (
            Callable[["TransactionContext"], Awaitable[None]] | None
        ) = None
        self._on_publish_scheduled_event: Callable[[DomainEvent], Awaitable[None]] | None = None
        self._on_exit_transaction_context: (
            Callable[["TransactionContext", BaseException | None], Awaitable[None]] | None
        ) = None
        self._middlewares: list[
            Callable[["TransactionContext", Callable[[], Awaitable[Any]]], Awaitable[Any]]
        ] = []
        self._handlers_iterator: (
            Callable[
                [Command[Any] | Query[Any] | DomainEvent],
                Generator[Callable[..., Awaitable[Any]], None, None],
            ]
            | None
        ) = None

    def configure(
        self,
        handlers_iterator: Callable[
            [Command[Any] | Query[Any] | DomainEvent],
            Generator[Callable[..., Awaitable[Any]], None, None],
        ],
        on_enter_transaction_context: Callable[["TransactionContext"], Awaitable[None]]
        | None = None,
        on_publish_scheduled_event: Callable[[DomainEvent], Awaitable[None]] | None = None,
        on_exit_transaction_context: Callable[
            ["TransactionContext", BaseException | None], Awaitable[None]
        ]
        | None = None,
        middlewares: list[
            Callable[["TransactionContext", Callable[[], Awaitable[Any]]], Awaitable[Any]]
        ]
        | None = None,
    ) -> None:
        self._handlers_iterator = handlers_iterator
        self._on_enter_transaction_context = on_enter_transaction_context
        self._on_publish_scheduled_event = on_publish_scheduled_event
        self._on_exit_transaction_context = on_exit_transaction_context
        self._middlewares = middlewares or []

    def get_dependency[TDependency](self, identifier: type[TDependency] | str) -> TDependency:
        return self._dependency_provider.get_dependency(identifier)

    def set_dependency[TDependency](self, key: type[TDependency] | str, value: TDependency) -> None:
        self._dependency_provider.register_dependency(key, value)

    async def __aenter__(self) -> "TransactionContext":
        if self._on_enter_transaction_context:
            await self._on_enter_transaction_context(self)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._on_exit_transaction_context:
            await self._on_exit_transaction_context(self, exc_val)

    async def _resolve_parameters[TResult](
        self,
        handler: Callable[..., Awaitable[TResult]]
        | Callable[..., Awaitable[tuple[int, TResult]]]
        | Callable[..., Awaitable[None]],
        message: Command[TResult] | Query[TResult] | DomainEvent,
        pagination: PaginationQuery | None = None,
    ) -> dict[str, object]:
        sinature = inspect.signature(handler)
        parameters: dict[str, object] = {}
        for name, param in sinature.parameters.items():
            annotation = param.annotation

            if annotation == inspect.Parameter.empty:
                continue

            if get_origin(annotation) is UnionType and any(
                issubclass(arg, PaginationQuery) for arg in get_args(annotation)
            ):
                parameters[name] = pagination
                continue

            if isinstance(annotation, type) and issubclass(
                annotation, (Command, Query, DomainEvent)
            ):
                parameters[name] = message
                continue

            if isinstance(annotation, type):
                parameters[name] = self.get_dependency(annotation)
                continue

            parameters[name] = self.get_dependency(name)

        return parameters

    @overload
    async def call[TResult](
        self,
        handler: Callable[..., Awaitable[TResult]],
        message: Command[TResult],
        pagination: None = None,
    ) -> None: ...

    @overload
    async def call[TResult](
        self,
        handler: Callable[..., Awaitable[TResult]],
        message: Query[TResult],
        pagination: None = None,
    ) -> TResult: ...

    @overload
    async def call[TResult](
        self,
        handler: Callable[..., Awaitable[tuple[int, TResult]]],
        message: Query[TResult],
        pagination: PaginationQuery,
    ) -> tuple[int, TResult]: ...

    @overload
    async def call(
        self,
        handler: Callable[..., Awaitable[None]],
        message: DomainEvent,
        pagination: None = None,
    ) -> None: ...

    async def call[TResult](
        self,
        handler: Callable[..., Awaitable[None]]
        | Callable[..., Awaitable[TResult]]
        | Callable[..., Awaitable[tuple[int, TResult]]],
        message: Command[TResult] | Query[TResult] | DomainEvent,
        pagination: PaginationQuery | None = None,
    ) -> TResult | tuple[int, TResult] | None:
        parameters = await self._resolve_parameters(handler, message, pagination)

        call_next = partial(handler, **parameters)

        for middleware in self._middlewares:
            call_next = partial(middleware, self, call_next)

        return await call_next()

    async def execute_command[TResult](self, command: Command[TResult]) -> None:
        if self._handlers_iterator is None:
            raise RuntimeError("Handlers iterator is not configured")

        try:
            for handler in self._handlers_iterator(command):
                return await self.call(handler, command)
        except Exception as ex:
            self._dependency_provider.get_dependency(Logger).error(
                f"Executing command {command} failed with error: {ex}"
            )
            raise

        raise Exception(f"No handler found for command: {command}")

    @overload
    async def execute_query[TResult](
        self, query: Query[TResult], pagination: None = None
    ) -> TResult: ...

    @overload
    async def execute_query[TResult](
        self, query: Query[TResult], pagination: PaginationQuery
    ) -> tuple[int, TResult]: ...

    async def execute_query[TResult](
        self,
        query: Query[TResult],
        pagination: PaginationQuery | None = None,
    ) -> TResult | tuple[int, TResult]:
        if self._handlers_iterator is None:
            raise RuntimeError("Handlers iterator is not configured")

        try:
            for handler in self._handlers_iterator(query):
                return await self.call(handler, query, pagination)
        except Exception as ex:
            self._dependency_provider.get_dependency(Logger).error(
                f"Executing query {query} failed with error: {ex}"
            )
            raise

        raise Exception(f"No handler found for query: {query}")

    async def execute_event(self, event: DomainEvent) -> None:
        if self._handlers_iterator is None:
            raise RuntimeError("Handlers iterator is not configured")

        try:
            for handler in self._handlers_iterator(event):
                await self.call(handler, event)
        except Exception as ex:
            self._dependency_provider.get_dependency(Logger).error(
                f"Executing event {event} failed with error: {ex}"
            )
            raise

    async def publish_event(self, message: DomainEvent) -> None:
        kafka_producer = self.get_dependency(AIOProducer)
        correlation_id = self.get_dependency(UUID)

        message.correlation_id = correlation_id
        await kafka_producer.produce(
            topic=self.DOMAIN_EVENTS_TOPIC,
            value=message.model_dump_json().encode("utf-8"),
            key=str(message.aggregate_id).encode("utf-8"),
        )
