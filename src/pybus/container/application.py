from collections.abc import Awaitable, Callable
from functools import partial
from typing import Any, overload

from confluent_kafka.aio import AIOProducer
from dependency_injector import containers, providers
from sqlalchemy.ext.asyncio import create_async_engine

from pybus.application import ApplicationModule
from pybus.application.commands import Command
from pybus.application.common.pagination import PaginationQuery
from pybus.application.queries import Query
from pybus.domain.events import DomainEvent
from pybus.domain.repositories import GenericRepository
from pybus.infrastructure.database.session import DataBaseSession
from pybus.infrastructure.database.sqlalchemy import SqlAlchemySession

from .config import ApplicationSettings
from .transaction import DependencyProvider, TransactionContainer, TransactionContext


def create_application(
    name: str,
    container: "ApplicationContainer",
    modules: list[ApplicationModule],
) -> "Application":
    application = Application(name=name, container=container)

    for module in modules:
        application.include_module(module)

    @application.on_enter_transaction_context
    async def on_enter_transaction_context(context: TransactionContext) -> None:  # pyright: ignore[reportUnusedFunction]
        context.set_dependency("publish_event", context.publish_event)

    @application.on_exit_transaction_context
    async def on_exit_transaction_context(  # pyright: ignore[reportUnusedFunction]
        context: TransactionContext, exc_val: BaseException | None
    ) -> None:
        session = context.get_dependency(DataBaseSession)
        if exc_val:
            await session.rollback()
        else:
            await session.commit()
        await session.close()

    @application.transaction_middleware
    async def event_collector_middleware(  # pyright: ignore[reportUnusedFunction]
        context: TransactionContext, call_next: Callable[[], Awaitable[Any]]
    ) -> Any:
        result = await call_next()

        if isinstance(call_next, partial):
            repository_dependencies: list[GenericRepository[Any]] = [
                dependency
                for dependency in call_next.keywords.values()
                if isinstance(dependency, GenericRepository)
            ]

            domain_events = [
                domain_event
                for repository_dependency in repository_dependencies
                for domain_event in await repository_dependency.save_domain_events()
            ]

            for domain_event in domain_events:
                await context.publish_event(domain_event)

        return result

    return application


class ApplicationContainer(containers.DeclarativeContainer):
    config: providers.Provider[ApplicationSettings] = providers.Dependency(
        instance_of=ApplicationSettings
    )

    application_modules: providers.Provider[list[ApplicationModule]] = providers.List()

    application: providers.Provider["Application"] = providers.Singleton(
        create_application,
        name=config.provided.APPLICATION_NAME,
        container=providers.Self(),
        modules=application_modules,
    )

    session: providers.Provider[DataBaseSession] = providers.Selector(
        config.provided.DATABASE_TYPE,
        sqlalchemy=providers.Factory(
            SqlAlchemySession,
            engine=providers.Singleton(
                create_async_engine,
                url=providers.Callable(str, config.provided.SQLALCHEMY_DATABASE_URI),
            ),
        ),
    )

    kafka_producer: providers.Provider[AIOProducer] = providers.Singleton(
        AIOProducer,
        bootstrap_servers=config.provided.KAFKA_BOOTSTRAP_SERVERS,
    )

    transaction_cls: providers.Provider[type[TransactionContainer]] = providers.Object(
        TransactionContainer
    )

    transaction_container: providers.Provider[TransactionContainer] = providers.Factory(
        transaction_cls.provided,
        session=session,
        kafka_producer=kafka_producer,
    )


class Application(ApplicationModule):
    def __init__(self, name: str, container: ApplicationContainer):
        super().__init__(name=name)
        self._container: ApplicationContainer = container
        self._on_enter_transaction_context: (
            Callable[[TransactionContext], Awaitable[None]] | None
        ) = None
        self._on_publish_scheduled_event: Callable[[DomainEvent], Awaitable[None]] | None = None
        self._on_exit_transaction_context: (
            Callable[[TransactionContext, BaseException | None], Awaitable[None]] | None
        ) = None
        self._transaction_middleware: list[
            Callable[[TransactionContext, Callable[[], Awaitable[Any]]], Awaitable[Any]]
        ] = []

    @overload
    async def execute[TResult](
        self, message: Command[TResult], pagination: None = None
    ) -> TResult: ...

    @overload
    async def execute[TResult](
        self, message: Query[TResult], pagination: None = None
    ) -> TResult: ...

    @overload
    async def execute[TResult](
        self, message: Query[TResult], pagination: PaginationQuery
    ) -> tuple[int, TResult]: ...

    @overload
    async def execute(self, message: DomainEvent, pagination: None = None) -> None: ...

    async def execute[TResult](
        self,
        message: Command[TResult] | Query[TResult] | DomainEvent,
        pagination: PaginationQuery | None = None,
    ) -> TResult | tuple[int, TResult] | None:
        async with self.transaction_context() as ctx:
            if isinstance(message, Command):
                return await ctx.execute_command(message)
            if isinstance(message, DomainEvent):
                return await ctx.execute_event(message)

            return await ctx.execute_query(message, pagination)

    def on_enter_transaction_context(self, func: Callable[[TransactionContext], Awaitable[None]]):
        self._on_enter_transaction_context = func
        return func

    def on_exit_transaction_context(
        self, func: Callable[[TransactionContext, BaseException | None], Awaitable[None]]
    ):
        self._on_exit_transaction_context = func
        return func

    def transaction_middleware(
        self,
        func: Callable[[TransactionContext, Callable[[], Awaitable[Any]]], Awaitable[Any]],
    ):
        self._transaction_middleware.append(func)
        return func

    def transaction_context(self) -> TransactionContext:
        ctx = TransactionContext(DependencyProvider(self._container.transaction_container()))
        ctx.configure(
            handlers_iterator=self.get_handlers,
            on_enter_transaction_context=self._on_enter_transaction_context,
            on_publish_scheduled_event=self._on_publish_scheduled_event,
            on_exit_transaction_context=self._on_exit_transaction_context,
            middlewares=self._transaction_middleware,
        )
        return ctx
