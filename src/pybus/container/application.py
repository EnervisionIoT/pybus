import uuid
from collections.abc import Awaitable, Callable
from functools import partial
from logging import Logger
from typing import Any, overload

from confluent_kafka.aio import AIOConsumer, AIOProducer
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
from pybus.infrastructure.logging import init_logger

from .config import ApplicationSettings
from .transaction import DependencyProvider, TransactionContainer, TransactionContext


def create_application(
    name: str, container: "ApplicationContainer", modules: list[ApplicationModule]
) -> "Application":
    application = Application(name=name, container=container)

    for module in modules:
        application.include_module(module)

    @application.on_enter_transaction_context
    async def on_enter_transaction_context(context: TransactionContext) -> None:  # pyright: ignore[reportUnusedFunction]
        # Bound to `enqueue_event`, not `publish_event`, deliberately. A
        # handler that injected this and produced directly would produce from
        # inside the transaction, which is exactly the ordering the exit hook
        # below exists to prevent. Nothing injects it today; binding it to the
        # buffer means nothing can.
        context.set_dependency("publish_event", context.enqueue_event)

    @application.on_exit_transaction_context
    async def on_exit_transaction_context(  # pyright: ignore[reportUnusedFunction]
        context: TransactionContext, exc_val: BaseException | None
    ) -> None:
        session = context.get_dependency(DataBaseSession)
        logger = context.get_dependency(Logger)

        try:
            if exc_val:
                await session.rollback()
                # Buffered events are simply never taken. Nothing that failed
                # to commit is allowed onto the topic.
                return
            await session.commit()
        finally:
            await session.close()

        # Past here the write is durable and cannot be taken back, so a
        # produce failure is not something the caller can act on. Raising
        # would report a committed command as failed and invite a retry that
        # writes it twice. Log and carry on: the row is in `domain_events`,
        # which is the record a replay would work from.
        for domain_event in context.take_pending_events():
            try:
                await context.publish_event(domain_event)
            except Exception:
                logger.exception(
                    "Transaction committed but publishing %s (id=%s) failed. "
                    "The row is in domain_events; the message is not on the topic.",
                    domain_event.message_type,
                    domain_event.id,
                )

    @application.transaction_middleware
    async def event_collector_middleware(  # pyright: ignore[reportUnusedFunction]
        context: TransactionContext, call_next: Callable[[], Awaitable[Any]]
    ) -> Any:
        """Write the outbox rows and buffer what to publish -- but publish
        nothing.

        This used to produce to Kafka right here. A produce could succeed and
        the transaction then roll back in `on_exit_transaction_context`,
        leaving the broker holding an event the database never recorded.
        """
        result = await call_next()

        if isinstance(call_next, partial):
            repository_dependencies: list[GenericRepository[Any]] = [
                dependency
                for dependency in call_next.keywords.values()
                if isinstance(dependency, GenericRepository)
            ]

            # `save_domain_events()` stays here, inside the transaction: it
            # is destructive (it drains each aggregate's pending events) and
            # its INSERTs belong to this unit of work. Only the Kafka produce
            # moves -- see `TransactionContext.enqueue_event`.
            for repository_dependency in repository_dependencies:
                for domain_event in await repository_dependency.save_domain_events():
                    await context.enqueue_event(domain_event)

        return result

    return application


def build_transaction_container[TTransactionContainer](
    cls: type[TTransactionContainer], **kwargs: Any
) -> TTransactionContainer:
    return cls(**kwargs)


class ApplicationContainer(containers.DeclarativeContainer):
    __self__: providers.Provider["ApplicationContainer"] = providers.Self()  # type: ignore

    config: providers.Provider[ApplicationSettings] = providers.Dependency(
        instance_of=ApplicationSettings
    )
    """Subclass redeclaration footgun: if a subclass redeclares `config` as a new
    `providers.Dependency(...)` object (instead of aliasing it), the other inherited
    providers that reference `config.provided.X` (`application`, `session`,
    `kafka_producer`, `logger`) still point at the ORIGINAL base class's `config`
    object. The subclass's `config=` constructor kwarg never reaches them because
    provider references are captured at class-declaration time, before instance
    initialization. This causes the subclass's config to be orphaned, and resolving
    any of the dependent providers crashes with an infinite-recursion error.

    Two correct patterns:

    1. Alias instead of redeclare (preferred): use `config = ApplicationContainer.config`
       in your subclass. This works because `providers.Dependency(instance_of=ApplicationSettings)`
       accepts subclass instances via `isinstance` checks (see `test_dependency_accepts_subclass`).
    2. Redeclare all dependent providers together: if you redeclare `config`, you must
       also redeclare all providers that reference it (`application`, `session`,
       `kafka_producer`, `logger`) in the SAME subclass body so they capture the new
       `config` reference.
    """

    application_modules: providers.Provider[list[ApplicationModule]] = providers.List()
    """Subject to the same redeclaration footgun as `config` above — do not redeclare
    alone without also redeclaring `application`, which references it."""

    application: providers.Provider["Application"] = providers.Singleton(
        create_application,
        name=config.provided.APPLICATION_NAME,
        container=__self__,
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
        # AIOProducer takes a librdkafka-style config dict as producer_conf,
        # not a bootstrap_servers kwarg -- providers.Dict (not a plain {..}
        # literal) is required here so the nested config.provided reference
        # actually gets resolved instead of passed through as a Provider
        # object.
        producer_conf=providers.Dict(
            {"bootstrap.servers": config.provided.KAFKA_BOOTSTRAP_SERVERS}
        ),
    )

    kafka_consumer: providers.Provider[AIOConsumer] = providers.Singleton(
        AIOConsumer,
        consumer_conf=providers.Dict(
            {
                "bootstrap.servers": config.provided.KAFKA_BOOTSTRAP_SERVERS,
                "group.id": config.provided.KAFKA_CONSUMER_GROUP_ID,
                "enable.auto.commit": False,
                # librdkafka defaults this to `latest`, which loses events
                # silently. A group with no committed offset -- a first
                # start, a renamed group, a restart after Kafka's offset
                # retention expired -- would begin at the end of the
                # partition, so everything already sitting there is skipped
                # with no error, no log line and no lag reported.
                #
                # That is not survivable for a domain-event outbox: the
                # producer committed its rows and considers the fact
                # published. `earliest` means such a group instead replays
                # what the topic still holds, which handlers must already
                # tolerate -- delivery is at-least-once, so they are
                # idempotent by contract, and a replay is only a longer
                # redelivery.
                #
                # The cost is real and is the lesser one: a brand-new
                # consumer group in an established system works through the
                # retained history before it catches up.
                "auto.offset.reset": "earliest",
            }
        ),
    )

    logger = providers.Resource(init_logger, logger_name=config.provided.APPLICATION_NAME)

    transaction_cls: providers.Provider[type[TransactionContainer]] = providers.Object(
        TransactionContainer
    )

    transaction_container: providers.Provider[TransactionContainer] = providers.Factory(
        build_transaction_container,
        cls=transaction_cls,
        kafka_producer=kafka_producer,
        session=session,
        logger=logger,
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
        self,
        message: Command[TResult],
        pagination: None = None,
        created_by_id: uuid.UUID | None = None,
        tenant_id: uuid.UUID | None = None,
    ) -> TResult: ...

    @overload
    async def execute[TResult](
        self,
        message: Query[TResult],
        pagination: None = None,
        created_by_id: uuid.UUID | None = None,
        tenant_id: uuid.UUID | None = None,
    ) -> TResult: ...

    @overload
    async def execute[TResult](
        self,
        message: Query[TResult],
        pagination: PaginationQuery,
        created_by_id: uuid.UUID | None = None,
        tenant_id: uuid.UUID | None = None,
    ) -> tuple[int, TResult]: ...

    @overload
    async def execute(
        self,
        message: DomainEvent,
        pagination: None = None,
        created_by_id: uuid.UUID | None = None,
        tenant_id: uuid.UUID | None = None,
    ) -> None: ...

    async def execute[TResult](
        self,
        message: Command[TResult] | Query[TResult] | DomainEvent,
        pagination: PaginationQuery | None = None,
        created_by_id: uuid.UUID | None = None,
        tenant_id: uuid.UUID | None = None,
    ) -> TResult | tuple[int, TResult] | None:
        async with self.transaction_context() as ctx:
            if created_by_id is not None or tenant_id is not None:
                session = ctx.get_dependency(DataBaseSession)

                if created_by_id is not None:
                    session.connection.info["created_by_id"] = created_by_id

                if tenant_id is not None:
                    # Stashed for `save_domain_events()`, the same way
                    # `created_by_id` is on the line above.
                    # `set_tenant_context` issues SET LOCAL app.tenant_id,
                    # which puts the value where Postgres's policies can read
                    # it and nowhere Python can read it back.
                    session.connection.info["tenant_id"] = tenant_id
                    await session.set_tenant_context(tenant_id)

            if isinstance(message, Command):
                return await ctx.execute_command(message)
            if isinstance(message, DomainEvent):
                return await ctx.execute_event(message)

            return await ctx.execute_query(message, pagination)

    def on_enter_transaction_context(self, func: Callable[[TransactionContext], Awaitable[None]]):
        self._on_enter_transaction_context = func
        return func

    def on_exit_transaction_context(
        self,
        func: Callable[[TransactionContext, BaseException | None], Awaitable[None]],
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
