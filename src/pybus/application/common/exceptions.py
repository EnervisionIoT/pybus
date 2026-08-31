class ApplicationException(Exception):
    message: str
    status_code: int

    def __init__(self, message: str, status_code: int) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code


class AuthorizationException(ApplicationException):
    def __init__(self, message: str) -> None:
        super().__init__(message, 403)


class NotFoundException(ApplicationException):
    def __init__(self, message: str) -> None:
        super().__init__(message, 404)


class ServerException(ApplicationException):
    def __init__(self, message: str) -> None:
        super().__init__(message, 500)


class NoHandlerFound(ServerException):
    """Nothing was registered for a message type that was dispatched.

    A deployment fault, not a request fault: the caller sent something the
    bus understands, and this process was assembled without the handler for
    it. The usual cause is an `ApplicationModule` whose `commands` /
    `queries` / `events` subpackages were never imported, so the decorators
    never ran and the module registered empty.

    Named `NoHandlerFound` rather than `HandlerNotFound` on purpose. Both
    services map any exception whose class name ends in `NotFound` to gRPC
    `NOT_FOUND`, by name and without importing anything -- and answering
    `NOT_FOUND` here would tell a caller their tenant or role does not
    exist, when what is actually missing is half of this process.
    """
