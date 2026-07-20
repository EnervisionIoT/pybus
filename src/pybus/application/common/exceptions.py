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
