from pybus.application.common.exceptions import (
    ApplicationException,
    AuthorizationException,
    NotFoundException,
    ServerException,
)


def test_application_exception_stores_message_and_status_code():
    exc = ApplicationException("boom", 418)
    assert exc.message == "boom"
    assert exc.status_code == 418
    assert str(exc) == "boom"


def test_authorization_exception_uses_403():
    exc = AuthorizationException("forbidden")
    assert exc.status_code == 403
    assert exc.message == "forbidden"


def test_not_found_exception_uses_404():
    exc = NotFoundException("missing")
    assert exc.status_code == 404


def test_server_exception_uses_500():
    exc = ServerException("oops")
    assert exc.status_code == 500
