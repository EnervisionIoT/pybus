import io
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from pybus.domain.value_objects import FileObject
from pybus.infrastructure.storage.rustfs import FileNotFound, RustFS


@pytest.fixture
def session():
    with patch("pybus.infrastructure.storage.rustfs.aioboto3.Session") as session_cls:
        session = MagicMock()
        session_cls.return_value = session
        yield session


@pytest.fixture
def mock_client(session: MagicMock) -> AsyncMock:
    """The client `async with session.client(...)` yields.

    Every method's calls are awaited, so an AsyncMock -- a plain MagicMock
    here would hand the adapter a coroutine-less value and every `await`
    would raise before the assertion under test was reached.
    """
    client = AsyncMock()
    session.client.return_value.__aenter__.return_value = client
    return client


@pytest.fixture
def storage(mock_client: AsyncMock) -> RustFS:
    return RustFS(endpoint="localhost:9000", access_key="ak", secret_key="sk")


def make_client_error(code: str, operation: str = "HeadObject") -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "error"}}, operation)


def make_body(content: bytes) -> MagicMock:
    """aiobotocore's streaming body: an async context manager whose `read`
    is awaited. Read outside that block and the connection it streams from
    is already gone."""
    body = MagicMock()
    body.__aenter__.return_value = body
    body.read = AsyncMock(return_value=content)
    return body


def file_of(content: bytes) -> FileObject:
    return FileObject(
        filename="a.bin",
        content_type="application/octet-stream",
        size=0,
        stream=io.BytesIO(content),
    )


async def test_the_client_is_path_style_against_the_endpoint_given(
    storage: RustFS, session: MagicMock
):
    """Virtual-hosted style is the default and puts the bucket in the
    hostname -- `bucket.rustfs:9000` -- which nothing inside compose
    resolves. Every request would fail at DNS, not at S3."""
    await storage.check_file_exists("bucket", "key")

    _, kwargs = session.client.call_args
    assert kwargs["endpoint_url"] == "http://localhost:9000"
    assert kwargs["config"].s3 == {"addressing_style": "path"}


async def test_secure_builds_an_https_endpoint(session: MagicMock, mock_client: AsyncMock):
    storage = RustFS(endpoint="s3.example.com", access_key="ak", secret_key="sk", secure=True)

    await storage.check_file_exists("bucket", "key")

    _, kwargs = session.client.call_args
    assert kwargs["endpoint_url"] == "https://s3.example.com"


async def test_each_call_closes_the_client_it_opened(storage: RustFS, session: MagicMock):
    """The adapter holds no client between calls -- see `RustFS`'s
    docstring -- so a call that left its client open would leak an aiohttp
    connection pool every time, with a warning at interpreter exit as the
    only symptom."""
    await storage.check_file_exists("bucket", "key")
    await storage.upload_file("bucket", file_of(b"x"), "key")

    context = session.client.return_value
    assert context.__aenter__.await_count == 2
    assert context.__aexit__.await_count == 2


async def test_set_bucket_lifecycle_creates_bucket_when_missing(
    storage: RustFS, mock_client: AsyncMock
):
    mock_client.head_bucket.side_effect = make_client_error("404", "HeadBucket")

    await storage.set_bucket_lifecycle("my-bucket", days=30)

    mock_client.create_bucket.assert_awaited_once_with(Bucket="my-bucket")
    mock_client.put_bucket_lifecycle_configuration.assert_awaited_once()
    _, kwargs = mock_client.put_bucket_lifecycle_configuration.call_args
    assert kwargs["Bucket"] == "my-bucket"
    (rule,) = kwargs["LifecycleConfiguration"]["Rules"]
    assert rule["Expiration"] == {"Days": 30}
    assert rule["Status"] == "Enabled"


async def test_set_bucket_lifecycle_skips_creation_when_bucket_exists(
    storage: RustFS, mock_client: AsyncMock
):
    await storage.set_bucket_lifecycle("my-bucket", days=30)

    mock_client.create_bucket.assert_not_awaited()


async def test_a_bucket_check_that_fails_is_not_taken_for_a_missing_bucket(
    storage: RustFS, mock_client: AsyncMock
):
    """`head_bucket` raising is how the client says both "no such bucket"
    and "you may not look". Creating a bucket on the second would turn a
    denied credential into a confusing CreateBucket error, or into a bucket
    nobody meant to make."""
    mock_client.head_bucket.side_effect = make_client_error("403", "HeadBucket")

    with pytest.raises(ClientError):
        await storage.set_bucket_lifecycle("my-bucket", days=30)

    mock_client.create_bucket.assert_not_awaited()


async def test_check_file_exists_creates_bucket_then_returns_true_on_success(
    storage: RustFS, mock_client: AsyncMock
):
    mock_client.head_bucket.side_effect = make_client_error("404", "HeadBucket")

    result = await storage.check_file_exists("bucket", "path/file.txt")

    assert result is True
    mock_client.create_bucket.assert_awaited_once_with(Bucket="bucket")
    mock_client.head_object.assert_awaited_once_with(Bucket="bucket", Key="path/file.txt")


@pytest.mark.parametrize("code", ["404", "NotFound", "NoSuchKey", "NoSuchBucket"])
async def test_check_file_exists_returns_false_when_the_object_is_missing(
    storage: RustFS, mock_client: AsyncMock, code: str
):
    """A real server answers a missing key with "404": HEAD has no body, so
    botocore reports the status as the code. The named codes are there for
    the GET path and for servers that answer HEAD differently."""
    mock_client.head_object.side_effect = make_client_error(code)

    assert await storage.check_file_exists("bucket", "path/file.txt") is False


async def test_check_file_exists_does_not_report_a_broken_backend_as_absence(
    storage: RustFS, mock_client: AsyncMock
):
    """An earlier version caught everything and answered False.

    So an unreachable backend, an expired credential and a denied bucket
    policy all came back as "that file is not there" -- which a caller
    deciding whether to upload reads as permission to overwrite, and a
    caller checking before a read reads as a clean absence. Only a miss is
    an absence; a failure has to propagate.
    """
    mock_client.head_object.side_effect = make_client_error("AccessDenied")

    with pytest.raises(ClientError):
        await storage.check_file_exists("bucket", "path/file.txt")


async def test_get_file_builds_file_object_from_response(storage: RustFS, mock_client: AsyncMock):
    content = b"hello world"
    body = make_body(content)
    mock_client.get_object.return_value = {"Body": body, "ContentType": "text/plain"}

    file_obj = await storage.get_file("bucket", "path/file.txt")

    assert isinstance(file_obj, FileObject)
    assert file_obj.to_bytes() == content
    assert file_obj.content_type == "text/plain"
    assert file_obj.size == len(content)
    body.__aexit__.assert_awaited_once()


@pytest.mark.parametrize(
    ("key", "content_type", "expected"),
    [
        # A key that has an extension keeps it, and gets no second one.
        ("path/file.txt", "text/plain", "file.txt"),
        ("path/report.pdf", "application/octet-stream", "report.pdf"),
        # No extension: one is guessed from the content type...
        ("uploads/abc123", "image/png", "abc123.png"),
        # ...including when the type carries a parameter.
        ("uploads/abc123", "text/plain; charset=utf-8", "abc123.txt"),
        # Nothing to guess from: the name stays bare, not `abc123None`.
        ("uploads/abc123", "application/x-nothing-knows-this", "abc123"),
    ],
)
async def test_get_file_names_the_file_after_its_key(
    storage: RustFS, mock_client: AsyncMock, key: str, content_type: str, expected: str
):
    mock_client.get_object.return_value = {"Body": make_body(b"x"), "ContentType": content_type}

    assert (await storage.get_file("bucket", key)).filename == expected


@pytest.mark.parametrize("code", ["NoSuchKey", "NoSuchBucket"])
async def test_get_file_raises_file_not_found_when_the_object_is_missing(
    storage: RustFS, mock_client: AsyncMock, code: str
):
    """`FileNotFound`, not a bare `Exception`.

    The class name is what decides the answer a caller gets: both services'
    interceptors map anything ending in `NotFound` to gRPC `NOT_FOUND` by
    name. As a bare Exception this became `INTERNAL` -- the server claiming
    it had broken, about a file the caller had simply asked for and which
    is not there.
    """
    mock_client.get_object.side_effect = make_client_error(code, "GetObject")

    with pytest.raises(FileNotFound, match="bucket/missing.txt"):
        await storage.get_file("bucket", "missing.txt")


def test_file_not_found_is_named_so_the_interceptors_map_it():
    """Asserted rather than left to the name staying put. The mapping is by
    string suffix and nothing imports this class, so a rename would move a
    404 to a 500 with no test and no compiler noticing."""
    assert FileNotFound.__name__.endswith("NotFound")


async def test_get_file_reraises_other_client_errors(storage: RustFS, mock_client: AsyncMock):
    mock_client.get_object.side_effect = make_client_error("InternalError", "GetObject")

    with pytest.raises(ClientError):
        await storage.get_file("bucket", "missing.txt")


async def test_upload_file_creates_bucket_when_missing_and_uploads(
    storage: RustFS, mock_client: AsyncMock
):
    mock_client.head_bucket.side_effect = make_client_error("404", "HeadBucket")
    content = b"payload"

    await storage.upload_file("bucket", file_of(content), "object-name")

    mock_client.create_bucket.assert_awaited_once_with(Bucket="bucket")
    mock_client.put_object.assert_awaited_once()
    _, kwargs = mock_client.put_object.call_args
    assert kwargs["Bucket"] == "bucket"
    assert kwargs["Key"] == "object-name"
    assert kwargs["Body"] == content
    assert kwargs["ContentLength"] == len(content)
    assert kwargs["ContentType"] == "application/octet-stream"


async def test_upload_file_skips_bucket_creation_when_it_exists(
    storage: RustFS, mock_client: AsyncMock
):
    await storage.upload_file("bucket", file_of(b"x"), "object-name")

    mock_client.create_bucket.assert_not_awaited()


async def test_delete_file_deletes_the_object(storage: RustFS, mock_client: AsyncMock):
    await storage.delete_file("bucket", "a/b")

    mock_client.delete_object.assert_awaited_once_with(Bucket="bucket", Key="a/b")


async def test_delete_file_of_a_missing_bucket_is_not_an_error(
    storage: RustFS, mock_client: AsyncMock
):
    """S3 already answers 204 for a missing key; a missing bucket is the one
    absence that raises. Either way the object is gone, which is what the
    caller asked for -- and a retried delete must not fail on its own success."""
    mock_client.delete_object.side_effect = make_client_error("NoSuchBucket", "DeleteObject")

    await storage.delete_file("bucket", "a/b")


async def test_delete_file_reraises_a_real_fault(storage: RustFS, mock_client: AsyncMock):
    mock_client.delete_object.side_effect = make_client_error("AccessDenied", "DeleteObject")

    with pytest.raises(ClientError):
        await storage.delete_file("bucket", "a/b")
