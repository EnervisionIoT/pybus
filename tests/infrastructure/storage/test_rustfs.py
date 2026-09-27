import io
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from pybus.domain.value_objects import FileObject
from pybus.infrastructure.storage.rustfs import FileNotFound, RustFS


@pytest.fixture
def mock_client():
    with patch("pybus.infrastructure.storage.rustfs.boto3.client") as client_factory:
        client = MagicMock()
        client_factory.return_value = client
        yield client


@pytest.fixture
def storage(mock_client) -> RustFS:
    return RustFS(endpoint="localhost:9000", access_key="ak", secret_key="sk")


def make_client_error(code: str, operation: str = "HeadObject") -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "error"}}, operation)


def test_the_client_is_path_style_against_the_endpoint_given():
    """Virtual-hosted style is boto3's default and puts the bucket in the
    hostname -- `bucket.rustfs:9000` -- which nothing inside compose
    resolves. Every request would fail at DNS, not at S3."""
    with patch("pybus.infrastructure.storage.rustfs.boto3.client") as client_factory:
        RustFS(endpoint="rustfs:9000", access_key="ak", secret_key="sk")

    _, kwargs = client_factory.call_args
    assert kwargs["endpoint_url"] == "http://rustfs:9000"
    assert kwargs["config"].s3 == {"addressing_style": "path"}


def test_secure_builds_an_https_endpoint():
    with patch("pybus.infrastructure.storage.rustfs.boto3.client") as client_factory:
        RustFS(endpoint="s3.example.com", access_key="ak", secret_key="sk", secure=True)

    _, kwargs = client_factory.call_args
    assert kwargs["endpoint_url"] == "https://s3.example.com"


def test_set_bucket_lifecycle_creates_bucket_when_missing(storage: RustFS, mock_client: MagicMock):
    mock_client.head_bucket.side_effect = make_client_error("404", "HeadBucket")

    storage.set_bucket_lifecycle("my-bucket", days=30)

    mock_client.create_bucket.assert_called_once_with(Bucket="my-bucket")
    mock_client.put_bucket_lifecycle_configuration.assert_called_once()
    _, kwargs = mock_client.put_bucket_lifecycle_configuration.call_args
    assert kwargs["Bucket"] == "my-bucket"
    (rule,) = kwargs["LifecycleConfiguration"]["Rules"]
    assert rule["Expiration"] == {"Days": 30}
    assert rule["Status"] == "Enabled"


def test_set_bucket_lifecycle_skips_creation_when_bucket_exists(
    storage: RustFS, mock_client: MagicMock
):
    storage.set_bucket_lifecycle("my-bucket", days=30)

    mock_client.create_bucket.assert_not_called()


def test_a_bucket_check_that_fails_is_not_taken_for_a_missing_bucket(
    storage: RustFS, mock_client: MagicMock
):
    """`head_bucket` raising is how boto3 says both "no such bucket" and
    "you may not look". Creating a bucket on the second would turn a denied
    credential into a confusing CreateBucket error, or into a bucket nobody
    meant to make."""
    mock_client.head_bucket.side_effect = make_client_error("403", "HeadBucket")

    with pytest.raises(ClientError):
        storage.set_bucket_lifecycle("my-bucket", days=30)

    mock_client.create_bucket.assert_not_called()


def test_check_file_exists_creates_bucket_then_returns_true_on_success(
    storage: RustFS, mock_client: MagicMock
):
    mock_client.head_bucket.side_effect = make_client_error("404", "HeadBucket")

    result = storage.check_file_exists("bucket", "path/file.txt")

    assert result is True
    mock_client.create_bucket.assert_called_once_with(Bucket="bucket")
    mock_client.head_object.assert_called_once_with(Bucket="bucket", Key="path/file.txt")


@pytest.mark.parametrize("code", ["404", "NotFound", "NoSuchKey", "NoSuchBucket"])
def test_check_file_exists_returns_false_when_the_object_is_missing(
    storage: RustFS, mock_client: MagicMock, code: str
):
    """A real server answers a missing key with "404": HEAD has no body, so
    botocore reports the status as the code. The named codes are there for the GET
    path and for servers that answer HEAD differently."""
    mock_client.head_object.side_effect = make_client_error(code)

    assert storage.check_file_exists("bucket", "path/file.txt") is False


def test_check_file_exists_does_not_report_a_broken_backend_as_absence(
    storage: RustFS, mock_client: MagicMock
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
        storage.check_file_exists("bucket", "path/file.txt")


def test_get_file_builds_file_object_from_response(storage: RustFS, mock_client: MagicMock):
    content = b"hello world"
    body = MagicMock()
    body.read.return_value = content
    mock_client.get_object.return_value = {"Body": body, "ContentType": "text/plain"}

    file_obj = storage.get_file("bucket", "path/file.txt")

    assert isinstance(file_obj, FileObject)
    assert file_obj.to_bytes() == content
    assert file_obj.content_type == "text/plain"
    assert file_obj.size == len(content)
    body.close.assert_called_once()


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
def test_get_file_names_the_file_after_its_key(
    storage: RustFS, mock_client: MagicMock, key: str, content_type: str, expected: str
):
    mock_client.get_object.return_value = {"Body": MagicMock(), "ContentType": content_type}
    mock_client.get_object.return_value["Body"].read.return_value = b"x"

    assert storage.get_file("bucket", key).filename == expected


@pytest.mark.parametrize("code", ["NoSuchKey", "NoSuchBucket"])
def test_get_file_raises_file_not_found_when_the_object_is_missing(
    storage: RustFS, mock_client: MagicMock, code: str
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
        storage.get_file("bucket", "missing.txt")


def test_file_not_found_is_named_so_the_interceptors_map_it():
    """Asserted rather than left to the name staying put. The mapping is by
    string suffix and nothing imports this class, so a rename would move a
    404 to a 500 with no test and no compiler noticing."""
    assert FileNotFound.__name__.endswith("NotFound")


def test_get_file_reraises_other_client_errors(storage: RustFS, mock_client: MagicMock):
    mock_client.get_object.side_effect = make_client_error("InternalError", "GetObject")

    with pytest.raises(ClientError):
        storage.get_file("bucket", "missing.txt")


def test_upload_file_creates_bucket_when_missing_and_uploads(
    storage: RustFS, mock_client: MagicMock
):
    mock_client.head_bucket.side_effect = make_client_error("404", "HeadBucket")
    content = b"payload"
    file_obj = FileObject(
        filename="a.bin",
        content_type="application/octet-stream",
        size=0,
        stream=io.BytesIO(content),
    )

    storage.upload_file("bucket", file_obj, "object-name")

    mock_client.create_bucket.assert_called_once_with(Bucket="bucket")
    mock_client.put_object.assert_called_once()
    _, kwargs = mock_client.put_object.call_args
    assert kwargs["Bucket"] == "bucket"
    assert kwargs["Key"] == "object-name"
    assert kwargs["Body"] == content
    assert kwargs["ContentLength"] == len(content)
    assert kwargs["ContentType"] == "application/octet-stream"


def test_upload_file_skips_bucket_creation_when_it_exists(storage: RustFS, mock_client: MagicMock):
    file_obj = FileObject(
        filename="a.bin",
        content_type="application/octet-stream",
        size=0,
        stream=io.BytesIO(b"x"),
    )

    storage.upload_file("bucket", file_obj, "object-name")

    mock_client.create_bucket.assert_not_called()
