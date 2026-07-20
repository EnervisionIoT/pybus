import io
from unittest.mock import MagicMock, patch

import pytest
from minio.error import S3Error

from pybus.domain.value_objects import FileObject
from pybus.infrastructure.storage.minio import Minio


@pytest.fixture
def mock_client():
    with patch("pybus.infrastructure.storage.minio.MinioClient") as mock_cls:
        client = MagicMock()
        mock_cls.return_value = client
        yield client


@pytest.fixture
def storage(mock_client) -> Minio:
    return Minio(endpoint="localhost:9000", access_key="ak", secret_key="sk")


def make_s3_error(code: str) -> S3Error:
    return S3Error(
        code=code,
        message="error",
        resource="resource",
        request_id="req",
        host_id="host",
        response=MagicMock(),
    )


def test_set_bucket_lifecycle_creates_bucket_when_missing(storage: Minio, mock_client: MagicMock):
    mock_client.bucket_exists.return_value = False

    storage.set_bucket_lifecycle("my-bucket", days=30)

    mock_client.make_bucket.assert_called_once_with(bucket_name="my-bucket")
    mock_client.set_bucket_lifecycle.assert_called_once()
    _, kwargs = mock_client.set_bucket_lifecycle.call_args
    assert kwargs["bucket_name"] == "my-bucket"


def test_set_bucket_lifecycle_skips_creation_when_bucket_exists(
    storage: Minio, mock_client: MagicMock
):
    mock_client.bucket_exists.return_value = True

    storage.set_bucket_lifecycle("my-bucket", days=30)

    mock_client.make_bucket.assert_not_called()


def test_check_file_exists_creates_bucket_then_returns_true_on_success(
    storage: Minio, mock_client: MagicMock
):
    mock_client.bucket_exists.return_value = False
    mock_client.stat_object.return_value = MagicMock()

    result = storage.check_file_exists("bucket", "path/file.txt")

    assert result is True
    mock_client.make_bucket.assert_called_once_with(bucket_name="bucket")


def test_check_file_exists_returns_false_when_stat_object_raises(
    storage: Minio, mock_client: MagicMock
):
    mock_client.bucket_exists.return_value = True
    mock_client.stat_object.side_effect = Exception("not found")

    result = storage.check_file_exists("bucket", "path/file.txt")

    assert result is False


def test_get_file_builds_file_object_from_response(storage: Minio, mock_client: MagicMock):
    content = b"hello world"
    response = MagicMock()
    response.read.return_value = content
    response.headers = {"Content-Type": "text/plain"}
    mock_client.get_object.return_value = response

    file_obj = storage.get_file("bucket", "path/file.txt")

    assert isinstance(file_obj, FileObject)
    assert file_obj.to_bytes() == content
    assert file_obj.content_type == "text/plain"
    assert file_obj.size == len(content)
    response.close.assert_called_once()
    response.release_conn.assert_called_once()


def test_get_file_raises_wrapped_exception_for_no_such_key(storage: Minio, mock_client: MagicMock):
    mock_client.get_object.side_effect = make_s3_error("NoSuchKey")

    with pytest.raises(Exception, match="File not found"):
        storage.get_file("bucket", "missing.txt")


def test_get_file_raises_wrapped_exception_for_no_such_bucket(
    storage: Minio, mock_client: MagicMock
):
    mock_client.get_object.side_effect = make_s3_error("NoSuchBucket")

    with pytest.raises(Exception, match="File not found"):
        storage.get_file("bucket", "missing.txt")


def test_get_file_reraises_other_s3_errors(storage: Minio, mock_client: MagicMock):
    error = make_s3_error("InternalError")
    mock_client.get_object.side_effect = error

    with pytest.raises(S3Error):
        storage.get_file("bucket", "missing.txt")


def test_upload_file_creates_bucket_when_missing_and_uploads(
    storage: Minio, mock_client: MagicMock
):
    mock_client.bucket_exists.return_value = False
    content = b"payload"
    file_obj = FileObject(
        filename="a.bin",
        content_type="application/octet-stream",
        size=0,
        stream=io.BytesIO(content),
    )

    storage.upload_file("bucket", file_obj, "object-name")

    mock_client.make_bucket.assert_called_once_with(bucket_name="bucket")
    mock_client.put_object.assert_called_once()
    _, kwargs = mock_client.put_object.call_args
    assert kwargs["bucket_name"] == "bucket"
    assert kwargs["object_name"] == "object-name"
    assert kwargs["length"] == len(content)
    assert kwargs["content_type"] == "application/octet-stream"


def test_upload_file_skips_bucket_creation_when_it_exists(storage: Minio, mock_client: MagicMock):
    mock_client.bucket_exists.return_value = True
    file_obj = FileObject(
        filename="a.bin",
        content_type="application/octet-stream",
        size=0,
        stream=io.BytesIO(b"x"),
    )

    storage.upload_file("bucket", file_obj, "object-name")

    mock_client.make_bucket.assert_not_called()
