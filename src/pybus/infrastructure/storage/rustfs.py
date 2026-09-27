"""The `Storage` port over RustFS.

This was `Minio`, over the `minio` SDK, until the `minio/minio` image stopped
being pullable from Docker Hub and the compose stack moved to RustFS. The SDK
went too: RustFS ships no Python client of its own and documents boto3, and a
MinIO-named dependency talking to a server that is not MinIO would leave the
next reader guessing which of the two the code targets. Nothing here is
RustFS-specific beyond the name -- it is plain S3.

It was boto3 for one release (v1.7) and is aioboto3 now, which is why the
`Storage` port is async. Every caller is an async handler, and a synchronous
S3 call there blocks the event loop for the length of the round trip --
every other request the process is serving waits on somebody's upload.
"""

import io
import mimetypes
import secrets
from contextlib import AbstractAsyncContextManager
from pathlib import Path
from typing import TYPE_CHECKING, override

import aioboto3
from aiobotocore.config import AioConfig
from botocore.exceptions import ClientError

from pybus.application.interfaces import Storage
from pybus.domain.value_objects import FileObject

if TYPE_CHECKING:
    from types_aiobotocore_s3 import S3Client

# The error codes that mean a lookup missed, as opposed to failed. Everything
# else a ClientError carries -- permissions, a bad region, a bucket policy --
# is a fault, not an absence. "404" and "NotFound" are here as well as the
# named codes because a HEAD response has no body to carry a code in, so
# botocore reports the status instead.
_MISSING_CODES = ("NoSuchKey", "NoSuchBucket", "NotFound", "404")


def _is_missing(ex: ClientError) -> bool:
    return ex.response.get("Error", {}).get("Code") in _MISSING_CODES


def _filename(key: str, content_type: str) -> str:
    """The key's last segment, with an extension only if it has none.

    This used to append the guessed extension unconditionally, so
    `a/hello.txt` came back as `hello.txt.txt` -- and when nothing could be
    guessed it appended `None`, which is also what every content type with a
    parameter got, since `text/plain; charset=utf-8` is not in the table
    `guess_extension` reads. The parameter is stripped for the lookup; an
    unguessable type leaves the name bare rather than inventing a suffix.
    """
    name = Path(key).name
    if Path(name).suffix:
        return name
    extension = mimetypes.guess_extension(content_type.split(";", 1)[0].strip())
    return f"{name}{extension or ''}"


class FileNotFound(Exception):
    """No object at that key.

    The name is load-bearing: both services' gRPC interceptors map any
    exception whose class name ends in `NotFound` to `NOT_FOUND`, matching
    on the name rather than on an import. This used to be a bare
    `Exception("File not found")`, which fell through to `INTERNAL` and told
    a caller the server had broken when in fact they had asked for
    something that is not there.
    """


class RustFS(Storage):
    """A client per call, opened and closed inside it, over one session.

    aiobotocore's client is an async context manager: it owns an aiohttp
    connection pool that has to be closed on the event loop that opened it.
    Holding one for the adapter's lifetime would mean a `close()` every
    service has to remember to await at shutdown, and pybus's container has
    no hook that would do it for them -- while `Storage` has no consumer yet
    to say the cost is worth it. So each method opens its own. The known
    cost is a fresh connection per call; the service model is loaded once,
    because the session caches it. Revisit when something calls this often
    enough to notice.
    """

    def __init__(
        self, endpoint: str, access_key: str, secret_key: str, secure: bool = False
    ) -> None:
        self._session = aioboto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            # Required by SigV4 and ignored by RustFS; us-east-1 is the value
            # every S3-compatible server accepts.
            region_name="us-east-1",
        )
        # `endpoint` stays `host:port` with a separate `secure`, the shape the
        # minio SDK took, so a caller's settings did not have to change with
        # the client. botocore wants a URL, so it is built here.
        self._endpoint_url = f"{'https' if secure else 'http'}://{endpoint}"
        # Path-style, because the default virtual-hosted style puts the bucket
        # in the hostname (`bucket.rustfs:9000`), which no DNS inside compose
        # resolves.
        self._config = AioConfig(s3={"addressing_style": "path"})

    def _client(self) -> "AbstractAsyncContextManager[S3Client]":
        return self._session.client("s3", endpoint_url=self._endpoint_url, config=self._config)

    @staticmethod
    async def _ensure_bucket(client: "S3Client", bucket: str) -> None:
        try:
            await client.head_bucket(Bucket=bucket)
        except ClientError as ex:
            if not _is_missing(ex):
                raise
            await client.create_bucket(Bucket=bucket)

    @override
    async def set_bucket_lifecycle(self, bucket: str, days: int) -> None:
        async with self._client() as client:
            await self._ensure_bucket(client, bucket)
            await client.put_bucket_lifecycle_configuration(
                Bucket=bucket,
                LifecycleConfiguration={
                    "Rules": [
                        {
                            "ID": secrets.token_urlsafe(32),
                            "Status": "Enabled",
                            "Filter": {"Prefix": ""},
                            "Expiration": {"Days": days},
                        }
                    ]
                },
            )

    @override
    async def check_file_exists(self, bucket: str, file_path: str) -> bool:
        async with self._client() as client:
            await self._ensure_bucket(client, bucket)
            try:
                await client.head_object(Bucket=bucket, Key=file_path)
                return True
            except ClientError as ex:
                # Only a miss is False. This used to catch everything, so an
                # unreachable backend, an expired credential or a denied policy
                # all answered "that file does not exist" -- a caller deciding
                # whether to upload would be told to overwrite, and a caller
                # checking before a read would report a clean absence. A
                # storage backend that is down has to say so.
                if _is_missing(ex):
                    return False
                raise

    @override
    async def get_file(self, bucket: str, file_path: str) -> FileObject:
        async with self._client() as client:
            try:
                response = await client.get_object(Bucket=bucket, Key=file_path)
            except ClientError as ex:
                if _is_missing(ex):
                    raise FileNotFound(f"No object at {bucket}/{file_path}") from ex
                raise

            # Read inside the client's block: the body streams from the
            # client's connection pool, which closes when the block exits.
            async with response["Body"] as body:
                content = await body.read()  # 小檔案可直接一次讀
        content_type = response.get("ContentType", "application/octet-stream")

        return FileObject(
            stream=io.BytesIO(content),
            size=len(content),
            content_type=content_type,
            filename=_filename(file_path, content_type),
        )

    @override
    async def upload_file(self, bucket: str, file: FileObject, object_name: str) -> None:
        async with self._client() as client:
            await self._ensure_bucket(client, bucket)
            await client.put_object(
                Bucket=bucket,
                Key=object_name,
                # Bytes rather than the stream. FileObject.stream is declared
                # io.IOBase so pydantic's isinstance check accepts an
                # io.BytesIO, and that is not a type the stubs take as a Body.
                # Reading it out is cheap because FileObject refuses anything
                # over 2MB.
                Body=file.to_bytes(),
                ContentLength=file.size,
                ContentType=file.content_type,
            )


__all__ = ["FileNotFound", "RustFS"]
