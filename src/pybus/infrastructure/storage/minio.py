import io
import mimetypes
import secrets
from pathlib import Path
from typing import override

from minio import Minio as MinioClient
from minio.commonconfig import ENABLED, Filter
from minio.error import S3Error
from minio.lifecycleconfig import Expiration, LifecycleConfig, Rule

from pybus.application.interfaces import Storage
from pybus.domain.value_objects import FileObject


class Minio(Storage):
    def __init__(
        self, endpoint: str, access_key: str, secret_key: str, secure: bool = False
    ) -> None:
        self._client: MinioClient = MinioClient(
            endpoint=endpoint, access_key=access_key, secret_key=secret_key, secure=secure
        )

    @override
    def set_bucket_lifecycle(self, bucket: str, days: int) -> None:
        if not self._client.bucket_exists(bucket_name=bucket):
            self._client.make_bucket(bucket_name=bucket)

        self._client.set_bucket_lifecycle(
            bucket_name=bucket,
            config=LifecycleConfig(
                rules=[
                    Rule(
                        status=ENABLED,
                        rule_filter=Filter(prefix=""),
                        rule_id=secrets.token_urlsafe(32),
                        expiration=Expiration(days=days),
                    )
                ]
            ),
        )

    def __check_bucket_exists(self, bucket: str) -> None:
        if not self._client.bucket_exists(bucket_name=bucket):
            self._client.make_bucket(bucket_name=bucket)

    @override
    def check_file_exists(self, bucket: str, file_path: str) -> bool:
        self.__check_bucket_exists(bucket=bucket)
        try:
            self._client.stat_object(bucket_name=bucket, object_name=file_path)
            return True
        except Exception:
            return False

    @override
    def get_file(self, bucket: str, file_path: str) -> FileObject:
        try:
            response = self._client.get_object(bucket_name=bucket, object_name=file_path)
        except S3Error as ex:
            if ex.code in ("NoSuchKey", "NoSuchBucket"):
                raise Exception("File not found") from ex
            raise

        content = response.read()  # 小檔案可直接一次讀
        response.close()
        response.release_conn()
        content_type = response.headers.get("Content-Type", "application/octet-stream")

        return FileObject(
            stream=io.BytesIO(content),
            size=len(content),
            content_type=content_type,
            filename=f"{Path(file_path).name}{mimetypes.guess_extension(content_type)}",
        )

    @override
    def upload_file(self, bucket: str, file: FileObject, object_name: str) -> None:
        if not self._client.bucket_exists(bucket_name=bucket):
            self._client.make_bucket(bucket_name=bucket)

        file.stream.seek(0)
        self._client.put_object(
            bucket_name=bucket,
            object_name=object_name,
            data=file.stream,
            length=file.size,
            content_type=file.content_type,
        )
