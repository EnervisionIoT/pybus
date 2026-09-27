from abc import ABC, abstractmethod

from ...domain.value_objects import FileObject


class Storage(ABC):
    @abstractmethod
    async def set_bucket_lifecycle(self, bucket: str, days: int) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def check_file_exists(self, bucket: str, file_path: str) -> bool:
        raise NotImplementedError()

    @abstractmethod
    async def get_file(self, bucket: str, file_path: str) -> FileObject:
        raise NotImplementedError()

    @abstractmethod
    async def upload_file(self, bucket: str, file: FileObject, object_name: str) -> None:
        raise NotImplementedError()
