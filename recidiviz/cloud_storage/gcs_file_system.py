# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""An abstraction for manipulating files on the Google Cloud Storage File System"""

import abc
import logging
import os
import tempfile
import uuid
from contextlib import contextmanager
from io import TextIOWrapper
from typing import IO, Any, Callable, Dict, Iterator, List, Optional, TextIO, Union
from zipfile import ZipExtFile, ZipFile

from google.api_core import exceptions, retry
from google.cloud import storage
from google.cloud.exceptions import NotFound
from paramiko import SFTPFile
from paramiko.sftp_client import SFTPClient

from recidiviz.cloud_storage.content_types import (
    FileContentsHandle,
    FileContentsRowType,
    IoType,
)
from recidiviz.cloud_storage.gcsfs_path import (
    GcsfsBucketPath,
    GcsfsDirectoryPath,
    GcsfsFilePath,
    GcsfsPath,
)
from recidiviz.cloud_storage.verifiable_bytes_reader import VerifiableBytesReader

BYTES_CONTENT_TYPE = "application/octet-stream"


class GCSBlobDoesNotExistError(ValueError):
    pass


class GcsfsFileContentsHandle(FileContentsHandle[str, IO]):
    """Handle to a local copy of a file from (or to be uploaded) to GCS"""

    def __init__(self, local_file_path: str, cleanup_file: bool = True):
        super().__init__(local_file_path=local_file_path)
        self.cleanup_file = cleanup_file

    def get_contents_iterator(self) -> Iterator[str]:
        """Lazy function (generator) to read a file line by line."""
        with self.open() as f:
            while line := f.readline():
                yield line

    @classmethod
    def from_bytes(cls, contents: bytes) -> "GcsfsFileContentsHandle":
        local_path = generate_random_temp_path()
        with open(local_path, "wb") as f:
            f.write(contents)
        return cls(local_path, cleanup_file=True)

    @contextmanager
    def open(self, mode: str = "r") -> Iterator[IO]:
        encoding = None if "b" in mode else "utf-8"
        with open(self.local_file_path, mode=mode, encoding=encoding) as f:
            yield f

    def __del__(self) -> None:
        """This ensures that the file contents on local disk are deleted when
        this handle is garbage collected.
        """
        if self.cleanup_file and os.path.exists(self.local_file_path):
            os.remove(self.local_file_path)


class SftpFileContentsHandle(FileContentsHandle[bytes, SFTPFile]):
    def __init__(self, local_file_path: str, sftp_client: SFTPClient):
        super().__init__(local_file_path=local_file_path)
        self.sftp_client = sftp_client

    def get_contents_iterator(self) -> Iterator[bytes]:
        with self.open() as f:
            while line := f.readline():
                yield line

    @contextmanager
    def open(self, mode: str = "r") -> Iterator[SFTPFile]:  # type: ignore
        with self.sftp_client.open(filename=self.local_file_path, mode=mode) as f:
            yield f


class ZipFileContentsHandle(FileContentsHandle[bytes, ZipExtFile]):
    def __init__(self, local_file_path: str, zip_file: ZipFile):
        super().__init__(local_file_path=local_file_path)
        self.zip_file = zip_file

    def get_contents_iterator(self) -> Iterator[bytes]:
        with self.open() as f:
            while line := f.readline():
                yield line

    @contextmanager
    def open(self, _: str = "r") -> Iterator[ZipExtFile]:  # type: ignore
        with self.zip_file.open(self.local_file_path, mode="r") as f:
            yield f  # type: ignore


class GCSFileSystem:
    """An abstraction for manipulating files on the Google Cloud Storage File System"""

    _RENAME_RETRIES = 5

    def mv(self, src_path: GcsfsFilePath, dst_path: GcsfsPath) -> None:
        """Moves object from bucket 1 to bucket 2 with optional rename. Note:
        this is *not* an atomic move - there is a failure case where you'd end
        up with a copied version of the file at |dst_path| but it has not been
        deleted from the original location.
        """
        self.copy(src_path, dst_path)
        self.delete(src_path)

    @abc.abstractmethod
    def copy(self, src_path: GcsfsFilePath, dst_path: GcsfsPath) -> None:
        """Copies object at |src_path| to |dst_path|."""

    @abc.abstractmethod
    def delete(self, path: GcsfsFilePath) -> None:
        """Deletes object at |path|."""

    @abc.abstractmethod
    def exists(self, path: Union[GcsfsBucketPath, GcsfsFilePath]) -> bool:
        """Returns True if the object exists in the fs, False otherwise."""

    @abc.abstractmethod
    def get_file_size(self, path: GcsfsFilePath) -> Optional[int]:
        """Returns the file size of the object if it exists in the fs, None otherwise."""

    @abc.abstractmethod
    def get_metadata(self, path: GcsfsFilePath) -> Optional[Dict[str, str]]:
        """
        Returns the metadata for the object at the given path if it exists in the fs, None otherwise. Returns
        Dict[str, str] instead of Dict[str, Any] because all values of the dictionary are typed casted to strings.
        """

    def clear_metadata(self, path: GcsfsFilePath) -> None:
        """
        Clears all of the custom metadata and sets it to None at the given path if it exists in the fs, returns
        None even if the path does not exist.
        """

    @abc.abstractmethod
    def update_metadata(
        self,
        path: GcsfsFilePath,
        new_metadata: Dict[str, str],
    ) -> None:
        """
        Updates the custom metadata for the object at the given path if it exists in the fs. If there are preexisting keys
        in the metadata that match the new_metadata keys those keys will be overriden. If custom metadata has keys that new_metadata
        does not those keys will still exist in the custom metadata. To clear preexisiting keys not in new_metadata
        call clear_metadata() before updating.

        Required to pass in Dict[str, str] since gcs appears to just type cast non string values of dicts to strings. Recommended
        to call json.dumps() prior to calling update_metadata.
        """

    @abc.abstractmethod
    def download_as_string(self, path: GcsfsFilePath, encoding: str = "utf-8") -> str:
        """
        Downloads object contents from the given path to a string,
        decoding it from the specified `encoding` (default UTF-8)
        """

    @abc.abstractmethod
    def download_as_bytes(self, path: GcsfsFilePath) -> bytes:
        """
        Downloads object contents from the given path to bytes.
        """

    @abc.abstractmethod
    def download_to_temp_file(
        self, path: GcsfsFilePath, retain_original_filename: bool = False
    ) -> Optional[GcsfsFileContentsHandle]:
        """Generates a new file in a temporary directory on the local file
        system (App Engine VM when in prod/staging), and downloads file contents
        from the provided GCS path into that file, returning a handle to temp
        file on the local App Engine VM file system, or None if the GCS file is
        not found.
        """

    @abc.abstractmethod
    def upload_from_string(
        self, path: GcsfsFilePath, contents: str, content_type: str
    ) -> None:
        """Uploads string contents to a file path."""

    @abc.abstractmethod
    def upload_from_contents_handle_stream(
        self,
        path: GcsfsFilePath,
        contents_handle: FileContentsHandle[FileContentsRowType, IoType],
        content_type: str,
    ) -> None:
        """Uploads contents in handle via a file stream to a file path."""

    @abc.abstractmethod
    def ls_with_blob_prefix(
        self, bucket_name: str, blob_prefix: str
    ) -> List[Union[GcsfsDirectoryPath, GcsfsFilePath]]:
        """Returns absolute paths of objects in the bucket with the given |relative_path|. """

    @abc.abstractmethod
    def set_content_type(self, path: GcsfsFilePath, content_type: str) -> None:
        """Allows for the content type of a certain file path to be reset."""

    @abc.abstractmethod
    def is_dir(self, path: str) -> bool:
        """Returns whether the given path exists and is a GcsfsDirectoryPath or not."""

    @abc.abstractmethod
    def is_file(self, path: str) -> bool:
        """Returns whether the given path exists and is a GcsfsFilePath or not."""

    @contextmanager
    @abc.abstractmethod
    def open(
        self,
        path: GcsfsFilePath,
        chunk_size: Optional[int] = None,
        encoding: Optional[str] = None,
    ) -> Iterator[TextIO]:
        """Returns a read-only file handler for the blob contents.

        Streams the contents from GCS for reduced memory consumption. If all of the
        contents are read, verifies that the checksum is correct (or raises).
        """


def retry_predicate(exception: Exception) -> Callable[[Exception], bool]:
    """"A function that will determine whether we should retry a given Google exception."""
    return retry.if_transient_error(exception) or retry.if_exception_type(
        exceptions.GatewayTimeout
    )(exception)


def generate_random_temp_path(filename: Optional[str] = None) -> str:
    temp_dir = os.path.join(tempfile.gettempdir(), "gcs_temp_files")
    os.makedirs(temp_dir, exist_ok=True)

    return os.path.join(temp_dir, filename if filename else str(uuid.uuid4()))


class GCSFileSystemImpl(GCSFileSystem):
    """An implementation of the GCSFileSystem built on top of a real GCSFileSystem. """

    def __init__(self, client: storage.Client):
        self.storage_client = client

    @retry.Retry(predicate=retry_predicate)
    def exists(self, path: Union[GcsfsBucketPath, GcsfsFilePath]) -> bool:
        bucket = self.storage_client.bucket(path.bucket_name)
        if isinstance(path, GcsfsBucketPath):
            return bucket.exists(self.storage_client)

        if isinstance(path, GcsfsFilePath):
            blob = bucket.get_blob(path.blob_name)
            if not blob:
                return False
            return blob.exists(self.storage_client)

        raise ValueError(f"Unexpected path type [{type(path)}]")

    def _get_blob(self, path: GcsfsFilePath) -> storage.Blob:
        try:
            bucket = self.storage_client.bucket(path.bucket_name)
            blob = bucket.get_blob(path.blob_name)
        except NotFound as error:
            logging.warning(
                "Blob at [%s] does not exist - might have already been deleted",
                path.uri(),
            )

            raise GCSBlobDoesNotExistError(
                f"Blob at [{path.uri()}] does not exist"
            ) from error
        else:
            if not blob:
                logging.warning(
                    "Blob at [%s] does not exist - might have already been deleted",
                    path.uri(),
                )

                raise GCSBlobDoesNotExistError(f"Blob at [{path.uri()}] does not exist")

            return blob

    @retry.Retry(predicate=retry_predicate)
    def get_file_size(self, path: GcsfsFilePath) -> Optional[int]:
        try:
            blob = self._get_blob(path)
            return blob.size
        except GCSBlobDoesNotExistError:
            return None

    @retry.Retry(predicate=retry_predicate)
    def get_metadata(self, path: GcsfsFilePath) -> Optional[Dict[str, Any]]:
        try:
            blob = self._get_blob(path)
            return blob.metadata
        except GCSBlobDoesNotExistError:
            return None

    def clear_metadata(self, path: GcsfsFilePath) -> None:
        try:
            blob = self._get_blob(path)
            blob.metadata = None
            blob.patch()
        except GCSBlobDoesNotExistError:
            return

    @retry.Retry(predicate=retry_predicate)
    def update_metadata(
        self,
        path: GcsfsFilePath,
        new_metadata: Dict[str, str],
    ) -> None:
        try:
            blob = self._get_blob(path)
            # keys in new_metadata must match keys in the metadata to overwrite data.
            # if new_metadata keys do not match the existing keys the old keys and values will still exist.
            blob.metadata = new_metadata
            blob.patch()
        except GCSBlobDoesNotExistError:
            return

    @retry.Retry(predicate=retry_predicate)
    def copy(self, src_path: GcsfsFilePath, dst_path: GcsfsPath) -> None:
        src_bucket = self.storage_client.bucket(src_path.bucket_name)
        src_blob = self._get_blob(src_path)

        dst_bucket = self.storage_client.bucket(dst_path.bucket_name)

        if isinstance(dst_path, GcsfsFilePath):
            dst_blob_name = dst_path.blob_name
        elif isinstance(dst_path, GcsfsDirectoryPath):
            dst_blob_name = GcsfsFilePath.from_directory_and_file_name(
                dst_path, src_path.file_name
            ).blob_name
        else:
            raise ValueError(f"Unexpected path type [{type(dst_path)}]")

        src_bucket.copy_blob(src_blob, dst_bucket, dst_blob_name)

    @retry.Retry(predicate=retry_predicate)
    def delete(self, path: GcsfsFilePath) -> None:
        if not isinstance(path, GcsfsFilePath):
            raise ValueError(f"Unexpected path type [{type(path)}]")

        try:
            blob = self._get_blob(path)

            blob.delete(self.storage_client)
        except GCSBlobDoesNotExistError:
            return

    @retry.Retry(predicate=retry_predicate)
    def download_to_temp_file(
        self, path: GcsfsFilePath, retain_original_filename: bool = False
    ) -> Optional[GcsfsFileContentsHandle]:
        temp_file_path = generate_random_temp_path(
            path.file_name if retain_original_filename else None
        )

        try:
            blob = self._get_blob(path)

            logging.info(
                "Started download of file [{%s}] to local file [%s].",
                path.abs_path(),
                temp_file_path,
            )
            blob.download_to_filename(temp_file_path)
            logging.info(
                "Completed download of file [{%s}] to local file [%s].",
                path.abs_path(),
                temp_file_path,
            )
            return GcsfsFileContentsHandle(temp_file_path)
        except GCSBlobDoesNotExistError:
            return None

    @retry.Retry(predicate=retry_predicate)
    def download_as_string(self, path: GcsfsFilePath, encoding: str = "utf-8") -> str:
        blob = self._get_blob(path)

        return blob.download_as_bytes().decode(encoding)

    @retry.Retry(predicate=retry_predicate)
    def download_as_bytes(self, path: GcsfsFilePath) -> bytes:
        blob = self._get_blob(path)
        return blob.download_as_bytes()

    @retry.Retry(predicate=retry_predicate)
    def upload_from_string(
        self, path: GcsfsFilePath, contents: str, content_type: str
    ) -> None:
        bucket = self.storage_client.bucket(path.bucket_name)
        bucket.blob(path.blob_name).upload_from_string(
            contents, content_type=content_type
        )

    @retry.Retry(predicate=retry_predicate)
    def upload_from_contents_handle_stream(
        self,
        path: GcsfsFilePath,
        contents_handle: FileContentsHandle[FileContentsRowType, IoType],
        content_type: str,
    ) -> None:
        bucket = self.storage_client.bucket(path.bucket_name)
        with contents_handle.open("rb") as file_stream:
            bucket.blob(path.blob_name).upload_from_file(
                file_stream, content_type=content_type
            )

    @retry.Retry(predicate=retry_predicate)
    def ls_with_blob_prefix(
        self, bucket_name: str, blob_prefix: str
    ) -> List[Union[GcsfsDirectoryPath, GcsfsFilePath]]:
        blobs = self.storage_client.list_blobs(bucket_name, prefix=blob_prefix)
        return [GcsfsPath.from_blob(blob) for blob in blobs]

    @retry.Retry(predicate=retry_predicate)
    def set_content_type(self, path: GcsfsFilePath, content_type: str) -> None:
        blob = self._get_blob(path)
        blob.content_type = content_type
        blob.patch()

    @retry.Retry(predicate=retry_predicate)
    def is_dir(self, path: str) -> bool:
        try:
            directory = GcsfsDirectoryPath.from_absolute_path(path)
            # If the directory is empty, has_dir will have 1 entry, which is the Blob representing the directory
            # Otherwise, if the directory doesn't exist on GCS, has_dir will return an empty list
            has_dir = self.ls_with_blob_prefix(
                bucket_name=directory.bucket_name, blob_prefix=directory.relative_path
            )
            return len(has_dir) > 0
        except ValueError:
            return False

    @retry.Retry(predicate=retry_predicate)
    def is_file(self, path: str) -> bool:
        try:
            file = GcsfsFilePath.from_absolute_path(path)
            return self.exists(file)
        except ValueError:
            return False

    @contextmanager
    def open(
        self,
        path: GcsfsFilePath,
        chunk_size: Optional[int] = None,
        encoding: Optional[str] = None,
    ) -> Iterator[TextIO]:
        blob = self._get_blob(path)
        with blob.open("rb", chunk_size=chunk_size) as f:
            verifiable_reader = VerifiableBytesReader(f, name=path.uri())
            try:
                yield TextIOWrapper(buffer=verifiable_reader, encoding=encoding)
            finally:
                verifiable_reader.verify_crc32c(blob.crc32c)
