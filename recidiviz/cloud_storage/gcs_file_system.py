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
from typing import List, Optional, Union, Iterator, Callable, Dict

from google.api_core import retry, exceptions
from google.cloud import storage
from google.cloud.exceptions import NotFound

from recidiviz.cloud_storage.content_types import FileContentsHandle
from recidiviz.cloud_storage.gcsfs_path import GcsfsPath, \
    GcsfsFilePath, GcsfsDirectoryPath, GcsfsBucketPath


class GcsfsFileContentsHandle(FileContentsHandle[str]):
    def __init__(self, local_file_path: str):
        self.local_file_path = local_file_path

    def get_contents_iterator(self) -> Iterator[str]:
        """Lazy function (generator) to read a file line by line."""
        with open(self.local_file_path, encoding='utf-8') as f:
            while True:
                line = f.readline()
                if not line:
                    break
                yield line

    def __del__(self) -> None:
        """This ensures that the file contents on local disk are deleted when
        this handle is garbage collected.
        """
        if os.path.exists(self.local_file_path):
            os.remove(self.local_file_path)


class GCSFileSystem:
    """An abstraction for manipulating files on the Google Cloud Storage File System"""
    _RENAME_RETRIES = 5

    def mv(self,
           src_path: GcsfsFilePath,
           dst_path: GcsfsPath) -> None:
        """Moves object from bucket 1 to bucket 2 with optional rename. Note:
        this is *not* an atomic move - there is a failure case where you'd end
        up with a copied version of the file at |dst_path| but it has not been
        deleted from the original location.
        """
        self.copy(src_path, dst_path)
        self.delete(src_path)

    @abc.abstractmethod
    def copy(self,
             src_path: GcsfsFilePath,
             dst_path: GcsfsPath) -> None:
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
        """Returns the metadata for the object at the given path if it exists in the fs, None otherwise."""

    @abc.abstractmethod
    def download_to_temp_file(self, path: GcsfsFilePath) -> Optional[GcsfsFileContentsHandle]:
        """Generates a new file in a temporary directory on the local file
        system (App Engine VM when in prod/staging), and downloads file contents
        from the provided GCS path into that file, returning a handle to temp
        file on the local App Engine VM file system, or None if the GCS file is
        not found.
        """

    @abc.abstractmethod
    def upload_from_string(self,
                           path: GcsfsFilePath,
                           contents: str,
                           content_type: str) -> None:
        """Uploads string contents to a file path."""

    @abc.abstractmethod
    def upload_from_contents_handle(self,
                                    path: GcsfsFilePath,
                                    contents_handle: GcsfsFileContentsHandle,
                                    content_type: str) -> None:
        """Uploads contents in handle to a file path."""

    @abc.abstractmethod
    def ls_with_blob_prefix(self,
                            bucket_name: str,
                            blob_prefix: str) -> List[Union[GcsfsDirectoryPath, GcsfsFilePath]]:
        """Returns absolute paths of objects in the bucket with the given |relative_path|. """


def retry_predicate(exception: Exception) -> Callable[[Exception], bool]:
    """"A function that will determine whether we should retry a given Google exception."""
    return retry.if_transient_error(exception) or retry.if_exception_type(exceptions.GatewayTimeout)(exception)


def generate_random_temp_path() -> str:
    temp_dir = os.path.join(tempfile.gettempdir(), 'gcs_temp_files')

    if not os.path.exists(temp_dir):
        os.mkdir(temp_dir)

    return os.path.join(temp_dir, str(uuid.uuid4()))


class GCSFileSystemImpl(GCSFileSystem):
    """An implementation of the GCSFileSystem built on top of a real GCSFileSystem. """

    def __init__(self, client: storage.Client):
        self.storage_client = client

    @retry.Retry(predicate=retry_predicate)
    def exists(self, path: Union[GcsfsBucketPath, GcsfsFilePath]) -> bool:
        bucket = self.storage_client.get_bucket(path.bucket_name)
        if isinstance(path, GcsfsBucketPath):
            return bucket.exists(self.storage_client)

        if isinstance(path, GcsfsFilePath):
            blob = bucket.get_blob(path.blob_name)
            if not blob:
                return False
            return blob.exists(self.storage_client)

        raise ValueError(f'Unexpected path type [{type(path)}]')

    @retry.Retry(predicate=retry_predicate)
    def get_file_size(self, path: GcsfsFilePath) -> Optional[int]:
        bucket = self.storage_client.get_bucket(path.bucket_name)
        blob = bucket.get_blob(path.blob_name)
        return blob.size if blob else None

    @retry.Retry(predicate=retry_predicate)
    def get_metadata(self, path: GcsfsFilePath) -> Optional[Dict[str, str]]:
        bucket = self.storage_client.get_bucket(path.bucket_name)
        blob = bucket.get_blob(path.blob_name)
        return blob.metadata if blob else None

    @retry.Retry(predicate=retry_predicate)
    def copy(self,
             src_path: GcsfsFilePath,
             dst_path: GcsfsPath) -> None:
        src_bucket = self.storage_client.get_bucket(src_path.bucket_name)
        src_blob = src_bucket.get_blob(src_path.blob_name)
        if not src_blob:
            raise ValueError(
                f'Blob at path [{src_path.abs_path()}] does not exist')
        dst_bucket = self.storage_client.get_bucket(dst_path.bucket_name)

        if isinstance(dst_path, GcsfsFilePath):
            dst_blob_name = dst_path.blob_name
        elif isinstance(dst_path, GcsfsDirectoryPath):
            dst_blob_name = \
                GcsfsFilePath.from_directory_and_file_name(
                    dst_path, src_path.file_name).blob_name
        else:
            raise ValueError(f'Unexpected path type [{type(dst_path)}]')

        src_bucket.copy_blob(src_blob, dst_bucket, dst_blob_name)

    @retry.Retry(predicate=retry_predicate)
    def delete(self, path: GcsfsFilePath) -> None:
        if not isinstance(path, GcsfsFilePath):
            raise ValueError(f'Unexpected path type [{type(path)}]')

        bucket = self.storage_client.get_bucket(path.bucket_name)
        blob = bucket.get_blob(path.blob_name)

        if not blob:
            logging.warning("Path [%s] already does not exist, returning.",
                            path.abs_path())
            return

        blob.delete(self.storage_client)

    @retry.Retry(predicate=retry_predicate)
    def download_to_temp_file(self, path: GcsfsFilePath) -> Optional[GcsfsFileContentsHandle]:
        bucket = self.storage_client.get_bucket(path.bucket_name)
        blob = bucket.get_blob(path.blob_name)
        if not blob:
            logging.info(
                "File path [%s] no longer exists - might have already "
                "been processed or deleted", path.abs_path())
            return None

        temp_file_path = generate_random_temp_path()

        try:
            logging.info(
                "Started download of file [{%s}] to local file [%s].",
                path.abs_path(), temp_file_path)
            blob.download_to_filename(temp_file_path)
            logging.info(
                "Completed download of file [{%s}] to local file [%s].",
                path.abs_path(), temp_file_path)
            return GcsfsFileContentsHandle(temp_file_path)
        except NotFound:
            logging.info(
                "File path [%s] no longer exists - might have already "
                "been processed or deleted", path.abs_path())
            return None

    @retry.Retry(predicate=retry_predicate)
    def upload_from_string(self, path: GcsfsFilePath,
                           contents: str,
                           content_type: str) -> None:
        bucket = self.storage_client.get_bucket(path.bucket_name)
        bucket.blob(path.blob_name).upload_from_string(
            contents, content_type=content_type)

    @retry.Retry(predicate=retry_predicate)
    def upload_from_contents_handle(self,
                                    path: GcsfsFilePath,
                                    contents_handle: GcsfsFileContentsHandle,
                                    content_type: str) -> None:
        bucket = self.storage_client.get_bucket(path.bucket_name)
        bucket.blob(path.blob_name).upload_from_filename(
            contents_handle.local_file_path, content_type=content_type)

    @retry.Retry(predicate=retry_predicate)
    def ls_with_blob_prefix(
            self,
            bucket_name: str,
            blob_prefix: str) -> List[Union[GcsfsDirectoryPath, GcsfsFilePath]]:
        blobs = self.storage_client.list_blobs(bucket_name, prefix=blob_prefix)
        return [GcsfsPath.from_blob(blob) for blob in blobs]
