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
"""An abstraction built on top of the GCSFileSystem class with helpers for
manipulating files and filenames expected by direct ingest.
"""

import abc
import datetime
import logging
import os
from typing import List, Optional, Union

from google.cloud import storage

from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import \
    filename_parts_from_path
from recidiviz.ingest.direct.controllers.gcsfs_path import GcsfsPath, \
    GcsfsFilePath, GcsfsDirectoryPath, GcsfsBucketPath

DIRECT_INGEST_UNPROCESSED_PREFIX = 'unprocessed'
DIRECT_INGEST_PROCESSED_PREFIX = 'processed'
SPLIT_FILE_SUFFIX = 'file_split'
SPLIT_FILE_STORAGE_SUBDIR = 'split_files'


def _build_unprocessed_file_name(
        *,
        utc_iso_timestamp_str: str,
        file_tag: str,
        extension: str) -> str:

    file_name_parts = [
        DIRECT_INGEST_UNPROCESSED_PREFIX,
        utc_iso_timestamp_str,
        file_tag
    ]

    return "_".join(file_name_parts) + f".{extension}"


def to_normalized_unprocessed_file_path(
        original_file_path: str,
        dt: Optional[datetime.datetime] = None) -> str:
    if not dt:
        dt = datetime.datetime.utcnow()

    directory, file_name = os.path.split(original_file_path)
    utc_iso_timestamp_str = dt.strftime('%Y-%m-%dT%H:%M:%S:%f')
    file_tag, extension = file_name.split('.')

    updated_relative_path = _build_unprocessed_file_name(
        utc_iso_timestamp_str=utc_iso_timestamp_str,
        file_tag=file_tag,
        extension=extension)

    return os.path.join(directory, updated_relative_path)


class DirectIngestGCSFileSystem:
    """An abstraction built on top of the GCSFileSystem class with helpers for
    manipulating files and filenames expected by direct ingest.
    """
    _RENAME_RETRIES = 5

    @abc.abstractmethod
    def exists(self, path: Union[GcsfsBucketPath, GcsfsFilePath]) -> bool:
        """Returns True if the object exists in the fs, False otherwise."""

    @abc.abstractmethod
    def download_as_string(self, path: GcsfsFilePath) -> bytes:
        """Downloads file contents into string format."""

    @abc.abstractmethod
    def upload_from_string(self,
                           path: GcsfsFilePath,
                           contents: str,
                           content_type: str):
        """Uploads string contents to a file path."""

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

    @staticmethod
    def is_processed_file(path: GcsfsFilePath):
        return path.file_name.startswith(DIRECT_INGEST_PROCESSED_PREFIX)

    @staticmethod
    def is_seen_unprocessed_file(path: GcsfsFilePath):
        return path.file_name.startswith(DIRECT_INGEST_UNPROCESSED_PREFIX)

    @staticmethod
    def is_split_file(path: GcsfsFilePath):
        return filename_parts_from_path(path).is_file_split

    @staticmethod
    def is_normalized_file_path(path: GcsfsFilePath) -> bool:
        return DirectIngestGCSFileSystem.is_seen_unprocessed_file(path) or \
            DirectIngestGCSFileSystem.is_processed_file(path)

    def get_unnormalized_file_paths(
            self, directory_path: GcsfsDirectoryPath) -> List[GcsfsFilePath]:
        """Returns all paths in the given directory without normalized paths.
        """
        paths = self._ls_with_file_prefix(directory_path, '')

        return [path
                for path in paths if not self.is_normalized_file_path(path)]

    def get_unprocessed_file_paths(
            self, directory_path: GcsfsDirectoryPath) -> List[GcsfsFilePath]:
        """Returns all paths in the given directory that have yet to be
        processed.
        """
        return self._ls_with_file_prefix(
            directory_path,
            DIRECT_INGEST_UNPROCESSED_PREFIX)

    def get_unprocessed_file_paths_for_day(
            self,
            directory_path: GcsfsDirectoryPath,
            date_str: str) -> List[GcsfsFilePath]:
        """Returns all paths in the given directory that were uploaded on the
        day specified in date_str that have yet to be processed.
        """
        return self._ls_with_file_prefix(
            directory_path,
            f"{DIRECT_INGEST_UNPROCESSED_PREFIX}_{date_str}")

    def get_processed_file_paths(
            self,
            directory_path: GcsfsDirectoryPath) -> List[GcsfsFilePath]:
        """Returns all paths in the given directory that have been
        processed.
        """
        return self._ls_with_file_prefix(directory_path,
                                         DIRECT_INGEST_PROCESSED_PREFIX)

    def get_processed_file_paths_for_day(self,
                                         directory_path: GcsfsDirectoryPath,
                                         date_str: str) -> List[GcsfsFilePath]:
        """Returns all paths in the given directory that were uploaded on the
        day specified in date_str that have been processed.
        """
        return self._ls_with_file_prefix(
            directory_path,
            f"{DIRECT_INGEST_PROCESSED_PREFIX}_{date_str}")

    def mv_path_to_normalized_path(self,
                                   path: GcsfsFilePath,
                                   dt: Optional[datetime.datetime] = None):
        updated_file_path = \
            GcsfsFilePath.from_absolute_path(
                to_normalized_unprocessed_file_path(path.abs_path(), dt))

        if self.exists(updated_file_path):
            raise ValueError(
                f"Desired path [{updated_file_path.abs_path()}] "
                f"already exists, returning")

        logging.info("Moving [%s] to normalized path [%s].",
                     path.abs_path(), updated_file_path.abs_path())
        self.mv(path, updated_file_path)

    def mv_path_to_processed_path(self, path: GcsfsFilePath):
        processed_path = self._to_processed_file_path(path)
        logging.info("Moving [%s] to processed path [%s].",
                     path.abs_path(), processed_path.abs_path())
        self.mv(path, processed_path)

    def mv_processed_paths_before_date_to_storage(
            self,
            directory_path: GcsfsDirectoryPath,
            storage_directory_path: GcsfsDirectoryPath,
            date_str_bound: str,
            include_bound: bool):

        processed_file_paths = self.get_processed_file_paths(directory_path)

        for file_path in processed_file_paths:
            date_str = filename_parts_from_path(file_path).date_str
            if date_str < date_str_bound or \
                    (include_bound and date_str == date_str_bound):
                logging.info(
                    "Found file [%s] from [%s] which abides by provided bound "
                    "[%s]. Moving to storage.",
                    file_path.abs_path(), date_str, date_str_bound)
                self.mv_path_to_storage(file_path, storage_directory_path)

    def mv_path_to_storage(self,
                           path: GcsfsFilePath,
                           storage_directory_path: GcsfsDirectoryPath):
        optional_storage_subdir = None
        if self.is_split_file(path):
            optional_storage_subdir = SPLIT_FILE_STORAGE_SUBDIR

        parts = filename_parts_from_path(path)
        storage_path = self._storage_path(
            storage_directory_path,
            optional_storage_subdir,
            parts.date_str,
            path.file_name)

        logging.info("Moving [%s] to storage path [%s].",
                     path.abs_path(), storage_path.abs_path())
        self.mv(path, storage_path)

    def _ls_with_file_prefix(self,
                             directory_path: GcsfsDirectoryPath,
                             file_prefix: str) -> List[GcsfsFilePath]:
        """Returns absolute paths of files in the directory with the given
        |file_prefix|.
        """
        blob_prefix = os.path.join(*[directory_path.relative_path, file_prefix])
        return self._ls_with_blob_prefix(directory_path.bucket_name,
                                         blob_prefix)

    @abc.abstractmethod
    def _ls_with_blob_prefix(self,
                             bucket_name: str,
                             blob_prefix: str) -> List[GcsfsFilePath]:
        """Returns absolute paths of files in the bucket with the given
        |relative_path|.
        """

    @staticmethod
    def _to_processed_file_path(
            unprocessed_file_path: GcsfsFilePath) -> GcsfsFilePath:
        processed_file_name = unprocessed_file_path.file_name.replace(
            DIRECT_INGEST_UNPROCESSED_PREFIX, DIRECT_INGEST_PROCESSED_PREFIX)

        return GcsfsFilePath.with_new_file_name(unprocessed_file_path,
                                                processed_file_name)

    def _storage_path(self,
                      storage_directory_path: GcsfsDirectoryPath,
                      opt_storage_subdir: Optional[str],
                      date_str: str,
                      file_name: str) -> GcsfsFilePath:
        """Returns the storage file path for the input |file_name|,
        |storage_bucket|, and |ingest_date_str|"""
        if opt_storage_subdir is None:
            opt_storage_subdir = ''

        for file_num in range(self._RENAME_RETRIES):
            name, ext = file_name.split('.')
            actual_file_name = \
                file_name if file_num == 0 else f'{name}-({file_num}).{ext}'

            storage_path_str = os.path.join(
                storage_directory_path.bucket_name,
                storage_directory_path.relative_path,
                date_str,
                opt_storage_subdir,
                actual_file_name)
            storage_path = GcsfsFilePath.from_absolute_path(storage_path_str)

            if not self.exists(storage_path):
                return storage_path

            logging.error(
                "Storage path [%s] already exists, attempting rename",
                storage_path.abs_path())

        raise ValueError(
            f'Could not find valid storage path for file {file_name}.')


class DirectIngestGCSFileSystemImpl(DirectIngestGCSFileSystem):
    """An implementation of the DirectIngestGCSFileSystem built on top of a real
    GCSFileSystem.
    """
    def __init__(self, client: storage.Client):
        self.storage_client = client

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

    def download_as_string(self, path: GcsfsFilePath) -> bytes:
        bucket = self.storage_client.get_bucket(path.bucket_name)
        blob = bucket.get_blob(path.blob_name)
        if not blob:
            raise ValueError(f'Blob at path [{path.abs_path()}] does not exist')
        return blob.download_as_string()

    def upload_from_string(self, path: GcsfsFilePath,
                           contents: str,
                           content_type: str):
        bucket = self.storage_client.get_bucket(path.bucket_name)
        bucket.blob(path.blob_name).upload_from_string(
            contents, content_type=content_type)

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

    def _ls_with_blob_prefix(
            self, bucket_name: str, blob_prefix: str) -> List[GcsfsFilePath]:
        blobs = self.storage_client.list_blobs(bucket_name, prefix=blob_prefix)
        return [path
                for path in [GcsfsPath.from_blob(blob) for blob in blobs]
                if isinstance(path, GcsfsFilePath)]
