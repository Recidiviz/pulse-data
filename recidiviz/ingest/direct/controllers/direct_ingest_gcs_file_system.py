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

from recidiviz.ingest.direct.controllers.gcsfs_path import GcsfsPath, \
    GcsfsFilePath, GcsfsDirectoryPath, GcsfsBucketPath

DIRECT_INGEST_UNPROCESSED_PREFIX = 'unprocessed'
DIRECT_INGEST_PROCESSED_PREFIX = 'processed'


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

    @abc.abstractmethod
    def exists(self, path: Union[GcsfsBucketPath, GcsfsFilePath]) -> bool:
        """Returns True if the object exists in the fs, False otherwise."""

    @abc.abstractmethod
    def download_as_string(self, path: GcsfsFilePath) -> bytes:
        pass

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
    def have_seen_file_path(path: GcsfsFilePath) -> bool:
        return path.file_name.startswith(DIRECT_INGEST_UNPROCESSED_PREFIX) or \
            path.file_name.startswith(DIRECT_INGEST_PROCESSED_PREFIX)

    def normalize_file_path_if_necessary(self, path: GcsfsFilePath):
        if self.have_seen_file_path(path):
            logging.info(
                "Not normalizing file path for already seen file %s",
                path.abs_path())
            return

        updated_file_path = \
            GcsfsFilePath.from_absolute_path(
                to_normalized_unprocessed_file_path(path.abs_path()))

        if self.exists(updated_file_path):
            logging.error("Desired path [%s] already exists, returning",
                          updated_file_path.abs_path())
            return

        logging.info("Moving file from %s to %s",
                     path.abs_path(), updated_file_path.abs_path())
        self.mv(path, updated_file_path)

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

    def mv_path_to_processed_path(self, path: GcsfsFilePath):
        self.mv(path, self._to_processed_file_path(path))

    def mv_paths_from_date_to_storage(
            self,
            directory_path: GcsfsDirectoryPath,
            date_str: str,
            storage_directory_path: GcsfsDirectoryPath):
        file_paths = self.get_processed_file_paths_for_day(
            directory_path, date_str)

        for file_path in file_paths:
            stripped_path = self._strip_processed_file_name_prefix(file_path)
            storage_path = self._storage_path(
                storage_directory_path,
                date_str,
                stripped_path.file_name)
            self.mv(file_path, storage_path)

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

    @staticmethod
    def _strip_processed_file_name_prefix(
            processed_file_path: GcsfsFilePath) -> GcsfsFilePath:
        stripped_file_name = \
            processed_file_path.file_name.replace(
                f'{DIRECT_INGEST_PROCESSED_PREFIX}_', '')
        return GcsfsFilePath.with_new_file_name(processed_file_path,
                                                stripped_file_name)

    def _storage_path(self,
                      storage_directory_path: GcsfsDirectoryPath,
                      date_str: str,
                      file_name: str) -> GcsfsFilePath:
        """Returns the storage file path for the input |file_name|,
        |storage_bucket|, and |ingest_date_str|"""

        storage_path = os.path.join(storage_directory_path.bucket_name,
                                    storage_directory_path.relative_path,
                                    date_str,
                                    file_name)
        path = GcsfsFilePath.from_absolute_path(storage_path)

        # TODO(1628): We should not fail the whole task on a failure to move
        #  to storage - let's just flexibly rename and fire an error.
        if self.exists(path):
            raise ValueError(f'Storage path [{storage_path}] already exists, '
                             f'not moving file to storage.')

        return path


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
            return blob.exists(self.storage_client)

        raise ValueError(f'Unexpected path type [{type(path)}]')

    def download_as_string(self, path: GcsfsFilePath) -> bytes:
        bucket = self.storage_client.get_bucket(path.bucket_name)
        blob = bucket.get_blob(path.blob_name)
        return blob.download_as_string()

    def copy(self,
             src_path: GcsfsFilePath,
             dst_path: GcsfsPath) -> None:
        src_bucket = self.storage_client.get_bucket(src_path.bucket_name)
        src_blob = src_bucket.blob(src_path.blob_name)
        src_bucket = \
            self.storage_client.get_bucket(dst_path.bucket_name)

        if isinstance(dst_path, GcsfsFilePath):
            dst_blob_name = dst_path.blob_name
        elif isinstance(dst_path, GcsfsDirectoryPath):
            dst_blob_name = \
                GcsfsFilePath.from_directory_and_file_name(
                    dst_path, src_path.file_name)
        else:
            raise ValueError(f'Unexpected path type [{type(dst_path)}]')

        src_bucket.copy_blob(
            src_blob, src_bucket, dst_blob_name)

    def delete(self, path: GcsfsFilePath) -> None:
        if not isinstance(path, GcsfsFilePath):
            raise ValueError(f'Unexpected path type [{type(path)}]')

        source_bucket = self.storage_client.get_bucket(path.bucket_name)
        source_blob = source_bucket.blob(path.blob_name)
        source_blob.delete(self.storage_client)

    def _ls_with_blob_prefix(
            self, bucket_name: str, blob_prefix: str) -> List[GcsfsFilePath]:
        return [GcsfsFilePath.from_blob(blob)
                for blob in self.storage_client.list_blobs(
                    bucket_name, prefix=blob_prefix)]
