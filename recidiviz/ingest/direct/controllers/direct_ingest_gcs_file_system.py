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
import tempfile
import uuid
from typing import List, Optional, Union, Iterator, Callable


from google.api_core import retry, exceptions
from google.cloud import storage
from google.cloud.exceptions import NotFound

from recidiviz.ingest.direct.controllers.direct_ingest_types import IngestContentsHandle
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import \
    filename_parts_from_path, GcsfsDirectIngestFileType
from recidiviz.ingest.direct.controllers.gcsfs_path import GcsfsPath, \
    GcsfsFilePath, GcsfsDirectoryPath, GcsfsBucketPath

DIRECT_INGEST_UNPROCESSED_PREFIX = 'unprocessed'
DIRECT_INGEST_PROCESSED_PREFIX = 'processed'
SPLIT_FILE_SUFFIX = 'file_split'
SPLIT_FILE_STORAGE_SUBDIR = 'split_files'


def _build_file_name(
        *,
        utc_iso_timestamp_str: str,
        file_type: GcsfsDirectIngestFileType,
        base_file_name: str,
        extension: str,
        prefix: str) -> str:
    file_name_parts = [
        prefix,
        utc_iso_timestamp_str,
    ]

    if file_type != GcsfsDirectIngestFileType.UNSPECIFIED:
        file_name_parts += [file_type.value]

    file_name_parts += [
        base_file_name
    ]

    return "_".join(file_name_parts) + f".{extension}"


def _build_unprocessed_file_name(
        *,
        utc_iso_timestamp_str: str,
        file_type: GcsfsDirectIngestFileType,
        base_file_name: str,
        extension: str) -> str:
    return _build_file_name(utc_iso_timestamp_str=utc_iso_timestamp_str,
                            file_type=file_type,
                            base_file_name=base_file_name,
                            extension=extension,
                            prefix=DIRECT_INGEST_UNPROCESSED_PREFIX)


def _build_processed_file_name(
        *,
        utc_iso_timestamp_str: str,
        file_type: GcsfsDirectIngestFileType,
        base_file_name: str,
        extension: str) -> str:
    return _build_file_name(utc_iso_timestamp_str=utc_iso_timestamp_str,
                            file_type=file_type,
                            base_file_name=base_file_name,
                            extension=extension,
                            prefix=DIRECT_INGEST_PROCESSED_PREFIX)


def _to_normalized_file_name(
        file_name: str,
        file_type: GcsfsDirectIngestFileType,
        build_function: Callable,
        dt: Optional[datetime.datetime] = None) -> str:
    if not dt:
        dt = datetime.datetime.utcnow()

    utc_iso_timestamp_str = dt.strftime('%Y-%m-%dT%H:%M:%S:%f')
    file_name, extension = file_name.split('.')

    return build_function(utc_iso_timestamp_str=utc_iso_timestamp_str,
                          file_type=file_type,
                          base_file_name=file_name,
                          extension=extension)


def to_normalized_unprocessed_file_name(
        file_name: str,
        file_type: GcsfsDirectIngestFileType,
        dt: Optional[datetime.datetime] = None) -> str:
    return _to_normalized_file_name(
        file_name=file_name,
        file_type=file_type,
        build_function=_build_unprocessed_file_name,
        dt=dt)


def to_normalized_processed_file_name(
        file_name: str,
        file_type: GcsfsDirectIngestFileType,
        dt: Optional[datetime.datetime] = None) -> str:
    return _to_normalized_file_name(
        file_name=file_name,
        file_type=file_type,
        build_function=_build_processed_file_name,
        dt=dt)


def to_normalized_unprocessed_file_path(
        original_file_path: str,
        file_type: GcsfsDirectIngestFileType,
        dt: Optional[datetime.datetime] = None) -> str:
    if not dt:
        dt = datetime.datetime.utcnow()
    directory, file_name = os.path.split(original_file_path)
    updated_relative_path = to_normalized_unprocessed_file_name(file_name=file_name, file_type=file_type, dt=dt)
    return os.path.join(directory, updated_relative_path)


def _to_normalized_file_path_from_normalized_path(
        original_normalized_file_path: str,
        build_function: Callable,
        file_type_override: Optional[GcsfsDirectIngestFileType] = None) -> str:
    """Moves any normalized path back to a unprocessed/processed path with the same information embedded in the file
    name. If |file_type_override| is provided, we will always overwrite the original path file type with the override
    file type."""

    directory, _ = os.path.split(original_normalized_file_path)
    parts = filename_parts_from_path(GcsfsFilePath.from_absolute_path(original_normalized_file_path))

    file_type = file_type_override if file_type_override else parts.file_type

    utc_iso_timestamp_str = parts.utc_upload_datetime.strftime('%Y-%m-%dT%H:%M:%S:%f')

    suffix_str = \
        f'_{parts.filename_suffix}' if parts.filename_suffix else ''
    base_file_name = f'{parts.file_tag}{suffix_str}'

    path_to_return = build_function(utc_iso_timestamp_str=utc_iso_timestamp_str,
                                    file_type=file_type,
                                    base_file_name=base_file_name,
                                    extension=parts.extension)

    return os.path.join(directory, path_to_return)


def to_normalized_unprocessed_file_path_from_normalized_path(
        original_normalized_file_path: str,
        file_type_override: Optional[GcsfsDirectIngestFileType] = None
) -> str:
    return _to_normalized_file_path_from_normalized_path(
        original_normalized_file_path=original_normalized_file_path,
        build_function=_build_unprocessed_file_name,
        file_type_override=file_type_override)


def to_normalized_processed_file_path_from_normalized_path(
        original_normalized_file_path: str,
        file_type_override: Optional[GcsfsDirectIngestFileType] = None
) -> str:
    return _to_normalized_file_path_from_normalized_path(
        original_normalized_file_path=original_normalized_file_path,
        build_function=_build_processed_file_name,
        file_type_override=file_type_override)


class GcsfsFileContentsHandle(IngestContentsHandle[str]):
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

    def __del__(self):
        """This ensures that the file contents on local disk are deleted when
        this handle is garbage collected.
        """
        if os.path.exists(self.local_file_path):
            os.remove(self.local_file_path)


class DirectIngestGCSFileSystem:
    """An abstraction built on top of the GCSFileSystem class with helpers for
    manipulating files and filenames expected by direct ingest.
    """
    _RENAME_RETRIES = 5

    @abc.abstractmethod
    def exists(self, path: Union[GcsfsBucketPath, GcsfsFilePath]) -> bool:
        """Returns True if the object exists in the fs, False otherwise."""

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
                           content_type: str):
        """Uploads string contents to a file path."""

    @abc.abstractmethod
    def upload_from_contents_handle(self,
                                    path: GcsfsFilePath,
                                    contents_handle: GcsfsFileContentsHandle,
                                    content_type: str):
        """Uploads contents in handle to a file path."""

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
        paths = self._ls_with_file_prefix(directory_path, '', file_type_filter=None)

        return [path
                for path in paths if not self.is_normalized_file_path(path)]

    def get_unprocessed_file_paths(
            self,
            directory_path: GcsfsDirectoryPath,
            file_type_filter: Optional[GcsfsDirectIngestFileType]) -> List[GcsfsFilePath]:
        """Returns all paths of a given type in the given directory that have yet to be
        processed. If |file_type_filter| is specified, returns only files with that file type and throws if encountering
        a file with UNSPECIFIED file type.
        """
        return self._ls_with_file_prefix(
            directory_path,
            DIRECT_INGEST_UNPROCESSED_PREFIX,
            file_type_filter)

    def get_unprocessed_file_paths_for_day(
            self,
            directory_path: GcsfsDirectoryPath,
            date_str: str,
            file_type_filter: Optional[GcsfsDirectIngestFileType]) -> List[GcsfsFilePath]:
        """Returns all paths in the given directory that were uploaded on the
        day specified in date_str that have yet to be processed. If |file_type_filter| is specified, returns only files
        with that file type and throws if encountering a file with UNSPECIFIED file type.
        """
        return self._ls_with_file_prefix(
            directory_path,
            f"{DIRECT_INGEST_UNPROCESSED_PREFIX}_{date_str}",
            file_type_filter)

    def get_processed_file_paths(
            self,
            directory_path: GcsfsDirectoryPath,
            file_type_filter: Optional[GcsfsDirectIngestFileType]) -> List[GcsfsFilePath]:
        """Returns all paths in the given directory that have been
        processed. If |file_type_filter| is specified, returns only files with that file type and throws if encountering
        a file with UNSPECIFIED file type.
        """
        return self._ls_with_file_prefix(directory_path,
                                         DIRECT_INGEST_PROCESSED_PREFIX,
                                         file_type_filter)

    def get_processed_file_paths_for_day(self,
                                         directory_path: GcsfsDirectoryPath,
                                         date_str: str,
                                         file_type_filter: Optional[GcsfsDirectIngestFileType]) -> List[GcsfsFilePath]:
        """Returns all paths in the given directory that were uploaded on the
        day specified in date_str that have been processed. If |file_type_filter| is specified, returns only files with
        that file type and throws if encountering a file with UNSPECIFIED file type.
        """
        return self._ls_with_file_prefix(directory_path,
                                         f"{DIRECT_INGEST_PROCESSED_PREFIX}_{date_str}",
                                         file_type_filter)

    def mv_path_to_normalized_path(self,
                                   path: GcsfsFilePath,
                                   file_type: GcsfsDirectIngestFileType,
                                   dt: Optional[datetime.datetime] = None) -> GcsfsFilePath:
        """Renames a file with an unnormalized file name to a file with a normalized file name in the same directory. If
        |dt| is specified, the file will contain that timestamp, otherwise will contain the current timestamp.

        Returns the new normalized path location of this file after the move completes.
        """
        updated_file_path = \
            GcsfsFilePath.from_absolute_path(
                to_normalized_unprocessed_file_path(path.abs_path(), file_type, dt))

        if self.exists(updated_file_path):
            raise ValueError(
                f"Desired path [{updated_file_path.abs_path()}] "
                f"already exists, returning")

        logging.info("Moving [%s] to normalized path [%s].",
                     path.abs_path(), updated_file_path.abs_path())
        self.mv(path, updated_file_path)
        return updated_file_path

    def mv_path_to_processed_path(self, path: GcsfsFilePath) -> GcsfsFilePath:
        """Renames file with an unprocessed file prefix to a path in the same directory with a 'processed' prefix.

        Returns the new processed path location of this file after the move completes.
        """

        processed_path = self._to_processed_file_path(path)
        logging.info("Moving [%s] to processed path [%s].",
                     path.abs_path(), processed_path.abs_path())
        self.mv(path, processed_path)
        return processed_path

    def mv_processed_paths_before_date_to_storage(
            self,
            directory_path: GcsfsDirectoryPath,
            storage_directory_path: GcsfsDirectoryPath,
            file_type_filter: Optional[GcsfsDirectIngestFileType],
            date_str_bound: str,
            include_bound: bool):

        """Moves all files with timestamps before the provided |date_str_bound| to the appropriate storage location for
        that file. If a |file_type_filter| is provided, only moves files of a certain file type and throws if
        encountering a file of type UNSPECIFIED in the directory path.
        """

        processed_file_paths = self.get_processed_file_paths(directory_path, file_type_filter)

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
        """Moves a normalized path to it's appropriate storage location based on the date and file type information
        embedded in the file name."""
        storage_path = self._storage_path(storage_directory_path, path)

        logging.info("Moving [%s] to storage path [%s].",
                     path.abs_path(), storage_path.abs_path())
        self.mv(path, storage_path)

    def _ls_with_file_prefix(self,
                             directory_path: GcsfsDirectoryPath,
                             file_prefix: str,
                             file_type_filter: Optional[GcsfsDirectIngestFileType]) -> List[GcsfsFilePath]:
        """Returns absolute paths of files in the directory with the given |file_prefix|.
        """
        blob_prefix = os.path.join(*[directory_path.relative_path, file_prefix])
        blob_paths = self._ls_with_blob_prefix(directory_path.bucket_name, blob_prefix)

        result = []
        for path in blob_paths:
            if not isinstance(path, GcsfsFilePath):
                continue

            if not file_type_filter:
                result.append(path)
                continue

            file_type = filename_parts_from_path(path).file_type
            if file_type == GcsfsDirectIngestFileType.UNSPECIFIED:
                raise ValueError(f'Found path {path.abs_path()} with unexpected UNSPECIFIED type.')

            if file_type == file_type_filter:
                result.append(path)

        return result

    @abc.abstractmethod
    def _ls_with_blob_prefix(self,
                             bucket_name: str,
                             blob_prefix: str) -> List[Union[GcsfsDirectoryPath, GcsfsFilePath]]:
        """Returns absolute paths of objects in the bucket with the given |relative_path|. """

    @staticmethod
    def _to_processed_file_path(
            unprocessed_file_path: GcsfsFilePath) -> GcsfsFilePath:
        processed_file_name = unprocessed_file_path.file_name.replace(
            DIRECT_INGEST_UNPROCESSED_PREFIX, DIRECT_INGEST_PROCESSED_PREFIX)

        return GcsfsFilePath.with_new_file_name(unprocessed_file_path,
                                                processed_file_name)

    def _storage_path(self,
                      storage_directory_path: GcsfsDirectoryPath,
                      path: GcsfsFilePath) -> GcsfsFilePath:
        """Returns the storage file path for the input |file_name|,
        |storage_bucket|, and |ingest_date_str|"""

        parts = filename_parts_from_path(path)

        if self.is_split_file(path):
            opt_storage_subdir = SPLIT_FILE_STORAGE_SUBDIR
        else:
            opt_storage_subdir = ''

        if parts.file_type is None or parts.file_type == GcsfsDirectIngestFileType.UNSPECIFIED:
            file_type_subidr = ''
            date_subdir = parts.date_str
        else:
            file_type_subidr = parts.file_type.value
            date_subdir = os.path.join(
                f'{parts.utc_upload_datetime.year:04}',
                f'{parts.utc_upload_datetime.month:02}',
                f'{parts.utc_upload_datetime.day:02}'
            )

        for file_num in range(self._RENAME_RETRIES):
            name, ext = path.file_name.split('.')
            actual_file_name = \
                path.file_name if file_num == 0 else f'{name}-({file_num}).{ext}'

            storage_path_str = os.path.join(
                storage_directory_path.bucket_name,
                storage_directory_path.relative_path,
                file_type_subidr,
                date_subdir,
                opt_storage_subdir,
                actual_file_name)
            storage_path = GcsfsFilePath.from_absolute_path(storage_path_str)

            if not self.exists(storage_path):
                return storage_path

            logging.error(
                "Storage path [%s] already exists, attempting rename",
                storage_path.abs_path())

        raise ValueError(
            f'Could not find valid storage path for file {path.file_name}.')

    @classmethod
    def generate_random_temp_path(cls) -> str:
        temp_dir = os.path.join(tempfile.gettempdir(), 'direct_ingest')

        if not os.path.exists(temp_dir):
            os.mkdir(temp_dir)

        return os.path.join(temp_dir, str(uuid.uuid4()))


def retry_predicate(exception: Exception) -> Callable[[Exception], bool]:
    """"A function that will determine whether we should retry a given Google exception."""
    return retry.if_transient_error(exception) or retry.if_exception_type(exceptions.GatewayTimeout)(exception)


class DirectIngestGCSFileSystemImpl(DirectIngestGCSFileSystem):
    """An implementation of the DirectIngestGCSFileSystem built on top of a real
    GCSFileSystem.
    """

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
    def download_to_temp_file(self, path: GcsfsFilePath) -> Optional[GcsfsFileContentsHandle]:
        bucket = self.storage_client.get_bucket(path.bucket_name)
        blob = bucket.get_blob(path.blob_name)
        if not blob:
            logging.info(
                "File path [%s] no longer exists - might have already "
                "been processed or deleted", path.abs_path())
            return None

        temp_file_path = self.generate_random_temp_path()

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
                           content_type: str):
        bucket = self.storage_client.get_bucket(path.bucket_name)
        bucket.blob(path.blob_name).upload_from_string(
            contents, content_type=content_type)

    @retry.Retry(predicate=retry_predicate)
    def upload_from_contents_handle(self,
                                    path: GcsfsFilePath,
                                    contents_handle: GcsfsFileContentsHandle,
                                    content_type: str):
        bucket = self.storage_client.get_bucket(path.bucket_name)
        bucket.blob(path.blob_name).upload_from_filename(
            contents_handle.local_file_path, content_type=content_type)

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
    def _ls_with_blob_prefix(
            self,
            bucket_name: str,
            blob_prefix: str) -> List[Union[GcsfsDirectoryPath, GcsfsFilePath]]:
        blobs = self.storage_client.list_blobs(bucket_name, prefix=blob_prefix)
        return [GcsfsPath.from_blob(blob) for blob in blobs]
