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
"""An wrapper class built on top of the GCSFileSystem class with helpers for
manipulating files and filenames expected by direct ingest.
"""

import datetime
import logging
import os
import tempfile
import uuid
from contextlib import contextmanager
from typing import (
    Callable,
    Dict,
    Generic,
    Iterator,
    List,
    Optional,
    TextIO,
    TypeVar,
    Union,
)

import pytz

from recidiviz.cloud_storage.content_types import (
    FileContentsHandle,
    FileContentsRowType,
    IoType,
)
from recidiviz.cloud_storage.gcs_file_system import (
    GCSFileSystem,
    GcsfsFileContentsHandle,
)
from recidiviz.cloud_storage.gcsfs_path import (
    GcsfsBucketPath,
    GcsfsDirectoryPath,
    GcsfsFilePath,
    GcsfsPath,
)
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    GcsfsDirectIngestFileType,
    filename_parts_from_path,
)

DIRECT_INGEST_UNPROCESSED_PREFIX = "unprocessed"
DIRECT_INGEST_PROCESSED_PREFIX = "processed"
SPLIT_FILE_SUFFIX = "file_split"
SPLIT_FILE_STORAGE_SUBDIR = "split_files"


def _build_file_name(
    *,
    utc_iso_timestamp_str: str,
    file_type: GcsfsDirectIngestFileType,
    base_file_name: str,
    extension: str,
    prefix: str,
) -> str:
    file_name_parts = [prefix, utc_iso_timestamp_str, file_type.value, base_file_name]

    return "_".join(file_name_parts) + f".{extension}"


def _build_unprocessed_file_name(
    *,
    utc_iso_timestamp_str: str,
    file_type: GcsfsDirectIngestFileType,
    base_file_name: str,
    extension: str,
) -> str:
    return _build_file_name(
        utc_iso_timestamp_str=utc_iso_timestamp_str,
        file_type=file_type,
        base_file_name=base_file_name,
        extension=extension,
        prefix=DIRECT_INGEST_UNPROCESSED_PREFIX,
    )


def _build_processed_file_name(
    *,
    utc_iso_timestamp_str: str,
    file_type: GcsfsDirectIngestFileType,
    base_file_name: str,
    extension: str,
) -> str:
    return _build_file_name(
        utc_iso_timestamp_str=utc_iso_timestamp_str,
        file_type=file_type,
        base_file_name=base_file_name,
        extension=extension,
        prefix=DIRECT_INGEST_PROCESSED_PREFIX,
    )


def _to_normalized_file_name(
    file_name: str,
    file_type: GcsfsDirectIngestFileType,
    build_function: Callable,
    dt: Optional[datetime.datetime] = None,
) -> str:
    if not dt:
        dt = datetime.datetime.now(tz=pytz.UTC)

    utc_iso_timestamp_str = dt.strftime("%Y-%m-%dT%H:%M:%S:%f")
    file_name, extension = file_name.split(".")

    return build_function(
        utc_iso_timestamp_str=utc_iso_timestamp_str,
        file_type=file_type,
        base_file_name=file_name,
        extension=extension,
    )


def to_normalized_unprocessed_file_name(
    file_name: str,
    file_type: GcsfsDirectIngestFileType,
    dt: Optional[datetime.datetime] = None,
) -> str:
    return _to_normalized_file_name(
        file_name=file_name,
        file_type=file_type,
        build_function=_build_unprocessed_file_name,
        dt=dt,
    )


def to_normalized_processed_file_name(
    file_name: str,
    file_type: GcsfsDirectIngestFileType,
    dt: Optional[datetime.datetime] = None,
) -> str:
    return _to_normalized_file_name(
        file_name=file_name,
        file_type=file_type,
        build_function=_build_processed_file_name,
        dt=dt,
    )


def to_normalized_unprocessed_file_path(
    original_file_path: str,
    file_type: GcsfsDirectIngestFileType,
    dt: Optional[datetime.datetime] = None,
) -> str:
    if not dt:
        dt = datetime.datetime.now(tz=pytz.UTC)
    directory, file_name = os.path.split(original_file_path)
    updated_relative_path = to_normalized_unprocessed_file_name(
        file_name=file_name, file_type=file_type, dt=dt
    )
    return os.path.join(directory, updated_relative_path)


def _to_normalized_file_path_from_normalized_path(
    original_normalized_file_path: str,
    build_function: Callable,
    file_type_override: Optional[GcsfsDirectIngestFileType] = None,
) -> str:
    """Moves any normalized path back to a unprocessed/processed path with the same information embedded in the file
    name. If |file_type_override| is provided, we will always overwrite the original path file type with the override
    file type."""

    directory, _ = os.path.split(original_normalized_file_path)
    parts = filename_parts_from_path(
        GcsfsFilePath.from_absolute_path(original_normalized_file_path)
    )

    file_type = file_type_override if file_type_override else parts.file_type

    utc_iso_timestamp_str = parts.utc_upload_datetime.strftime("%Y-%m-%dT%H:%M:%S:%f")

    suffix_str = f"_{parts.filename_suffix}" if parts.filename_suffix else ""
    base_file_name = f"{parts.file_tag}{suffix_str}"

    path_to_return = build_function(
        utc_iso_timestamp_str=utc_iso_timestamp_str,
        file_type=file_type,
        base_file_name=base_file_name,
        extension=parts.extension,
    )

    return os.path.join(directory, path_to_return)


def to_normalized_unprocessed_file_path_from_normalized_path(
    original_normalized_file_path: str,
    file_type_override: Optional[GcsfsDirectIngestFileType] = None,
) -> str:
    return _to_normalized_file_path_from_normalized_path(
        original_normalized_file_path=original_normalized_file_path,
        build_function=_build_unprocessed_file_name,
        file_type_override=file_type_override,
    )


def to_normalized_processed_file_path_from_normalized_path(
    original_normalized_file_path: str,
) -> str:
    return _to_normalized_file_path_from_normalized_path(
        original_normalized_file_path=original_normalized_file_path,
        build_function=_build_processed_file_name,
    )


GCSFileSystemType = TypeVar("GCSFileSystemType", bound=GCSFileSystem)


class DirectIngestGCSFileSystem(Generic[GCSFileSystemType], GCSFileSystem):
    """A wrapper built on top of the GCSFileSystem class with helpers for manipulating files and file names
    expected by direct ingest.
    """

    def __init__(self, gcs_file_system: GCSFileSystemType):
        self.gcs_file_system = gcs_file_system

    def exists(self, path: Union[GcsfsBucketPath, GcsfsFilePath]) -> bool:
        return self.gcs_file_system.exists(path)

    def get_file_size(self, path: GcsfsFilePath) -> Optional[int]:
        return self.gcs_file_system.get_file_size(path)

    def get_metadata(self, path: GcsfsFilePath) -> Optional[Dict]:
        return self.gcs_file_system.get_metadata(path)

    def mv(self, src_path: GcsfsFilePath, dst_path: GcsfsPath) -> None:
        self.gcs_file_system.mv(src_path, dst_path)

    def copy(self, src_path: GcsfsFilePath, dst_path: GcsfsPath) -> None:
        self.gcs_file_system.copy(src_path, dst_path)

    def delete(self, path: GcsfsFilePath) -> None:
        self.gcs_file_system.delete(path)

    def download_to_temp_file(
        self, path: GcsfsFilePath
    ) -> Optional[GcsfsFileContentsHandle]:
        return self.gcs_file_system.download_to_temp_file(path)

    def download_as_string(self, path: GcsfsFilePath, encoding: str = "utf-8") -> str:
        return self.gcs_file_system.download_as_string(path, encoding)

    def upload_from_string(
        self, path: GcsfsFilePath, contents: str, content_type: str
    ) -> None:
        return self.gcs_file_system.upload_from_string(path, contents, content_type)

    def upload_from_contents_handle_stream(
        self,
        path: GcsfsFilePath,
        contents_handle: FileContentsHandle[FileContentsRowType, IoType],
        content_type: str,
    ) -> None:
        return self.gcs_file_system.upload_from_contents_handle_stream(
            path, contents_handle, content_type
        )

    def ls_with_blob_prefix(
        self, bucket_name: str, blob_prefix: str
    ) -> List[Union[GcsfsDirectoryPath, GcsfsFilePath]]:
        return self.gcs_file_system.ls_with_blob_prefix(bucket_name, blob_prefix)

    def set_content_type(self, path: GcsfsFilePath, content_type: str) -> None:
        return self.gcs_file_system.set_content_type(path, content_type)

    def is_dir(self, path: str) -> bool:
        return self.gcs_file_system.is_dir(path)

    def is_file(self, path: str) -> bool:
        return self.gcs_file_system.is_file(path)

    @contextmanager
    def open(
        self,
        path: GcsfsFilePath,
        chunk_size: Optional[int] = None,
        encoding: Optional[str] = None,
    ) -> Iterator[TextIO]:
        with self.gcs_file_system.open(path, chunk_size, encoding) as f:
            yield f

    @staticmethod
    def is_processed_file(path: GcsfsFilePath) -> bool:
        return path.file_name.startswith(DIRECT_INGEST_PROCESSED_PREFIX)

    @staticmethod
    def is_seen_unprocessed_file(path: GcsfsFilePath) -> bool:
        return path.file_name.startswith(DIRECT_INGEST_UNPROCESSED_PREFIX)

    @staticmethod
    def is_split_file(path: GcsfsFilePath) -> bool:
        return filename_parts_from_path(path).is_file_split

    @staticmethod
    def is_normalized_file_path(path: GcsfsFilePath) -> bool:
        return DirectIngestGCSFileSystem.is_seen_unprocessed_file(
            path
        ) or DirectIngestGCSFileSystem.is_processed_file(path)

    def get_unnormalized_file_paths(
        self, directory_path: GcsfsDirectoryPath
    ) -> List[GcsfsFilePath]:
        """Returns all paths in the given directory without normalized paths."""
        paths = self._ls_with_file_prefix(directory_path, "", file_type_filter=None)

        return [path for path in paths if not self.is_normalized_file_path(path)]

    def get_unprocessed_file_paths(
        self,
        directory_path: GcsfsDirectoryPath,
        file_type_filter: Optional[GcsfsDirectIngestFileType],
    ) -> List[GcsfsFilePath]:
        """Returns all paths of a given type in the given directory that have yet to be
        processed. If |file_type_filter| is specified, returns only files with that file type.
        """
        return self._ls_with_file_prefix(
            directory_path, DIRECT_INGEST_UNPROCESSED_PREFIX, file_type_filter
        )

    def get_unprocessed_file_paths_for_day(
        self,
        directory_path: GcsfsDirectoryPath,
        date_str: str,
        file_type_filter: Optional[GcsfsDirectIngestFileType],
    ) -> List[GcsfsFilePath]:
        """Returns all paths in the given directory that were uploaded on the
        day specified in date_str that have yet to be processed. If |file_type_filter| is specified, returns only files
        with that file type and throws if encountering a file with UNSPECIFIED file type.
        """
        return self._ls_with_file_prefix(
            directory_path,
            f"{DIRECT_INGEST_UNPROCESSED_PREFIX}_{date_str}",
            file_type_filter,
        )

    def get_processed_file_paths(
        self,
        directory_path: GcsfsDirectoryPath,
        file_type_filter: Optional[GcsfsDirectIngestFileType],
    ) -> List[GcsfsFilePath]:
        """Returns all paths in the given directory that have been
        processed. If |file_type_filter| is specified, returns only files with that file type and throws if encountering
        a file with UNSPECIFIED file type.
        """
        return self._ls_with_file_prefix(
            directory_path, DIRECT_INGEST_PROCESSED_PREFIX, file_type_filter
        )

    def get_processed_file_paths_for_day(
        self,
        directory_path: GcsfsDirectoryPath,
        date_str: str,
        file_type_filter: Optional[GcsfsDirectIngestFileType],
    ) -> List[GcsfsFilePath]:
        """Returns all paths in the given directory that were uploaded on the
        day specified in date_str that have been processed. If |file_type_filter| is specified, returns only files with
        that file type and throws if encountering a file with UNSPECIFIED file type.
        """
        return self._ls_with_file_prefix(
            directory_path,
            f"{DIRECT_INGEST_PROCESSED_PREFIX}_{date_str}",
            file_type_filter,
        )

    def mv_path_to_normalized_path(
        self,
        path: GcsfsFilePath,
        file_type: GcsfsDirectIngestFileType,
        dt: Optional[datetime.datetime] = None,
    ) -> GcsfsFilePath:
        """Renames a file with an unnormalized file name to a file with a normalized file name in the same directory. If
        |dt| is specified, the file will contain that timestamp, otherwise will contain the current timestamp.

        Returns the new normalized path location of this file after the move completes.
        """
        updated_file_path = GcsfsFilePath.from_absolute_path(
            to_normalized_unprocessed_file_path(path.abs_path(), file_type, dt)
        )

        if self.exists(updated_file_path):
            raise ValueError(
                f"Desired path [{updated_file_path.abs_path()}] "
                f"already exists, returning"
            )

        logging.info(
            "Moving [%s] to normalized path [%s].",
            path.abs_path(),
            updated_file_path.abs_path(),
        )
        self.mv(path, updated_file_path)
        return updated_file_path

    def mv_path_to_processed_path(self, path: GcsfsFilePath) -> GcsfsFilePath:
        """Renames file with an unprocessed file prefix to a path in the same directory with a 'processed' prefix.

        Returns the new processed path location of this file after the move completes.
        """

        processed_path = self._to_processed_file_path(path)
        logging.info(
            "Moving [%s] to processed path [%s].",
            path.abs_path(),
            processed_path.abs_path(),
        )
        self.mv(path, processed_path)
        return processed_path

    def mv_processed_paths_before_date_to_storage(
        self,
        directory_path: GcsfsDirectoryPath,
        storage_directory_path: GcsfsDirectoryPath,
        file_type_filter: Optional[GcsfsDirectIngestFileType],
        date_str_bound: str,
        include_bound: bool,
    ) -> None:
        """Moves all files with timestamps before the provided |date_str_bound| to the appropriate storage location for
        that file. If a |file_type_filter| is provided, only moves files of a certain file type and throws if
        encountering a file of type UNSPECIFIED in the directory path.
        """

        processed_file_paths = self.get_processed_file_paths(
            directory_path, file_type_filter
        )

        for file_path in processed_file_paths:
            date_str = filename_parts_from_path(file_path).date_str
            if date_str < date_str_bound or (
                include_bound and date_str == date_str_bound
            ):
                logging.info(
                    "Found file [%s] from [%s] which abides by provided bound "
                    "[%s]. Moving to storage.",
                    file_path.abs_path(),
                    date_str,
                    date_str_bound,
                )
                self.mv_path_to_storage(file_path, storage_directory_path)

    def mv_path_to_storage(
        self, path: GcsfsFilePath, storage_directory_path: GcsfsDirectoryPath
    ) -> None:
        """Moves a normalized path to it's appropriate storage location based on the date and file type information
        embedded in the file name."""
        storage_path = self._storage_path(storage_directory_path, path)

        logging.info(
            "Moving [%s] to storage path [%s].",
            path.abs_path(),
            storage_path.abs_path(),
        )
        self.mv(path, storage_path)

    def _ls_with_file_prefix(
        self,
        directory_path: GcsfsDirectoryPath,
        file_prefix: str,
        file_type_filter: Optional[GcsfsDirectIngestFileType],
    ) -> List[GcsfsFilePath]:
        """Returns absolute paths of files in the directory with the given |file_prefix|."""
        blob_prefix = os.path.join(*[directory_path.relative_path, file_prefix])
        blob_paths = self.gcs_file_system.ls_with_blob_prefix(
            directory_path.bucket_name, blob_prefix
        )

        result = []
        for path in blob_paths:
            if not isinstance(path, GcsfsFilePath):
                continue

            if not file_type_filter:
                result.append(path)
                continue

            file_type = filename_parts_from_path(path).file_type

            if file_type == file_type_filter:
                result.append(path)

        return result

    @staticmethod
    def _to_processed_file_path(unprocessed_file_path: GcsfsFilePath) -> GcsfsFilePath:
        processed_file_name = unprocessed_file_path.file_name.replace(
            DIRECT_INGEST_UNPROCESSED_PREFIX, DIRECT_INGEST_PROCESSED_PREFIX
        )

        return GcsfsFilePath.with_new_file_name(
            unprocessed_file_path, processed_file_name
        )

    def _storage_path(
        self, storage_directory_path: GcsfsDirectoryPath, path: GcsfsFilePath
    ) -> GcsfsFilePath:
        """Returns the storage file path for the input |file_name|,
        |storage_bucket|, and |ingest_date_str|"""

        parts = filename_parts_from_path(path)

        if self.is_split_file(path):
            opt_storage_subdir = SPLIT_FILE_STORAGE_SUBDIR
        else:
            opt_storage_subdir = ""

        file_type_subdir = parts.file_type.value
        date_subdir = os.path.join(
            f"{parts.utc_upload_datetime.year:04}",
            f"{parts.utc_upload_datetime.month:02}",
            f"{parts.utc_upload_datetime.day:02}",
        )

        for file_num in range(self._RENAME_RETRIES):
            name, ext = path.file_name.split(".")
            actual_file_name = (
                path.file_name if file_num == 0 else f"{name}-({file_num}).{ext}"
            )

            storage_path_str = os.path.join(
                storage_directory_path.bucket_name,
                storage_directory_path.relative_path,
                file_type_subdir,
                date_subdir,
                opt_storage_subdir,
                actual_file_name,
            )
            storage_path = GcsfsFilePath.from_absolute_path(storage_path_str)

            if not self.exists(storage_path):
                return storage_path

            logging.error(
                "Storage path [%s] already exists, attempting rename",
                storage_path.abs_path(),
            )

        raise ValueError(
            f"Could not find valid storage path for file {path.file_name}."
        )

    @classmethod
    def generate_random_temp_path(cls) -> str:
        temp_dir = os.path.join(tempfile.gettempdir(), "direct_ingest")

        if not os.path.exists(temp_dir):
            os.mkdir(temp_dir)

        return os.path.join(temp_dir, str(uuid.uuid4()))
