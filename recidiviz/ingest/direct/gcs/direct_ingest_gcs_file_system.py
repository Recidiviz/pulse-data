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
from enum import Enum
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

from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_path import (
    GcsfsBucketPath,
    GcsfsDirectoryPath,
    GcsfsFilePath,
    GcsfsPath,
)
from recidiviz.common.io.file_contents_handle import FileContentsHandle
from recidiviz.common.io.local_file_contents_handle import LocalFileContentsHandle
from recidiviz.ingest.direct.gcs.filename_parts import filename_parts_from_path
from recidiviz.ingest.direct.types.errors import DirectIngestError

DIRECT_INGEST_UNPROCESSED_PREFIX = "unprocessed"
DIRECT_INGEST_PROCESSED_PREFIX = "processed"


def _build_raw_file_name(
    *,
    utc_iso_timestamp_str: str,
    base_file_name: str,
    extension: str,
    prefix: str,
) -> str:
    file_name_parts = [prefix, utc_iso_timestamp_str, "raw", base_file_name]

    return "_".join(file_name_parts) + f".{extension}"


def _build_unprocessed_raw_file_name(
    *,
    utc_iso_timestamp_str: str,
    base_file_name: str,
    extension: str,
) -> str:
    return _build_raw_file_name(
        utc_iso_timestamp_str=utc_iso_timestamp_str,
        base_file_name=base_file_name,
        extension=extension,
        prefix=DIRECT_INGEST_UNPROCESSED_PREFIX,
    )


def _build_processed_raw_file_name(
    *,
    utc_iso_timestamp_str: str,
    base_file_name: str,
    extension: str,
) -> str:
    return _build_raw_file_name(
        utc_iso_timestamp_str=utc_iso_timestamp_str,
        base_file_name=base_file_name,
        extension=extension,
        prefix=DIRECT_INGEST_PROCESSED_PREFIX,
    )


def _to_normalized_raw_file_name(
    file_name: str,
    build_function: Callable,
    dt: Optional[datetime.datetime] = None,
) -> str:
    if not dt:
        dt = datetime.datetime.now(tz=pytz.UTC)

    utc_iso_timestamp_str = dt.strftime("%Y-%m-%dT%H:%M:%S:%f")
    file_name, extension = file_name.split(".")

    return build_function(
        utc_iso_timestamp_str=utc_iso_timestamp_str,
        base_file_name=file_name,
        extension=extension,
    )


def to_normalized_unprocessed_raw_file_name(
    file_name: str,
    dt: Optional[datetime.datetime] = None,
) -> str:
    return _to_normalized_raw_file_name(
        file_name=file_name,
        build_function=_build_unprocessed_raw_file_name,
        dt=dt,
    )


def to_normalized_processed_raw_file_name(
    file_name: str, dt: Optional[datetime.datetime] = None
) -> str:
    return _to_normalized_raw_file_name(
        file_name=file_name,
        build_function=_build_processed_raw_file_name,
        dt=dt,
    )


def to_normalized_unprocessed_raw_file_path(
    original_file_path: str, dt: Optional[datetime.datetime] = None
) -> str:
    if not dt:
        dt = datetime.datetime.now(tz=pytz.UTC)
    directory, file_name = os.path.split(original_file_path)
    updated_relative_path = to_normalized_unprocessed_raw_file_name(
        file_name=file_name, dt=dt
    )
    return os.path.join(directory, updated_relative_path)


def _to_normalized_file_path_from_normalized_path(
    original_normalized_file_path: str, build_function: Callable
) -> str:
    """Moves any normalized path back to a unprocessed/processed path with the same
    information embedded in the file name.
    """

    directory, _ = os.path.split(original_normalized_file_path)
    parts = filename_parts_from_path(
        GcsfsFilePath.from_absolute_path(original_normalized_file_path)
    )

    utc_iso_timestamp_str = parts.utc_upload_datetime.strftime("%Y-%m-%dT%H:%M:%S:%f")

    suffix_str = f"-{parts.filename_suffix}" if parts.filename_suffix else ""
    base_file_name = f"{parts.file_tag}{suffix_str}"

    path_to_return = build_function(
        utc_iso_timestamp_str=utc_iso_timestamp_str,
        base_file_name=base_file_name,
        extension=parts.extension,
    )

    return os.path.join(directory, path_to_return)


def to_normalized_unprocessed_file_path_from_normalized_path(
    original_normalized_file_path: str,
) -> str:
    return _to_normalized_file_path_from_normalized_path(
        original_normalized_file_path=original_normalized_file_path,
        build_function=_build_unprocessed_raw_file_name,
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

    def clear_metadata(self, path: GcsfsFilePath) -> None:
        return self.gcs_file_system.clear_metadata(path)

    def update_metadata(
        self,
        path: GcsfsFilePath,
        new_metadata: Dict[str, str],
    ) -> None:
        self.gcs_file_system.update_metadata(
            path,
            new_metadata,
        )

    def mv(self, src_path: GcsfsFilePath, dst_path: GcsfsPath) -> None:
        self.gcs_file_system.mv(src_path, dst_path)

    def copy(self, src_path: GcsfsFilePath, dst_path: GcsfsPath) -> None:
        self.gcs_file_system.copy(src_path, dst_path)

    def delete(self, path: GcsfsFilePath) -> None:
        self.gcs_file_system.delete(path)

    def download_to_temp_file(
        self, path: GcsfsFilePath, retain_original_filename: bool = False
    ) -> Optional[LocalFileContentsHandle]:
        return self.gcs_file_system.download_to_temp_file(
            path, retain_original_filename
        )

    def download_as_string(self, path: GcsfsFilePath, encoding: str = "utf-8") -> str:
        return self.gcs_file_system.download_as_string(path, encoding)

    def download_as_bytes(self, path: GcsfsFilePath) -> bytes:
        return self.gcs_file_system.download_as_bytes(path)

    def upload_from_string(
        self, path: GcsfsFilePath, contents: str, content_type: str
    ) -> None:
        return self.gcs_file_system.upload_from_string(path, contents, content_type)

    def upload_from_contents_handle_stream(
        self,
        path: GcsfsFilePath,
        contents_handle: FileContentsHandle,
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

    def rename_blob(self, path: GcsfsFilePath, new_path: GcsfsFilePath) -> None:
        return self.gcs_file_system.rename_blob(path, new_path)

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
    def is_normalized_file_path(path: GcsfsFilePath) -> bool:
        return DirectIngestGCSFileSystem.is_seen_unprocessed_file(
            path
        ) or DirectIngestGCSFileSystem.is_processed_file(path)

    def get_unnormalized_file_paths(
        self, directory_path: GcsfsDirectoryPath
    ) -> List[GcsfsFilePath]:
        """Returns all paths in the given directory without normalized paths."""
        paths = self._ls_with_file_prefix(
            directory_path, "", filter_type=self._FilterType.UNNORMALIZED_ONLY
        )

        return [path for path in paths if not self.is_normalized_file_path(path)]

    def get_unprocessed_raw_file_paths(
        self, directory_path: GcsfsDirectoryPath
    ) -> List[GcsfsFilePath]:
        """Returns all paths of a given type in the given directory that have yet to be
        processed.
        """
        return self._ls_with_file_prefix(
            directory_path,
            DIRECT_INGEST_UNPROCESSED_PREFIX,
            filter_type=self._FilterType.NORMALIZED_ONLY,
        )

    def get_processed_file_paths(
        self, directory_path: GcsfsDirectoryPath
    ) -> List[GcsfsFilePath]:
        """Returns all paths in the given directory that have been processed."""
        return self._ls_with_file_prefix(
            directory_path,
            DIRECT_INGEST_PROCESSED_PREFIX,
            filter_type=self._FilterType.NORMALIZED_ONLY,
        )

    def mv_raw_file_to_normalized_path(
        self, path: GcsfsFilePath, dt: Optional[datetime.datetime] = None
    ) -> GcsfsFilePath:
        """Renames a raw data file with an unnormalized file name to a file with a
        normalized file name in the same directory. If |dt| is specified, the file will
        contain that timestamp, otherwise will contain the current timestamp.

        Returns the new normalized path location of this file after the move completes.
        """
        updated_file_path = GcsfsFilePath.from_absolute_path(
            to_normalized_unprocessed_raw_file_path(path.abs_path(), dt)
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

    def mv_raw_file_to_storage(
        self, path: GcsfsFilePath, storage_directory_path: GcsfsDirectoryPath
    ) -> None:
        """Moves a normalized path to it's appropriate storage location based on the date and file type information
        embedded in the file name."""
        storage_path = self._raw_file_storage_path(storage_directory_path, path)

        logging.info(
            "Moving [%s] to storage path [%s].",
            path.abs_path(),
            storage_path.abs_path(),
        )
        self.mv(path, storage_path)

    class _FilterType(Enum):
        NO_FILTER = "NO_FILTER"
        UNNORMALIZED_ONLY = "UNNORMALIZED_ONLY"
        NORMALIZED_ONLY = "NORMALIZED_ONLY"

    def _ls_with_file_prefix(
        self,
        directory_path: GcsfsDirectoryPath,
        file_prefix: str,
        filter_type: _FilterType,
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

            try:
                _ = filename_parts_from_path(path)
                is_normalized = True
            except DirectIngestError:
                is_normalized = False

            if (
                is_normalized and filter_type != self._FilterType.UNNORMALIZED_ONLY
            ) or (
                not is_normalized and filter_type != self._FilterType.NORMALIZED_ONLY
            ):
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

    def _raw_file_storage_path(
        self, storage_directory_path: GcsfsDirectoryPath, path: GcsfsFilePath
    ) -> GcsfsFilePath:
        """Returns the storage file path for the input |file_name|, |storage_bucket|,
        and |ingest_date_str|.
        """

        parts = filename_parts_from_path(path)

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
                "raw",
                date_subdir,
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
