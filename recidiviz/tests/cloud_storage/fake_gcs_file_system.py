# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Test-only implementation of the GCSFileSystem"""

import abc
import base64
import os
import shutil
import threading
from collections import defaultdict
from contextlib import contextmanager
from fnmatch import fnmatch
from typing import IO, Any, Dict, Iterator, List, Optional, Set, Union

import attr
import google_crc32c

from recidiviz.cloud_storage.gcs_file_system import (
    GCSBlobDoesNotExistError,
    GCSFileSystem,
)
from recidiviz.cloud_storage.gcs_file_system_impl import (
    generate_random_temp_path,
    unzip,
)
from recidiviz.cloud_storage.gcsfs_path import (
    GcsfsBucketPath,
    GcsfsDirectoryPath,
    GcsfsFilePath,
    GcsfsPath,
)
from recidiviz.common.io.file_contents_handle import FileContentsHandle
from recidiviz.common.io.flask_file_storage_contents_handle import (
    FlaskFileStorageContentsHandle,
)
from recidiviz.common.io.local_file_contents_handle import LocalFileContentsHandle
from recidiviz.common.io.sftp_file_contents_handle import SftpFileContentsHandle
from recidiviz.common.io.zip_file_contents_handle import ZipFileContentsHandle


class FakeGCSFileSystemDelegate:
    @abc.abstractmethod
    def on_file_added(self, path: GcsfsFilePath) -> None:
        """Will be called whenever a new file path is successfully added to the file system."""

    @abc.abstractmethod
    def on_file_delete(self, path: GcsfsFilePath) -> bool:
        """Will be called whenever a new file path is to be deleted from the file system or not."""
        return True


@attr.s(frozen=True)
class FakeGCSFileSystemEntry:
    gcs_path: Union[GcsfsFilePath, GcsfsDirectoryPath] = attr.ib()

    # Path to file in local file system. If it is None, still pretend the file exists in the file system but any
    # attempt to access it will fail.
    local_path: Optional[str] = attr.ib()
    # Content type for the file entry
    content_type: Optional[str] = attr.ib()


class FakeGCSFileSystem(GCSFileSystem):
    """Test-only implementation of the GCSFileSystem."""

    def __init__(self) -> None:
        self.mutex = threading.Lock()
        # Maps the absolute GCS path to an entry, which gives us the schematized path and where the file is in the
        # local filesystem. E.g. {
        #   'my-bucket/temp.csv': (GcsfsFilePath(bucket_name='my-bucket', blob_name='temp.csv'), '/tmp/1234/temp.csv'),
        # }
        self.files: Dict[str, FakeGCSFileSystemEntry] = {}
        self.delegate: Optional[FakeGCSFileSystemDelegate] = None

        self.metadata_store: Dict[str, Dict[str, Any]] = defaultdict(dict)

        # Only for convenience so that it is kept around after any temporarily uploaded files are deleted
        self.uploaded_paths: Set[Union[GcsfsFilePath, GcsfsDirectoryPath]] = set()

    @property
    def all_paths(self) -> List[Union[GcsfsFilePath, GcsfsDirectoryPath]]:
        return [entry.gcs_path for _abs_gcs_path, entry in self.files.items()]

    def test_set_delegate(self, delegate: FakeGCSFileSystemDelegate) -> None:
        self.delegate = delegate

    def test_add_path(
        self,
        path: Union[GcsfsFilePath, GcsfsDirectoryPath],
        local_path: Optional[str],
        fail_handle_file_call: bool = False,
    ) -> None:
        if not isinstance(path, (GcsfsFilePath, GcsfsDirectoryPath)):
            raise ValueError(f"Path has unexpected type {type(path)}")

        # Copies from local_path, if not provided.
        if local_path:
            copied_path = generate_random_temp_path()
            shutil.copyfile(local_path, copied_path)
            local_path = copied_path

        self._add_entry(
            FakeGCSFileSystemEntry(path, local_path, "application/octet-stream"),
            fail_handle_file_call,
        )

    def _add_entry(
        self, entry: FakeGCSFileSystemEntry, fail_handle_file_call: bool = False
    ) -> None:
        with self.mutex:
            self.files[entry.gcs_path.abs_path()] = entry

        if (
            not fail_handle_file_call
            and self.delegate
            and isinstance(entry.gcs_path, GcsfsFilePath)
        ):
            self.delegate.on_file_added(entry.gcs_path)

    def exists(self, path: Union[GcsfsBucketPath, GcsfsFilePath]) -> bool:
        with self.mutex:
            return path.abs_path() in self.files

    def get_crc32c(self, path: GcsfsFilePath) -> str:
        with self.open(path=path, mode="rb") as f:
            return base64.b64encode(google_crc32c.Checksum(f.read()).digest()).decode(
                "utf-8"
            )

    def get_file_size(self, path: GcsfsFilePath) -> Optional[int]:
        if not self.exists(path):
            return None
        return os.path.getsize(self.real_absolute_path_for_path(path))

    def get_metadata(self, path: GcsfsFilePath) -> Optional[Dict[str, str]]:
        path_key = path.abs_path()
        return self.metadata_store[path_key]

    def clear_metadata(self, path: GcsfsFilePath) -> None:
        path_key = path.abs_path()
        self.metadata_store[path_key] = {}

    def update_metadata(
        self,
        path: GcsfsFilePath,
        new_metadata: Dict[str, str],
    ) -> None:
        path_key = path.abs_path()
        new_values = {k: str(v) for k, v in new_metadata.items()}
        self.metadata_store[path_key].update(new_values)

    def real_absolute_path_for_path(self, path: GcsfsFilePath) -> str:
        with self.mutex:
            entry = self.files.get(path.abs_path())
            if not entry:
                raise FileNotFoundError(
                    f"File not found in FakeGCS, add it using 'test_add_path': {path}"
                )
            if not entry.local_path:
                raise FileNotFoundError(
                    f"No real path backing this file, supply a 'local_path' to 'test_add_path': {entry}"
                )
            return entry.local_path

    def download_to_temp_file(
        self, path: GcsfsFilePath, retain_original_filename: bool = False
    ) -> Optional[LocalFileContentsHandle]:
        """Downloads file contents into local temporary_file, returning path to
        temp file, or None if the path no-longer exists in the GCS file system.
        """
        if not self.exists(path):
            return None

        return LocalFileContentsHandle(self.real_absolute_path_for_path(path))

    def download_as_string(self, path: GcsfsFilePath, encoding: str = "utf-8") -> str:
        """Downloads file contents into memory, returning the contents as a string,
        or raising if the path no-longer exists in the GCS file system"""
        if not self.exists(path):
            raise GCSBlobDoesNotExistError(f"Could not find blob at {path}")

        with open(self.real_absolute_path_for_path(path), "r", encoding=encoding) as f:
            return f.read()

    def download_as_bytes(self, path: GcsfsFilePath) -> bytes:
        """Downloads file contents into memory, returning the contents as bytes
        or raising if the path no-longer exists in the GCS file system"""
        if not self.exists(path):
            raise GCSBlobDoesNotExistError(f"Could not find blob at {path}")

        with open(self.real_absolute_path_for_path(path), "rb") as f:
            return f.read()

    def upload_from_string(
        self, path: GcsfsFilePath, contents: str, content_type: str
    ) -> None:
        temp_path = generate_random_temp_path()
        with open(temp_path, "w", encoding="utf-8") as f:
            f.write(contents)

        self._add_entry(FakeGCSFileSystemEntry(path, temp_path, content_type))
        self.uploaded_paths.add(path)

    def upload_from_contents_handle_stream(
        self,
        path: GcsfsFilePath,
        contents_handle: FileContentsHandle,
        content_type: str,
        timeout: int = 60,
        metadata: Optional[Dict[str, str]] = None,
    ) -> None:
        temp_path = generate_random_temp_path()
        if isinstance(contents_handle, FlaskFileStorageContentsHandle):
            contents_path = contents_handle.file_storage.filename or ""
        elif isinstance(contents_handle, LocalFileContentsHandle):
            contents_path = contents_handle.local_file_path
        elif isinstance(contents_handle, ZipFileContentsHandle):
            contents_path = None
            with contents_handle.open() as zip_contents:
                with open(temp_path, mode="wb") as temp_file:
                    temp_file.write(zip_contents.read())
        elif isinstance(contents_handle, SftpFileContentsHandle):
            contents_path = contents_handle.sftp_file_path or ""
        else:
            raise ValueError(
                f"Unsupported contents handle type: [{type(contents_handle)}]"
            )
        if contents_path and os.path.exists(contents_path):
            shutil.copyfile(contents_path, temp_path)
        self._add_entry(FakeGCSFileSystemEntry(path, temp_path, content_type))
        self.uploaded_paths.add(path)
        if metadata:
            self.metadata_store[path.abs_path()] = metadata

    def mv_file_to_directory_safe(
        self, src_path: GcsfsFilePath, dst_directory: GcsfsDirectoryPath
    ) -> GcsfsFilePath:
        dst_path = GcsfsFilePath.from_directory_and_file_name(
            dst_directory, src_path.file_name
        )

        if self.exists(dst_path):
            raise ValueError(
                f"Destination path [{dst_path.abs_path()}] already exists, returning"
            )

        self.mv(src_path, dst_path)
        return dst_path

    def copy(self, src_path: GcsfsFilePath, dst_path: GcsfsPath) -> None:
        if isinstance(dst_path, GcsfsFilePath):
            path = dst_path
        elif isinstance(dst_path, GcsfsDirectoryPath):
            path = GcsfsFilePath.from_directory_and_file_name(
                dst_path, src_path.file_name
            )
        else:
            raise ValueError(f"Unexpected path type [{type(dst_path)}]")

        with self.mutex:
            entry = self.files[src_path.abs_path()]
            self.files[path.abs_path()] = FakeGCSFileSystemEntry(
                path, entry.local_path, "application/octet-stream"
            )

        if self.delegate:
            self.delegate.on_file_added(path)

    def delete(self, path: GcsfsFilePath) -> None:
        with self.mutex:
            if not self.delegate or self.delegate.on_file_delete(path):
                try:
                    self.files.pop(path.abs_path())
                except KeyError:
                    # mimics behavior of catching GCSBlobDoesNotExistError and not
                    # re-raising
                    pass

    def mv(self, src_path: GcsfsFilePath, dst_path: GcsfsPath) -> None:
        self.copy(src_path, dst_path)
        self.delete(src_path)

    def ls(
        self,
        bucket_name: str,
        *,
        blob_prefix: str | None = None,
        match_glob: str | None = None,
    ) -> List[Union[GcsfsDirectoryPath, GcsfsFilePath]]:
        if blob_prefix and match_glob:
            raise ValueError(
                "Can only specify at most one of blob_prefix and match_glob"
            )

        prefix = GcsfsPath.from_bucket_and_blob_name(bucket_name, blob_prefix or "")
        with self.mutex:
            results: List[Union[GcsfsDirectoryPath, GcsfsFilePath]] = []
            for abs_path, entry in self.files.items():
                if match_glob:
                    if fnmatch(
                        entry.gcs_path.abs_path()
                        .lstrip(entry.gcs_path.bucket_name)
                        .lstrip("/"),
                        match_glob,
                    ):
                        results.append(entry.gcs_path)
                elif abs_path.startswith(prefix.abs_path()):
                    results.append(entry.gcs_path)

            return results

    def list_directories(self, path: GcsfsDirectoryPath) -> List[GcsfsDirectoryPath]:
        with self.mutex:
            results: set[GcsfsDirectoryPath] = set()
            for abs_path, entry in self.files.items():
                # if it's a directory, make sure it ends with an '/'
                abs_path = (
                    abs_path
                    if isinstance(entry, GcsfsFilePath)
                    else f'{abs_path.rstrip("/")}/'
                )
                if abs_path.startswith(path.abs_path()):
                    relative_subpath = os.path.relpath(abs_path, path.abs_path())
                    top_directory = os.path.dirname(relative_subpath).split("/")[0]
                    if len(top_directory):
                        results.add(
                            GcsfsDirectoryPath.from_dir_and_subdir(path, top_directory)
                        )
            return list(results)

    def set_content_type(self, path: GcsfsFilePath, content_type: str) -> None:
        with self.mutex:
            entry = self.files[path.abs_path()]
            self.files[path.abs_path()] = FakeGCSFileSystemEntry(
                path, entry.local_path, content_type
            )

    def is_dir(self, path: str) -> bool:
        try:
            directory = GcsfsDirectoryPath.from_absolute_path(path)
            has_dir = self.ls(
                bucket_name=directory.bucket_name, blob_prefix=directory.relative_path
            )
            return len(has_dir) > 0
        except ValueError:
            return False

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
        mode: str = "r",
        chunk_size: Optional[int] = None,
        encoding: Optional[str] = None,
        verifiable: bool = True,
    ) -> Iterator[IO]:
        if not self.exists(path):
            raise GCSBlobDoesNotExistError(f"Could not find blob at {path}")

        with open(
            self.real_absolute_path_for_path(path), mode=mode, encoding=encoding
        ) as f:
            yield f

    def rename_blob(self, path: GcsfsFilePath, new_path: GcsfsFilePath) -> None:
        return None

    def unzip(
        self, zip_file_path: GcsfsFilePath, destination_dir: GcsfsDirectoryPath
    ) -> List[GcsfsFilePath]:
        return unzip(self, zip_file_path, destination_dir)
