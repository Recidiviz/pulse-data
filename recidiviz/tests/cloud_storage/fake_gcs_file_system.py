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
import shutil
import threading
from typing import Union, Dict, Optional, List, Set

import attr

from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem, GcsfsFileContentsHandle, generate_random_temp_path
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath, GcsfsDirectoryPath, GcsfsBucketPath, GcsfsPath


class FakeGCSFileSystemDelegate:
    @abc.abstractmethod
    def on_file_added(self, path: GcsfsFilePath):
        """Will be called whenever a new file path is successfully added to the file system."""

@attr.s(frozen=True)
class FakeGCSFileSystemEntry:
    gcs_path: Union[GcsfsFilePath, GcsfsDirectoryPath] = attr.ib()

    # Path to file in local file system. If it is None, still pretend the file exists in the file system but any
    # attempt to access it will fail.
    local_path: Optional[str] = attr.ib()

class FakeGCSFileSystem(GCSFileSystem):
    """Test-only implementation of the GCSFileSystem."""

    def __init__(self):
        self.mutex = threading.Lock()
        # Maps the absolute GCS path to an entry, which gives us the schematized path and where the file is in the
        # local filesystem. E.g. {
        #   'my-bucket/temp.csv': (GcsfsFilePath(bucket_name='my-bucket', blob_name='temp.csv'), '/tmp/1234/temp.csv'),
        # }
        self.files: Dict[str, FakeGCSFileSystemEntry] = {}
        self.delegate: Optional[FakeGCSFileSystemDelegate] = None

        # Only for convenience so that it is kept around after any temporarily uploaded files are deleted
        self.uploaded_paths: Set[Union[GcsfsFilePath, GcsfsDirectoryPath]] = set()

    @property
    def all_paths(self):
        return [entry.gcs_path for _abs_gcs_path, entry in self.files.items()]

    def test_set_delegate(self, delegate: FakeGCSFileSystemDelegate) -> None:
        self.delegate = delegate

    def test_add_path(self,
                      path: Union[GcsfsFilePath, GcsfsDirectoryPath],
                      local_path: Optional[str],
                      fail_handle_file_call=False) -> None:
        if not isinstance(path, (GcsfsFilePath, GcsfsDirectoryPath)):
            raise ValueError(f'Path has unexpected type {type(path)}')

        # Copies from local_path, if not provided.
        if local_path:
            copied_path = generate_random_temp_path()
            shutil.copyfile(local_path, copied_path)
            local_path = copied_path

        self._add_entry(FakeGCSFileSystemEntry(path, local_path), fail_handle_file_call)

    def _add_entry(self, entry: FakeGCSFileSystemEntry, fail_handle_file_call=False) -> None:
        with self.mutex:
            self.files[entry.gcs_path.abs_path()] = entry

        if not fail_handle_file_call and self.delegate and isinstance(entry.gcs_path, GcsfsFilePath):
            self.delegate.on_file_added(entry.gcs_path)

    def exists(self, path: Union[GcsfsBucketPath, GcsfsFilePath]) -> bool:
        with self.mutex:
            return path.abs_path() in self.files

    def get_file_size(self, path: GcsfsFilePath) -> Optional[int]:
        raise ValueError('Must be implemented for use in tests.')

    def get_metadata(self, path: GcsfsFilePath) -> Optional[Dict[str, str]]:
        raise ValueError('Must be implemented for use in tests.')

    def real_absolute_path_for_path(self, path: GcsfsFilePath) -> str:
        with self.mutex:
            entry = self.files.get(path.abs_path())
            if not entry:
                raise FileNotFoundError(f"File not found in FakeGCS, add it using 'test_add_path': {path}")
            if not entry.local_path:
                raise FileNotFoundError(
                    f"No real path backing this file, supply a 'local_path' to 'test_add_path': {entry}")
            return entry.local_path

    def download_to_temp_file(self, path: GcsfsFilePath) -> Optional[GcsfsFileContentsHandle]:
        """Downloads file contents into local temporary_file, returning path to
        temp file, or None if the path no-longer exists in the GCS file system.
        """
        if not self.exists(path):
            return None

        return GcsfsFileContentsHandle(self.real_absolute_path_for_path(path))

    def upload_from_string(self,
                           path: GcsfsFilePath,
                           contents: str,
                           _content_type: str):
        temp_path = generate_random_temp_path()
        with open(temp_path, 'w') as f:
            f.write(contents)

        self._add_entry(FakeGCSFileSystemEntry(path, temp_path))
        self.uploaded_paths.add(path)

    def upload_from_contents_handle(self,
                                    path: GcsfsFilePath,
                                    contents_handle: GcsfsFileContentsHandle,
                                    _content_type: str):
        temp_path = generate_random_temp_path()
        shutil.copyfile(contents_handle.local_file_path, temp_path)
        self._add_entry(FakeGCSFileSystemEntry(path, temp_path))
        self.uploaded_paths.add(path)

    def copy(self,
             src_path: GcsfsFilePath,
             dst_path: GcsfsPath) -> None:
        if isinstance(dst_path, GcsfsFilePath):
            path = dst_path
        elif isinstance(dst_path, GcsfsDirectoryPath):
            path = \
                GcsfsFilePath.from_directory_and_file_name(dst_path,
                                                           src_path.file_name)
        else:
            raise ValueError(f'Unexpected path type [{type(dst_path)}]')

        with self.mutex:
            entry = self.files[src_path.abs_path()]
            self.files[path.abs_path()] = FakeGCSFileSystemEntry(path, entry.local_path)

        if self.delegate:
            self.delegate.on_file_added(path)

    def delete(self, path: GcsfsFilePath) -> None:
        with self.mutex:
            self.files.pop(path.abs_path())

    def ls_with_blob_prefix(self,
                            bucket_name: str,
                            blob_prefix: str) -> List[Union[GcsfsDirectoryPath, GcsfsFilePath]]:
        prefix = GcsfsFilePath.from_bucket_and_blob_name(bucket_name, blob_prefix)
        with self.mutex:
            results: List[Union[GcsfsDirectoryPath, GcsfsFilePath]] = []
            for abs_path, entry in self.files.items():
                if abs_path.startswith(prefix.abs_path()):
                    results.append(entry.gcs_path)

            return results
