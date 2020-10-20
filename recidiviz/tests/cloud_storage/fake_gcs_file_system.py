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
import os
import shutil
import threading
from typing import Set, Union, Dict, Optional, List

from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem, GcsfsFileContentsHandle, generate_random_temp_path
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import filename_parts_from_path
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath, GcsfsDirectoryPath, GcsfsBucketPath, GcsfsPath
from recidiviz.tests.ingest import fixtures


class FakeGCSFileSystemDelegate:
    @abc.abstractmethod
    def on_file_added(self, path: GcsfsFilePath):
        """Will be called whenever a new file path is successfully added to the file system."""


class FakeGCSFileSystem(GCSFileSystem):
    """Test-only implementation of the GCSFileSystem."""

    def __init__(self):
        self.mutex = threading.Lock()
        self.all_paths: Set[Union[GcsfsFilePath, GcsfsDirectoryPath]] = set()
        self.uploaded_test_path_to_actual: Dict[str, str] = {}
        self.delegate: Optional[FakeGCSFileSystemDelegate] = None

    def test_set_delegate(self, delegate: FakeGCSFileSystemDelegate) -> None:
        self.delegate = delegate

    def test_add_path(self,
                      path: Union[GcsfsFilePath, GcsfsDirectoryPath],
                      fail_handle_file_call=False) -> None:
        if not isinstance(path, (GcsfsFilePath, GcsfsDirectoryPath)):
            raise ValueError(f'Path has unexpected type {type(path)}')
        self._add_path(path, fail_handle_file_call)

    def _add_path(self,
                  path: Union[GcsfsFilePath, GcsfsDirectoryPath],
                  fail_handle_file_call=False) -> None:
        with self.mutex:
            self.all_paths.add(path)

        if not fail_handle_file_call and self.delegate and isinstance(path, GcsfsFilePath):
            self.delegate.on_file_added(path)

    def exists(self, path: Union[GcsfsBucketPath, GcsfsFilePath]) -> bool:
        with self.mutex:
            return path.abs_path() in [p.abs_path() for p in self.all_paths]

    def get_file_size(self, path: GcsfsFilePath) -> Optional[int]:
        raise ValueError('Must be implemented for use in tests.')

    def get_metadata(self, path: GcsfsFilePath) -> Optional[Dict[str, str]]:
        raise ValueError('Must be implemented for use in tests.')

    def real_absolute_path_for_path(self, path: GcsfsFilePath) -> str:
        if path.abs_path() in self.uploaded_test_path_to_actual:
            return self.uploaded_test_path_to_actual[path.abs_path()]

        directory_path, _ = os.path.split(path.abs_path())

        parts = filename_parts_from_path(path)
        suffix = f'_{parts.filename_suffix}' if parts.filename_suffix else ''
        fixture_filename = f'{parts.file_tag}{suffix}.{parts.extension}'

        actual_fixture_file_path = \
            fixtures.file_path_from_relative_path(
                os.path.join(directory_path, fixture_filename))

        tempfile_path = generate_random_temp_path()
        return shutil.copyfile(actual_fixture_file_path, tempfile_path)

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
                           content_type: str):
        # pylint: disable=unused-argument
        temp_path = generate_random_temp_path()
        with open(temp_path, 'w') as f:
            f.write(contents)

        self.uploaded_test_path_to_actual[path.abs_path()] = temp_path
        self._add_path(path)

    def upload_from_contents_handle(self,
                                    path: GcsfsFilePath,
                                    contents_handle: GcsfsFileContentsHandle,
                                    content_type: str):
        # pylint: disable=unused-argument
        temp_path = generate_random_temp_path()
        shutil.copyfile(contents_handle.local_file_path, temp_path)
        self.uploaded_test_path_to_actual[path.abs_path()] = temp_path
        self._add_path(path)

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

        if src_path.abs_path() in self.uploaded_test_path_to_actual:
            self.uploaded_test_path_to_actual[dst_path.abs_path()] = \
                self.uploaded_test_path_to_actual[src_path.abs_path()]

        self._add_path(path)

    def delete(self, path: GcsfsFilePath) -> None:
        with self.mutex:
            path_to_remove = None
            for p in self.all_paths:
                if p == path:
                    path_to_remove = p
                    break

            if path_to_remove is not None:
                self.all_paths.remove(path_to_remove)

    def ls_with_blob_prefix(self,
                            bucket_name: str,
                            blob_prefix: str) -> List[Union[GcsfsDirectoryPath, GcsfsFilePath]]:
        with self.mutex:
            result: List[Union[GcsfsDirectoryPath, GcsfsFilePath]] = []
            for path in self.all_paths:
                if path.bucket_name != bucket_name:
                    continue
                if isinstance(path, GcsfsFilePath) and path.blob_name and path.blob_name.startswith(blob_prefix):
                    result.append(path)
                if isinstance(path, GcsfsDirectoryPath) \
                        and path.relative_path and path.relative_path.startswith(blob_prefix):
                    result.append(path)

            return result
