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
import os
from typing import List

from gcsfs import GCSFileSystem

from recidiviz.cloud_functions.cloud_function_utils import \
    DIRECT_INGEST_UNPROCESSED_PREFIX, DIRECT_INGEST_PROCESSED_PREFIX


class DirectIngestGCSFileSystem:
    """An abstraction built on top of the GCSFileSystem class with helpers for
    manipulating files and filenames expected by direct ingest.
    """

    @abc.abstractmethod
    def exists(self, path: str) -> bool:
        """Returns True if the path exists in the fs, False otherwise."""

    @abc.abstractmethod
    def open(self, path: str):
        """Returns a TextIOWrapper for reading the file at the given path."""

    @abc.abstractmethod
    def mv(self, path1: str, path2: str) -> None:
        """Moves file at path1 to path2."""

    def get_unprocessed_file_paths(self, directory_path: str):
        """Returns all paths in the given directory that have yet to be
        processed.
        """
        return self._ls_with_file_prefix(
            directory_path,
            DIRECT_INGEST_UNPROCESSED_PREFIX)

    def get_unprocessed_file_paths_for_day(self,
                                           directory_path: str,
                                           date_str: str) -> List[str]:
        """Returns all paths in the given directory that were uploaded on the
        day specified in date_str that have yet to be processed.
        """
        return self._ls_with_file_prefix(
            directory_path,
            f"{DIRECT_INGEST_UNPROCESSED_PREFIX}_{date_str}")

    def get_processed_file_paths(self,
                                 directory_path: str) -> List[str]:
        """Returns all paths in the given directory that have been
        processed.
        """
        return self._ls_with_file_prefix(directory_path,
                                         DIRECT_INGEST_PROCESSED_PREFIX)

    def get_processed_file_paths_for_day(self,
                                         directory_path: str,
                                         date_str: str) -> List[str]:
        """Returns all paths in the given directory that were uploaded on the
        day specified in date_str that have been processed.
        """
        return self._ls_with_file_prefix(
            directory_path,
            f"{DIRECT_INGEST_PROCESSED_PREFIX}_{date_str}")

    def mv_path_to_processed_path(self, path: str):
        self.mv(path, self._to_processed_file_path(path))

    def mv_paths_from_date_to_storage(self,
                                      directory_path: str,
                                      date_str: str,
                                      storage_directory_path: str):
        file_paths = self.get_processed_file_paths_for_day(
            directory_path, date_str)

        for file_path in file_paths:
            stripped_path = self._strip_processed_file_name_prefix(file_path)
            file_name = os.path.split(stripped_path)[1]
            storage_path = self._storage_path(
                storage_directory_path, date_str, file_name)
            self.mv(file_path, storage_path)

    @abc.abstractmethod
    def _ls_with_file_prefix(self,
                             directory_path: str,
                             file_prefix: str) -> List[str]:
        """Returns absolute paths of files in the directory with the given
        |file_prefix|.
        """

    @staticmethod
    def _to_processed_file_path(unprocessed_file_path: str):
        directory, unprocessed_name = os.path.split(unprocessed_file_path)
        processed_file_name = unprocessed_name.replace(
            DIRECT_INGEST_UNPROCESSED_PREFIX, DIRECT_INGEST_PROCESSED_PREFIX)

        return os.path.join(directory, processed_file_name)

    @staticmethod
    def _strip_processed_file_name_prefix(processed_file_path: str) -> str:
        directory, processed_name = os.path.split(processed_file_path)
        processed_file_name = \
            processed_name.replace(f'{DIRECT_INGEST_PROCESSED_PREFIX}_', '')
        return os.path.join(directory, processed_file_name)

    def _storage_path(self,
                      storage_directory_path: str,
                      date_str: str,
                      file_name: str) -> str:
        """Returns the storage file path for the input |file_name|,
        |storage_bucket|, and |ingest_date_str|"""

        storage_path = os.path.join(storage_directory_path,
                                    date_str,
                                    file_name)

        # TODO(1628): We should not fail the whole task on a failure to move
        #  to storage - let's just flexibly rename and fire an error.
        if self.exists(storage_path):
            raise ValueError(f'Storage path [{storage_path}] already exists, '
                             f'not moving file to storage.')

        return storage_path


class DirectIngestGCSFileSystemImpl(DirectIngestGCSFileSystem):
    """An implementation of the DirectIngestGCSFileSystem built on top of a real
    GCSFileSystem.
    """

    def __init__(self, fs: GCSFileSystem):
        self.fs = fs

    def exists(self, path: str) -> bool:
        return self.fs.exists(path)

    def open(self, path: str):
        return self.fs.open(path)

    def mv(self, path1: str, path2: str) -> None:
        self.fs.mv(path1, path2)

    def _ls_with_file_prefix(self,
                             directory_path: str,
                             file_prefix: str) -> List[str]:
        path = os.path.join(directory_path, file_prefix)
        file_paths = self.fs.ls(path=path)
        return file_paths
