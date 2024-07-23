# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests for Airflow tasks for the clean up and storage step of the raw data import dag"""
import re
from unittest import TestCase
from unittest.mock import patch

from recidiviz.airflow.dags.raw_data.clean_up_tasks import clean_up_temporary_files
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.fakes.fake_gcs_file_system import (
    FakeGCSFileSystem,
    FakeGCSFileSystemDelegate,
)


class CleanUpTemporaryFilesTest(TestCase):
    """Unit tests for clean_up_temporary_files task"""

    def setUp(self) -> None:
        self.fs = FakeGCSFileSystem()
        self.storage_patch = patch(
            "recidiviz.airflow.dags.raw_data.clean_up_tasks.GcsfsFactory.build",
            return_value=self.fs,
        )
        self.storage_mock = self.storage_patch.start()

    def tearDown(self) -> None:
        self.storage_patch.stop()

    def test_no_files(self) -> None:
        clean_up_temporary_files.function([])

    def test_files_exist(self) -> None:
        paths = [
            GcsfsFilePath.from_absolute_path("temp/test-file-1.txt"),
            GcsfsFilePath.from_absolute_path("temp/test-file-2.csv"),
        ]

        for path in paths:
            self.fs.test_add_path(path, local_path=None)

        assert set(self.fs.all_paths) == set(paths)
        clean_up_temporary_files.function([p.abs_path() for p in paths])
        assert len(self.fs.all_paths) == 0

    def test_files_dont_exist_ok(self) -> None:
        paths = [
            GcsfsFilePath.from_absolute_path("temp/test-file-1.txt"),
            GcsfsFilePath.from_absolute_path("temp/test-file-2.csv"),
        ]

        no_exists = [
            GcsfsFilePath.from_absolute_path("temp/i-no-exist-file-2.csv"),
        ]

        for path in paths:
            self.fs.test_add_path(path, local_path=None)

        assert set(self.fs.all_paths) == set(paths)
        clean_up_temporary_files.function([p.abs_path() for p in [*no_exists, *paths]])
        assert len(self.fs.all_paths) == 0

    def test_failed_all_others_execute(self) -> None:
        paths = [
            GcsfsFilePath.from_absolute_path("temp/test-file-BAD.txt"),
            GcsfsFilePath.from_absolute_path("temp/test-file-1.csv"),
            GcsfsFilePath.from_absolute_path("temp/test-file-2.csv"),
            GcsfsFilePath.from_absolute_path("temp/test-file-3.csv"),
        ]

        for path in paths:
            self.fs.test_add_path(path, local_path=None)

        assert set(self.fs.all_paths) == set(paths)

        class DeleteErrorFakeGCSFileSystemDelegate(FakeGCSFileSystemDelegate):
            def on_file_added(self, path: GcsfsFilePath) -> None:
                """Will be called whenever a new file path is successfully added to the file system."""

            def on_file_delete(self, path: GcsfsFilePath) -> bool:
                """Will be called whenever a new file path is to be deleted from the file system or not."""
                if path == paths[0]:
                    raise ValueError("nope!")

                return True

        self.fs.delegate = DeleteErrorFakeGCSFileSystemDelegate()

        with self.assertRaisesRegex(
            ExceptionGroup,
            re.escape("Errors occurred during path deletion (1 sub-exception)"),
        ):
            clean_up_temporary_files.function([p.abs_path() for p in paths])
        assert self.fs.all_paths == [paths[0]]
