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

from recidiviz.airflow.dags.raw_data.clean_up_tasks import (
    move_successfully_imported_paths_to_storage,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_direct_ingest_bucket_for_state,
    gcsfs_direct_ingest_storage_directory_path_for_state,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tests.cloud_storage.fake_gcs_file_system import (
    FakeGCSFileSystem,
    FakeGCSFileSystemDelegate,
)


class RenameAndMoveFilesTest(TestCase):
    """Unit tests for move_successfully_imported_paths_to_storage task"""

    def setUp(self) -> None:
        self.metadata_patcher = patch(
            "recidiviz.utils.metadata.project_id", return_value="recidiviz-fake"
        )
        self.metadata_patcher.start()
        self.fs = FakeGCSFileSystem()
        self.storage_patch = patch(
            "recidiviz.airflow.dags.raw_data.clean_up_tasks.GcsfsFactory.build",
            return_value=self.fs,
        )
        self.storage_mock = self.storage_patch.start()
        self.ingest_bucket = gcsfs_direct_ingest_bucket_for_state(
            region_code="US_XX", ingest_instance=DirectIngestInstance.PRIMARY
        )
        self.storage_path = gcsfs_direct_ingest_storage_directory_path_for_state(
            region_code="US_XX", ingest_instance=DirectIngestInstance.PRIMARY
        )

    def tearDown(self) -> None:
        self.storage_patch.stop()

    def test_no_files(self) -> None:
        move_successfully_imported_paths_to_storage.function(
            "US_XX", DirectIngestInstance.PRIMARY, []
        )

    def test_files_exist(self) -> None:
        paths = [
            GcsfsFilePath.from_directory_and_file_name(
                self.ingest_bucket,
                "unprocessed_2024-01-25T16:35:33:617135_raw_test_file_tag.csv",
            ),
            GcsfsFilePath.from_directory_and_file_name(
                self.ingest_bucket,
                "unprocessed_2024-01-26T16:35:33:617135_raw_test_file_tag_two.csv",
            ),
        ]

        for path in paths:
            self.fs.test_add_path(path, local_path=None)

        assert set(self.fs.all_paths) == set(paths)
        move_successfully_imported_paths_to_storage.function(
            "US_XX", DirectIngestInstance.PRIMARY, [p.abs_path() for p in paths]
        )
        assert set(self.fs.all_paths) == {
            GcsfsFilePath.from_absolute_path(
                f"{self.storage_path.abs_path()}raw/2024/01/25/processed_2024-01-25T16:35:33:617135_raw_test_file_tag.csv",
            ),
            GcsfsFilePath.from_absolute_path(
                f"{self.storage_path.abs_path()}raw/2024/01/26/processed_2024-01-26T16:35:33:617135_raw_test_file_tag_two.csv",
            ),
        }

    def test_files_fail(self) -> None:
        paths = [
            GcsfsFilePath.from_directory_and_file_name(
                self.ingest_bucket,
                "unprocessed_2024-01-25T16:35:33:617135_raw_test_file_tag.csv",
            ),
            GcsfsFilePath.from_directory_and_file_name(
                self.ingest_bucket,
                "unprocessed_2024-01-26T16:35:33:617135_raw_test_file_tag_two.csv",
            ),
            GcsfsFilePath.from_directory_and_file_name(
                self.ingest_bucket,
                "unprocessed_2024-01-27T16:35:33:617135_raw_test_file_tag_two.csv",
            ),
            GcsfsFilePath.from_directory_and_file_name(
                self.ingest_bucket,
                "unprocessed_2024-01-28T16:35:33:617135_raw_test_file_tag_FAIL.csv",
            ),
            GcsfsFilePath.from_directory_and_file_name(
                self.ingest_bucket,
                "unprocessed_2024-01-29T16:35:33:617135_raw_test_file_tag_two.csv",
            ),
        ]

        class FileAddErrorFakeGCSFileSystemDelegate(FakeGCSFileSystemDelegate):
            def on_file_added(self, path: GcsfsFilePath) -> None:
                """Will be called whenever a new file path is successfully added to the file system."""
                path_str = path.abs_path()
                if "FAIL" in path_str and "storage" in path_str:
                    raise ValueError("nope sorry!")

            def on_file_delete(self, path: GcsfsFilePath) -> bool:
                """Will be called whenever a new file path is to be deleted from the file system or not."""
                return True

        self.fs.delegate = FileAddErrorFakeGCSFileSystemDelegate()

        for path in paths:
            self.fs.test_add_path(path, local_path=None)

        assert set(self.fs.all_paths) == set(paths)
        with self.assertRaisesRegex(
            ExceptionGroup,
            re.escape(
                "Errors occurred moving files to their processed paths in storage (1 sub-exception)"
            ),
        ):
            move_successfully_imported_paths_to_storage.function(
                "US_XX", DirectIngestInstance.PRIMARY, [p.abs_path() for p in paths]
            )
        assert set(self.fs.all_paths) == {
            GcsfsFilePath.from_absolute_path(
                f"{self.storage_path.abs_path()}raw/2024/01/25/processed_2024-01-25T16:35:33:617135_raw_test_file_tag.csv",
            ),
            GcsfsFilePath.from_absolute_path(
                f"{self.storage_path.abs_path()}raw/2024/01/26/processed_2024-01-26T16:35:33:617135_raw_test_file_tag_two.csv",
            ),
            GcsfsFilePath.from_absolute_path(
                f"{self.storage_path.abs_path()}raw/2024/01/27/processed_2024-01-27T16:35:33:617135_raw_test_file_tag_two.csv",
            ),
            GcsfsFilePath.from_absolute_path(
                f"{self.storage_path.abs_path()}raw/2024/01/28/processed_2024-01-28T16:35:33:617135_raw_test_file_tag_FAIL.csv",
            ),
            GcsfsFilePath.from_absolute_path(
                f"{self.storage_path.abs_path()}raw/2024/01/29/processed_2024-01-29T16:35:33:617135_raw_test_file_tag_two.csv",
            ),
            # because of where we failed, we are in a weird state of both having it in
            # the storage bucket as well as in the ingest bucket
            GcsfsFilePath.from_directory_and_file_name(
                self.ingest_bucket,
                "unprocessed_2024-01-28T16:35:33:617135_raw_test_file_tag_FAIL.csv",
            ),
        }
