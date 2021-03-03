# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests for upload_state_files_to_ingest_bucket_with_date.py"""
import datetime
import unittest
from unittest.mock import patch, Mock

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath, GcsfsDirectoryPath
from recidiviz.ingest.direct.controllers.upload_state_files_to_ingest_bucket_with_date import (
    UploadStateFilesToIngestBucketController,
)
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem


TODAY = datetime.datetime.today()


@patch("recidiviz.ingest.direct.direct_ingest_control.GcsfsFactory.build")
class TestUploadStateFilesToIngestBucketController(unittest.TestCase):
    """Tests for UploadStateFilesToIngestBucketController."""

    def setUp(self) -> None:
        self.project_id = "test-project"
        self.region = "us_xx"

    def test_do_upload_succeeds(self, mock_fs_factory: Mock) -> None:
        mock_fs = FakeGCSFileSystem()
        mock_fs.test_add_path(
            path=GcsfsFilePath.from_bucket_and_blob_name(
                "test-project-direct-ingest-state-us-xx", "raw_data/test_file.txt"
            ),
            local_path=None,
        )
        mock_fs_factory.return_value = mock_fs
        controller = UploadStateFilesToIngestBucketController(
            paths_with_timestamps=[
                ("test-project-direct-ingest-state-us-xx/raw_data/test_file.txt", TODAY)
            ],
            project_id="test-project",
            region="us_xx",
        )
        expected_result = [
            "test-project-direct-ingest-state-us-xx/raw_data/test_file.txt"
        ]
        uploaded_files, unable_to_upload_files = controller.do_upload()
        self.assertEqual(uploaded_files, expected_result)
        self.assertEqual(len(unable_to_upload_files), 0)

    def test_do_upload_graceful_failures(self, mock_fs_factory: Mock) -> None:
        mock_fs = FakeGCSFileSystem()
        mock_fs.test_add_path(
            path=GcsfsFilePath.from_bucket_and_blob_name(
                "test-project-direct-ingest-state-us-xx", "raw_data/test_file.txt"
            ),
            local_path=None,
        )
        mock_fs_factory.return_value = mock_fs
        controller = UploadStateFilesToIngestBucketController(
            paths_with_timestamps=[
                (
                    "test-project-direct-ingest-state-us-xx/raw_data/test_file.txt",
                    TODAY,
                ),
                (
                    "test-project-direct-ingest-state-us-xx/raw_data/non_existent_file.txt",
                    TODAY,
                ),
            ],
            project_id="test-project",
            region="us_xx",
        )
        uploaded_files, unable_to_upload_files = controller.do_upload()
        self.assertEqual(
            uploaded_files,
            ["test-project-direct-ingest-state-us-xx/raw_data/test_file.txt"],
        )
        self.assertEqual(
            unable_to_upload_files,
            ["test-project-direct-ingest-state-us-xx/raw_data/non_existent_file.txt"],
        )

    def test_do_upload_sets_correct_content_type(self, mock_fs_factory: Mock) -> None:
        mock_fs = FakeGCSFileSystem()
        mock_fs.test_add_path(
            path=GcsfsFilePath.from_bucket_and_blob_name(
                "test-project-direct-ingest-state-us-xx", "raw_data/test_file.txt"
            ),
            local_path=None,
        )
        mock_fs.test_add_path(
            path=GcsfsFilePath.from_bucket_and_blob_name(
                "test-project-direct-ingest-state-us-xx", "raw_data/test_file.csv"
            ),
            local_path=None,
        )
        mock_fs_factory.return_value = mock_fs
        controller = UploadStateFilesToIngestBucketController(
            paths_with_timestamps=[
                (
                    "test-project-direct-ingest-state-us-xx/raw_data/test_file.txt",
                    TODAY,
                ),
                (
                    "test-project-direct-ingest-state-us-xx/raw_data/test_file.csv",
                    TODAY,
                ),
            ],
            project_id="test-project",
            region="us_xx",
        )
        uploaded_files, _ = controller.do_upload()
        self.assertListEqual(
            uploaded_files,
            [
                "test-project-direct-ingest-state-us-xx/raw_data/test_file.txt",
                "test-project-direct-ingest-state-us-xx/raw_data/test_file.csv",
            ],
        )
        resulting_content_types = [file.content_type for file in mock_fs.files.values()]
        self.assertListEqual(resulting_content_types, ["text/plain", "text/csv"])

    def test_get_paths_to_upload_is_correct(self, mock_fs_factory: Mock) -> None:
        mock_fs = FakeGCSFileSystem()
        mock_fs.test_add_path(
            path=GcsfsFilePath.from_bucket_and_blob_name(
                "test-project-direct-ingest-state-us-xx", "raw_data/test_file.txt"
            ),
            local_path=None,
        )
        mock_fs.test_add_path(
            path=GcsfsFilePath.from_bucket_and_blob_name(
                "test-project-direct-ingest-state-us-xx",
                "raw_data/subdir1/test_file.txt",
            ),
            local_path=None,
        )
        mock_fs.test_add_path(
            path=GcsfsDirectoryPath.from_bucket_and_blob_name(
                "test-project-direct-ingest-state-us-xx", "raw_data/subdir2/"
            ),
            local_path=None,
        )
        mock_fs_factory.return_value = mock_fs
        controller = UploadStateFilesToIngestBucketController(
            paths_with_timestamps=[
                ("test-project-direct-ingest-state-us-xx/raw_data/", TODAY),
            ],
            project_id="test-project",
            region="us_xx",
        )
        result = [
            ("test-project-direct-ingest-state-us-xx/raw_data/test_file.txt", TODAY),
            (
                "test-project-direct-ingest-state-us-xx/raw_data/subdir1/test_file.txt",
                TODAY,
            ),
        ]
        self.assertListEqual(result, controller.get_paths_to_upload())
