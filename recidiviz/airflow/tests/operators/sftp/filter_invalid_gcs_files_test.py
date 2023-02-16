# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests for the FilterInvalidGcsFilesOperator"""
import os
import unittest
from typing import Dict, List, Union

from recidiviz.airflow.dags.operators.sftp.filter_invalid_gcs_files import (
    FilterInvalidGcsFilesOperator,
)
from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem


# pylint: disable=abstract-method
class FakeGcsFileSystem(GCSFileSystem):
    def is_dir(self, path: str) -> bool:
        return path.endswith("/") or "." not in os.path.basename(path)

    def is_file(self, path: str) -> bool:
        return "." in os.path.basename(path)


class TestFilterInvalidGcsFileOperator(unittest.TestCase):
    """Tests for the FilterInvalidGcsFilesOperator."""

    def setUp(self) -> None:
        self.operator = FilterInvalidGcsFilesOperator(
            task_id="test_task", collect_all_post_processed_files_task_id="test_task_id"
        )

    def test_execute(self) -> None:
        sample_data: List[Dict[str, Union[str, int]]] = [
            {
                "remote_file_path": "file1.csv",
                "sftp_timestamp": 1,
                "downloaded_file_path": "gs://test-bucket/2023-01-01:00:00:00.000000/file1.csv",
                "post_processed_file_path": "gs://test-bucket/2023-01-01:00:00:00.000000/file1.csv",
            },
            {
                "remote_file_path": "file2.txt",
                "sftp_timestamp": 1,
                "downloaded_file_path": "gs://test-bucket/2023-01-01:00:00:00.000000/file2.txt",
                "post_processed_file_path": "gs://test-bucket/2023-01-01:00:00:00.000000/file2.txt",
            },
            {
                "remote_file_path": "invalid.xls",
                "sftp_timestamp": 1,
                "downloaded_file_path": "gs://test-bucket/2023-01-01:00:00:00.000000/invalid.xls",
                "post_processed_file_path": "gs://test-bucket/2023-01-01:00:00:00.000000/invalid.xls",
            },
        ]
        expected_data: List[Dict[str, Union[str, int]]] = [
            {
                "remote_file_path": "file1.csv",
                "sftp_timestamp": 1,
                "downloaded_file_path": "gs://test-bucket/2023-01-01:00:00:00.000000/file1.csv",
                "post_processed_file_path": "gs://test-bucket/2023-01-01:00:00:00.000000/file1.csv",
                "ingest_ready_file_path": "test-bucket/2023-01-01:00:00:00.000000/file1.csv",
                "post_processed_normalized_file_path": "2023-01-01:00:00:00.000000/file1.csv",
            },
            {
                "remote_file_path": "file2.txt",
                "sftp_timestamp": 1,
                "downloaded_file_path": "gs://test-bucket/2023-01-01:00:00:00.000000/file2.txt",
                "post_processed_file_path": "gs://test-bucket/2023-01-01:00:00:00.000000/file2.txt",
                "ingest_ready_file_path": "test-bucket/2023-01-01:00:00:00.000000/file2.txt",
                "post_processed_normalized_file_path": "2023-01-01:00:00:00.000000/file2.txt",
            },
        ]
        results = self.operator.filter_invalid_files(FakeGcsFileSystem(), sample_data)  # type: ignore
        self.assertListEqual(results, expected_data)

    def test_execute_throws_if_directory(self) -> None:
        sample_data: List[Dict[str, Union[str, int]]] = [
            {
                "remote_file_path": "folder1.zip",
                "sftp_timestamp": 1,
                "downloaded_file_path": "gs://test-bucket/2023-01-01:00:00:00.000000/folder1.zip",
                "post_processed_file_path": "gs://test-bucket/2023-01-01:00:00:00.000000/folder1",
            },
        ]
        with self.assertRaises(ValueError):
            _ = self.operator.filter_invalid_files(FakeGcsFileSystem(), sample_data)  # type: ignore
