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
"""Tests for the SFTPGcsToGcsOperator"""
import unittest

from recidiviz.airflow.dags.operators.sftp.gcs_to_gcs_operator import (
    SFTPGcsToGcsOperator,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath


class TestSFTPGcsToGcsOperator(unittest.TestCase):
    """Tests for the SFTPGcsToGcsOperator"""

    def test_creates_correct_upload_path(self) -> None:
        operator = SFTPGcsToGcsOperator(
            task_id="test-task",
            project_id="recidiviz-testing",
            region_code="US_XX",
            remote_file_path="outside_folder/inside_folder/file.txt",
            post_processed_normalized_file_path="2023-01-01T01:00:00:000000/inside_folder/file.txt",
        )
        expected_file = GcsfsFilePath.from_bucket_and_blob_name(
            bucket_name="recidiviz-testing-direct-ingest-state-us-xx-test",
            blob_name="unprocessed_2023-01-01T01:00:00:000000_raw_file.txt",
        )
        self.assertEqual(
            expected_file.abs_path(), operator.build_upload_path().abs_path()
        )
