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
"""Tests for the SftpToGcsOperator"""
import datetime
import unittest

import pytz

from recidiviz.airflow.dags.operators.sftp.sftp_to_gcs_operator import (
    RecidivizSftpToGcsOperator,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath


class TestSftpToGcsOperator(unittest.TestCase):
    """Tests for the SftpToGcsOperator"""

    def test_creates_correct_download_path(self) -> None:
        operator = RecidivizSftpToGcsOperator(
            task_id="test-task",
            project_id="recidiviz-testing",
            region_code="US_XX",
            remote_file_path="outside_folder/inside_folder/file.txt",
            sftp_timestamp=int(
                datetime.datetime(2023, 1, 1, 1, 0, 0, 0, tzinfo=pytz.UTC).timestamp()
            ),
        )
        expected_file = GcsfsFilePath.from_bucket_and_blob_name(
            bucket_name="recidiviz-testing-direct-ingest-state-us-xx-sftp-test",
            blob_name="2023-01-01T01:00:00:000000/inside_folder/file.txt",
        )
        self.assertEqual(
            expected_file.abs_path(), operator.build_download_path().abs_path()
        )
