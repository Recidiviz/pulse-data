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
"""Tests for RecidivizGcsFileTransformOperator"""
import datetime
import unittest
from typing import List
from unittest.mock import MagicMock, create_autospec, patch

from airflow import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from recidiviz.airflow.dags.operators.sftp.gcs_transform_file_operator import (
    RecidivizGcsFileTransformOperator,
)
from recidiviz.airflow.tests.test_utils import execute_task
from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcs_file_system_impl import GCSFileSystemImpl
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.sftp.base_sftp_download_delegate import (
    BaseSftpDownloadDelegate,
)
from recidiviz.ingest.direct.sftp.sftp_download_delegate_factory import (
    SftpDownloadDelegateFactory,
)


class FakeSftpDownloadDelegate(BaseSftpDownloadDelegate):
    def root_directory(self, candidate_paths: List[str]) -> str:
        return "/"

    def filter_paths(self, candidate_paths: List[str]) -> List[str]:
        return candidate_paths

    def supported_environments(self) -> List[str]:
        return [TEST_PROJECT_ID]

    def post_process_downloads(
        self, downloaded_path: GcsfsFilePath, gcsfs: GCSFileSystem
    ) -> List[str]:
        if "fail" in downloaded_path.abs_path():
            return []
        return [downloaded_path.abs_path()]


TEST_PROJECT_ID = "recidiviz-testing"


@patch.object(
    SftpDownloadDelegateFactory, "build", return_value=FakeSftpDownloadDelegate()
)
class TestRecidivizGcsFileTransformOperator(unittest.TestCase):
    """Tests for RecidivizGcsFileTransformOperator"""

    def setUp(self) -> None:
        self.mock_gcs_hook = create_autospec(GCSHook)
        self.mock_gcs_hook_patcher = patch(
            "recidiviz.airflow.dags.operators.sftp.gcs_transform_file_operator.GCSHook"
        )
        self.mock_gcs_hook_patcher.start().return_value = self.mock_gcs_hook

        self.mock_gcs_file_system = create_autospec(GCSFileSystemImpl)
        self.mock_gcs_file_system_patcher = patch(
            "recidiviz.airflow.dags.operators.sftp.gcs_transform_file_operator.GCSFileSystemImpl"
        )
        self.mock_gcs_file_system_patcher.start().return_value = (
            self.mock_gcs_file_system
        )

    def tearDown(self) -> None:
        self.mock_gcs_hook_patcher.stop()
        self.mock_gcs_file_system_patcher.stop()

    def test_execute(self, _mock_sftp_delegate: MagicMock) -> None:
        dag = DAG(dag_id="test_dag", start_date=datetime.datetime.now())
        gcs_transform_task = RecidivizGcsFileTransformOperator(
            task_id="test_task",
            project_id=TEST_PROJECT_ID,
            region_code="US_XX",
            remote_file_path="test_file.txt",
            downloaded_file_path="gs://test-bucket/test_file.txt",
            sftp_timestamp=1,
        )
        result = execute_task(dag, gcs_transform_task)
        self.assertEqual(
            result,
            [
                {
                    "remote_file_path": "test_file.txt",
                    "sftp_timestamp": 1,
                    "downloaded_file_path": "gs://test-bucket/test_file.txt",
                    "post_processed_file_path": "test-bucket/test_file.txt",
                }
            ],
        )

    def test_execute_failure(self, _mock_sftp_delegate: MagicMock) -> None:
        dag = DAG(dag_id="test_dag", start_date=datetime.datetime.now())
        gcs_transform_task = RecidivizGcsFileTransformOperator(
            task_id="test_task",
            project_id=TEST_PROJECT_ID,
            region_code="US_XX",
            remote_file_path="test_fail_file.txt",
            downloaded_file_path="gs://test-bucket/test_fail_file.txt",
            sftp_timestamp=1,
        )
        with self.assertRaises(ValueError):
            _ = execute_task(dag, gcs_transform_task)
