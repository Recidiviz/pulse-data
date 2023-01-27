# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Tests for FindSftpFilesOperator"""
import datetime
import stat
import unittest
from typing import List
from unittest.mock import MagicMock, create_autospec, patch

from airflow import DAG
from paramiko import SFTPAttributes, SFTPClient

from recidiviz.airflow.dags.hooks.sftp_hook import RecidivizSFTPHook
from recidiviz.airflow.dags.operators.find_sftp_files_operator import (
    FindSftpFilesOperator,
)
from recidiviz.airflow.tests.test_utils import execute_task
from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.sftp.base_sftp_download_delegate import (
    BaseSftpDownloadDelegate,
)
from recidiviz.ingest.direct.sftp.sftp_download_delegate_factory import (
    SftpDownloadDelegateFactory,
)

TEST_PROJECT_ID = "recidiviz-testing"

TODAY = datetime.datetime.fromtimestamp(int(datetime.datetime.today().timestamp()))
YESTERDAY = TODAY - datetime.timedelta(1)
TWO_DAYS_AGO = TODAY - datetime.timedelta(2)


class FakeSftpDownloadDelegate(BaseSftpDownloadDelegate):
    def root_directory(self, candidate_paths: List[str]) -> str:
        return "."

    def filter_paths(self, candidate_paths: List[str]) -> List[str]:
        return [path for path in candidate_paths if path.startswith("test")]

    def supported_environments(self) -> List[str]:
        return [TEST_PROJECT_ID]

    def post_process_downloads(
        self, downloaded_path: GcsfsFilePath, gcsfs: GCSFileSystem
    ) -> List[str]:
        return [downloaded_path.abs_path()]


@patch.object(
    SftpDownloadDelegateFactory, "build", return_value=FakeSftpDownloadDelegate()
)
class TestFindSftpFilesOperator(unittest.TestCase):
    """Tests for FindSftpFilesOperator"""

    def setUp(self) -> None:
        self.mock_sftp_hook = create_autospec(RecidivizSFTPHook)
        self.mock_sftp_patcher = patch(
            "recidiviz.airflow.dags.operators.find_sftp_files_operator.RecidivizSFTPHook"
        )
        self.mock_sftp_patcher.start().return_value = self.mock_sftp_hook

        self.mock_sftp_connection = create_autospec(SFTPClient)
        self.mock_sftp_hook.get_conn.return_value = self.mock_sftp_connection

    def tearDown(self) -> None:
        self.mock_sftp_patcher.stop()

    def test_execute(self, _mock_sftp_delegate: MagicMock) -> None:
        self.mock_sftp_hook.list_directory.side_effect = [
            ["testToday", "testTwoDaysAgo", "nottest.txt"],
            ["file1.txt"],
            ["file1.txt"],
        ]
        self.mock_sftp_connection.listdir_attr.side_effect = [
            [
                self.create_sftp_attrs(
                    int(TODAY.timestamp()), "testToday", stat.S_IFDIR
                ),
                self.create_sftp_attrs(
                    int(TWO_DAYS_AGO.timestamp()), "testTwoDaysAgo", stat.S_IFDIR
                ),
                self.create_sftp_attrs(
                    int(YESTERDAY.timestamp()), "nottest.txt", stat.S_IFREG
                ),
            ]
        ]
        self.mock_sftp_connection.stat.side_effect = [
            self.create_sftp_attrs(int(TODAY.timestamp()), "file1.txt", stat.S_IFREG),
            self.create_sftp_attrs(
                int(TWO_DAYS_AGO.timestamp()), "file1.txt", stat.S_IFREG
            ),
        ]

        dag = DAG(dag_id="test_dag", start_date=datetime.datetime.now())
        find_files_task = FindSftpFilesOperator(
            task_id="test_task", state_code="US_XX", dag=dag
        )

        result = execute_task(dag, find_files_task)
        self.assertEqual(
            result,
            [
                {"file": "./testToday/file1.txt", "timestamp": int(TODAY.timestamp())},
                {
                    "file": "./testTwoDaysAgo/file1.txt",
                    "timestamp": int(TWO_DAYS_AGO.timestamp()),
                },
            ],
        )

    def create_sftp_attrs(self, mtime: int, filename: str, mode: int) -> SFTPAttributes:
        attr = SFTPAttributes()
        attr.st_mtime = mtime
        attr.filename = filename
        attr.st_mode = mode
        return attr
