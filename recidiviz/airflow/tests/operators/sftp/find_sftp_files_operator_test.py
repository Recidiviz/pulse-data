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
from recidiviz.airflow.dags.operators.sftp.find_sftp_files_operator import (
    FindSftpFilesOperator,
)
from recidiviz.airflow.tests.operators.sftp.sftp_test_utils import (
    FakeSftpDownloadDelegate,
)
from recidiviz.airflow.tests.test_utils import execute_task
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.local_file_paths import filepath_relative_to_caller
from recidiviz.ingest.direct.sftp.sftp_download_delegate_factory import (
    SftpDownloadDelegateFactory,
)
from recidiviz.utils.yaml_dict import YAMLDict

TEST_PROJECT_ID = "recidiviz-testing"

TODAY = datetime.datetime.fromtimestamp(int(datetime.datetime.today().timestamp()))
YESTERDAY = TODAY - datetime.timedelta(1)
TWO_DAYS_AGO = TODAY - datetime.timedelta(2)


class FakeFindSftpDownloadDelegate(FakeSftpDownloadDelegate):
    def filter_paths(self, candidate_paths: List[str]) -> List[str]:
        return [path for path in candidate_paths if path.startswith("test")]


@patch.object(
    SftpDownloadDelegateFactory, "build", return_value=FakeFindSftpDownloadDelegate()
)
class TestFindSftpFilesOperator(unittest.TestCase):
    """Tests for FindSftpFilesOperator"""

    def setUp(self) -> None:
        self.mock_sftp_hook = create_autospec(RecidivizSFTPHook)
        self.mock_sftp_patcher = patch(
            "recidiviz.airflow.dags.operators.sftp.find_sftp_files_operator.RecidivizSFTPHook"
        )
        self.mock_sftp_patcher.start().return_value = self.mock_sftp_hook

        self.mock_sftp_connection = create_autospec(SFTPClient)
        self.mock_sftp_hook.get_conn.return_value = self.mock_sftp_connection

        self.mock_read_config_patcher = patch(
            "recidiviz.airflow.dags.operators.sftp.find_sftp_files_operator.read_yaml_config"
        )

        self.mock_read_config_fn = self.mock_read_config_patcher.start()
        self.mock_read_config_fn.return_value = YAMLDict.from_path(
            filepath_relative_to_caller(
                "sftp_excluded_remote_file_paths_default.yaml", "fixtures"
            )
        )
        self.fake_excluded_files_config_gcs_path = GcsfsFilePath(
            bucket_name="my-configs-bucket", blob_name="my_excluded_files.yaml"
        )

    def tearDown(self) -> None:
        self.mock_sftp_patcher.stop()
        self.mock_read_config_patcher.stop()

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
            task_id="test_task",
            state_code="US_XX",
            dag=dag,
            excluded_remote_files_config_path=self.fake_excluded_files_config_gcs_path,
        )

        result = execute_task(dag, find_files_task)
        self.assertEqual(
            result,
            [
                {
                    "remote_file_path": "/testToday/file1.txt",
                    "sftp_timestamp": int(TODAY.timestamp()),
                },
                {
                    "remote_file_path": "/testTwoDaysAgo/file1.txt",
                    "sftp_timestamp": int(TWO_DAYS_AGO.timestamp()),
                },
            ],
        )

        self.mock_read_config_fn.assert_called_with(
            self.fake_excluded_files_config_gcs_path
        )

    def test_execute_with_file_exclusions(self, _mock_sftp_delegate: MagicMock) -> None:
        # Update to read from a config where /testToday/file1.txt is excluded
        self.mock_read_config_fn.return_value = YAMLDict.from_path(
            filepath_relative_to_caller(
                "sftp_excluded_remote_file_paths_with_exclusions.yaml", "fixtures"
            )
        )

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
            task_id="test_task",
            state_code="US_XX",
            dag=dag,
            excluded_remote_files_config_path=self.fake_excluded_files_config_gcs_path,
        )

        result = execute_task(dag, find_files_task)
        self.assertEqual(
            result,
            [
                {
                    "remote_file_path": "/testTwoDaysAgo/file1.txt",
                    "sftp_timestamp": int(TWO_DAYS_AGO.timestamp()),
                },
            ],
        )

        self.mock_read_config_fn.assert_called_with(
            self.fake_excluded_files_config_gcs_path
        )

    def test_execute_with_no_files(self, _mock_sftp_delegate: MagicMock) -> None:
        self.mock_sftp_hook.list_directory.side_effect = [[]]
        self.mock_sftp_connection.listdir_attr.side_effect = [[]]
        dag = DAG(dag_id="test_dag", start_date=datetime.datetime.now())
        find_files_task = FindSftpFilesOperator(
            task_id="test_task",
            state_code="US_XX",
            dag=dag,
            excluded_remote_files_config_path=self.fake_excluded_files_config_gcs_path,
        )
        with self.assertRaisesRegex(
            ValueError,
            r"No files found to download for US_XX state. This indicates that the SFTP directory is empty. Please check to see if the state has uploaded any files to SFTP.",
        ):
            _ = execute_task(dag, find_files_task)

    def test_execute_with_no_files_exempt_michigan(
        self, _mock_sftp_delegate: MagicMock
    ) -> None:
        self.mock_sftp_hook.list_directory.side_effect = [[]]
        self.mock_sftp_connection.listdir_attr.side_effect = [[]]
        dag = DAG(dag_id="test_dag", start_date=datetime.datetime.now())
        find_files_task = FindSftpFilesOperator(
            task_id="test_task",
            state_code="US_MI",
            dag=dag,
            excluded_remote_files_config_path=self.fake_excluded_files_config_gcs_path,
        )
        result = execute_task(dag, find_files_task)
        self.assertEqual(result, [])

    def create_sftp_attrs(self, mtime: int, filename: str, mode: int) -> SFTPAttributes:
        attr = SFTPAttributes()
        attr.st_mtime = mtime
        attr.filename = filename
        attr.st_mode = mode
        return attr
