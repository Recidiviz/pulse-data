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
"""Unit tests for MarkFilesDownloadedSqlQueryGenerator"""
import datetime
import unittest
from typing import Set, Tuple
from unittest.mock import create_autospec

import freezegun
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.sftp.mark_remote_files_downloaded_sql_query_generator import (
    MarkRemoteFilesDownloadedSqlQueryGenerator,
)


class TestMarkFilesDownloadedSqlQueryGenerator(unittest.TestCase):
    """Unit tests for MarkRemoteFilesDownloadedSqlQueryGenerator"""

    def setUp(self) -> None:
        self.generator = MarkRemoteFilesDownloadedSqlQueryGenerator(
            region_code="US_XX", post_process_sftp_files_task_id="test_task_id"
        )

    @freezegun.freeze_time(datetime.datetime(2023, 2, 1, 0, 0, 0, 0))
    def test_generates_update_sql_correctly(self) -> None:
        sample_data_set: Set[Tuple[str, int]] = {("file1.csv", 1), ("file2.csv", 2)}
        expected_query = """
UPDATE direct_ingest_sftp_remote_file_metadata SET file_download_time = '2023-02-01 00:00:00.000000 UTC'
WHERE region_code = 'US_XX' AND (remote_file_path, sftp_timestamp) IN (('file1.csv', 1),('file2.csv', 2));"""
        self.assertEqual(
            self.generator.update_sql_query(sample_data_set), expected_query
        )

    @freezegun.freeze_time(datetime.datetime(2023, 2, 1, 0, 0, 0, 0))
    def test_marks_files_downloaded_correctly(self) -> None:
        mock_operator = create_autospec(CloudSqlQueryOperator)
        mock_postgres = create_autospec(PostgresHook)
        mock_context = create_autospec(Context)

        sample_data = [
            [
                {
                    "remote_file_path": "file1.csv",
                    "sftp_timestamp": 1,
                    "downloaded_file_path": "gs://test-bucket/file1.csv",
                    "post_processed_file_path": "gs://test-bucket/file1.csv",
                }
            ],
            [
                {
                    "remote_file_path": "file2.csv",
                    "sftp_timestamp": 2,
                    "downloaded_file_path": "gs://test-bucket/file2.csv",
                    "post_processed_file_path": "gs://test-bucket/file2.csv",
                }
            ],
        ]

        mock_operator.xcom_pull.return_value = sample_data
        results = self.generator.execute_postgres_query(
            mock_operator, mock_postgres, mock_context
        )

        mock_postgres.run.assert_called_with(
            """
UPDATE direct_ingest_sftp_remote_file_metadata SET file_download_time = '2023-02-01 00:00:00.000000 UTC'
WHERE region_code = 'US_XX' AND (remote_file_path, sftp_timestamp) IN (('file1.csv', 1),('file2.csv', 2));"""
        )

        self.assertListEqual(
            results,
            [
                {
                    "remote_file_path": "file1.csv",
                    "sftp_timestamp": 1,
                    "downloaded_file_path": "gs://test-bucket/file1.csv",
                    "post_processed_file_path": "gs://test-bucket/file1.csv",
                },
                {
                    "remote_file_path": "file2.csv",
                    "sftp_timestamp": 2,
                    "downloaded_file_path": "gs://test-bucket/file2.csv",
                    "post_processed_file_path": "gs://test-bucket/file2.csv",
                },
            ],
        )

    def test_marks_files_downloaded_no_files_to_mark(self) -> None:
        mock_operator = create_autospec(CloudSqlQueryOperator)
        mock_postgres = create_autospec(PostgresHook)
        mock_context = create_autospec(Context)

        mock_operator.xcom_pull.return_value = []

        results = self.generator.execute_postgres_query(
            mock_operator, mock_postgres, mock_context
        )

        mock_postgres.run.assert_not_called()

        self.assertEqual(len(results), 0)
