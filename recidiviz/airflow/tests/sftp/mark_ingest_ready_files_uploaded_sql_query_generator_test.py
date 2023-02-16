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
"""Unit tests for MarkIngestReadyFilesUploadedSqlQueryGenerator"""
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
from recidiviz.airflow.dags.sftp.mark_ingest_ready_files_uploaded_sql_query_generator import (
    MarkIngestReadyFilesUploadedSqlQueryGenerator,
)


class TestMarkIngestReadyFilesUploadedSqlQueryGenerator(unittest.TestCase):
    """Unit tests for MarkIngestReadyFilesUploadedSqlQueryGenerator"""

    def setUp(self) -> None:
        self.generator = MarkIngestReadyFilesUploadedSqlQueryGenerator(
            region_code="US_XX", upload_files_to_ingest_bucket_task_id="test_task_id"
        )

    @freezegun.freeze_time(datetime.datetime(2023, 2, 1, 0, 0, 0, 0))
    def test_generates_update_sql_correctly(self) -> None:
        sample_data_set: Set[Tuple[str, str]] = {
            ("2023-01-01T01:00:00:000000/file1.csv", "file1.csv"),
            ("2023-01-01T01:00:00:000000/file2.csv", "file2.csv"),
        }
        expected_query = """
UPDATE direct_ingest_sftp_ingest_ready_file_metadata SET file_upload_time = '2023-02-01 00:00:00.000000 UTC'
WHERE region_code = 'US_XX' AND (post_processed_normalized_file_path, remote_file_path) IN (('2023-01-01T01:00:00:000000/file1.csv', 'file1.csv'),('2023-01-01T01:00:00:000000/file2.csv', 'file2.csv'));"""
        self.assertEqual(
            self.generator.update_sql_query(sample_data_set), expected_query
        )

    @freezegun.freeze_time(datetime.datetime(2023, 2, 1, 0, 0, 0, 0))
    def test_marks_files_uploaded_correctly(self) -> None:
        mock_operator = create_autospec(CloudSqlQueryOperator)
        mock_postgres = create_autospec(PostgresHook)
        mock_context = create_autospec(Context)

        sample_data = [
            {
                "remote_file_path": "file1.csv",
                "sftp_timestamp": 1,
                "post_processed_file_path": "gs://test-bucket/2023-01-01T01:00:00:000000/file1.csv",
                "ingest_ready_file_path": "gs://test-bucket/2023-01-01T01:00:00:000000/file1.csv",
                "post_processed_normalized_file_path": "2023-01-01T01:00:00:000000/file1.csv",
                "uploaded_file_path": "gs://test-bucket-2/unprocessed_2023-01-01T01:00:00:00000_raw_file1.csv",
            },
            {
                "remote_file_path": "file2.csv",
                "sftp_timestamp": 1,
                "post_processed_file_path": "gs://test-bucket/2023-01-01T01:00:00:000000/file2.csv",
                "ingest_ready_file_path": "gs://test-bucket/2023-01-01T01:00:00:000000/file2.csv",
                "post_processed_normalized_file_path": "2023-01-01T01:00:00:000000/file2.csv",
                "uploaded_file_path": "gs://test-bucket-2/unprocessed_2023-01-01T01:00:00:00000_raw_file2.csv",
            },
        ]

        mock_operator.xcom_pull.return_value = sample_data
        results = self.generator.execute_postgres_query(
            mock_operator, mock_postgres, mock_context
        )

        mock_postgres.run.assert_called_with(
            """
UPDATE direct_ingest_sftp_ingest_ready_file_metadata SET file_upload_time = '2023-02-01 00:00:00.000000 UTC'
WHERE region_code = 'US_XX' AND (post_processed_normalized_file_path, remote_file_path) IN (('2023-01-01T01:00:00:000000/file1.csv', 'file1.csv'),('2023-01-01T01:00:00:000000/file2.csv', 'file2.csv'));"""
        )

        self.assertListEqual(results, sample_data)

    def test_marks_files_uploaded_no_files_to_mark(self) -> None:
        mock_operator = create_autospec(CloudSqlQueryOperator)
        mock_postgres = create_autospec(PostgresHook)
        mock_context = create_autospec(Context)

        mock_operator.xcom_pull.return_value = None

        results = self.generator.execute_postgres_query(
            mock_operator, mock_postgres, mock_context
        )

        mock_postgres.run.assert_not_called()

        self.assertEqual(len(results), 0)
