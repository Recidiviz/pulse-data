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
"""Unit tests for MarkIngestReadyFilesDiscoveredSqlQueryGenerator"""
import datetime
import unittest
from typing import Set, Tuple
from unittest.mock import create_autospec

import freezegun
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.sftp.mark_ingest_ready_files_discovered_sql_query_generator import (
    MarkIngestReadyFilesDiscoveredSqlQueryGenerator,
)


class TestMarkIngestReadyFilesDiscoveredSqlQueryGenerator(unittest.TestCase):
    """Unit tests for MarkIngestReadyFilesDiscoveredSqlQueryGenerator"""

    def setUp(self) -> None:
        self.generator = MarkIngestReadyFilesDiscoveredSqlQueryGenerator(
            region_code="US_XX", filter_invalid_gcs_files_task_id="test_task_id"
        )

    def test_generates_exists_sql_correctly(self) -> None:
        sample_data_set: Set[Tuple[str, str]] = {
            ("2023-01-01T01:00:00:000000/folder/file1.csv", "folder/file1.csv"),
            ("2023-01-01T01:00:00:000000/folder/file2.csv", "folder/file2.csv"),
        }
        expected_query = """
SELECT post_processed_normalized_file_path, remote_file_path FROM
 direct_ingest_sftp_ingest_ready_file_metadata
 WHERE region_code = 'US_XX' AND file_upload_time IS NULL
 AND (post_processed_normalized_file_path, remote_file_path) IN (('2023-01-01T01:00:00:000000/folder/file1.csv', 'folder/file1.csv'),('2023-01-01T01:00:00:000000/folder/file2.csv', 'folder/file2.csv'));"""
        self.assertEqual(
            self.generator.exists_sql_query(sample_data_set), expected_query
        )

    @freezegun.freeze_time(datetime.datetime(2023, 1, 26, 0, 0, 0, 0))
    def test_generates_insert_sql_correctly(self) -> None:
        sample_data_set: Set[Tuple[str, str]] = {
            ("2023-01-01T01:00:00:000000/folder/file1.csv", "folder/file1.csv"),
            ("2023-01-01T01:00:00:000000/folder/file2.csv", "folder/file2.csv"),
        }
        expected_query = """
INSERT INTO direct_ingest_sftp_ingest_ready_file_metadata
(region_code, post_processed_normalized_file_path, remote_file_path, file_discovery_time)
VALUES
('US_XX', '2023-01-01T01:00:00:000000/folder/file1.csv', 'folder/file1.csv', '2023-01-26 00:00:00.000000 UTC'),
('US_XX', '2023-01-01T01:00:00:000000/folder/file2.csv', 'folder/file2.csv', '2023-01-26 00:00:00.000000 UTC');"""
        self.assertEqual(
            self.generator.insert_sql_query(sample_data_set), expected_query
        )

    @freezegun.freeze_time(datetime.datetime(2023, 1, 26, 0, 0, 0, 0))
    def test_marks_files_discovered_correctly(self) -> None:
        mock_operator = create_autospec(CloudSqlQueryOperator)
        mock_postgres = create_autospec(PostgresHook)
        mock_context = create_autospec(Context)

        sample_data = [
            {
                "remote_file_path": "file1.csv",
                "post_processed_normalized_file_path": "2023-01-01T01:00:00:000000/file1.csv",
            },
            {
                "remote_file_path": "file2.csv",
                "post_processed_normalized_file_path": "2023-01-01T01:00:00:000000/file2.csv",
            },
        ]

        mock_operator.xcom_pull.return_value = sample_data
        mock_postgres.get_pandas_df.return_value = pd.DataFrame(
            [
                {
                    "post_processed_normalized_file_path": "2023-01-01T01:00:00:000000/file2.csv",
                    "remote_file_path": "file2.csv",
                }
            ]
        )

        results = self.generator.execute_postgres_query(
            mock_operator, mock_postgres, mock_context
        )

        mock_postgres.run.assert_called_with(
            """
INSERT INTO direct_ingest_sftp_ingest_ready_file_metadata
(region_code, post_processed_normalized_file_path, remote_file_path, file_discovery_time)
VALUES
('US_XX', '2023-01-01T01:00:00:000000/file1.csv', 'file1.csv', '2023-01-26 00:00:00.000000 UTC');"""
        )

        self.assertListEqual(results, sample_data)

    def test_marks_files_discovered_no_files_to_mark(self) -> None:
        mock_operator = create_autospec(CloudSqlQueryOperator)
        mock_postgres = create_autospec(PostgresHook)
        mock_context = create_autospec(Context)

        sample_data = [
            {
                "remote_file_path": "file1.csv",
                "post_processed_normalized_file_path": "2023-01-01T01:00:00:000000/file1.csv",
            },
            {
                "remote_file_path": "file2.csv",
                "post_processed_normalized_file_path": "2023-01-01T01:00:00:000000/file2.csv",
            },
        ]

        mock_operator.xcom_pull.return_value = sample_data
        mock_postgres.get_pandas_df.return_value = pd.DataFrame(sample_data)

        results = self.generator.execute_postgres_query(
            mock_operator, mock_postgres, mock_context
        )

        mock_postgres.run.assert_not_called()

        self.assertListEqual(results, sample_data)

    def test_marks_files_discovered_no_prior_files(self) -> None:
        mock_operator = create_autospec(CloudSqlQueryOperator)
        mock_postgres = create_autospec(PostgresHook)
        mock_context = create_autospec(Context)

        mock_operator.xcom_pull.return_value = []

        results = self.generator.execute_postgres_query(
            mock_operator, mock_postgres, mock_context
        )

        self.assertEqual(len(results), 0)
        mock_postgres.get_pandas_df.assert_not_called()
        mock_postgres.run.assert_not_called()
