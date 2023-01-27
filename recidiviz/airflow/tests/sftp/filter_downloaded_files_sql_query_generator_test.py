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
"""Unit tests for FilterDownloadedFilesSqlQueryGenerator"""
import unittest
from typing import Set, Tuple
from unittest.mock import create_autospec

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.sftp.filter_downloaded_files_sql_query_generator import (
    FilterDownloadedFilesSqlQueryGenerator,
)


class TestFilterDownloadedFilesSqlQueryGenerator(unittest.TestCase):
    """Unit tests for FilterDownloadedFilesSqlQueryGenerator"""

    def setUp(self) -> None:
        self.generator = FilterDownloadedFilesSqlQueryGenerator("test_task_id")

    def test_generates_sql_correctly(self) -> None:
        sample_data_set: Set[Tuple[str, int]] = {("file1.csv", 1), ("file2.csv", 1)}
        expected_query = """
SELECT remote_file_path, sftp_timestamp FROM direct_ingest_sftp_remote_file_metadata
 WHERE file_download_time IS NOT NULL AND (remote_file_path, sftp_timestamp) IN (('file1.csv', 1),('file2.csv', 1));"""

        self.assertEqual(self.generator.sql_query(sample_data_set), expected_query)

    def test_filters_files_and_timestamps_correctly(self) -> None:
        mock_operator = create_autospec(CloudSqlQueryOperator)
        mock_postgres = create_autospec(PostgresHook)
        mock_context = create_autospec(Context)

        mock_operator.xcom_pull.return_value = [
            {"file": "file1.csv", "timestamp": 1},
            {"file": "file2.csv", "timestamp": 2},
        ]

        mock_postgres.get_pandas_df.return_value = pd.DataFrame(
            [{"remote_file_path": "file2.csv", "sftp_timestamp": 2}]
        )

        expected_results = [
            {"remote_file_path": "file1.csv", "sftp_timestamp": 1},
        ]

        results = self.generator.execute_postgres_query(
            mock_operator, mock_postgres, mock_context
        )

        self.assertListEqual(results, expected_results)
