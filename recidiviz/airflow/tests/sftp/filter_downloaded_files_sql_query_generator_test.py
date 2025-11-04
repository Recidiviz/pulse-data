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
from unittest.mock import create_autospec, patch

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.sftp.filter_downloaded_files_sql_query_generator import (
    FilterDownloadedFilesSqlQueryGenerator,
)
from recidiviz.airflow.tests.operators.sftp.sftp_test_utils import (
    FakeUsXxSftpDownloadDelegate,
)
from recidiviz.ingest.direct.sftp.sftp_download_delegate_factory import (
    SftpDownloadDelegateFactory,
)


class TestFilterDownloadedFilesSqlQueryGenerator(unittest.TestCase):
    """Unit tests for FilterDownloadedFilesSqlQueryGenerator"""

    def setUp(self) -> None:
        self.generator = FilterDownloadedFilesSqlQueryGenerator(
            region_code="US_XX", find_sftp_files_task_id="test_task_id"
        )

    @patch.object(
        SftpDownloadDelegateFactory,
        "build",
        return_value=FakeUsXxSftpDownloadDelegate(),
    )
    def test_generates_sql_correctly(self, _mock_delegate_factory: None) -> None:
        sample_data_set: Set[Tuple[str, int]] = {("file1.csv", 1), ("file2.csv", 1)}
        expected_query = """
SELECT remote_file_path, sftp_timestamp FROM direct_ingest_sftp_remote_file_metadata
 WHERE region_code = 'US_XX' AND file_download_time IS NOT NULL
 AND (remote_file_path, sftp_timestamp) IN (('file1.csv', 1),('file2.csv', 1));"""

        self.assertEqual(self.generator.sql_query(sample_data_set), expected_query)

    @patch.object(
        SftpDownloadDelegateFactory,
        "build",
        return_value=FakeUsXxSftpDownloadDelegate(),
    )
    def test_filters_files_and_timestamps_correctly(
        self, _mock_delegate_factory: None
    ) -> None:
        mock_operator = create_autospec(CloudSqlQueryOperator)
        mock_postgres = create_autospec(PostgresHook)
        mock_context = create_autospec(Context)

        mock_operator.xcom_pull.return_value = [
            {"remote_file_path": "file1.csv", "sftp_timestamp": 1},
            {"remote_file_path": "file2.csv", "sftp_timestamp": 2},
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

    @patch.object(
        SftpDownloadDelegateFactory,
        "build",
        return_value=FakeUsXxSftpDownloadDelegate(),
    )
    def test_filters_files_and_timestamps_no_prior_values(
        self, _mock_delegate_factory: None
    ) -> None:
        mock_operator = create_autospec(CloudSqlQueryOperator)
        mock_postgres = create_autospec(PostgresHook)
        mock_context = create_autospec(Context)

        mock_operator.xcom_pull.return_value = []

        results = self.generator.execute_postgres_query(
            mock_operator, mock_postgres, mock_context
        )
        self.assertEqual(len(results), 0)
        mock_postgres.get_pandas_df.assert_not_called()

    def test_us_mi_fails_when_rediscovering_already_downloaded_files(self) -> None:
        """Test that US_MI raises an error when discovering already-downloaded files."""
        generator = FilterDownloadedFilesSqlQueryGenerator(
            region_code="US_MI", find_sftp_files_task_id="test_task_id"
        )

        mock_operator = create_autospec(CloudSqlQueryOperator)
        mock_postgres = create_autospec(PostgresHook)
        mock_context = create_autospec(Context)

        # Simulate discovering files on SFTP
        mock_operator.xcom_pull.return_value = [
            {
                "remote_file_path": "/CORrecidiviz/ADH_OFFENDER_SENTENCE.zip",
                "sftp_timestamp": 1760805474,
            },
            {
                "remote_file_path": "/CORrecidiviz/ADH_EMPLOYEE.zip",
                "sftp_timestamp": 1760805500,
            },
        ]

        # Simulate that this file was already downloaded
        mock_postgres.get_pandas_df.return_value = pd.DataFrame(
            [
                {
                    "remote_file_path": "/CORrecidiviz/ADH_OFFENDER_SENTENCE.zip",
                    "sftp_timestamp": 1760805474,
                }
            ]
        )

        # Should raise ValueError due to MI-specific validation
        with self.assertRaises(ValueError) as context:
            generator.execute_postgres_query(mock_operator, mock_postgres, mock_context)

        expected_exception = (
            "US_MI SFTP server is exposing 1 file(s) that were already downloaded. If "
            "these files are not properly moved to the downloaded folder or deleted, then "
            "we will fail to download all newer versions of each of these files. Files:\n"
            "  * /CORrecidiviz/ADH_OFFENDER_SENTENCE.zip (timestamp: 1760805474, 2025-10-18 12:37:54 EDT)\n"
            "\n"
            "To resolve: Manually download each of these files via the MI SFTP UI (see "
            "1Password for login) or contact Michigan to get them to delete the files."
        )
        self.assertEqual(expected_exception, str(context.exception))
