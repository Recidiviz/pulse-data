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
"""Unit tests for GetWatermarkSqlQueryGenerator"""
import datetime
import unittest
from unittest.mock import create_autospec

import freezegun
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

from recidiviz.airflow.dags.ingest.get_watermark_sql_query_generator import (
    GetWatermarkSqlQueryGenerator,
)
from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryOperator,
)


class TestGetWatermarkSqlQueryGenerator(unittest.TestCase):
    """Unit tests for GetWatermarkSqlQueryGenerator"""

    def setUp(self) -> None:
        self.generator = GetWatermarkSqlQueryGenerator(
            region_code="US_XX", ingest_instance="PRIMARY"
        )

    def test_generates_sql_correctly(self) -> None:
        expected_query = """
            SELECT raw_data_file_tag, watermark_datetime
            FROM direct_ingest_dataflow_raw_table_upper_bounds
            WHERE job_id IN (
                SELECT MAX(job_id) 
                FROM direct_ingest_dataflow_job
                WHERE region_code = 'US_XX' AND ingest_instance = 'PRIMARY'
            );
        """
        self.assertEqual(self.generator.sql_query(), expected_query)

    @freezegun.freeze_time(datetime.datetime(2023, 1, 26, 0, 0, 0, 0))
    def test_watermark_retrieved_correctly(self) -> None:
        mock_operator = create_autospec(CloudSqlQueryOperator)
        mock_postgres = create_autospec(PostgresHook)
        mock_context = create_autospec(Context)

        sample_data = {
            "test_file_tag": "2023-01-26 00:00:0.000000+00",
        }

        mock_postgres.get_pandas_df.return_value = pd.DataFrame(
            [
                {
                    "raw_data_file_tag": "test_file_tag",
                    "watermark_datetime": "2023-01-26 00:00:0.000000+00",
                }
            ]
        )

        results = self.generator.execute_postgres_query(
            mock_operator, mock_postgres, mock_context
        )

        self.assertDictEqual(results, sample_data)
