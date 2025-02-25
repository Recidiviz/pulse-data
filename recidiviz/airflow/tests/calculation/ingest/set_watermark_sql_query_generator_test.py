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
"""Unit tests for SetWatermarkSqlQueryGenerator"""
import unittest
from unittest.mock import create_autospec

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

from recidiviz.airflow.dags.calculation.ingest.set_watermark_sql_query_generator import (
    SetWatermarkSqlQueryGenerator,
)
from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryOperator,
)


class TestSetWatermarkSqlQueryGenerator(unittest.TestCase):
    """Unit tests for SetWatermarkSqlQueryGenerator"""

    def setUp(self) -> None:
        self.generator = SetWatermarkSqlQueryGenerator(
            region_code="US_XX",
            get_max_update_datetime_task_id="test_get_max_update_datetime_task_id",
            run_pipeline_task_id="test_dataflow_pipeline_task_id",
        )

    def test_generates_sql_correctly(self) -> None:
        expected_query = """
            INSERT INTO direct_ingest_dataflow_raw_table_upper_bounds
                (region_code, raw_data_file_tag, watermark_datetime, job_id)
            VALUES
                ('US_XX', 'test_raw_data_file_tag', '2023-01-26 00:00:0.000000+00', 'test_job_id'), ('US_XX', 'test_raw_data_file_tag_2', '2023-01-27 00:00:0.000000+00', 'test_job_id');
        """

        data = {
            "test_raw_data_file_tag": "2023-01-26 00:00:0.000000+00",
            "test_raw_data_file_tag_2": "2023-01-27 00:00:0.000000+00",
        }
        result = self.generator.insert_sql_query(
            job_id="test_job_id",
            max_update_datetimes=data,
        )
        self.assertEqual(result, expected_query)

    def test_watermark_retrieved_correctly(self) -> None:
        mock_operator = create_autospec(CloudSqlQueryOperator)
        mock_postgres = create_autospec(PostgresHook)
        mock_context = create_autospec(Context)

        sample_data = {
            "test_raw_data_file_tag": "2023-01-26 00:00:0.000000+00",
            "test_raw_data_file_tag_2": "2023-01-27 00:00:0.000000+00",
        }

        mock_operator.xcom_pull.side_effect = (
            lambda context, key, task_ids: {"id": "test_job_id"}
            if "dataflow_pipeline" in task_ids
            else sample_data
        )

        self.generator.execute_postgres_query(
            mock_operator, mock_postgres, mock_context
        )

        mock_postgres.run.assert_called_with(
            """
            INSERT INTO direct_ingest_dataflow_raw_table_upper_bounds
                (region_code, raw_data_file_tag, watermark_datetime, job_id)
            VALUES
                ('US_XX', 'test_raw_data_file_tag', '2023-01-26 00:00:0.000000+00', 'test_job_id'), ('US_XX', 'test_raw_data_file_tag_2', '2023-01-27 00:00:0.000000+00', 'test_job_id');
        """
        )
