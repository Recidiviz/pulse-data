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
"""Unit tests for AddIngestJobCompletionSqlQueryGenerator"""
import unittest
from unittest.mock import Mock, create_autospec

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

from recidiviz.airflow.dags.ingest.add_ingest_job_completion_sql_query_generator import (
    AddIngestJobCompletionSqlQueryGenerator,
)
from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryOperator,
)


class TestAddIngestJobCompletionSqlQueryGenerator(unittest.TestCase):
    """Unit tests for AddIngestJobCompletionSqlQueryGenerator"""

    def test_generates_sql_correctly(self) -> None:
        expected_query = """
            INSERT INTO direct_ingest_dataflow_job
                (job_id, region_code, ingest_instance, completion_time , is_invalidated)
            VALUES
                ('test_job_id', 'US_XX', 'PRIMARY', NOW(), FALSE);
        """

        result = AddIngestJobCompletionSqlQueryGenerator.insert_sql_query(
            job_id="test_job_id", region_code="US_XX", ingest_instance="PRIMARY"
        )
        self.assertEqual(result, expected_query)

    # TODO(#27378): Delete when we remove the ingest_instance param
    #  from AddIngestJobCompletionSqlQueryGenerator.
    def test_insert_statement_generated_correctly_legacy(self) -> None:
        generator = AddIngestJobCompletionSqlQueryGenerator(
            region_code="US_XX",
            ingest_instance="PRIMARY",
            run_pipeline_task_id="test_dataflow_pipeline_task_id",
        )
        mock_operator = create_autospec(CloudSqlQueryOperator)
        mock_postgres = create_autospec(PostgresHook)
        mock_context = create_autospec(Context)

        mock_operator.xcom_pull.return_value = {"id": "test_job_id"}

        generator.execute_postgres_query(mock_operator, mock_postgres, mock_context)

        mock_postgres.run.assert_called_with(
            """
            INSERT INTO direct_ingest_dataflow_job
                (job_id, region_code, ingest_instance, completion_time , is_invalidated)
            VALUES
                ('test_job_id', 'US_XX', 'PRIMARY', NOW(), FALSE);
        """
        )

    def test_insert_statement_generated_correctly(self) -> None:
        generator = AddIngestJobCompletionSqlQueryGenerator(
            region_code="US_XX",
            ingest_instance=None,
            run_pipeline_task_id="test_dataflow_pipeline_task_id",
        )
        mock_operator = create_autospec(CloudSqlQueryOperator)
        mock_postgres = create_autospec(PostgresHook)

        dag_run = Mock()
        dag_run.conf = {"ingest_instance": "PRIMARY"}
        dag_run.dag_id = "test_dag"
        dag_run.run_id = "test_run"
        mock_context = {"dag_run": dag_run}

        mock_operator.xcom_pull.return_value = {"id": "test_job_id"}

        generator.execute_postgres_query(mock_operator, mock_postgres, mock_context)

        mock_postgres.run.assert_called_with(
            """
            INSERT INTO direct_ingest_dataflow_job
                (job_id, region_code, ingest_instance, completion_time , is_invalidated)
            VALUES
                ('test_job_id', 'US_XX', 'PRIMARY', NOW(), FALSE);
        """
        )
