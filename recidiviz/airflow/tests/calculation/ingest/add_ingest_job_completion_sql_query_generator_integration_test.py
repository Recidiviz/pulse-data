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
"""Integration tests for AddIngestJobCompletionSqlQueryGenerator"""
from unittest.mock import create_autospec

import pytest
from airflow.utils.context import Context

from recidiviz.airflow.dags.calculation.ingest.add_ingest_job_completion_sql_query_generator import (
    AddIngestJobCompletionSqlQueryGenerator,
)
from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryOperator,
)
from recidiviz.airflow.tests.test_utils import AirflowIntegrationTest
from recidiviz.ingest.direct.types.ingest_pipeline_type import IngestPipelineType
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey


@pytest.mark.uses_db
class AddIngestJobCompletionSqlQueryGeneratorIntegrationTest(AirflowIntegrationTest):
    """Integration tests for AddIngestJobCompletionSqlQueryGenerator"""

    operations_db = SQLAlchemyDatabaseKey(
        schema_type=SchemaType.OPERATIONS, db_name=SchemaType.OPERATIONS.name.lower()
    )
    additional_databases_to_create = [operations_db]

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()

    def _run_insert_and_verify(
        self, pipeline_type: IngestPipelineType, job_id: str
    ) -> None:
        """Tests that the insert query writes the correct values for the given
        pipeline_type.
        """
        generator = AddIngestJobCompletionSqlQueryGenerator(
            region_code="US_XX",
            pipeline_type=pipeline_type,
            run_pipeline_task_id="test_dataflow_pipeline_task_id",
        )

        postgres_hook = self.postgres_hooks[self.operations_db]

        mock_operator = create_autospec(CloudSqlQueryOperator)
        mock_context = create_autospec(Context)

        mock_operator.xcom_pull.return_value = {
            "id": job_id,
            "location": "us-east1",
        }

        generator.execute_postgres_query(mock_operator, postgres_hook, mock_context)

        result = postgres_hook.get_first(
            """
            SELECT job_id, region_code, location, ingest_instance, pipeline_type, is_invalidated
            FROM direct_ingest_dataflow_job
            WHERE job_id = %(job_id)s
            """,
            parameters={"job_id": job_id},
        )

        self.assertIsNotNone(result)
        self.assertEqual(result[0], job_id)
        self.assertEqual(result[1], "US_XX")
        self.assertEqual(result[2], "us-east1")
        self.assertEqual(result[3], "PRIMARY")
        self.assertEqual(result[4], pipeline_type.value)
        self.assertFalse(result[5])

    def test_insert_query_executes_successfully_for_activity(self) -> None:
        self._run_insert_and_verify(
            pipeline_type=IngestPipelineType.ACTIVITY,
            job_id="test_activity_job_id",
        )

    def test_insert_query_executes_successfully_for_identity(self) -> None:
        self._run_insert_and_verify(
            pipeline_type=IngestPipelineType.IDENTITY,
            job_id="test_identity_job_id",
        )
