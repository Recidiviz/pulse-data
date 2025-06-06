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
"""The CloudSQLQueryGenerator for adding job completion info to DirectIngestDataflowJob."""
from typing import Any, Dict

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryGenerator,
    CloudSqlQueryOperator,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


class AddIngestJobCompletionSqlQueryGenerator(CloudSqlQueryGenerator[None]):
    """Custom query generator for adding job completion info to DirectIngestDataflowJob."""

    def __init__(self, region_code: str, run_pipeline_task_id: str) -> None:
        super().__init__()
        self.region_code = region_code
        self.run_pipeline_task_id = run_pipeline_task_id

    def execute_postgres_query(
        self,
        operator: CloudSqlQueryOperator,
        postgres_hook: PostgresHook,
        context: Context,
    ) -> None:
        """Inserts the job completion info into DirectIngestDataflowJob."""

        pipeline: Dict[str, Any] = operator.xcom_pull(
            context, key="return_value", task_ids=self.run_pipeline_task_id
        )

        postgres_hook.run(
            self.insert_sql_query(
                job_id=pipeline["id"],
                region_code=self.region_code,
                ingest_instance=DirectIngestInstance.PRIMARY.value,
            )
        )

    @staticmethod
    def insert_sql_query(job_id: str, region_code: str, ingest_instance: str) -> str:
        return f"""
            INSERT INTO direct_ingest_dataflow_job
                (job_id, region_code, ingest_instance, completion_time , is_invalidated)
            VALUES
                ('{job_id}', '{region_code.upper()}', '{ingest_instance.upper()}', NOW(), FALSE);
        """
