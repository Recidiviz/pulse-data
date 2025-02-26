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
"""The CloudSQLQueryGenerator for setting the watermark in DirectIngestDataflowRawTableUpperBounds."""
from typing import Dict

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

from recidiviz.airflow.dags.ingest.metadata import RAW_DATA_FILE_TAG, WATERMARK_DATETIME
from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryGenerator,
    CloudSqlQueryOperator,
)
from recidiviz.persistence.database.schema.operations.schema import (
    DirectIngestDataflowRawTableUpperBounds,
)


class SetWatermarkSqlQueryGenerator(CloudSqlQueryGenerator[None]):
    """Custom query generator for setting the watermark in DirectIngestDataflowRawTableUpperBounds."""

    def __init__(
        self,
        region_code: str,
        ingest_instance: str,
        get_max_update_datetime_task_id: str,
        dataflow_pipeline_task_id: str,
    ) -> None:
        super().__init__()
        self.region_code = region_code
        self.ingest_instance = ingest_instance
        self.get_max_update_datetime_task_id = get_max_update_datetime_task_id
        self.dataflow_pipeline_task_id = dataflow_pipeline_task_id

    def execute_postgres_query(
        self,
        operator: CloudSqlQueryOperator,
        postgres_hook: PostgresHook,
        context: Context,
    ) -> None:
        """Returns the max watermark from DirectIngestDataflowRawTableUpperBounds."""

        max_update_datetimes: Dict[str, str] = operator.xcom_pull(
            context, key="return_value", task_ids=self.get_max_update_datetime_task_id
        )

        job_id: str = operator.xcom_pull(
            context, key="return_value", task_ids=self.dataflow_pipeline_task_id
        )

        postgres_hook.run(
            self.insert_sql_query(job_id, max_update_datetimes),
        )

    def insert_sql_query(
        self,
        job_id: str,
        max_update_datetimes: Dict[str, str],
    ) -> str:

        values = ", ".join(
            [
                f"('{self.region_code.upper()}', '{file_tag}', '{max_update_datetime}', '{job_id}')"
                for file_tag, max_update_datetime in max_update_datetimes.items()
            ]
        )

        return f"""
            INSERT INTO {DirectIngestDataflowRawTableUpperBounds.__tablename__}
                (region_code, {RAW_DATA_FILE_TAG}, {WATERMARK_DATETIME}, job_id)
            VALUES
                {values};
        """
