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
"""The CloudSQLQueryGenerator for getting the max watermark from DirectIngestDataflowRawTableUpperBounds."""
from typing import Dict

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

from recidiviz.airflow.dags.ingest.metadata import RAW_DATA_FILE_TAG, WATERMARK_DATETIME
from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryGenerator,
    CloudSqlQueryOperator,
)
from recidiviz.persistence.database.schema.operations.schema import (
    DirectIngestDataflowJob,
    DirectIngestDataflowRawTableUpperBounds,
)


class GetWatermarkSqlQueryGenerator(CloudSqlQueryGenerator[Dict[str, str]]):
    """Custom query generator for getting the max watermark from DirectIngestDataflowRawTableUpperBounds."""

    def __init__(self, region_code: str, ingest_instance: str) -> None:
        super().__init__()
        self.region_code = region_code
        self.ingest_instance = ingest_instance

    def execute_postgres_query(
        self,
        operator: CloudSqlQueryOperator,
        postgres_hook: PostgresHook,
        context: Context,
    ) -> Dict[str, str]:
        """Returns the max watermark from DirectIngestDataflowRawTableUpperBounds."""

        watermark: Dict[str, str] = {
            row[RAW_DATA_FILE_TAG]: row[WATERMARK_DATETIME].isoformat()
            for _, row in postgres_hook.get_pandas_df(self.sql_query()).iterrows()
        }

        return watermark

    def sql_query(self) -> str:
        return f"""
            SELECT {RAW_DATA_FILE_TAG}, {WATERMARK_DATETIME}
            FROM {DirectIngestDataflowRawTableUpperBounds.__tablename__}
            WHERE job_id IN (
                SELECT MAX(job_id) 
                FROM {DirectIngestDataflowJob.__tablename__}
                WHERE region_code = '{self.region_code.upper()}' AND ingest_instance = '{self.ingest_instance.upper()}'
            );
        """
