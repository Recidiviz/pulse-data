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

from recidiviz.airflow.dags.calculation.ingest.metadata import (
    RAW_DATA_FILE_TAG,
    WATERMARK_DATETIME,
)
from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryGenerator,
    CloudSqlQueryOperator,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


class GetWatermarkSqlQueryGenerator(CloudSqlQueryGenerator[Dict[str, str]]):
    """Custom query generator for getting the max watermark from DirectIngestDataflowRawTableUpperBounds."""

    def __init__(self, region_code: str) -> None:
        super().__init__()
        self.region_code = region_code

    def execute_postgres_query(
        self,
        operator: CloudSqlQueryOperator,
        postgres_hook: PostgresHook,
        context: Context,
    ) -> Dict[str, str]:
        """Returns the max watermark from DirectIngestDataflowRawTableUpperBounds."""
        watermark: Dict[str, str] = {
            row[RAW_DATA_FILE_TAG]: row[WATERMARK_DATETIME].strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            for _, row in postgres_hook.get_pandas_df(
                self.sql_query(
                    region_code=self.region_code,
                    ingest_instance=DirectIngestInstance.PRIMARY.value,
                )
            ).iterrows()
        }

        return watermark

    @staticmethod
    def sql_query(region_code: str, ingest_instance: str) -> str:
        return f"""
            SELECT {RAW_DATA_FILE_TAG}, {WATERMARK_DATETIME}
            FROM direct_ingest_dataflow_raw_table_upper_bounds
            WHERE job_id IN (
                SELECT MAX(job_id) 
                FROM direct_ingest_dataflow_job
                WHERE region_code = '{region_code.upper()}' AND ingest_instance = '{ingest_instance.upper()}'
                AND is_invalidated = FALSE
            );
        """
