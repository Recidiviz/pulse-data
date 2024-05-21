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
from typing import Dict, Optional

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

from recidiviz.airflow.dags.ingest.metadata import RAW_DATA_FILE_TAG, WATERMARK_DATETIME
from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryGenerator,
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.utils.config_utils import get_ingest_instance


class GetWatermarkSqlQueryGenerator(CloudSqlQueryGenerator[Dict[str, str]]):
    """Custom query generator for getting the max watermark from DirectIngestDataflowRawTableUpperBounds."""

    def __init__(
        self,
        region_code: str,
        # TODO(#27378): Delete this arg and always pull from the context once the ingest
        #  pipelines are only run in the calc DAG.
        ingest_instance: Optional[str],
    ) -> None:
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

        if self.ingest_instance:
            ingest_instance: Optional[str] = self.ingest_instance
        else:
            ingest_instance = get_ingest_instance(context["dag_run"])

        if not ingest_instance:
            raise ValueError(f"Expected to find ingest_instance argument: {context}")

        watermark: Dict[str, str] = {
            row[RAW_DATA_FILE_TAG]: row[WATERMARK_DATETIME].strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            for _, row in postgres_hook.get_pandas_df(
                self.sql_query(
                    region_code=self.region_code, ingest_instance=ingest_instance
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
