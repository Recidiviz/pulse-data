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
"""The CloudSQLQueryGenerator for getting the max update datetime from direct_ingest_raw_file_metadata."""
from typing import Dict

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

from recidiviz.airflow.dags.calculation.ingest.metadata import (
    FILE_TAG,
    MAX_UPDATE_DATETIME,
)
from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryGenerator,
    CloudSqlQueryOperator,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.string import StrictStringFormatter

MAX_UPDATE_DATETIMES_QUERY = """
SELECT {file_tag}, MAX(update_datetime) AS {max_update_datetime}
FROM direct_ingest_raw_big_query_file_metadata
WHERE raw_data_instance = '{raw_data_instance}' 
AND is_invalidated IS FALSE
AND file_processed_time IS NOT NULL 
AND region_code = '{region_code}'
GROUP BY {file_tag};
"""


class GetMaxUpdateDateTimeSqlQueryGenerator(CloudSqlQueryGenerator[Dict[str, str]]):
    """Custom query generator for getting the max update datetime from direct_ingest_raw_file_metadata."""

    def __init__(self, region_code: str) -> None:
        super().__init__()
        self.region_code = region_code

    def execute_postgres_query(
        self,
        operator: CloudSqlQueryOperator,
        postgres_hook: PostgresHook,
        context: Context,
    ) -> Dict[str, str]:
        """Returns the max update datetime from direct_ingest_raw_file_metadata."""
        max_update_datetimes: Dict[str, str] = {
            row[FILE_TAG]: row[MAX_UPDATE_DATETIME].strftime("%Y-%m-%d %H:%M:%S.%f")
            for _, row in postgres_hook.get_pandas_df(
                self.update_datetimes_sql_query(
                    region_code=self.region_code,
                    ingest_instance=DirectIngestInstance.PRIMARY.value,
                )
            ).iterrows()
        }

        return max_update_datetimes

    @staticmethod
    def update_datetimes_sql_query(region_code: str, ingest_instance: str) -> str:
        return StrictStringFormatter().format(
            MAX_UPDATE_DATETIMES_QUERY,
            file_tag=FILE_TAG,
            max_update_datetime=MAX_UPDATE_DATETIME,
            raw_data_instance=ingest_instance.upper(),
            region_code=region_code.upper(),
        )
