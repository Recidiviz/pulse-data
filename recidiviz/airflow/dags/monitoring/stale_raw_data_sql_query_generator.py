# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""A CloudSQL generator for retrieving the most recent successful import
update_datetime per (region_code, file_tag) for files with raw_rows > 0."""
import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryGenerator,
    CloudSqlQueryOperator,
)

GET_MOST_RECENT_RAW_DATA_UPDATE_DATETIMES_QUERY = """
SELECT
    bq.region_code,
    bq.file_tag,
    MAX(bq.update_datetime) AS most_recent_update_datetime
FROM direct_ingest_raw_file_import AS fi
INNER JOIN direct_ingest_raw_big_query_file_metadata AS bq
    ON fi.file_id = bq.file_id
WHERE fi.import_status = 'SUCCEEDED'
  AND fi.raw_data_instance = 'PRIMARY'
  AND fi.raw_rows > 0
  AND bq.is_invalidated = FALSE 
GROUP BY bq.region_code, bq.file_tag;
"""


class StaleRawDataSqlQueryGenerator(CloudSqlQueryGenerator[dict[str, dict[str, str]]]):
    """A CloudSQL query generator that retrieves the most recent update_datetime
    for each (region_code, file_tag) pair that has had at least one successful import
    with raw_rows > 0.

    Returns a nested dictionary: {region_code: {file_tag: ISO-formatted utc datetime string}}.
    """

    def execute_postgres_query(
        self,
        operator: CloudSqlQueryOperator,
        postgres_hook: PostgresHook,
        context: Context,
    ) -> dict[str, dict[str, str]]:
        results: list[tuple[str, str, datetime.datetime]] = postgres_hook.get_records(
            GET_MOST_RECENT_RAW_DATA_UPDATE_DATETIMES_QUERY
        )

        if not results:
            raise ValueError("No successful raw data file imports found in postgres.")

        update_datetimes_by_region: dict[str, dict[str, str]] = {}
        for region_code, file_tag, most_recent_update_datetime in results:
            update_datetimes_by_region.setdefault(region_code, {})[
                file_tag
            ] = most_recent_update_datetime.isoformat()

        return update_datetimes_by_region
