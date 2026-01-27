# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""A CloudSQL generator for retrieving most recent SFTP ingest ready file upload times by region"""
import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryGenerator,
    CloudSqlQueryOperator,
)

GET_SFTP_UPLOAD_TIMES_QUERY = """
SELECT
    region_code,
    MAX(file_upload_time) as most_recent_upload_time
FROM direct_ingest_sftp_ingest_ready_file_metadata
WHERE file_upload_time IS NOT NULL
GROUP BY region_code
ORDER BY region_code;
"""


class SftpIngestReadyFileUploadTimesSqlQueryGenerator(
    CloudSqlQueryGenerator[dict[str, str]]
):
    """A CloudSQL query generator that retrieves the most recent datetime that we uploaded
    files to the state ingest bucket for each SFTP region.

    Returns a dictionary mapping region_code to ISO-formatted utc upload datetime string.
    """

    def execute_postgres_query(
        self,
        operator: CloudSqlQueryOperator,
        postgres_hook: PostgresHook,
        context: Context,
    ) -> dict[str, str]:
        results: list[tuple[str, datetime.datetime]] = postgres_hook.get_records(
            GET_SFTP_UPLOAD_TIMES_QUERY
        )

        if not results:
            raise ValueError(
                "No SFTP ingest ready file upload times found in postgres."
            )

        return {
            region_code: utc_upload_time.isoformat()
            for region_code, utc_upload_time in results
        }
