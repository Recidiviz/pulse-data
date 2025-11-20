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
"""The CloudSqlQueryGenerator for gathering all discovered, not yet downloaded remote
files for SFTP."""
from typing import Dict, List, Union

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryGenerator,
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.sftp.metadata import REMOTE_FILE_PATH, SFTP_TIMESTAMP


class GatherDiscoveredRemoteFilesSqlQueryGenerator(
    CloudSqlQueryGenerator[List[Dict[str, Union[str, int]]]]
):
    """The CloudSqlQueryGenerator for gathering all discovered, not yet downloaded
    remote files for SFTP."""

    def __init__(self, region_code: str) -> None:
        super().__init__()
        self.region_code = region_code

    # pylint: disable=unused-argument
    def execute_postgres_query(
        self,
        operator: CloudSqlQueryOperator,
        postgres_hook: PostgresHook,
        context: Context,
    ) -> List[Dict[str, Union[str, int]]]:
        # TODO(#53587) Define custom types for operator XCom outputs
        return [
            {
                REMOTE_FILE_PATH: row[REMOTE_FILE_PATH],
                SFTP_TIMESTAMP: int(row[SFTP_TIMESTAMP]),
            }
            for _, row in postgres_hook.get_pandas_df(sql=self.sql_query).iterrows()
        ]

    @property
    def sql_query(self) -> str:
        return f"""
SELECT remote_file_path, sftp_timestamp FROM direct_ingest_sftp_remote_file_metadata
 WHERE region_code = '{self.region_code}' AND file_download_time IS NULL AND
 file_discovery_time IS NOT NULL;"""
