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
"""The CloudSQLQueryGenerator for marking all SFTP files as discovered."""
from typing import Dict, List, Set, Tuple, Union

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryGenerator,
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.sftp.metadata import REMOTE_FILE_PATH, SFTP_TIMESTAMP
from recidiviz.airflow.dags.utils.cloud_sql import (
    postgres_formatted_current_datetime_utc_str,
)
from recidiviz.utils.types import assert_type


class MarkRemoteFilesDiscoveredSqlQueryGenerator(
    CloudSqlQueryGenerator[List[Dict[str, Union[str, int]]]]
):
    """Custom query generator for marking all files as discovered."""

    def __init__(self, region_code: str, filter_downloaded_files_task_id: str) -> None:
        super().__init__()
        self.region_code = region_code
        self.filter_downloaded_files_task_id = filter_downloaded_files_task_id

    def execute_postgres_query(
        self,
        operator: CloudSqlQueryOperator,
        postgres_hook: PostgresHook,
        context: Context,
    ) -> List[Dict[str, Union[str, int]]]:
        """Returns the original list of all files to download after marking all new files
        as discovered in the Postgres database."""
        # The filter_download_files task will always return a List.
        sftp_files_with_timestamps: List[
            Dict[str, Union[str, int]]
        ] = operator.xcom_pull(
            context,
            key="return_value",
            task_ids=self.filter_downloaded_files_task_id,
        )
        sftp_file_to_timestamp_set: Set[Tuple[str, int]] = {
            (
                assert_type(metadata[REMOTE_FILE_PATH], str),
                assert_type(metadata[SFTP_TIMESTAMP], int),
            )
            for metadata in sftp_files_with_timestamps
        }

        discovered_file_to_timestamp_set: Set[Tuple[str, int]] = (
            {
                (row[REMOTE_FILE_PATH], row[SFTP_TIMESTAMP])
                for _, row in postgres_hook.get_pandas_df(
                    self.exists_sql_query(sftp_file_to_timestamp_set)
                ).iterrows()
            }
            if sftp_file_to_timestamp_set
            else set()
        )

        files_to_mark_discovered: Set[Tuple[str, int]] = (
            sftp_file_to_timestamp_set - discovered_file_to_timestamp_set
        )

        if files_to_mark_discovered:
            postgres_hook.run(self.insert_sql_query(files_to_mark_discovered))

        # Due to how Airflow wraps XCOM values, we need to access the underlying
        # dictionary in order to properly serialize for the next task
        # TODO(#53587) Define custom types for operator XCom outputs
        return [{**metadata} for metadata in sftp_files_with_timestamps]

    def exists_sql_query(self, file_to_timestamp_set: Set[Tuple[str, int]]) -> str:
        sql_tuples = ",".join(
            [
                f"('{file}', {timestamp})"
                for file, timestamp in sorted(list(file_to_timestamp_set))
            ]
        )
        return f"""
SELECT remote_file_path, sftp_timestamp FROM direct_ingest_sftp_remote_file_metadata
 WHERE region_code = '{self.region_code}' AND file_download_time IS NULL
 AND (remote_file_path, sftp_timestamp) IN ({sql_tuples});"""

    def insert_sql_query(self, files_to_mark_discovered: Set[Tuple[str, int]]) -> str:
        values = ",".join(
            [
                f"\n('{self.region_code}', '{file}', {timestamp}, '{postgres_formatted_current_datetime_utc_str()}')"
                for file, timestamp in sorted(list(files_to_mark_discovered))
            ]
        )

        return f"""
INSERT INTO direct_ingest_sftp_remote_file_metadata (region_code, remote_file_path, sftp_timestamp, file_discovery_time)
VALUES{values};"""
