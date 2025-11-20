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
"""The CloudSQLQueryGenerator for filtering out already downloaded SFTP files from
discovered files on the SFTP server."""
from typing import Dict, List, Set, Tuple, Union

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryGenerator,
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.sftp.metadata import REMOTE_FILE_PATH, SFTP_TIMESTAMP
from recidiviz.ingest.direct.sftp.sftp_download_delegate_factory import (
    SftpDownloadDelegateFactory,
)
from recidiviz.utils.types import assert_type


class FilterDownloadedFilesSqlQueryGenerator(
    CloudSqlQueryGenerator[List[Dict[str, Union[str, int]]]]
):
    """Custom query generator for filtering out already downloaded files from SFTP."""

    def __init__(self, region_code: str, find_sftp_files_task_id: str) -> None:
        super().__init__()
        self.region_code = region_code
        self.find_sftp_files_task_id = find_sftp_files_task_id

    def execute_postgres_query(
        self,
        operator: CloudSqlQueryOperator,
        postgres_hook: PostgresHook,
        context: Context,
    ) -> List[Dict[str, Union[str, int]]]:
        """Returns filtered out results from the original SFTP files with the already
        downloaded files."""
        # The find_sftp_files task will always return a List.
        files_from_sftp_with_timestamps: List[
            Dict[str, Union[str, int]]
        ] = operator.xcom_pull(
            context, key="return_value", task_ids=self.find_sftp_files_task_id
        )
        discovered_file_to_timestamp_set: Set[Tuple[str, int]] = {
            (
                assert_type(metadata[REMOTE_FILE_PATH], str),
                assert_type(metadata[SFTP_TIMESTAMP], int),
            )
            for metadata in files_from_sftp_with_timestamps
        }

        downloaded_file_to_timestamp_set: Set[Tuple[str, int]] = (
            {
                (row[REMOTE_FILE_PATH], int(row[SFTP_TIMESTAMP]))
                for _, row in postgres_hook.get_pandas_df(
                    self.sql_query(discovered_file_to_timestamp_set)
                ).iterrows()
            }
            if discovered_file_to_timestamp_set
            else set()
        )

        discovered_not_yet_downloaded_files = (
            discovered_file_to_timestamp_set - downloaded_file_to_timestamp_set
        )

        # Allow state-specific validation of discovered files
        delegate = SftpDownloadDelegateFactory.build(region_code=self.region_code)
        delegate.validate_file_discovery(
            discovered_file_to_timestamp_set=discovered_file_to_timestamp_set,
            already_downloaded_file_to_timestamp_set=downloaded_file_to_timestamp_set,
        )

        # TODO(#53587) Define custom types for operator XCom outputs
        return [
            {REMOTE_FILE_PATH: file, SFTP_TIMESTAMP: timestamp}
            for file, timestamp in discovered_not_yet_downloaded_files
        ]

    def sql_query(self, file_to_timestamp_set: Set[Tuple[str, int]]) -> str:
        sql_tuples = ",".join(
            [
                f"('{file}', {timestamp})"
                for file, timestamp in sorted(list(file_to_timestamp_set))
            ]
        )
        return f"""
SELECT remote_file_path, sftp_timestamp FROM direct_ingest_sftp_remote_file_metadata
 WHERE region_code = '{self.region_code}' AND file_download_time IS NOT NULL
 AND (remote_file_path, sftp_timestamp) IN ({sql_tuples});"""
