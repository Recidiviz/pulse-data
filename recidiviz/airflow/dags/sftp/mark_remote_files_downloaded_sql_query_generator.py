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
"""The CloudSqlQueryGenerator for marking SFTP files as downloaded."""
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


class MarkRemoteFilesDownloadedSqlQueryGenerator(
    CloudSqlQueryGenerator[List[Dict[str, Union[str, int]]]]
):
    """Custom query generator for marking all files as downloaded."""

    def __init__(self, region_code: str, post_process_sftp_files_task_id: str) -> None:
        self.region_code = region_code
        self.post_process_sftp_files_task_id = post_process_sftp_files_task_id
        super().__init__()

    def execute_postgres_query(
        self,
        operator: CloudSqlQueryOperator,
        postgres_hook: PostgresHook,
        context: Context,
    ) -> List[Dict[str, Union[str, int]]]:
        """Returns the list of all files that were downloaded and post processed
        after marking all files as downloaded in the Postgres database."""
        # Based on the prior check, this list should always be non-empty and because
        # post_process_sftp_files returns a List[List[Dict]], we first need to flatten it.
        sftp_files_with_timestamps_and_downloaded_paths: List[
            List[Dict[str, Union[str, int]]]
        ] = operator.xcom_pull(
            context, key="return_value", task_ids=self.post_process_sftp_files_task_id
        )
        sftp_files_with_timestamp_set: Set[Tuple[str, int]] = {
            (
                assert_type(metadata[REMOTE_FILE_PATH], str),
                assert_type(metadata[SFTP_TIMESTAMP], int),
            )
            for metadata_list in sftp_files_with_timestamps_and_downloaded_paths
            for metadata in metadata_list
        }

        if sftp_files_with_timestamp_set:
            postgres_hook.run(self.update_sql_query(sftp_files_with_timestamp_set))

        # Due to how Airflow wraps XCOM values, we need to access the underlying
        # dictionary in order to properly serialize for the next task
        # TODO(#53587) Define custom types for operator XCom outputs
        return [
            {**metadata}
            for metadata_list in sftp_files_with_timestamps_and_downloaded_paths
            for metadata in metadata_list
        ]

    def update_sql_query(
        self,
        sftp_files_with_timestamp_set: Set[Tuple[str, int]],
    ) -> str:
        sql_tuples = ",".join(
            [
                f"('{file}', {timestamp})"
                for file, timestamp in list(sorted(sftp_files_with_timestamp_set))
            ]
        )

        return f"""
UPDATE direct_ingest_sftp_remote_file_metadata SET file_download_time = '{postgres_formatted_current_datetime_utc_str()}'
WHERE region_code = '{self.region_code}' AND (remote_file_path, sftp_timestamp) IN ({sql_tuples});"""
