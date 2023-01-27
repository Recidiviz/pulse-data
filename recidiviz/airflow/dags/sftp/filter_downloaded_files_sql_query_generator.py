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

from recidiviz.utils.types import assert_type

# Custom Airflow operators in the recidiviz.airflow.dags.operators package are imported
# into the Cloud Composer environment at the top-level. However, for unit tests, we
# still need to import the recidiviz-top-level.
# pylint: disable=ungrouped-imports
try:
    from operators.cloud_sql_query_operator import (  # type: ignore
        CloudSqlQueryGenerator,
        CloudSqlQueryOperator,
    )
except ImportError:
    from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
        CloudSqlQueryGenerator,
        CloudSqlQueryOperator,
    )


class FilterDownloadedFilesSqlQueryGenerator(
    CloudSqlQueryGenerator[List[Dict[str, Union[str, int]]]]
):
    """Custom query generator for filtering out already downloaded files from SFTP."""

    def __init__(self, find_sftp_files_task_id: str) -> None:
        super().__init__()
        self.find_sftp_files_task_id = find_sftp_files_task_id

    def execute_postgres_query(
        self,
        operator: CloudSqlQueryOperator,
        postgres_hook: PostgresHook,
        context: Context,
    ) -> List[Dict[str, Union[str, int]]]:
        """Returns filtered out results from the original SFTP files with the already
        downloaded files."""
        files_from_sftp_with_timestamps: List[
            Dict[str, Union[str, int]]
        ] = operator.xcom_pull(
            context, key="return_value", task_ids=self.find_sftp_files_task_id
        )
        file_to_timestamp_set: Set[Tuple[str, int]] = {
            (
                assert_type(metadata["file"], str),
                assert_type(metadata["timestamp"], int),
            )
            for metadata in files_from_sftp_with_timestamps
        }

        sql = self.sql_query(file_to_timestamp_set)

        already_downloaded_df = postgres_hook.get_pandas_df(sql)
        downloaded_file_to_timestamp_set: Set[Tuple[str, int]] = {
            (row["remote_file_path"], int(row["sftp_timestamp"]))
            for _, row in already_downloaded_df.iterrows()
        }

        return [
            {"remote_file_path": file, "sftp_timestamp": timestamp}
            for file, timestamp in file_to_timestamp_set
            - downloaded_file_to_timestamp_set
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
 WHERE file_download_time IS NOT NULL AND (remote_file_path, sftp_timestamp) IN ({sql_tuples});"""
