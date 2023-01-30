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
import datetime
from typing import Dict, List, Set, Tuple, Union

import pytz
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


class MarkFilesDiscoveredSqlQueryGenerator(
    CloudSqlQueryGenerator[List[Dict[str, Union[str, int]]]]
):
    """Custom query generator for marking all files as discovered."""

    def __init__(self, region_code: str, filter_downloaded_files_task_id: str) -> None:
        super().__init__()
        self.region_code = region_code
        self.filter_downloaded_files_task_id = filter_downloaded_files_task_id

    def execute_postgres_query(
        self,
        operator: "CloudSqlQueryOperator",
        postgres_hook: PostgresHook,
        context: Context,
    ) -> List[Dict[str, Union[str, int]]]:
        """Returns the original list of all files to download after marking all new files
        as discovered in the Postgres database."""
        sftp_files_with_timestamps: List[
            Dict[str, Union[str, int]]
        ] = operator.xcom_pull(
            context, key="return_value", task_ids=self.filter_downloaded_files_task_id
        )
        sftp_file_to_timestamp_set: Set[Tuple[str, int]] = {
            (
                assert_type(metadata["remote_file_path"], str),
                assert_type(metadata["sftp_timestamp"], int),
            )
            for metadata in sftp_files_with_timestamps
        }

        already_discovered_df = postgres_hook.get_pandas_df(
            self.exists_sql_query(sftp_file_to_timestamp_set)
        )
        discovered_file_to_timestamp_set: Set[Tuple[str, int]] = {
            (row["remote_file_path"], row["sftp_timestamp"])
            for _, row in already_discovered_df.iterrows()
        }

        files_to_mark_discovered: Set[Tuple[str, int]] = (
            sftp_file_to_timestamp_set - discovered_file_to_timestamp_set
        )

        if files_to_mark_discovered:
            postgres_hook.run(self.insert_sql_query(files_to_mark_discovered))

        return sftp_files_with_timestamps

    def exists_sql_query(self, file_to_timestamp_set: Set[Tuple[str, int]]) -> str:
        sql_tuples = ",".join(
            [
                f"('{file}', {timestamp})"
                for file, timestamp in sorted(list(file_to_timestamp_set))
            ]
        )
        return f"""
SELECT remote_file_path, sftp_timestamp FROM direct_ingest_sftp_remote_file_metadata
 WHERE file_download_time IS NULL AND (remote_file_path, sftp_timestamp) IN ({sql_tuples});"""

    def insert_sql_query(self, files_to_mark_discovered: Set[Tuple[str, int]]) -> str:
        current_date = datetime.datetime.now(tz=pytz.UTC).strftime(
            "%Y-%m-%d %H:%M:%S.%f %Z"
        )
        values = ",".join(
            [
                f"\n('{self.region_code}', '{file}', {timestamp}, '{current_date}')"
                for file, timestamp in sorted(list(files_to_mark_discovered))
            ]
        )

        return f"""
INSERT INTO direct_ingest_sftp_remote_file_metadata (region_code, remote_file_path, sftp_timestamp, file_discovery_time)
VALUES{values};"""
