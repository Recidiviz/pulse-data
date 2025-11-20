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
"""The CloudSqlQueryGenerator for marking all SFTP ingest ready files as discovered."""
from typing import Dict, List, Set, Tuple, Union

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryGenerator,
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.sftp.metadata import (
    POST_PROCESSED_NORMALIZED_FILE_PATH,
    REMOTE_FILE_PATH,
)
from recidiviz.airflow.dags.utils.cloud_sql import (
    postgres_formatted_current_datetime_utc_str,
)
from recidiviz.utils.types import assert_type


class MarkIngestReadyFilesDiscoveredSqlQueryGenerator(
    CloudSqlQueryGenerator[List[Dict[str, Union[str, int]]]]
):
    """Custom query generator for marking all ingest ready files as discovered."""

    def __init__(self, region_code: str, filter_invalid_gcs_files_task_id: str) -> None:
        super().__init__()
        self.region_code = region_code
        self.filter_invalid_gcs_files_task_id = filter_invalid_gcs_files_task_id

    def execute_postgres_query(
        self,
        operator: "CloudSqlQueryOperator",
        postgres_hook: PostgresHook,
        context: Context,
    ) -> List[Dict[str, Union[str, int]]]:
        """Returns the original list of all files to download after marking all new files
        as discovered in the Postgres database."""
        # The prior step should always return a List
        post_processed_file_metadatas: List[
            Dict[str, Union[str, int]]
        ] = operator.xcom_pull(
            context,
            key="return_value",
            task_ids=self.filter_invalid_gcs_files_task_id,
        )
        post_processed_file_to_remote_file_set: Set[Tuple[str, str]] = {
            (
                assert_type(metadata[POST_PROCESSED_NORMALIZED_FILE_PATH], str),
                assert_type(metadata[REMOTE_FILE_PATH], str),
            )
            for metadata in post_processed_file_metadatas
        }

        discovered_file_to_timestamp_set: Set[Tuple[str, str]] = (
            {
                (row[POST_PROCESSED_NORMALIZED_FILE_PATH], row[REMOTE_FILE_PATH])
                for _, row in postgres_hook.get_pandas_df(
                    self.exists_sql_query(post_processed_file_to_remote_file_set)
                ).iterrows()
            }
            if post_processed_file_to_remote_file_set
            else set()
        )

        files_to_mark_discovered: Set[Tuple[str, str]] = (
            post_processed_file_to_remote_file_set - discovered_file_to_timestamp_set
        )

        if files_to_mark_discovered:
            postgres_hook.run(self.insert_sql_query(files_to_mark_discovered))

        # Due to how Airflow wraps XCOM values, we need to access the underlying
        # dictionary in order to properly serialize for the next task
        # TODO(#53587) Define custom types for operator XCom outputs
        return [{**metadata} for metadata in post_processed_file_metadatas]

    def exists_sql_query(
        self, post_processed_file_to_remote_file_set: Set[Tuple[str, str]]
    ) -> str:
        sql_tuples = ",".join(
            [
                f"('{post_processed_file}', '{remote_file}')"
                for post_processed_file, remote_file in sorted(
                    list(post_processed_file_to_remote_file_set)
                )
            ]
        )
        return f"""
SELECT post_processed_normalized_file_path, remote_file_path FROM
 direct_ingest_sftp_ingest_ready_file_metadata
 WHERE region_code = '{self.region_code}' AND file_upload_time IS NULL
 AND (post_processed_normalized_file_path, remote_file_path) IN ({sql_tuples});"""

    def insert_sql_query(self, files_to_mark_discovered: Set[Tuple[str, str]]) -> str:
        values = ",".join(
            [
                f"\n('{self.region_code}', '{post_processed_file}', '{remote_file}', '{postgres_formatted_current_datetime_utc_str()}')"
                for post_processed_file, remote_file in sorted(
                    list(files_to_mark_discovered)
                )
            ]
        )

        return f"""
INSERT INTO direct_ingest_sftp_ingest_ready_file_metadata
(region_code, post_processed_normalized_file_path, remote_file_path, file_discovery_time)
VALUES{values};"""
