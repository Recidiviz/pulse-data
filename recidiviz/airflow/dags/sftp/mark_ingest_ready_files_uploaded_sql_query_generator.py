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
"""The CloudSQLQueryGenerator for marking SFTP files as uploaded."""
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


class MarkIngestReadyFilesUploadedSqlQueryGenerator(
    CloudSqlQueryGenerator[List[Dict[str, Union[str, int]]]]
):
    """Custom query generator for marking all ingest ready files as uploaded."""

    def __init__(
        self, region_code: str, upload_files_to_ingest_bucket_task_id: str
    ) -> None:
        self.region_code = region_code
        self.upload_files_to_ingest_bucket_task_id = (
            upload_files_to_ingest_bucket_task_id
        )

    def execute_postgres_query(
        self,
        operator: "CloudSqlQueryOperator",
        postgres_hook: PostgresHook,
        context: Context,
    ) -> List[Dict[str, Union[str, int]]]:
        """Returns the list of all files that were downloaded and should be post processed
        after marking all files as downloaded in the Postgres database."""
        # Based on the prior check, this list should always be non-empty.
        ingest_ready_files_with_uploaded_paths: List[
            Dict[str, Union[str, int]]
        ] = operator.xcom_pull(
            context,
            key="return_value",
            task_ids=self.upload_files_to_ingest_bucket_task_id,
        )
        post_processed_path_to_remote_path_set: Set[Tuple[str, str]] = {
            (
                assert_type(metadata[POST_PROCESSED_NORMALIZED_FILE_PATH], str),
                assert_type(metadata[REMOTE_FILE_PATH], str),
            )
            for metadata in ingest_ready_files_with_uploaded_paths
        }
        if post_processed_path_to_remote_path_set:
            postgres_hook.run(
                self.update_sql_query(post_processed_path_to_remote_path_set)
            )

        # Due to how Airflow wraps XCOM values, we need to access the underlying
        # dictionary in order to properly serialize for the next task
        # TODO(#53587) Define custom types for operator XCom outputs
        return [{**metadata} for metadata in ingest_ready_files_with_uploaded_paths]

    def update_sql_query(
        self,
        post_processed_path_to_remote_path_set: Set[Tuple[str, str]],
    ) -> str:
        sql_tuples = ",".join(
            [
                f"('{post_processed_normalized_file_path}', '{remote_file_path}')"
                for post_processed_normalized_file_path, remote_file_path in list(
                    sorted(post_processed_path_to_remote_path_set)
                )
            ]
        )

        return f"""
UPDATE direct_ingest_sftp_ingest_ready_file_metadata SET file_upload_time = '{postgres_formatted_current_datetime_utc_str()}'
WHERE region_code = '{self.region_code}' AND (post_processed_normalized_file_path, remote_file_path) IN ({sql_tuples});"""
