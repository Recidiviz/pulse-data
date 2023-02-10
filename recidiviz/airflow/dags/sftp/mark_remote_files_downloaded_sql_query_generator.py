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
"""The CloudSQLQueryGenerator for marking SFTP files as downloaded."""
import datetime
from typing import Dict, List, Optional, Set, Tuple, Union

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
    from sftp.metadata import REMOTE_FILE_PATH, SFTP_TIMESTAMP  # type: ignore
except ImportError:
    from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
        CloudSqlQueryGenerator,
        CloudSqlQueryOperator,
    )
    from recidiviz.airflow.dags.sftp.metadata import REMOTE_FILE_PATH, SFTP_TIMESTAMP


class MarkRemoteFilesDownloadedSqlQueryGenerator(
    CloudSqlQueryGenerator[List[Dict[str, Union[str, int]]]]
):
    """Custom query generator for marking all files as downloaded."""

    def __init__(self, region_code: str, download_sftp_files_task_id: str) -> None:
        self.region_code = region_code
        self.download_sftp_files_task_id = download_sftp_files_task_id

    def execute_postgres_query(
        self,
        operator: "CloudSqlQueryOperator",
        postgres_hook: PostgresHook,
        context: Context,
    ) -> List[Dict[str, Union[str, int]]]:
        """Returns the list of all files that were downloaded and should be post processed
        after marking all files as downloaded in the Postgres database."""
        sftp_files_with_timestamps_and_downloaded_paths: Optional[
            List[Dict[str, Union[str, int]]]
        ] = operator.xcom_pull(
            context, key="return_value", task_ids=self.download_sftp_files_task_id
        )
        if sftp_files_with_timestamps_and_downloaded_paths:
            sftp_files_with_timestamp_set: Set[Tuple[str, int]] = {
                (
                    assert_type(metadata[REMOTE_FILE_PATH], str),
                    assert_type(metadata[SFTP_TIMESTAMP], int),
                )
                for metadata in sftp_files_with_timestamps_and_downloaded_paths
            }

            postgres_hook.run(self.update_sql_query(sftp_files_with_timestamp_set))

            return sftp_files_with_timestamps_and_downloaded_paths
        return []

    def update_sql_query(
        self,
        sftp_files_with_timestamp_set: Set[Tuple[str, int]],
    ) -> str:
        current_date = datetime.datetime.now(tz=pytz.UTC).strftime(
            "%Y-%m-%d %H:%M:%S.%f %Z"
        )
        sql_tuples = ",".join(
            [
                f"('{file}', {timestamp})"
                for file, timestamp in list(sorted(sftp_files_with_timestamp_set))
            ]
        )

        return f"""
UPDATE direct_ingest_sftp_remote_file_metadata SET file_download_time = '{current_date}'
WHERE region_code = '{self.region_code}' AND (remote_file_path, sftp_timestamp) IN ({sql_tuples});"""
