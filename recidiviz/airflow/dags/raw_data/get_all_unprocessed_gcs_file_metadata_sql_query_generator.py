# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""A CloudSQLQueryGenerator that processes raw files paths and returns gcs file metadata"""
import datetime
from typing import List

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryGenerator,
    CloudSqlQueryOperator,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.gcs.filename_parts import filename_parts_from_path
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_types import RawGCSFileMetadata
from recidiviz.utils.string import StrictStringFormatter

GET_EXISTING_FILES_BY_PATH = """
SELECT gcs_file_id, file_id, normalized_file_name
FROM direct_ingest_raw_gcs_file_metadata
WHERE region_code = '{region_code}'
AND raw_data_instance = '{raw_data_instance}'
AND is_invalidated is FALSE
AND normalized_file_name in ({normalized_file_names});"""


# file_id on this query will always be None, but is included in the RETURNING statement
# to match the schema of the existing files by path return format
ADD_ROWS_WITHOUT_FILE_ID = """
INSERT INTO direct_ingest_raw_gcs_file_metadata (region_code, raw_data_instance, file_tag, normalized_file_name, update_datetime, file_discovery_time, is_invalidated) 
VALUES {values}
RETURNING gcs_file_id, file_id, normalized_file_name;"""


class GetAllUnprocessedGCSFileMetadataSqlQueryGenerator(
    CloudSqlQueryGenerator[List[str]]
):
    """Custom query generator that processes raw files paths and returns raw gcs file
    metadata
    """

    def __init__(
        self,
        region_code: str,
        raw_data_instance: DirectIngestInstance,
        list_normalized_unprocessed_gcs_file_paths_task_id: str,
    ) -> None:
        super().__init__()
        self._region_code = region_code
        self._raw_data_instance = raw_data_instance
        self._list_normalized_unprocessed_gcs_file_paths_task_id = (
            list_normalized_unprocessed_gcs_file_paths_task_id
        )

    def execute_postgres_query(
        self,
        operator: CloudSqlQueryOperator,
        postgres_hook: PostgresHook,
        context: Context,
    ) -> List[str]:
        """After pulling in a list of unprocessed paths from xcom, registers not yet
        seen gcs paths with the raw gcs file metadata table. Returns gcs file metadata
        in the form of (gcs_file_id, file_id, abs_path) for every unprocessed file pulled
        from xcom.
        """
        # --- get existing path info from xcom -----------------------------------------

        unprocessed_paths_in_bucket = [
            GcsfsFilePath.from_absolute_path(path)
            for path in operator.xcom_pull(
                context,
                key="return_value",
                task_ids=self._list_normalized_unprocessed_gcs_file_paths_task_id,
            )
        ]

        if not unprocessed_paths_in_bucket:
            return []

        unprocessed_blob_names_to_path = {
            path.blob_name: path for path in unprocessed_paths_in_bucket
        }

        # --- first, determine which files we've seen before --------------------------

        already_seen_gcs_metadata = [
            RawGCSFileMetadata.from_gcs_metadata_table_row(
                metadata_row, unprocessed_blob_names_to_path[metadata_row[2]]
            )
            for metadata_row in postgres_hook.get_records(
                self._get_existing_files_by_path_sql_query(unprocessed_paths_in_bucket)
            )
        ]

        already_seen_paths_in_bucket = {
            metadata.path.blob_name for metadata in already_seen_gcs_metadata
        }

        # --- then, register new files in gcs metadata table ---------------------------

        new_paths_in_bucket = [
            path
            for path in unprocessed_paths_in_bucket
            if path.blob_name not in already_seen_paths_in_bucket
        ]

        new_metadata_table_rows = (
            postgres_hook.get_records(
                self._register_new_files_sql_query(new_paths_in_bucket)
            )
            if new_paths_in_bucket
            else []
        )

        new_gcs_metadata = [
            RawGCSFileMetadata.from_gcs_metadata_table_row(
                metadata_row, unprocessed_blob_names_to_path[metadata_row[2]]
            )
            for metadata_row in new_metadata_table_rows
        ]

        # --- last, build xcom output (gcs_file_id, file_id, abs_path) -----------------

        return [
            metadata.serialize()
            for metadata in already_seen_gcs_metadata + new_gcs_metadata
        ]

    def _get_existing_files_by_path_sql_query(
        self,
        unprocessed_files_in_bucket: List[GcsfsFilePath],
    ) -> str:
        normalized_file_names = ", ".join(
            [f"'{file.blob_name}'" for file in unprocessed_files_in_bucket]
        )

        return StrictStringFormatter().format(
            GET_EXISTING_FILES_BY_PATH,
            region_code=self._region_code,
            raw_data_instance=self._raw_data_instance.value,
            normalized_file_names=normalized_file_names,
        )

    def _build_insert_row_for_file(self, file: GcsfsFilePath) -> str:
        filename_parts = filename_parts_from_path(file)
        row_contents = [
            self._region_code,
            self._raw_data_instance.value,
            filename_parts.file_tag,
            file.blob_name,
            filename_parts.utc_upload_datetime.isoformat(),
            datetime.datetime.now(tz=datetime.UTC).isoformat(),
            0,  # "0"::bool evaluates to False
        ]

        return "\n(" + ", ".join([f"'{value}'" for value in row_contents]) + ")"

    def _register_new_files_sql_query(
        self, unregistered_files: List[GcsfsFilePath]
    ) -> str:
        values = ",".join(
            [self._build_insert_row_for_file(file) for file in unregistered_files]
        )
        return StrictStringFormatter().format(ADD_ROWS_WITHOUT_FILE_ID, values=values)
