# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Generates SQL queries to verify all file_ids found in the BigQuery raw data table
have a corresponding non-invalidated, processed entry in the `direct_ingest_raw_big_query_file_metadata`
table in the operations postgres db.
"""
from collections import defaultdict

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context
from more_itertools import one

from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryGenerator,
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.raw_data.metadata import SKIPPED_FILE_ERRORS
from recidiviz.airflow.dags.raw_data.utils import logger
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.cloud_resources.platform_resource_labels import (
    IngestInstanceResourceLabel,
    PlatformOrchestrationResourceLabel,
    StateCodeResourceLabel,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.gating import (
    file_tag_exempt_from_automatic_raw_data_pruning,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_types import (
    RawBigQueryFileMetadata,
    RawDataFilesSkippedError,
)
from recidiviz.utils.string import StrictStringFormatter

POSTGRES_PROCESSED_BQ_FILE_IDS_QUERY = """
SELECT DISTINCT file_id
FROM direct_ingest_raw_big_query_file_metadata
WHERE region_code = '{region_code}'
AND raw_data_instance = '{raw_data_instance}'
AND file_tag = '{file_tag}'
AND is_invalidated IS FALSE
AND file_processed_time IS NOT NULL
"""

BQ_DATA_FILE_IDS_QUERY = """
SELECT DISTINCT file_id
FROM `{project_id}.{raw_data_dataset_id}.{raw_data_table_id}`
"""


class VerifyBigQueryPostgresAlignmentSQLQueryGenerator(
    CloudSqlQueryGenerator[list[str]]
):
    """Generates a SQL query to verify that the raw data table in BigQuery is aligned with the operations db."""

    def __init__(
        self,
        state_code: StateCode,
        raw_data_instance: DirectIngestInstance,
        project_id: str,
        get_all_unprocessed_bq_file_metadata_task_id: str,
    ) -> None:
        super().__init__()
        self._state_code = state_code
        self._raw_data_instance = raw_data_instance
        self._get_all_unprocessed_bq_file_metadata_task_id = (
            get_all_unprocessed_bq_file_metadata_task_id
        )
        self._project_id = project_id
        self._raw_data_dataset_id = raw_tables_dataset_for_region(
            self._state_code, self._raw_data_instance
        )
        self._big_query_client: BigQueryClientImpl | None = None

    @property
    def big_query_client(self) -> BigQueryClientImpl:
        # BigQueryClientImpl is not pickle-able
        # so it can't be set during initialization
        # or else the raw data import dag integration tests will fail
        if not self._big_query_client:
            self._big_query_client = BigQueryClientImpl(
                project_id=self._project_id,
                default_job_labels=[
                    StateCodeResourceLabel(value=self._state_code.value.lower()),
                    IngestInstanceResourceLabel(
                        value=self._raw_data_instance.value.lower()
                    ),
                    PlatformOrchestrationResourceLabel.RAW_DATA_IMPORT_DAG.value,
                ],
            )

        return self._big_query_client

    def execute_postgres_query(
        self,
        operator: CloudSqlQueryOperator,
        postgres_hook: PostgresHook,
        context: Context,
    ) -> list[str]:
        """For each file tag pending import, verifies that all file_ids found in the BigQuery
        raw data table have a corresponding processed, non-invalidated entry in the
         `direct_ingest_raw_big_query_file_metadata` table in the operations postgres db.

        Returns a serialized SkippedFileError for any files that fail this verification.
        """
        unprocessed_bq_file_metadata: list[RawBigQueryFileMetadata] = [
            RawBigQueryFileMetadata.deserialize(xcom_metadata)
            for xcom_metadata in operator.xcom_pull(
                context,
                key="return_value",
                task_ids=self._get_all_unprocessed_bq_file_metadata_task_id,
            )
        ]

        file_tag_to_bq_files = defaultdict(list)
        for bq_file in unprocessed_bq_file_metadata:
            file_tag_to_bq_files[bq_file.file_tag].append(bq_file)

        skipped_file_errors: list[RawDataFilesSkippedError] = []
        import_ready_bq_metadata: list[RawBigQueryFileMetadata] = []

        for file_tag, bq_files in file_tag_to_bq_files.items():
            try:
                # TODO(#8554) Remove this check once there are no longer any exempted files
                if not file_tag_exempt_from_automatic_raw_data_pruning(
                    self._state_code, self._raw_data_instance, file_tag
                ):
                    self._validate_bq_table_bq_metadata_alignment_for_file_tag(
                        postgres_hook, file_tag
                    )
                import_ready_bq_metadata.extend(bq_files)
            except Exception as e:
                logger.error(
                    "Error validating BigQuery/operations db alignment for file tag [%s]: %s",
                    file_tag,
                    e,
                )
                skipped_file_errors.extend(
                    [
                        RawDataFilesSkippedError(
                            file_tag=bq_file.file_tag,
                            file_paths=[
                                gcs_file.path for gcs_file in bq_file.gcs_files
                            ],
                            skipped_message=str(e),
                            update_datetime=bq_file.update_datetime,
                        )
                        for bq_file in bq_files
                    ]
                )

        operator.xcom_push(
            context=context,
            key=SKIPPED_FILE_ERRORS,
            value=[skipped_error.serialize() for skipped_error in skipped_file_errors],
        )

        return [metadata.serialize() for metadata in import_ready_bq_metadata]

    def _validate_bq_table_bq_metadata_alignment_for_file_tag(
        self,
        postgres_hook: PostgresHook,
        file_tag: str,
    ) -> None:
        """Validates that all file_ids found in the BigQuery raw data table have a corresponding non-invalidated
        entry in the `direct_ingest_raw_big_query_file_metadata` table in the operations database. We don't validate
        in the opposite direction because not every imported file will result in rows in the BigQuery raw table
        (empty files, pruned files with no differences). However, if the BigQuery table is empty and there are non-invalidated
        entries in the operations database, this is a condition that should never happen and we will fail.
        """
        bq_raw_table_file_ids = self._get_unique_bq_raw_table_file_ids(file_tag)

        postgres_bq_metadata_file_ids = self._get_unique_postgres_bq_metadata_file_ids(
            postgres_hook, file_tag
        )

        # We don't expect the first file for a file tag to be empty
        # and it shouldn't have any data pruned out of it
        # so we can reasonably assume something went wrong is this is the case
        if not bq_raw_table_file_ids and postgres_bq_metadata_file_ids:
            raise ValueError(
                f"No file_ids found in BigQuery raw data table for file_tag [{file_tag}], "
                f"but found non-invalidated, processed entries for file_ids {sorted(postgres_bq_metadata_file_ids)} "
                "in the `direct_ingest_raw_big_query_file_metadata` table"
            )

        bq_file_ids_missing_postgres_entry = (
            bq_raw_table_file_ids - postgres_bq_metadata_file_ids
        )

        if bq_file_ids_missing_postgres_entry:
            raise ValueError(
                f"BigQuery raw data table [{self._raw_data_dataset_id}.{file_tag}] contains file_ids {sorted(bq_file_ids_missing_postgres_entry)} "
                f"with no corresponding non-invalidated, processed entry in the `direct_ingest_raw_big_query_file_metadata` table"
            )

        logger.info(
            "Successfully validated %d file_id(s) for file_tag [%s]",
            len(bq_raw_table_file_ids),
            file_tag,
        )

    def _get_unique_bq_raw_table_file_ids(self, file_tag: str) -> set[int]:
        query = StrictStringFormatter().format(
            BQ_DATA_FILE_IDS_QUERY,
            project_id=self._project_id,
            raw_data_dataset_id=self._raw_data_dataset_id,
            raw_data_table_id=file_tag,
        )
        rows = self.big_query_client.run_query_async(
            query_str=query,
            use_query_cache=True,
        ).result()

        return {one(row) for row in rows}

    def _get_unique_postgres_bq_metadata_file_ids(
        self, postgres_hook: PostgresHook, file_tag: str
    ) -> set[int]:
        query = StrictStringFormatter().format(
            POSTGRES_PROCESSED_BQ_FILE_IDS_QUERY,
            region_code=self._state_code.value,
            raw_data_instance=self._raw_data_instance.value,
            file_tag=file_tag,
        )

        return {one(row) for row in postgres_hook.get_records(query)}
