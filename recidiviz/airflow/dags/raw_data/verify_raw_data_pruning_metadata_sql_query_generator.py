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
"""Generates SQL queries to verify and update raw data pruning metadata for
each file tag found in the unprocessed BigQuery file metadata for a region and instance.
"""
from collections import defaultdict

import attr
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context
from more_itertools import one

from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryGenerator,
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.raw_data.metadata import SKIPPED_FILE_ERRORS
from recidiviz.airflow.dags.raw_data.utils import (
    get_direct_ingest_region_raw_config,
    logger,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_data_pruning_utils import (
    automatic_raw_data_pruning_enabled_for_file_config,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_types import (
    RawBigQueryFileMetadata,
    RawDataFilesSkippedError,
)
from recidiviz.utils.string import StrictStringFormatter

GET_RAW_DATA_PRUNING_METADATA_QUERY = """
SELECT
    region_code,
    raw_data_instance,
    file_tag,
    updated_at,
    automatic_pruning_enabled,
    raw_file_primary_keys,
    raw_files_contain_full_historical_lookback
FROM direct_ingest_raw_data_pruning_metadata
WHERE region_code = '{region_code}'
AND raw_data_instance = '{raw_data_instance}'
AND file_tag = '{file_tag}'
"""

COUNT_IMPORTED_FILES_QUERY = """
SELECT COUNT(*)
FROM direct_ingest_raw_big_query_file_metadata
WHERE region_code = '{region_code}'
AND raw_data_instance = '{raw_data_instance}'
AND file_tag = '{file_tag}'
AND is_invalidated IS FALSE
AND file_processed_time IS NOT NULL
"""

UPSERT_PRUNING_METADATA_QUERY = """
INSERT INTO direct_ingest_raw_data_pruning_metadata (
    region_code,
    raw_data_instance,
    file_tag,
    updated_at,
    automatic_pruning_enabled,
    raw_file_primary_keys,
    raw_files_contain_full_historical_lookback
) VALUES (
    '{region_code}',
    '{raw_data_instance}',
    '{file_tag}',
    NOW(),
    {automatic_pruning_enabled},
    '{primary_keys}',
    {raw_files_contain_full_historical_lookback}
)
ON CONFLICT (region_code, raw_data_instance, file_tag)
DO UPDATE SET
    updated_at = EXCLUDED.updated_at,
    automatic_pruning_enabled = EXCLUDED.automatic_pruning_enabled,
    raw_file_primary_keys = EXCLUDED.raw_file_primary_keys,
    raw_files_contain_full_historical_lookback = EXCLUDED.raw_files_contain_full_historical_lookback
"""


@attr.define
class RawDataPruningConfig:
    """Configuration for needed to run raw data pruning for a file tag."""

    file_tag: str
    automatic_pruning_enabled: bool
    primary_keys: list[str]
    raw_files_contain_full_historical_lookback: bool

    def __str__(self) -> str:
        return (
            f"file_tag={self.file_tag}, "
            f"automatic_pruning_enabled={self.automatic_pruning_enabled}, "
            f"primary_keys={self.primary_keys}, "
            f"raw_files_contain_full_historical_lookback={self.raw_files_contain_full_historical_lookback}"
        )

    @classmethod
    def from_raw_file_config(
        cls,
        state_code: StateCode,
        raw_data_instance: DirectIngestInstance,
        raw_file_config: DirectIngestRawFileConfig,
    ) -> "RawDataPruningConfig":
        """Creates a RawDataPruningConfig from a DirectIngestRawFileConfig."""
        return cls(
            file_tag=raw_file_config.file_tag,
            automatic_pruning_enabled=automatic_raw_data_pruning_enabled_for_file_config(
                state_code=state_code,
                raw_data_instance=raw_data_instance,
                raw_file_config=raw_file_config,
            ),
            primary_keys=sorted(raw_file_config.primary_key_cols),
            raw_files_contain_full_historical_lookback=raw_file_config.always_historical_export,
        )

    @classmethod
    def from_metadata_row(cls, metadata_row: tuple) -> "RawDataPruningConfig":
        """Creates a RawDataPruningConfig from a database metadata row.

        Args:
            metadata_row: A tuple from the database in the format:
                (region_code, raw_data_instance, file_tag, updated_at,
                 automatic_pruning_enabled, raw_file_primary_keys,
                 raw_files_contain_full_historical_lookback)
        """
        # Tuple indices: 0=region_code, 1=raw_data_instance, 2=file_tag, 3=updated_at,
        # 4=automatic_pruning_enabled, 5=raw_file_primary_keys, 6=raw_files_contain_full_historical_lookback
        return cls(
            file_tag=metadata_row[2],
            automatic_pruning_enabled=metadata_row[4],
            primary_keys=sorted(metadata_row[5].split(",")) if metadata_row[5] else [],
            raw_files_contain_full_historical_lookback=metadata_row[6],
        )


class VerifyRawDataPruningMetadataSqlQueryGenerator(CloudSqlQueryGenerator[list[str]]):
    """Generates SQL queries to verify and update raw data pruning metadata for
    each file tag found in the unprocessed BigQuery file metadata for a region and instance.
    """

    def __init__(
        self,
        state_code: StateCode,
        raw_data_instance: DirectIngestInstance,
        get_all_unprocessed_bq_file_metadata_task_id: str,
    ) -> None:
        super().__init__()
        self._state_code = state_code
        self._raw_data_instance = raw_data_instance
        self._get_all_unprocessed_bq_file_metadata_task_id = (
            get_all_unprocessed_bq_file_metadata_task_id
        )
        self._region_raw_file_config: DirectIngestRegionRawFileConfig | None = None

    @property
    def region_raw_file_config(self) -> DirectIngestRegionRawFileConfig:
        if not self._region_raw_file_config:
            self._region_raw_file_config = get_direct_ingest_region_raw_config(
                self._state_code.value
            )
        return self._region_raw_file_config

    def execute_postgres_query(
        self,
        operator: CloudSqlQueryOperator,
        postgres_hook: PostgresHook,
        context: Context,
    ) -> list[str]:
        """Verifies and updates pruning metadata for each file tag with a file pending import.

        Once a raw data table has been pruned, we cannot update any of the metadata that we
        used to prune the file or else this may lead to corrupted data. Similarly, we cannot enable
        or disable pruning for a table that already contains data from multiple files because this
        would leave portions of the table pruned and other portions unpruned.

        This function fetches the existing pruning metadata for each file tag from the operations
        database and compares it against the current raw file configuration. If the
        configuration has changed and multiple files have already been imported,
        returns a RawDataFilesSkippedError for each bq file corresponding to that file tag.
        Otherwise, upserts the metadata to match the current configuration.

        Special case: If automatic pruning was disabled and remains disabled, updates
        are allowed even with multiple imported files, since pruning config changes
        don't affect non-pruned tables.
        """
        # --- get existing file info from xcom -----------------------------------------

        unprocessed_bq_file_metadata: list[RawBigQueryFileMetadata] = [
            RawBigQueryFileMetadata.deserialize(xcom_metadata)
            for xcom_metadata in operator.xcom_pull(
                context,
                key="return_value",
                task_ids=self._get_all_unprocessed_bq_file_metadata_task_id,
            )
        ]

        # --- verify pruning metadata for each unique file tag -------------------------

        skipped_file_errors: list[RawDataFilesSkippedError] = []
        import_ready_bq_metadata: list[RawBigQueryFileMetadata] = []

        file_tag_to_bq_files = defaultdict(list)
        for bq_file in unprocessed_bq_file_metadata:
            file_tag_to_bq_files[bq_file.file_tag].append(bq_file)

        for file_tag, bq_files in file_tag_to_bq_files.items():
            try:
                self._sync_file_tag_pruning_metadata(postgres_hook, file_tag)
                import_ready_bq_metadata.extend(bq_files)
            except Exception as e:
                logger.error(
                    "Error verifying pruning config for file tag [%s]: %s", file_tag, e
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

        # TODO(#33971) add multiple outputs here to automatically do this
        operator.xcom_push(
            context=context,
            key=SKIPPED_FILE_ERRORS,
            value=[skipped_error.serialize() for skipped_error in skipped_file_errors],
        )

        return [metadata.serialize() for metadata in import_ready_bq_metadata]

    def _sync_file_tag_pruning_metadata(
        self, postgres_hook: PostgresHook, file_tag: str
    ) -> None:
        """Verifies and updates the pruning metadata for a single file tag.

        Fetches the existing pruning metadata for the file tag from the operations
        database and compares it against the current raw file configuration. If the
        configuration has changed and multiple files have already been imported,
        raises a ValueError. Otherwise, upserts the metadata to match the current
        configuration.

        Special case: If automatic pruning was disabled and remains disabled, updates
        are allowed even with multiple imported files, since pruning config changes
        don't affect non-pruned tables.
        """
        raw_file_config = self.region_raw_file_config.raw_file_configs[file_tag]

        raw_data_pruning_metadata_query_result: list[tuple] = postgres_hook.get_records(
            self._get_raw_data_pruning_metadata_sql_query(file_tag)
        )
        metadata_row: tuple | None = (
            one(raw_data_pruning_metadata_query_result)
            if raw_data_pruning_metadata_query_result
            else None
        )

        current_raw_file_pruning_config: RawDataPruningConfig = (
            RawDataPruningConfig.from_raw_file_config(
                state_code=self._state_code,
                raw_data_instance=self._raw_data_instance,
                raw_file_config=raw_file_config,
            )
        )
        existing_raw_file_pruning_config: RawDataPruningConfig | None = (
            RawDataPruningConfig.from_metadata_row(metadata_row)
            if metadata_row
            else None
        )

        if current_raw_file_pruning_config == existing_raw_file_pruning_config:
            logger.info(
                "Pruning metadata for file_tag [%s] matches config; skipping update",
                file_tag,
            )
            return

        pruning_was_disabled_and_is_disabled = (
            not current_raw_file_pruning_config.automatic_pruning_enabled
            and (
                existing_raw_file_pruning_config is None
                or not existing_raw_file_pruning_config.automatic_pruning_enabled
            )
        )
        if not pruning_was_disabled_and_is_disabled:
            num_of_currently_imported_files = self._count_imported_files(
                postgres_hook, file_tag
            )
            if num_of_currently_imported_files > 1:
                error_msg = (
                    f"Pruning configuration for file_tag [{file_tag}] changed but "
                    f"[{num_of_currently_imported_files}] files have already been imported."
                    f"\nRaw file config: {current_raw_file_pruning_config}"
                )
                error_msg += (
                    f"\nExisting metadata: {existing_raw_file_pruning_config}"
                    if existing_raw_file_pruning_config
                    else "\nNo existing metadata found."
                )

                raise ValueError(error_msg)

        logger.info("Upserting pruning metadata for file_tag [%s]", file_tag)
        self._upsert_pruning_metadata(postgres_hook, current_raw_file_pruning_config)

    def _get_raw_data_pruning_metadata_sql_query(self, file_tag: str) -> str:
        """Returns SQL query to fetch pruning metadata for a file tag."""
        return StrictStringFormatter().format(
            GET_RAW_DATA_PRUNING_METADATA_QUERY,
            region_code=self._state_code.value,
            raw_data_instance=self._raw_data_instance.value,
            file_tag=file_tag,
        )

    def _count_imported_files(self, postgres_hook: PostgresHook, file_tag: str) -> int:
        """Counts the number of non-invalidated imported files for a file tag."""
        query = StrictStringFormatter().format(
            COUNT_IMPORTED_FILES_QUERY,
            region_code=self._state_code.value,
            raw_data_instance=self._raw_data_instance.value,
            file_tag=file_tag,
        )
        result = postgres_hook.get_records(query)
        return one(one(result))

    def _upsert_pruning_metadata(
        self, postgres_hook: PostgresHook, raw_data_pruning_config: RawDataPruningConfig
    ) -> None:
        """Upserts the pruning metadata for a file tag."""
        query = StrictStringFormatter().format(
            UPSERT_PRUNING_METADATA_QUERY,
            region_code=self._state_code.value,
            raw_data_instance=self._raw_data_instance.value,
            file_tag=raw_data_pruning_config.file_tag,
            automatic_pruning_enabled=raw_data_pruning_config.automatic_pruning_enabled,
            primary_keys=",".join(raw_data_pruning_config.primary_keys),
            raw_files_contain_full_historical_lookback=raw_data_pruning_config.raw_files_contain_full_historical_lookback,
        )
        postgres_hook.run(query)
