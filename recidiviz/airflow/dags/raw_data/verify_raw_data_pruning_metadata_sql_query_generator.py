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
from enum import Enum

import attr
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

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


@attr.define
class PruningMetadataQueryBuilder:
    """Builds SQL queries for managing raw data pruning metadata."""

    _GET_PRUNING_METADATA_TEMPLATE = """
SELECT
    file_tag,
    automatic_pruning_enabled,
    raw_file_primary_keys,
    raw_files_contain_full_historical_lookback
FROM direct_ingest_raw_data_pruning_metadata
WHERE region_code = '{region_code}'
AND raw_data_instance = '{raw_data_instance}'
AND file_tag IN ({file_tags})
"""

    _COUNT_IMPORTED_FILES_TEMPLATE = """
SELECT file_tag, COUNT(*) as file_count
FROM direct_ingest_raw_big_query_file_metadata
WHERE region_code = '{region_code}'
AND raw_data_instance = '{raw_data_instance}'
AND file_tag IN ({file_tags})
AND is_invalidated IS FALSE
AND file_processed_time IS NOT NULL
GROUP BY file_tag
"""

    _UPSERT_PRUNING_METADATA_TEMPLATE = """
INSERT INTO direct_ingest_raw_data_pruning_metadata (
    region_code,
    raw_data_instance,
    file_tag,
    updated_at,
    automatic_pruning_enabled,
    raw_file_primary_keys,
    raw_files_contain_full_historical_lookback
) VALUES
{values_clauses}
ON CONFLICT (region_code, raw_data_instance, file_tag)
DO UPDATE SET
    updated_at = EXCLUDED.updated_at,
    automatic_pruning_enabled = EXCLUDED.automatic_pruning_enabled,
    raw_file_primary_keys = EXCLUDED.raw_file_primary_keys,
    raw_files_contain_full_historical_lookback = EXCLUDED.raw_files_contain_full_historical_lookback
"""

    @staticmethod
    def build_get_metadata_query(
        region_code: str, raw_data_instance: str, file_tags: list[str]
    ) -> str:
        """Builds a query to fetch existing pruning metadata for the given |file_tags|."""
        file_tags_str = ",".join(f"'{tag}'" for tag in file_tags)
        return StrictStringFormatter().format(
            PruningMetadataQueryBuilder._GET_PRUNING_METADATA_TEMPLATE,
            region_code=region_code,
            raw_data_instance=raw_data_instance,
            file_tags=file_tags_str,
        )

    @staticmethod
    def build_count_files_query(
        region_code: str, raw_data_instance: str, file_tags: list[str]
    ) -> str:
        """Builds a query to count imported files for multiple file tags."""
        file_tags_str = ",".join(f"'{tag}'" for tag in file_tags)
        return StrictStringFormatter().format(
            PruningMetadataQueryBuilder._COUNT_IMPORTED_FILES_TEMPLATE,
            region_code=region_code,
            raw_data_instance=raw_data_instance,
            file_tags=file_tags_str,
        )

    @staticmethod
    def build_upsert_query(
        region_code: str,
        raw_data_instance: str,
        pruning_configs: list["RawDataPruningConfig"],
    ) -> str:
        """Builds a query to upsert pruning metadata in the given |pruning_configs|."""
        updated_at_str = "NOW()"

        values_clauses = [
            f"""(
    '{region_code}',
    '{raw_data_instance}',
    '{config.file_tag}',
    {updated_at_str},
    {config.automatic_pruning_enabled},
    '{",".join(config.primary_keys)}',
    {config.raw_files_contain_full_historical_lookback}
)"""
            for config in pruning_configs
        ]

        return StrictStringFormatter().format(
            PruningMetadataQueryBuilder._UPSERT_PRUNING_METADATA_TEMPLATE,
            values_clauses=",".join(values_clauses),
        )


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
        # Tuple indices: 0=file_tag, 1=automatic_pruning_enabled,
        # 2=raw_file_primary_keys, 3=raw_files_contain_full_historical_lookback
        return cls(
            file_tag=metadata_row[0],
            automatic_pruning_enabled=metadata_row[1],
            primary_keys=sorted(metadata_row[2].split(",")) if metadata_row[2] else [],
            raw_files_contain_full_historical_lookback=metadata_row[3],
        )


class RawFilePruningConfigChangeType(Enum):
    """Categorizes file tags by how they should be processed.

    Attributes:
        NO_UPDATES: Config hasn't changed - always safe to import
        NON_MEANINGFUL_UPDATES: Config changed but in non-breaking ways - always safe
        MEANINGFUL_UPDATES: Config changed in ways that affect pruning - requires validation
    """

    NO_UPDATES = "no_updates"
    NON_MEANINGFUL_UPDATES = "non_meaningful_updates"
    MEANINGFUL_UPDATES = "meaningful_updates"


@attr.define
class RawDataPruningFileTagComparisonMetadata:
    """Bundles all pruning-related data for a single file tag for easy comparison."""

    file_tag: str
    current_raw_file_pruning_config: RawDataPruningConfig
    previous_raw_file_pruning_config: RawDataPruningConfig | None

    @property
    def comparison_classification(self) -> RawFilePruningConfigChangeType:
        if not self._has_pruning_config_updates():
            return RawFilePruningConfigChangeType.NO_UPDATES

        if self._has_meaningful_pruning_config_updates():
            return RawFilePruningConfigChangeType.MEANINGFUL_UPDATES

        return RawFilePruningConfigChangeType.NON_MEANINGFUL_UPDATES

    def _has_pruning_config_updates(self) -> bool:
        return (
            self.current_raw_file_pruning_config
            != self.previous_raw_file_pruning_config
        )

    def _has_meaningful_pruning_config_updates(self) -> bool:
        if not self._has_pruning_config_updates():
            return False

        pruning_was_disabled_and_is_disabled = (
            not self.current_raw_file_pruning_config.automatic_pruning_enabled
            and (
                self.previous_raw_file_pruning_config is None
                or not self.previous_raw_file_pruning_config.automatic_pruning_enabled
            )
        )
        return not pruning_was_disabled_and_is_disabled


class VerifyRawDataPruningMetadataSqlQueryGenerator(CloudSqlQueryGenerator[list[str]]):
    """Generates SQL queries to verify and update raw data pruning metadata for
    each file tag found in the unprocessed BigQuery file metadata for a region and instance.
    """

    def __init__(
        self,
        state_code: StateCode,
        raw_data_instance: DirectIngestInstance,
        verify_big_query_postgres_alignment_task_id: str,
    ) -> None:
        super().__init__()
        self._state_code = state_code
        self._raw_data_instance = raw_data_instance
        self._verify_big_query_postgres_alignment_task_id = (
            verify_big_query_postgres_alignment_task_id
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

        How we handle failures in the batch queries depends on the stage of processing:
        - During the initial query to fetch existing metadata: we cannot proceed with the import and we fail
        the task outright rather than creating individual skipped file errors.
        - During the query to count the imported files (only runs for meaningful config changes):
            1. Any file tags with no metadata changes or non-meaningful changes should be imported.
            2. Any files that had meaningful config changes should be skipped with an error.
        - During the query to upsert metadata (runs for non-meaningful and validated meaningful changes):
            1. Any file tags with no metadata changes should be imported (no upsert attempted).
            2. Any file tags with non-meaningful changes should be imported even if upsert fails.
            3. Any files that had meaningful config changes should be skipped with an error that includes
                both the validation details and the upsert error.

        We allow files with non-meaningful changes to be imported even if the config upsert is not completed
        since the pruning config is not in use and we can re-attempt upsert on the next run.
        """
        # --- get existing files with congruent BigQuery and Postgres data from xcom -----------------------------------------

        unprocessed_bq_file_metadata: list[RawBigQueryFileMetadata] = [
            RawBigQueryFileMetadata.deserialize(xcom_metadata)
            for xcom_metadata in operator.xcom_pull(
                context,
                key="return_value",
                task_ids=self._verify_big_query_postgres_alignment_task_id,
            )
        ]

        skipped_file_errors: list[RawDataFilesSkippedError] = []
        import_ready_bq_metadata: list[RawBigQueryFileMetadata] = []

        file_tag_to_bq_files = defaultdict(list)
        for bq_file in unprocessed_bq_file_metadata:
            file_tag_to_bq_files[bq_file.file_tag].append(bq_file)

        file_tag_comparison_metadata_by_classification = (
            self._build_file_tag_pruning_comparison_metadata_by_classification(
                postgres_hook, file_tags=list(file_tag_to_bq_files.keys())
            )
        )

        # Mark file tags with no config updates or non-meaningful updates as ready to import
        for change_type in [
            RawFilePruningConfigChangeType.NO_UPDATES,
            RawFilePruningConfigChangeType.NON_MEANINGFUL_UPDATES,
        ]:
            import_ready_bq_metadata.extend(
                [
                    bq_file
                    for file_tag_metadatum in file_tag_comparison_metadata_by_classification[
                        change_type
                    ]
                    for bq_file in file_tag_to_bq_files[file_tag_metadatum.file_tag]
                ]
            )
        # Should upsert pruning metadata for non-meaningful updates
        pruning_configs_to_upsert: list[RawDataPruningConfig] = [
            file_tag_metadatum.current_raw_file_pruning_config
            for file_tag_metadatum in file_tag_comparison_metadata_by_classification[
                RawFilePruningConfigChangeType.NON_MEANINGFUL_UPDATES
            ]
        ]

        # Handle file tags with meaningful pruning config updates
        file_tag_comparison_metadata_requiring_validation = (
            file_tag_comparison_metadata_by_classification[
                RawFilePruningConfigChangeType.MEANINGFUL_UPDATES
            ]
        )
        # File tag to list of error messages for files that encounter some error during validation
        # or metadata upsert and/or that are invalid due to pruning config changes
        failed_file_tags: dict[str, list[str]] = defaultdict(list)
        # File tags with valid pruning config changes that are safe to import and upsert metadata for
        valid_metadatum: list[RawDataPruningFileTagComparisonMetadata] = []
        try:
            (
                valid_metadatum,
                invalid_file_tags,
            ) = self._validate_meaningful_pruning_config_updates(
                postgres_hook, file_tag_comparison_metadata_requiring_validation
            )
            for file_tag, error_msg in invalid_file_tags.items():
                failed_file_tags[file_tag].append(error_msg)

            pruning_configs_to_upsert.extend(
                file_tag_metadatum.current_raw_file_pruning_config
                for file_tag_metadatum in valid_metadatum
            )
            self._batch_upsert_pruning_metadata(
                postgres_hook, pruning_configs_to_upsert
            )
        except Exception as e:
            for (
                comparison_metadata
            ) in file_tag_comparison_metadata_requiring_validation:
                failed_file_tags[comparison_metadata.file_tag].append(str(e))

        import_ready_bq_metadata.extend(
            [
                bq_file
                for file_tag_metadatum in valid_metadatum
                for bq_file in file_tag_to_bq_files[file_tag_metadatum.file_tag]
                if file_tag_metadatum.file_tag not in failed_file_tags
            ]
        )

        skipped_file_errors.extend(
            [
                RawDataFilesSkippedError(
                    file_tag=bq_file.file_tag,
                    file_paths=[gcs_file.path for gcs_file in bq_file.gcs_files],
                    skipped_message="\n\n".join(error_msgs),
                    update_datetime=bq_file.update_datetime,
                )
                for file_tag, error_msgs in failed_file_tags.items()
                for bq_file in file_tag_to_bq_files[file_tag]
            ]
        )

        # TODO(#33971) add multiple outputs here to automatically do this
        operator.xcom_push(
            context=context,
            key=SKIPPED_FILE_ERRORS,
            value=[skipped_error.serialize() for skipped_error in skipped_file_errors],
        )

        return [metadata.serialize() for metadata in import_ready_bq_metadata]

    def _validate_meaningful_pruning_config_updates(
        self,
        postgres_hook: PostgresHook,
        file_tag_comparison_metadata_requiring_validation: list[
            RawDataPruningFileTagComparisonMetadata
        ],
    ) -> tuple[list[RawDataPruningFileTagComparisonMetadata], dict[str, str]]:
        """Validates that pruning configuration changes are safe given the number of currently
        imported files for each file tag.

        Returns:
            A tuple of:
            1. A list of RawDataPruningFileTagComparisonMetadata for file tags that are valid to import
            2. A dictionary mapping file_tag to error message for invalid file tags
        """
        valid_metadatum: list[RawDataPruningFileTagComparisonMetadata] = []
        invalid_file_tags: dict[str, str] = {}

        imported_file_counts_per_file_tag: dict[
            str, int
        ] = self._count_imported_files_per_file_tag(
            postgres_hook,
            file_tags=[
                config.file_tag
                for config in file_tag_comparison_metadata_requiring_validation
            ],
        )

        for file_tag_metadatum in file_tag_comparison_metadata_requiring_validation:
            num_of_currently_imported_files = imported_file_counts_per_file_tag.get(
                file_tag_metadatum.file_tag, 0
            )
            if num_of_currently_imported_files <= 1:
                valid_metadatum.append(file_tag_metadatum)
                continue

            if file_tag_metadatum.previous_raw_file_pruning_config:
                error_msg = (
                    f"Pruning configuration for file_tag [{file_tag_metadatum.file_tag}] changed but "
                    f"[{num_of_currently_imported_files}] files have already been imported."
                    f"\nRaw file config: {file_tag_metadatum.current_raw_file_pruning_config}"
                    f"\nExisting metadata: {file_tag_metadatum.previous_raw_file_pruning_config}"
                )
            else:
                error_msg = (
                    f"Pruning configuration for file_tag [{file_tag_metadatum.file_tag}] not found but "
                    f"[{num_of_currently_imported_files}] files have already been imported. "
                    "Something has gone wrong as this is not an expected state."
                )
            invalid_file_tags[file_tag_metadatum.file_tag] = error_msg

        return valid_metadatum, invalid_file_tags

    def _build_file_tag_pruning_comparison_metadata_by_classification(
        self,
        postgres_hook: PostgresHook,
        file_tags: list[str],
    ) -> dict[
        RawFilePruningConfigChangeType, list[RawDataPruningFileTagComparisonMetadata]
    ]:
        """Generates pruning comparison metadata for each file tag.

        For each file tag, this method:
        1. Fetches existing pruning configs from the database
        2. Builds current pruning configs from the raw file config
        3. Bundles all data into RawDataPruningFileTagComparisonMetadata objects
        4. Partitions them by comparison classification (no changes, non-meaningful changes, meaningful changes)
        """
        existing_pruning_config_by_tag = (
            self._fetch_existing_raw_data_pruning_configs_from_database(
                postgres_hook, file_tags
            )
        )
        current_pruning_config_by_tag = (
            self._build_current_raw_data_pruning_configs_from_file_config(file_tags)
        )

        comparison_metadata = []
        for file_tag in file_tags:
            comparison_metadata.append(
                RawDataPruningFileTagComparisonMetadata(
                    file_tag=file_tag,
                    current_raw_file_pruning_config=current_pruning_config_by_tag[
                        file_tag
                    ],
                    previous_raw_file_pruning_config=existing_pruning_config_by_tag.get(
                        file_tag
                    ),
                )
            )

        file_tag_comparison_metadata_by_status = defaultdict(list)
        for file_tag_metadatum in comparison_metadata:
            file_tag_comparison_metadata_by_status[
                file_tag_metadatum.comparison_classification
            ].append(file_tag_metadatum)

        return file_tag_comparison_metadata_by_status

    def _build_current_raw_data_pruning_configs_from_file_config(
        self, file_tags: list[str]
    ) -> dict[str, RawDataPruningConfig]:
        """Builds current pruning configurations from the raw file configuration.

        Returns:
            Dictionary mapping file_tag to its current RawDataPruningConfig
        """
        current_pruning_configs: dict[str, RawDataPruningConfig] = {}

        for file_tag in file_tags:
            raw_file_config = self.region_raw_file_config.raw_file_configs[file_tag]
            current_raw_file_pruning_config: RawDataPruningConfig = (
                RawDataPruningConfig.from_raw_file_config(
                    state_code=self._state_code,
                    raw_data_instance=self._raw_data_instance,
                    raw_file_config=raw_file_config,
                )
            )
            current_pruning_configs[file_tag] = current_raw_file_pruning_config

        return current_pruning_configs

    def _fetch_existing_raw_data_pruning_configs_from_database(
        self, postgres_hook: PostgresHook, file_tags: list[str]
    ) -> dict[str, RawDataPruningConfig]:
        """Fetches existing pruning configurations from the operations database.

        Returns:
            Dictionary mapping file_tag to its existing RawDataPruningConfig,
            or empty dict if no matches found
        """
        if not file_tags:
            return {}

        query = PruningMetadataQueryBuilder.build_get_metadata_query(
            region_code=self._state_code.value,
            raw_data_instance=self._raw_data_instance.value,
            file_tags=file_tags,
        )
        results = postgres_hook.get_records(query)

        pruning_configs = [
            RawDataPruningConfig.from_metadata_row(row) for row in results
        ]

        return {config.file_tag: config for config in pruning_configs}

    def _count_imported_files_per_file_tag(
        self,
        postgres_hook: PostgresHook,
        file_tags: list[str],
    ) -> dict[str, int]:
        """Counts the number of imported, non-invalidated files for each file tag in |file_tags|.

        Returns:
            Dictionary mapping file_tag to the count of imported files for that tag
        """
        if not file_tags:
            return {}

        query = PruningMetadataQueryBuilder.build_count_files_query(
            region_code=self._state_code.value,
            raw_data_instance=self._raw_data_instance.value,
            file_tags=file_tags,
        )
        results = postgres_hook.get_records(query)
        return {row[0]: row[1] for row in results}  # file_tag -> count

    def _batch_upsert_pruning_metadata(
        self,
        postgres_hook: PostgresHook,
        pruning_configs_to_upsert: list[RawDataPruningConfig],
    ) -> None:
        """Batch upserts pruning configurations to the operations database."""
        if not pruning_configs_to_upsert:
            logger.info("No pruning configurations to upsert.")
            return

        batch_query = PruningMetadataQueryBuilder.build_upsert_query(
            region_code=self._state_code.value,
            raw_data_instance=self._raw_data_instance.value,
            pruning_configs=pruning_configs_to_upsert,
        )
        postgres_hook.run(batch_query)
        logger.info(
            "Batch upserted pruning metadata for %d file tags",
            len(pruning_configs_to_upsert),
        )
