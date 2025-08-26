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
"""Class responsible for loading raw files into BigQuery"""
import datetime
import logging
from typing import List, Optional, Tuple

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.big_query.big_query_utils import to_big_query_valid_encoding
from recidiviz.cloud_resources.platform_resource_labels import (
    RawDataImportStepResourceLabel,
)
from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.csv import DEFAULT_CSV_ENCODING, DEFAULT_CSV_SEPARATOR
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import (
    raw_data_pruning_raw_data_diff_results_dataset,
    raw_data_temp_load_dataset,
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.gating import (
    automatic_raw_data_pruning_enabled_for_state_and_instance,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_migration_collector import (
    DirectIngestRawTableMigrationCollector,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_pre_import_validator import (
    DirectIngestRawTablePreImportValidator,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_transformation_query_builder import (
    DirectIngestTempRawTablePreMigrationTransformationQueryBuilder,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.types.direct_ingest_constants import (
    RAW_DATA_TRANSFORMED_TEMP_TABLE_SUFFIX,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_types import (
    AppendReadyFile,
    AppendSummary,
    ImportReadyFile,
    PreImportNormalizationType,
)
from recidiviz.ingest.direct.views.raw_data_diff_query_builder import (
    RawDataDiffQueryBuilder,
)
from recidiviz.utils import metadata


class DirectIngestRawFileLoadManager:
    """Class responsible for loading raw files into BigQuery"""

    def __init__(
        self,
        *,
        raw_data_instance: DirectIngestInstance,
        region_raw_file_config: DirectIngestRegionRawFileConfig,
        fs: GCSFileSystem,
        big_query_client: Optional[BigQueryClient] = None,
        sandbox_dataset_prefix: Optional[str] = None,
    ) -> None:
        self.region_code = region_raw_file_config.region_code
        self.state_code = StateCode(self.region_code.upper())
        self.raw_data_instance = raw_data_instance
        self.region_raw_file_config = region_raw_file_config
        self.big_query_client = big_query_client or BigQueryClientImpl()
        self.fs = fs
        self.transformation_query_builder = (
            DirectIngestTempRawTablePreMigrationTransformationQueryBuilder(
                region_raw_file_config, raw_data_instance
            )
        )
        self.raw_table_migrations = DirectIngestRawTableMigrationCollector(
            region_code=self.region_code,
            instance=self.raw_data_instance,
            regions_module_override=self.region_raw_file_config.region_module,
        )
        self.validator = DirectIngestRawTablePreImportValidator(
            region_raw_file_config,
            raw_data_instance,
            self.region_code,
            metadata.project_id(),
            self.big_query_client,
        )
        self.sandbox_dataset_prefix = sandbox_dataset_prefix

    @property
    def raw_data_dataset(self) -> str:
        return raw_tables_dataset_for_region(
            state_code=self.state_code,
            instance=self.raw_data_instance,
            sandbox_dataset_prefix=self.sandbox_dataset_prefix,
        )

    @property
    def raw_data_temp_load_dataset(self) -> str:
        return raw_data_temp_load_dataset(
            self.state_code,
            self.raw_data_instance,
            sandbox_dataset_prefix=self.sandbox_dataset_prefix,
        )

    def _delete_temp_files(self, temp_file_paths: List[GcsfsFilePath]) -> None:
        logging.info("Deleting [%s] temp paths", len(temp_file_paths))
        for temp_file_path in temp_file_paths:
            logging.info("\tDeleting temp file [%s].", temp_file_path.abs_path())
            self.fs.delete(temp_file_path)

    def _load_paths_to_temp_table(
        self,
        paths: List[GcsfsFilePath],
        destination_address: BigQueryAddress,
        should_delete_temp_files: bool,
        table_schema: List[bigquery.SchemaField],
        skip_leading_rows: int,
        encoding: str,
        separator: str,
    ) -> int:
        """Loads the raw data in the list of files at the provided |paths| into |destination_address|,
        not including recidiviz-managed fields, using the encoding and delimiter provided.
        We preserve ascii control characters on load else the import into BQ will fail if ascii 0
        or any other ascii control character are present. Since ascii is a subset of utf-8, we want to
        be able to load ascii into BQ without doing any normalization of the csv data, and we can translate
        any values containing only ascii control characters to nulls in the transformation step.
        """
        logging.info(
            "[%s] Starting load of [%s] paths into [%s]",
            datetime.datetime.now().isoformat(),
            len(paths),
            destination_address.to_str(),
        )
        load_job = self.big_query_client.load_table_from_cloud_storage(
            source_uris=[p.uri() for p in paths],
            destination_address=destination_address,
            destination_table_schema=table_schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            skip_leading_rows=skip_leading_rows,
            preserve_ascii_control_characters=True,
            encoding=encoding,
            field_delimiter=separator,
            job_labels=[RawDataImportStepResourceLabel.RAW_DATA_TEMP_LOAD.value],
        )
        logging.info(
            "[%s] BigQuery load of [%s] paths complete",
            datetime.datetime.now().isoformat(),
            len(paths),
        )
        if should_delete_temp_files:
            self._delete_temp_files(paths)

        loaded_row_count = load_job.output_rows

        if loaded_row_count is None:
            raise ValueError("Insert job row count indicates no rows were loaded")

        return loaded_row_count

    def _apply_pre_migration_transformations(
        self,
        source_table: BigQueryAddress,
        destination_table: BigQueryAddress,
        file_tag: str,
        file_id: int,
        update_datetime: datetime.datetime,
    ) -> int:
        """Applies transformations to |source_table| necessary for raw data
        migration queries to run, creating a new table |destination_table|.
        """

        transformation_query = (
            self.transformation_query_builder.build_pre_migration_transformations_query(
                project_id=metadata.project_id(),
                file_tag=file_tag,
                source_table=source_table,
                file_id=file_id,
                update_datetime=update_datetime,
                is_deleted=False,
            )
        )

        query_result = self.big_query_client.create_table_from_query(
            address=destination_table,
            query=transformation_query,
            overwrite=True,
            use_query_cache=False,
            job_labels=[
                RawDataImportStepResourceLabel.RAW_DATA_PRE_IMPORT_TRANSFORMATIONS.value
            ],
        )

        return query_result.total_rows

    def _apply_migrations(
        self, file_tag: str, update_datetime: datetime.datetime, table: BigQueryAddress
    ) -> None:
        """If relevant, applies raw data migrations for |file_tag| to |table|."""
        migration_queries = (
            self.raw_table_migrations.get_raw_table_migration_queries_for_file_tag(
                file_tag,
                table,
                data_update_datetime=update_datetime.replace(tzinfo=None),
            )
        )

        if not migration_queries:
            logging.info("No queries found for [%s]; continuing", file_tag)
            return

        for migration_query in migration_queries:
            query_job = self.big_query_client.run_query_async(
                query_str=migration_query,
                use_query_cache=False,
                job_labels=[RawDataImportStepResourceLabel.RAW_DATA_MIGRATIONS.value],
            )
            try:
                query_job.result()
            except Exception as e:
                logging.error(
                    "Migration query job [%s] failed with errors: [%s]",
                    query_job.job_id,
                    query_job.errors,
                )
                raise e

    @staticmethod
    def _get_encoding_and_separator(
        raw_file_config: DirectIngestRawFileConfig,
    ) -> Tuple[str, str]:
        """Returns the encoding and separator to use for loading a raw file into BigQuery."""
        normalization_type = (
            PreImportNormalizationType.required_pre_import_normalization_type(
                raw_file_config
            )
        )
        if (
            normalization_type
            == PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE
        ):
            return DEFAULT_CSV_ENCODING, DEFAULT_CSV_SEPARATOR
        if normalization_type == PreImportNormalizationType.ENCODING_UPDATE_ONLY:
            return DEFAULT_CSV_ENCODING, raw_file_config.separator

        return (
            to_big_query_valid_encoding(raw_file_config.encoding),
            raw_file_config.separator,
        )

    def load_and_prep_paths(
        self,
        file: ImportReadyFile,
        *,
        temp_table_prefix: str,
        skip_raw_data_migrations: bool = False,
        skip_blocking_validations: bool = False,
        persist_intermediary_tables: bool = False,
    ) -> AppendReadyFile:
        """Loads and transforms a raw data file into a temp table, in the order of:
            (1) load raw data directly into a temp table
            (2) apply pre-migration transformations
            (3) apply raw data migrations
            (4) run basic data integrity validations on the transformed temp table
            (5) delete the original temp table if all of the above steps are successful

        If we encounter an error during step (1), we will not clean up any temp files
        containing the normalized output, and they will be overridden on the next run
        or cleaned up by the bucket's lifecycle policy. If we encounter an error during
        steps (2) through (4), we will not clean up the temp table, and it will be overridden
        on the next run or cleaned up by the dataset's default table expiration policy.

        After this step, we should be ready to perform raw data pruning and append to
        the current raw data table.

        We use |temp_table_prefix| to prefix the temporary load tables to ensure that
        tables between runs can be made unique.
        """

        table_base_name = f"{temp_table_prefix}__{file.file_tag}__{file.file_id}"

        temp_raw_file_address = BigQueryAddress(
            dataset_id=self.raw_data_temp_load_dataset, table_id=table_base_name
        )

        temp_raw_file_with_transformations_address = BigQueryAddress(
            dataset_id=self.raw_data_temp_load_dataset,
            table_id=f"{table_base_name}__{RAW_DATA_TRANSFORMED_TEMP_TABLE_SUFFIX}",
        )

        encoding, separator = self._get_encoding_and_separator(
            self.region_raw_file_config.raw_file_configs[file.file_tag],
        )

        raw_rows_count = self._load_paths_to_temp_table(
            file.paths_to_load,
            temp_raw_file_address,
            bool(file.pre_import_normalized_file_paths),
            file.bq_load_config.schema_fields,
            file.bq_load_config.skip_leading_rows,
            encoding,
            separator,
        )

        self._apply_pre_migration_transformations(
            temp_raw_file_address,
            temp_raw_file_with_transformations_address,
            file.file_tag,
            file.file_id,
            file.update_datetime,
        )

        if not skip_raw_data_migrations:
            self._apply_migrations(
                file.file_tag,
                file.update_datetime,
                temp_raw_file_with_transformations_address,
            )

        # TODO(#34610) Skip import entirely if there are no rows in an incremental file
        if not skip_blocking_validations and (
            raw_rows_count != 0
            or self.region_raw_file_config.raw_file_configs[
                file.file_tag
            ].always_historical_export
        ):
            self.validator.run_raw_data_temp_table_validations(
                file.file_tag,
                file.update_datetime,
                temp_raw_file_with_transformations_address,
            )

        if not persist_intermediary_tables:
            self._clean_up_temp_tables(temp_raw_file_address)

        return AppendReadyFile(
            import_ready_file=file,
            append_ready_table_address=temp_raw_file_with_transformations_address,
            raw_rows_count=raw_rows_count,
        )

    def _generate_historical_diff(
        self,
        file_tag: str,
        file_id: int,
        update_datetime: datetime.datetime,
        temp_raw_data_diff_table_address: BigQueryAddress,
        temp_raw_file_address: BigQueryAddress,
    ) -> None:
        """Create and run raw data diff query between contents of |temp_raw_file_address|
        and the latest version of the raw data table on BQ, saving the results to a
        |temp_raw_data_diff_table_address|."""

        raw_data_diff_query = RawDataDiffQueryBuilder(
            project_id=metadata.project_id(),
            state_code=self.state_code,
            file_id=file_id,
            update_datetime=update_datetime,
            raw_data_instance=self.raw_data_instance,
            raw_file_config=self.region_raw_file_config.raw_file_configs[file_tag],
            new_raw_data_table_id=temp_raw_file_address.table_id,
            new_raw_data_dataset=temp_raw_file_address.dataset_id,
        ).build_query()

        try:
            self.big_query_client.create_table_from_query(
                address=temp_raw_data_diff_table_address,
                query=raw_data_diff_query,
                overwrite=True,
                use_query_cache=False,
                job_labels=[RawDataImportStepResourceLabel.RAW_DATA_PRUNING.value],
            )
        except Exception as e:
            logging.error(
                "Create job for [%s] with id [%s] failed",
                file_tag,
                file_id,
            )
            raise e

    # TODO(#12209): Delete once raw data pruning is live.
    def _should_generate_historical_diffs(self, file_tag: str) -> bool:
        """Returns whether or not we should apply historical diffs to this file during
        raw data import.
        """
        raw_data_pruning_enabled = (
            automatic_raw_data_pruning_enabled_for_state_and_instance(
                self.state_code, self.raw_data_instance
            )
        )
        if not raw_data_pruning_enabled:
            return False

        file_config = self.region_raw_file_config.raw_file_configs[file_tag]
        is_exempt_from_raw_data_pruning = (
            file_config.is_exempt_from_automatic_raw_data_pruning()
        )
        return not is_exempt_from_raw_data_pruning

    def _append_data_to_raw_table(
        self, source_table: BigQueryAddress, destination_table: BigQueryAddress
    ) -> None:
        """Appends the contents of |source_table| to |destination_table|."""

        append_job = self.big_query_client.insert_into_table_from_table_async(
            source_address=source_table,
            destination_address=destination_table,
            use_query_cache=False,
            job_labels=[RawDataImportStepResourceLabel.RAW_DATA_TABLE_APPEND.value],
        )

        try:
            append_job.result()
        except Exception as e:
            logging.error(
                "Insert job [%s] appending data from [%s] to [%s] failed with errors: [%s]",
                append_job.job_id,
                source_table.to_str(),
                destination_table.to_str(),
                append_job.errors,
            )
            raise e

    def _ensure_no_conflicting_rows_in_bigquery(
        self, raw_data_table: BigQueryAddress, file_id: int
    ) -> None:
        """Delete any rows that have already been uploaded with this file_id.
        These rows shouldn't exist and if they do that is indicative of some underlying
        error. To prevent against duplicates, however, we will run the deletion query
        just in case.
        """
        delete_job = self.big_query_client.delete_from_table_async(
            address=raw_data_table,
            filter_clause="WHERE file_id = " + str(file_id),
            job_labels=[RawDataImportStepResourceLabel.RAW_DATA_TABLE_APPEND.value],
        )
        result = delete_job.result()
        if result.num_dml_affected_rows:
            logging.error(
                "Found [%s] already existing rows with file id [%s] in [%s]",
                result.num_dml_affected_rows,
                file_id,
                raw_data_table.to_str(),
            )

    def _clean_up_temp_tables(self, *addresses: BigQueryAddress) -> None:
        for address in addresses:
            try:
                logging.info("Deleting [%s]", address)
                self.big_query_client.delete_table(address, not_found_ok=True)
            except Exception as e:
                logging.error(
                    "Error: failed to clean up [%s] with [%s]: %s",
                    address,
                    e.__class__,
                    e,
                )

    def append_to_raw_data_table(
        self,
        append_ready_file: AppendReadyFile,
        *,
        persist_intermediary_tables: bool = False,
    ) -> AppendSummary:
        """Appends already loaded and transformed data to the raw data table,
        optionally applying a historical data diff to the data if historical diffs
        are active. If we encounter an error during the append, we will not clean up
        the temp tables, and they will be overridden on the next run or cleaned up
        by the dataset's default table expiration policy.
        """
        file = append_ready_file.import_ready_file

        # TODO(#12209) add sandbox support for raw data pruning datasets
        temp_raw_data_diff_table_address = BigQueryAddress(
            dataset_id=raw_data_pruning_raw_data_diff_results_dataset(
                self.state_code, self.raw_data_instance
            ),
            table_id=f"{file.file_tag}__{file.file_id}",
        )

        raw_data_table = BigQueryAddress(
            dataset_id=self.raw_data_dataset,
            table_id=file.file_tag,
        )

        if historical_diffs_active := self._should_generate_historical_diffs(
            file.file_tag
        ):

            self._generate_historical_diff(
                file_tag=file.file_tag,
                file_id=file.file_id,
                update_datetime=file.update_datetime,
                temp_raw_data_diff_table_address=temp_raw_data_diff_table_address,
                temp_raw_file_address=append_ready_file.append_ready_table_address,
            )

            append_source_table = temp_raw_data_diff_table_address
        else:
            append_source_table = append_ready_file.append_ready_table_address

        self._ensure_no_conflicting_rows_in_bigquery(raw_data_table, file.file_id)

        self._append_data_to_raw_table(
            source_table=append_source_table, destination_table=raw_data_table
        )

        if not persist_intermediary_tables:
            self._clean_up_temp_tables(
                append_ready_file.append_ready_table_address,
                temp_raw_data_diff_table_address,
            )

        # TODO(#40153) add additional query to grab these stats
        return AppendSummary(
            file_id=file.file_id,
            net_new_or_updated_rows=None,
            deleted_rows=None,
            historical_diffs_active=historical_diffs_active,
        )
