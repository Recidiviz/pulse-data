# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Classes for performing direct ingest raw file imports to BigQuery."""
import csv
import datetime
import logging
import os
from types import ModuleType
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import pytz
from google.api_core import retry
from google.cloud import bigquery
from more_itertools import one

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.big_query.big_query_utils import normalize_column_name_for_bq
from recidiviz.cloud_storage.gcsfs_csv_reader import (
    UTF_8_ENCODING,
    GcsfsCsvReader,
    GcsfsCsvReaderDelegate,
)
from recidiviz.cloud_storage.gcsfs_csv_reader_delegates import (
    ReadOneGcsfsCsvReaderDelegate,
    SplittingGcsfsCsvReaderDelegate,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.common.retry_predicate import google_api_retry_predicate
from recidiviz.ingest.direct import regions
from recidiviz.ingest.direct.direct_ingest_regions import DirectIngestRegion
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
)
from recidiviz.ingest.direct.gcs.filename_parts import filename_parts_from_path
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_migration_collector import (
    DirectIngestRawTableMigrationCollector,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.types.direct_ingest_constants import (
    FILE_ID_COL_NAME,
    IS_DELETED_COL_NAME,
    UPDATE_DATETIME_COL_NAME,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.entity.operations.entities import DirectIngestRawFileMetadata


class DirectIngestRawFileReader:
    """Reads a raw CSV using the defined file config."""

    def __init__(
        self,
        *,
        csv_reader: GcsfsCsvReader,
        region_raw_file_config: DirectIngestRegionRawFileConfig,
        allow_incomplete_configs: bool = False,
    ) -> None:
        self.csv_reader = csv_reader
        self.region_raw_file_config = region_raw_file_config
        self.allow_incomplete_configs = allow_incomplete_configs

    def read_raw_file_from_gcs(
        self,
        path: GcsfsFilePath,
        delegate: GcsfsCsvReaderDelegate,
        chunk_size_override: Optional[int] = None,
    ) -> None:
        parts = filename_parts_from_path(path)
        file_config = self.region_raw_file_config.raw_file_configs[parts.file_tag]

        columns = self._get_validated_columns(path, file_config)

        self.csv_reader.streaming_read(
            path,
            delegate=delegate,
            chunk_size=chunk_size_override or file_config.import_chunk_size_rows,
            encodings_to_try=file_config.encodings_to_try(),
            index_col=False,
            header=0 if not file_config.infer_columns_from_config else None,
            names=columns,
            keep_default_na=False,
            **self._common_read_csv_kwargs(file_config),
        )

    def _get_validated_columns(
        self, path: GcsfsFilePath, file_config: DirectIngestRawFileConfig
    ) -> List[str]:
        """Returns a list of normalized column names for the raw data file at the given path."""

        delegate = ReadOneGcsfsCsvReaderDelegate()
        self.csv_reader.streaming_read(
            path,
            delegate=delegate,
            chunk_size=1,
            encodings_to_try=file_config.encodings_to_try(),
            nrows=1,
            **self._common_read_csv_kwargs(file_config),
        )
        df = delegate.df

        if not isinstance(df, pd.DataFrame):
            raise ValueError(f"Unexpected type for DataFrame: [{type(df)}]")

        columns_from_file_config = [column.name for column in file_config.columns]

        if file_config.infer_columns_from_config:
            if len(columns_from_file_config) != len(df.columns):
                raise ValueError(
                    f"Found {len(columns_from_file_config)} columns defined in {file_config.file_tag} "
                    f"but found {len(df.columns)} in the DataFrame from the CSV. Make sure "
                    f"all expected columns are defined in the raw data configuration."
                )
            try:
                csv_columns = [
                    normalize_column_name_for_bq(column_name)
                    for column_name in df.columns
                ]
            except IndexError:
                # This indicates that there are empty column values in the DF, which highly
                # suggests that we are working with a file that does not have header rows.
                return columns_from_file_config

            if set(csv_columns).intersection(set(columns_from_file_config)):
                raise ValueError(
                    "Found an unexpected header in the CSV. Please remove the header row from the CSV."
                )
            return columns_from_file_config

        csv_columns = [
            normalize_column_name_for_bq(column_name) for column_name in df.columns
        ]
        normalized_csv_columns = set()
        for i, column_name in enumerate(csv_columns):
            if not column_name:
                raise ValueError(f"Found empty column name in [{file_config.file_tag}]")

            # If the capitalization of the column name doesn't match the capitalization
            # listed in the file config, update the capitalization.
            if column_name not in file_config.columns:
                caps_normalized_col = file_config.caps_normalized_col(column_name)
                if caps_normalized_col:
                    column_name = caps_normalized_col

            if column_name in normalized_csv_columns:
                raise ValueError(
                    f"Multiple columns with name [{column_name}] after normalization."
                )
            normalized_csv_columns.add(column_name)
            csv_columns[i] = column_name

        if len(normalized_csv_columns) == 1:
            # A single-column file is almost always indicative of a parsing error. If
            # this column name is not registered in the file config, we throw.
            column = one(normalized_csv_columns)
            if column not in file_config.columns:
                raise ValueError(
                    f"Found only one column: [{column}]. Columns likely did not "
                    f"parse properly. Are you using the correct separator and encoding "
                    f"for this file? If this file really has just one column, the "
                    f"column name must be registered in the raw file config before "
                    f"upload."
                )

        if not self.allow_incomplete_configs:
            check_found_columns_are_subset_of_config(
                raw_file_config=file_config, found_columns=normalized_csv_columns
            )

        return csv_columns

    @staticmethod
    def _common_read_csv_kwargs(
        file_config: DirectIngestRawFileConfig,
    ) -> Dict[str, Any]:
        """Returns a set of arguments to be passed to the pandas.read_csv() call, based
        on the provided raw file config.
        """
        kwargs = {
            "sep": file_config.separator,
            "quoting": (
                csv.QUOTE_NONE if file_config.ignore_quotes else csv.QUOTE_MINIMAL
            ),
        }

        if file_config.custom_line_terminator:
            kwargs["lineterminator"] = file_config.custom_line_terminator

        # We get the following warning if we do not override the
        # engine in this case: "ParserWarning: Falling back to the 'python'
        # engine because the separator encoded in utf-8 is > 1 char
        # long, and the 'c' engine does not support such separators;
        # you can avoid this warning by specifying engine='python'.
        if len(file_config.separator.encode(UTF_8_ENCODING)) > 1:
            # The python engine is slower but more feature-complete.
            kwargs["engine"] = "python"

        return kwargs


class DirectIngestRawFileImportManager:
    """Class that stores raw data import configs for a region, with functionality for
    executing an import of a specific file.
    """

    def __init__(
        self,
        *,
        region: DirectIngestRegion,
        fs: DirectIngestGCSFileSystem,
        temp_output_directory_path: GcsfsDirectoryPath,
        big_query_client: BigQueryClient,
        csv_reader: GcsfsCsvReader,
        instance: DirectIngestInstance,
        region_raw_file_config: Optional[DirectIngestRegionRawFileConfig] = None,
        sandbox_dataset_prefix: Optional[str] = None,
        allow_incomplete_configs: bool = False,
    ):

        self.region = region
        self.fs = fs
        self.temp_output_directory_path = temp_output_directory_path
        self.big_query_client = big_query_client
        self.region_raw_file_config = (
            region_raw_file_config
            if region_raw_file_config
            else DirectIngestRegionRawFileConfig(
                region_code=self.region.region_code,
                region_module=self.region.region_module,
            )
        )
        self.raw_file_reader = DirectIngestRawFileReader(
            csv_reader=csv_reader,
            region_raw_file_config=self.region_raw_file_config,
            allow_incomplete_configs=allow_incomplete_configs,
        )
        self.instance = instance
        self.raw_table_migrations = DirectIngestRawTableMigrationCollector(
            region_code=self.region.region_code,
            instance=self.instance,
            regions_module_override=self.region.region_module,
        ).collect_raw_table_migration_queries(sandbox_dataset_prefix)
        self.raw_tables_dataset = raw_tables_dataset_for_region(
            state_code=StateCode(self.region.region_code.upper()),
            instance=instance,
            sandbox_dataset_prefix=sandbox_dataset_prefix,
        )

    def import_raw_file_to_big_query(
        self,
        path: GcsfsFilePath,
        file_metadata: DirectIngestRawFileMetadata,
    ) -> None:
        """Import a raw data file at the given path to the appropriate raw data table in BigQuery."""
        parts = filename_parts_from_path(path)
        if parts.file_tag not in self.region_raw_file_config.raw_file_tags:
            raise ValueError(
                f"Attempting to import raw file with tag [{parts.file_tag}] unspecified by [{self.region.region_code}] "
                f"config."
            )

        logging.info("Beginning BigQuery upload of raw file [%s]", path.abs_path())

        self._delete_conflicting_contents_from_bigquery(path, file_metadata.file_id)
        temp_output_paths, columns = self._upload_contents_to_temp_gcs_paths(
            path, file_metadata
        )
        if temp_output_paths:
            if not columns:
                raise ValueError("Found delegate output_columns is unexpectedly None.")
            self._load_contents_to_bigquery(parts.file_tag, temp_output_paths, columns)

        migration_queries = self.raw_table_migrations.get(parts.file_tag, [])

        logging.info(
            "Running [%s] migration queries for table [%s]",
            len(migration_queries),
            parts.file_tag,
        )
        for migration_query in migration_queries:
            query_job = self.big_query_client.run_query_async(
                query_str=migration_query, use_query_cache=False
            )
            try:
                # Wait for the migration query to complete before running the next one
                query_job.result()
            except Exception as e:
                logging.error(
                    "Migration query job [%s] failed with errors: [%s]",
                    query_job.job_id,
                    query_job.errors,
                )
                raise e

        logging.info("Completed BigQuery import of [%s]", path.abs_path())

    def _upload_contents_to_temp_gcs_paths(
        self,
        path: GcsfsFilePath,
        file_metadata: DirectIngestRawFileMetadata,
    ) -> Tuple[List[GcsfsFilePath], Optional[List[str]]]:
        """Uploads the contents of the file at the provided path to one or more GCS files, with whitespace stripped and
        additional metadata columns added.
        Returns a list of tuple pairs containing the destination paths and corrected CSV columns for that file.
        """

        logging.info("Starting chunked upload of contents to GCS")

        delegate = DirectIngestRawDataSplittingGcsfsCsvReaderDelegate(
            path, self.fs, file_metadata, self.temp_output_directory_path
        )

        self.raw_file_reader.read_raw_file_from_gcs(path, delegate)

        return delegate.output_paths, delegate.output_columns

    def _delete_conflicting_contents_from_bigquery(
        self, path: GcsfsFilePath, file_id: int
    ) -> None:
        """Delete any rows that have already been uploaded with this file_id.
        These rows could exist from a prior upload failing part way through
        and removing them prevents the table from ending up with duplicate
        rows after this upload"""

        table_id = filename_parts_from_path(path).file_tag
        if not self.big_query_client.table_exists(
            self.big_query_client.dataset_ref_for_id(self.raw_tables_dataset), table_id
        ):
            logging.info(
                "Skipping row cleanup as %s.%s does not yet exist",
                self.raw_tables_dataset,
                table_id,
            )
            return

        # Starts the deletion
        delete_job = self.big_query_client.delete_from_table_async(
            dataset_id=self.raw_tables_dataset,
            table_id=table_id,
            filter_clause="WHERE file_id = " + str(file_id),
        )
        # Waits for the deletion to complete
        delete_job.result()

    @retry.Retry(predicate=google_api_retry_predicate)
    def _load_contents_to_bigquery(
        self, file_tag: str, temp_output_paths: List[GcsfsFilePath], columns: List[str]
    ) -> None:
        """Loads the contents in the given handle to the appropriate table in BigQuery."""

        logging.info("Starting chunked load of contents to BigQuery")

        try:
            load_job = self.big_query_client.load_table_from_cloud_storage_async(
                source_uris=[p.uri() for p in temp_output_paths],
                destination_dataset_ref=self.big_query_client.dataset_ref_for_id(
                    self.raw_tables_dataset
                ),
                destination_table_id=file_tag,
                destination_table_schema=self.create_raw_table_schema_from_columns(
                    columns
                ),
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            )
        except Exception as e:
            logging.error("Failed to start load job - cleaning up temp paths")
            self._delete_temp_output_paths(temp_output_paths)
            raise e

        try:
            logging.info(
                "[%s] Waiting for load of [%s] paths into [%s]",
                datetime.datetime.now().isoformat(),
                len(temp_output_paths),
                load_job.destination,
            )
            load_job.result()
            logging.info(
                "[%s] BigQuery load of [%s] paths complete",
                datetime.datetime.now().isoformat(),
                len(temp_output_paths),
            )
        except Exception as e:
            logging.error(
                "Insert job [%s] failed with errors: [%s]",
                load_job.job_id,
                load_job.errors,
            )
            raise e
        finally:
            self._delete_temp_output_paths(temp_output_paths)

    def _delete_temp_output_paths(self, temp_output_paths: List[GcsfsFilePath]) -> None:
        for temp_output_path in temp_output_paths:
            logging.info("Deleting temp file [%s].", temp_output_path.abs_path())
            self.fs.delete(temp_output_path)

    @staticmethod
    def create_raw_table_schema_from_columns(
        columns: Iterable[str],
    ) -> List[bigquery.SchemaField]:
        """Creates schema for use in `to_gbq` based on the provided columns."""
        schema = []
        for name in columns:
            typ_str = bigquery.enums.SqlTypeNames.STRING.value
            mode = "NULLABLE"
            if name == FILE_ID_COL_NAME:
                mode = "REQUIRED"
                typ_str = bigquery.enums.SqlTypeNames.INTEGER.value
            if name == UPDATE_DATETIME_COL_NAME:
                mode = "REQUIRED"
                typ_str = bigquery.enums.SqlTypeNames.DATETIME.value
            if name == IS_DELETED_COL_NAME:
                mode = "REQUIRED"
                typ_str = bigquery.enums.SqlTypeNames.BOOLEAN.value
            schema.append(
                bigquery.SchemaField(name=name, field_type=typ_str, mode=mode)
            )
        return schema


class DirectIngestRawDataSplittingGcsfsCsvReaderDelegate(
    SplittingGcsfsCsvReaderDelegate
):
    """An implementation of the GcsfsCsvReaderDelegate that augments chunks of a raw data file and re-uploads each
    chunk to a temporary Google Cloud Storage path.
    """

    def __init__(
        self,
        path: GcsfsFilePath,
        fs: DirectIngestGCSFileSystem,
        file_metadata: DirectIngestRawFileMetadata,
        temp_output_directory_path: GcsfsDirectoryPath,
    ):
        super().__init__(path, fs, include_header=False)
        self.file_metadata = file_metadata
        self.temp_output_directory_path = temp_output_directory_path

    def transform_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        # Stripping white space from all fields
        df = df.applymap(lambda x: x.strip())

        num_rows_before_filter = df.shape[0]

        # Filter out rows where ALL values are null / empty string. Solution largely
        # copied from https://stackoverflow.com/a/41401892.
        df = df[~pd.isnull(df.applymap(lambda x: None if x == "" else x)).all(axis=1)]

        num_rows_after_filter = df.shape[0]
        if num_rows_before_filter > num_rows_after_filter:
            logging.error(
                "Filtered out [%s] rows that contained only empty/null values",
                num_rows_before_filter - num_rows_after_filter,
            )

        augmented_df = self._augment_raw_data_with_metadata_columns(
            path=self.path, file_metadata=self.file_metadata, raw_data_df=df
        )
        return augmented_df

    def get_output_path(self, chunk_num: int) -> GcsfsFilePath:
        name, _extension = os.path.splitext(self.path.file_name)

        return GcsfsFilePath.from_directory_and_file_name(
            self.temp_output_directory_path, f"temp_{name}_{chunk_num}.csv"
        )

    @staticmethod
    def _augment_raw_data_with_metadata_columns(
        path: GcsfsFilePath,
        file_metadata: DirectIngestRawFileMetadata,
        raw_data_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """Add file_id and update_datetime columns to all rows in the dataframe."""

        parts = filename_parts_from_path(path)

        return augment_raw_data_df_with_metadata_columns(
            raw_data_df=raw_data_df,
            file_id=file_metadata.file_id,
            utc_upload_datetime=parts.utc_upload_datetime,
        )


def check_found_columns_are_subset_of_config(
    raw_file_config: DirectIngestRawFileConfig, found_columns: Iterable[str]
) -> None:
    """Check that all of the columns that are in the raw data config are also in
    the columns found in the CSV. If there are columns that are not in the raw data
    configuration but found in the CSV, then we throw an error to have both match
    (unless we are in a state where we allow incomplete configurations, like
    testing).
    """

    # BQ is case-agnostic when evaluating column names so we can be as well.
    columns_from_file_config_lower = {
        column.name.lower() for column in raw_file_config.columns
    }
    found_columns_lower = set(c.lower() for c in found_columns)

    if len(found_columns_lower) != len(list(found_columns)):
        raise ValueError(
            f"Found duplicate columns in found_columns list: {list(found_columns)}"
        )

    if not found_columns_lower.issubset(columns_from_file_config_lower):
        extra_columns = found_columns_lower.difference(columns_from_file_config_lower)
        raise ValueError(
            f"Found columns in raw file {sorted(extra_columns)} that are not "
            f"defined in the raw data configuration for "
            f"[{raw_file_config.file_tag}]. Make sure that all columns from CSV "
            f"are defined in the raw data configuration."
        )


def augment_raw_data_df_with_metadata_columns(
    raw_data_df: pd.DataFrame,
    file_id: int,
    utc_upload_datetime: datetime.datetime,
) -> pd.DataFrame:
    logging.info(
        "Adding extra columns with file_id [%s] and update_datetime [%s]",
        file_id,
        utc_upload_datetime,
    )
    raw_data_df[FILE_ID_COL_NAME] = file_id
    # The update_datetime column in BQ is not timezone-aware, so we strip the timezone
    # info from the timestamp here.
    if utc_upload_datetime.tzinfo is None or utc_upload_datetime.tzinfo != pytz.UTC:
        raise ValueError(
            "Expected utc_upload_datetime.tzinfo value to be pytz.UTC. "
            f"Got: {utc_upload_datetime.tzinfo}"
        )
    raw_data_df[UPDATE_DATETIME_COL_NAME] = utc_upload_datetime.replace(tzinfo=None)

    # TODO(##18944): For now, default the value of `is_deleted` is False. Once raw data pruning is launched and the
    # value of `is_deleted` is conditionally set, delete this default value.
    raw_data_df[IS_DELETED_COL_NAME] = False

    return raw_data_df


_RAW_TABLE_CONFIGS_BY_STATE = {}


def get_region_raw_file_config(
    region_code: str, region_module: ModuleType = regions
) -> DirectIngestRegionRawFileConfig:
    region_code_lower = region_code.lower()
    if region_code_lower not in _RAW_TABLE_CONFIGS_BY_STATE:
        _RAW_TABLE_CONFIGS_BY_STATE[
            region_code_lower
        ] = DirectIngestRegionRawFileConfig(region_code_lower, region_module)

    return _RAW_TABLE_CONFIGS_BY_STATE[region_code_lower]


# TODO(#12390): Delete once raw data pruning is live.
def raw_data_pruning_enabled_in_state_and_instance(
    state_code: StateCode,  # pylint: disable=unused-argument
    instance: DirectIngestInstance,  # pylint: disable=unused-argument
) -> bool:
    return False
