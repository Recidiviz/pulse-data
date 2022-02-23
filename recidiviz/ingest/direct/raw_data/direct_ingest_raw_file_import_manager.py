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
import time
from types import ModuleType
from typing import Any, Dict, List, Optional, Set, Tuple

import attr
import pandas as pd
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery
from more_itertools import one

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.big_query.big_query_utils import normalize_column_name_for_bq
from recidiviz.cloud_storage.gcsfs_csv_reader import (
    COMMON_RAW_FILE_ENCODINGS,
    UTF_8_ENCODING,
    GcsfsCsvReader,
)
from recidiviz.cloud_storage.gcsfs_csv_reader_delegates import (
    ReadOneGcsfsCsvReaderDelegate,
    SplittingGcsfsCsvReaderDelegate,
)
from recidiviz.cloud_storage.gcsfs_path import (
    GcsfsBucketPath,
    GcsfsDirectoryPath,
    GcsfsFilePath,
)
from recidiviz.common import attr_validators
from recidiviz.ingest.direct import regions
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
)
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    GcsfsDirectIngestFileType,
    filename_parts_from_path,
)
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_migration_collector import (
    DirectIngestRawTableMigrationCollector,
)
from recidiviz.ingest.direct.types.direct_ingest_constants import (
    FILE_ID_COL_NAME,
    UPDATE_DATETIME_COL_NAME,
)
from recidiviz.persistence.entity.operations.entities import DirectIngestRawFileMetadata
from recidiviz.utils.regions import Region
from recidiviz.utils.yaml_dict import YAMLDict


@attr.s
class ColumnEnumValueInfo:
    # The literal enum value
    value: str = attr.ib(validator=attr_validators.is_non_empty_str)
    # The description that value maps to
    description: Optional[str] = attr.ib(validator=attr_validators.is_opt_str)


@attr.s
class RawTableColumnInfo:
    """Stores information about a single raw data table column."""

    # The column name in BigQuery-compatible, normalized form (e.g. punctuation stripped)
    name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    # True if a column is a date/time
    is_datetime: bool = attr.ib(validator=attr.validators.instance_of(bool))
    # Describes the column contents - if None, this column cannot be used for ingest, nor will you be able to write a
    # raw data migration involving this column.
    description: Optional[str] = attr.ib(validator=attr_validators.is_opt_str)
    # Describes possible enum values for this column if known
    known_values: Optional[List[ColumnEnumValueInfo]] = attr.ib(
        default=None, validator=attr_validators.is_opt_list
    )

    @property
    def is_enum(self) -> bool:
        """If true, this is an 'enum' field, with an enumerable set of values and the
        known_values field can be auto-refreshed with the enum fetching script."""
        return self.known_values is not None

    @property
    def known_values_nonnull(self) -> List[ColumnEnumValueInfo]:
        """Returns the known_values as a nonnull (but potentially empty) list. Raises if
        the known_values list is None (i.e. if this column is not an enum column."""
        if not self.is_enum:
            raise ValueError(f"Expected is_enum is True for column: [{self.name}]")
        if self.known_values is None:
            raise ValueError(
                f"Expected nonnull column known_values for column: [{self.name}]"
            )
        return self.known_values


@attr.s(frozen=True)
class DirectIngestRawFileConfig:
    """Struct for storing any configuration for raw data imports for a certain file tag."""

    # The file tag / table name that this file will get written to
    file_tag: str = attr.ib(validator=attr_validators.is_non_empty_str)

    # The path to the config file
    file_path: str = attr.ib(validator=attr_validators.is_non_empty_str)

    # Description of the raw data file contents
    file_description: str = attr.ib(validator=attr_validators.is_non_empty_str)

    # A list of columns that constitute the primary key for this file. If empty, this table cannot be used in an ingest
    # view query and a '*_latest' view will not be generated for this table. May be left empty for the purposes of
    # allowing us to quickly upload a new file into BQ and then determine the primary keys by querying BQ.
    primary_key_cols: List[str] = attr.ib(validator=attr.validators.instance_of(list))

    # A list of names and descriptions for each column in a file
    columns: List[RawTableColumnInfo] = attr.ib()

    # An additional string clause that will be added to the ORDER BY list that determines which is the most up-to-date
    # row to pick among all rows that have the same primary key.
    # NOTE: Right now this clause does not have access to the date-normalized version of the columns in datetime_cols,
    #  so must handle its own date parsing logic - if this becomes too cumbersome, we can restructure the query to do
    #  date normalization in a subquery before ordering.
    supplemental_order_by_clause: str = attr.ib()

    # Most likely string encoding for this file (e.g. UTF-8)
    encoding: str = attr.ib()

    # The separator character used to denote columns (e.g. ',' or '|').
    separator: str = attr.ib()

    # The line terminator character(s) used to denote CSV rows. If None, will default to
    # the Pandas default (any combination of \n and \r).
    custom_line_terminator: Optional[str] = attr.ib()

    # If true, quoted strings are ignored and separators inside of quotes are treated as column separators. This should
    # be used on any file that has free text fields where the quotes are not escaped and the separator is not common to
    # free text. For example, to handle this row from a pipe separated file that has an open quotation with no close
    # quote:
    #     123|456789|2|He said, "I will be there.|ASDF
    ignore_quotes: bool = attr.ib()

    # TODO(#4243): Add alerts for order in magnitude changes in exported files.
    # If true, means that we **always** will get a historical version of this raw data file from the state and will
    # never change to incremental uploads (for example, because we need to detect row deletions).
    always_historical_export: bool = attr.ib()

    # A comma-separated string representation of the primary keys
    primary_key_str: str = attr.ib()

    @primary_key_str.default
    def _primary_key_str(self) -> str:
        return ", ".join(self.primary_key_cols)

    def encodings_to_try(self) -> List[str]:
        """Returns an ordered list of encodings we should try for this file."""
        return [self.encoding] + [
            encoding
            for encoding in COMMON_RAW_FILE_ENCODINGS
            if encoding.upper() != self.encoding.upper()
        ]

    @property
    def available_columns(self) -> List[RawTableColumnInfo]:
        """Filters to only columns that can be used for ingest.

        Currently just excludes undocumented columns.
        """
        return [column for column in self.columns if column.description]

    @property
    def available_datetime_cols(self) -> List[str]:
        return [
            column.name
            for column in self.columns
            if column.is_datetime and column.description
        ]

    @property
    def available_non_datetime_cols(self) -> List[str]:
        return [
            column.name
            for column in self.columns
            if not column.is_datetime and column.description
        ]

    @property
    def non_datetime_cols(self) -> List[str]:
        return [column.name for column in self.columns if not column.is_datetime]

    @property
    def datetime_cols(self) -> List[str]:
        return [column.name for column in self.columns if column.is_datetime]

    @property
    def has_enums(self) -> bool:
        """If true, columns with enum values exist within this raw file, and this config is eligible to be refreshed
        with the for the fetch_column_values_for_state script."""
        return bool([column.name for column in self.columns if column.is_enum])

    @property
    def is_undocumented(self) -> bool:
        documented_cols = [column.name for column in self.columns if column.description]
        return not documented_cols

    def caps_normalized_col(self, col_name: str) -> Optional[str]:
        """If the provided column name has a case-insensitive match in the columns list,
        returns the proper capitalization of the column, as listed in the configuration
        file.
        """
        for registered_col in self.columns:
            if registered_col.name.lower() == col_name.lower():
                return registered_col.name
        return None

    @classmethod
    def from_yaml_dict(
        cls,
        file_tag: str,
        file_path: str,
        default_encoding: str,
        default_separator: str,
        default_line_terminator: Optional[str],
        default_ignore_quotes: bool,
        file_config_dict: YAMLDict,
        yaml_filename: str,
    ) -> "DirectIngestRawFileConfig":
        """Returns a DirectIngestRawFileConfig built from a YAMLDict"""
        primary_key_cols = file_config_dict.pop("primary_key_cols", list)
        file_description = file_config_dict.pop("file_description", str)
        columns = file_config_dict.pop("columns", list)

        column_names = [column["name"] for column in columns]
        if len(column_names) != len(set(column_names)):
            raise ValueError(f"Found duplicate columns in raw_file [{file_tag}]")

        missing_columns = set(primary_key_cols) - {column["name"] for column in columns}
        if missing_columns:
            raise ValueError(
                f"Column(s) marked as primary keys not listed in"
                f" columns list for file [{yaml_filename}]: {missing_columns}"
            )

        supplemental_order_by_clause = file_config_dict.pop_optional(
            "supplemental_order_by_clause", str
        )
        encoding = file_config_dict.pop_optional("encoding", str)
        separator = file_config_dict.pop_optional("separator", str)
        ignore_quotes = file_config_dict.pop_optional("ignore_quotes", bool)
        custom_line_terminator = file_config_dict.pop_optional(
            "custom_line_terminator", str
        )
        always_historical_export = file_config_dict.pop_optional(
            "always_historical_export", bool
        )

        if len(file_config_dict) > 0:
            raise ValueError(
                f"Found unexpected config values for raw file"
                f"[{file_tag}]: {repr(file_config_dict.get())}"
            )
        return DirectIngestRawFileConfig(
            file_tag=file_tag,
            file_path=file_path,
            file_description=file_description,
            primary_key_cols=primary_key_cols,
            columns=[
                RawTableColumnInfo(
                    name=column["name"],
                    is_datetime=column.get("is_datetime", False),
                    description=column.get("description", None),
                    known_values=[
                        ColumnEnumValueInfo(
                            value=str(x["value"]),
                            description=x.get("description", None),
                        )
                        for x in column["known_values"]
                    ]
                    if "known_values" in column
                    else None,
                )
                for column in columns
            ],
            supplemental_order_by_clause=supplemental_order_by_clause
            if supplemental_order_by_clause
            else "",
            encoding=encoding if encoding else default_encoding,
            separator=separator if separator else default_separator,
            custom_line_terminator=custom_line_terminator
            if custom_line_terminator
            else default_line_terminator,
            ignore_quotes=ignore_quotes if ignore_quotes else default_ignore_quotes,
            always_historical_export=always_historical_export
            if always_historical_export
            else False,
        )


@attr.s
class DirectIngestRawFileDefaultConfig:
    """Class that stores information about a region's default config"""

    # The default config file name
    filename: str = attr.ib(validator=attr_validators.is_non_empty_str)
    # The default encoding for raw files from this region
    default_encoding: str = attr.ib(validator=attr_validators.is_non_empty_str)
    # The default separator for raw files from this region
    default_separator: str = attr.ib(validator=attr_validators.is_non_empty_str)
    # The default setting for whether to ignore quotes in files from this region
    default_ignore_quotes: bool = attr.ib(validator=attr_validators.is_bool)
    # The default line terminator for raw files from this region
    default_line_terminator: Optional[str] = attr.ib(
        default=None,
        validator=attr_validators.is_opt_str,
    )


@attr.s
class DirectIngestRegionRawFileConfig:
    """Class that parses and stores raw data import configs for a region"""

    # TODO(#5262): Add documentation for the structure of the raw data yaml files
    region_code: str = attr.ib()
    region_module: ModuleType = attr.ib(default=regions)
    yaml_config_file_dir: str = attr.ib()
    raw_file_configs: Dict[str, DirectIngestRawFileConfig] = attr.ib()

    def default_config(self) -> DirectIngestRawFileDefaultConfig:
        default_filename = f"{self.region_code.lower()}_default.yaml"
        default_file_path = os.path.join(self.yaml_config_file_dir, default_filename)
        if not os.path.exists(default_file_path):
            raise ValueError(
                f"Missing default raw data configs for region: {self.region_code}. "
                f"None found at path: [{default_file_path}]"
            )
        default_contents = YAMLDict.from_path(default_file_path)
        default_encoding = default_contents.pop("default_encoding", str)
        default_separator = default_contents.pop("default_separator", str)
        default_line_terminator = default_contents.pop_optional(
            "default_line_terminator", str
        )
        default_ignore_quotes = default_contents.pop("default_ignore_quotes", bool)

        return DirectIngestRawFileDefaultConfig(
            filename=default_filename,
            default_encoding=default_encoding,
            default_separator=default_separator,
            default_line_terminator=default_line_terminator,
            default_ignore_quotes=default_ignore_quotes,
        )

    def _region_ingest_dir(self) -> str:
        if self.region_module.__file__ is None:
            raise ValueError(f"No file associated with {self.region_module}.")
        return os.path.join(
            os.path.dirname(self.region_module.__file__), f"{self.region_code.lower()}"
        )

    @yaml_config_file_dir.default
    def _config_file_dir(self) -> str:
        return os.path.join(self._region_ingest_dir(), "raw_data")

    @raw_file_configs.default
    def _raw_data_file_configs(self) -> Dict[str, DirectIngestRawFileConfig]:
        return self._get_raw_data_file_configs()

    def _get_raw_data_file_configs(self) -> Dict[str, DirectIngestRawFileConfig]:
        """Returns list of file tags we expect to see on raw files for this region."""
        if os.path.isdir(self.yaml_config_file_dir):
            default_config = self.default_config()

            raw_data_configs = {}
            for filename in os.listdir(self.yaml_config_file_dir):
                if filename == default_config.filename or not filename.endswith(
                    ".yaml"
                ):
                    continue
                yaml_file_path = os.path.join(self.yaml_config_file_dir, filename)
                if os.path.isdir(yaml_file_path):
                    continue

                yaml_contents = YAMLDict.from_path(yaml_file_path)

                file_tag = yaml_contents.pop("file_tag", str)
                if not file_tag:
                    raise ValueError(f"Missing file_tag in [{yaml_file_path}]")
                if filename != f"{self.region_code.lower()}_{file_tag}.yaml":
                    raise ValueError(
                        f"Mismatched file_tag [{file_tag}] and filename [{filename}]"
                        f" in [{yaml_file_path}]"
                    )
                if file_tag in raw_data_configs:
                    raise ValueError(
                        f"Found file tag [{file_tag}] in [{yaml_file_path}]"
                        f" that is already defined in another yaml file."
                    )

                raw_data_configs[file_tag] = DirectIngestRawFileConfig.from_yaml_dict(
                    file_tag,
                    yaml_file_path,
                    default_config.default_encoding,
                    default_config.default_separator,
                    default_config.default_line_terminator,
                    default_config.default_ignore_quotes,
                    yaml_contents,
                    filename,
                )
        else:
            raise ValueError(
                f"Missing raw data configs for region: {self.region_code}. "
                f"None found at path [{self.yaml_config_file_dir}]."
            )
        return raw_data_configs

    raw_file_tags: Set[str] = attr.ib()

    @raw_file_tags.default
    def _raw_file_tags(self) -> Set[str]:
        return set(self.raw_file_configs.keys())


_DEFAULT_BQ_UPLOAD_CHUNK_SIZE = 250000

# The number of seconds of spacing we need to have between each table load operation to avoid going over the
# "5 operations every 10 seconds per table" rate limit (with a little buffer): https://cloud.google.com/bigquery/quotas
_PER_TABLE_UPDATE_RATE_LIMITING_SEC = 2.5


class DirectIngestRawFileImportManager:
    """Class that stores raw data import configs for a region, with functionality for executing an import of a specific
    file.
    """

    def __init__(
        self,
        *,
        region: Region,
        fs: DirectIngestGCSFileSystem,
        ingest_bucket_path: GcsfsBucketPath,
        temp_output_directory_path: GcsfsDirectoryPath,
        big_query_client: BigQueryClient,
        region_raw_file_config: Optional[DirectIngestRegionRawFileConfig] = None,
        upload_chunk_size: int = _DEFAULT_BQ_UPLOAD_CHUNK_SIZE,
        sandbox_dataset_prefix: Optional[str] = None,
    ):

        self.region = region
        self.fs = fs
        self.ingest_bucket_path = ingest_bucket_path
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
        self.upload_chunk_size = upload_chunk_size
        self.csv_reader = GcsfsCsvReader(fs)
        self.raw_table_migrations = DirectIngestRawTableMigrationCollector(
            region_code=self.region.region_code,
            regions_module_override=self.region.region_module,
        ).collect_raw_table_migration_queries(sandbox_dataset_prefix)
        self.raw_tables_dataset = raw_tables_dataset_for_region(
            region_code=self.region.region_code,
            sandbox_dataset_prefix=sandbox_dataset_prefix,
        )

    def get_unprocessed_raw_files_to_import(self) -> List[GcsfsFilePath]:
        unprocessed_paths = self.fs.get_unprocessed_file_paths(
            self.ingest_bucket_path, GcsfsDirectIngestFileType.RAW_DATA
        )
        paths_to_import = []
        unrecognized_file_tags = set()
        for path in unprocessed_paths:
            parts = filename_parts_from_path(path)
            if parts.file_tag in self.region_raw_file_config.raw_file_tags:
                paths_to_import.append(path)
            else:
                unrecognized_file_tags.add(parts.file_tag)

        for file_tag in sorted(unrecognized_file_tags):
            logging.warning(
                "Unrecognized raw file tag [%s] for region [%s].",
                file_tag,
                self.region.region_code,
            )

        return paths_to_import

    def import_raw_file_to_big_query(
        self, path: GcsfsFilePath, file_metadata: DirectIngestRawFileMetadata
    ) -> None:
        """Import a raw data file at the given path to the appropriate raw data table in BigQuery."""
        parts = filename_parts_from_path(path)
        if parts.file_tag not in self.region_raw_file_config.raw_file_tags:
            raise ValueError(
                f"Attempting to import raw file with tag [{parts.file_tag}] unspecified by [{self.region.region_code}] "
                f"config."
            )

        if parts.file_type != GcsfsDirectIngestFileType.RAW_DATA:
            raise ValueError(
                f"Unexpected file type [{parts.file_type}] for path [{parts.file_tag}]."
            )

        logging.info("Beginning BigQuery upload of raw file [%s]", path.abs_path())

        self._delete_conflicting_contents_from_bigquery(path, file_metadata.file_id)
        temp_output_paths = self._upload_contents_to_temp_gcs_paths(path, file_metadata)
        self._load_contents_to_bigquery(path, temp_output_paths)

        migration_queries = self.raw_table_migrations.get(parts.file_tag, [])
        logging.info(
            "Running [%s] migration queries for table [%s]",
            len(migration_queries),
            parts.file_tag,
        )
        for migration_query in migration_queries:
            query_job = self.big_query_client.run_query_async(query_str=migration_query)
            # Wait for the migration query to complete before running the next one
            query_job.result()

        logging.info("Completed BigQuery import of [%s]", path.abs_path())

    def _upload_contents_to_temp_gcs_paths(
        self, path: GcsfsFilePath, file_metadata: DirectIngestRawFileMetadata
    ) -> List[Tuple[GcsfsFilePath, List[str]]]:
        """Uploads the contents of the file at the provided path to one or more GCS files, with whitespace stripped and
        additional metadata columns added.
        Returns a list of tuple pairs containing the destination paths and corrected CSV columns for that file.
        """

        logging.info("Starting chunked upload of contents to GCS")

        parts = filename_parts_from_path(path)
        file_config = self.region_raw_file_config.raw_file_configs[parts.file_tag]

        columns = self._get_validated_columns(path, file_config)

        delegate = DirectIngestRawDataSplittingGcsfsCsvReaderDelegate(
            path, self.fs, file_metadata, self.temp_output_directory_path
        )

        self.csv_reader.streaming_read(
            path,
            delegate=delegate,
            chunk_size=self.upload_chunk_size,
            encodings_to_try=file_config.encodings_to_try(),
            index_col=False,
            header=0,
            names=columns,
            keep_default_na=False,
            **self._common_read_csv_kwargs(file_config),
        )

        return delegate.output_paths_with_columns

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

    def _load_contents_to_bigquery(
        self,
        path: GcsfsFilePath,
        temp_paths_with_columns: List[Tuple[GcsfsFilePath, List[str]]],
    ) -> None:
        """Loads the contents in the given handle to the appropriate table in BigQuery."""

        logging.info("Starting chunked load of contents to BigQuery")
        temp_output_paths = [path for path, _ in temp_paths_with_columns]
        temp_path_to_load_job: Dict[GcsfsFilePath, bigquery.LoadJob] = {}

        try:
            for i, (temp_output_path, columns) in enumerate(temp_paths_with_columns):
                if i > 0:
                    # Note: If this sleep becomes a serious performance issue, we could refactor to intersperse reading
                    # chunks to temp paths with starting each load job. In this case, we'd have to be careful to delete
                    # any partially uploaded uploaded portion of the file if we fail to parse a chunk in the middle.
                    logging.info(
                        "Sleeping for [%s] seconds to avoid exceeding per-table update rate quotas.",
                        _PER_TABLE_UPDATE_RATE_LIMITING_SEC,
                    )
                    time.sleep(_PER_TABLE_UPDATE_RATE_LIMITING_SEC)

                parts = filename_parts_from_path(path)
                load_job = self.big_query_client.load_into_table_from_cloud_storage_async(
                    source_uri=temp_output_path.uri(),
                    destination_dataset_ref=self.big_query_client.dataset_ref_for_id(
                        self.raw_tables_dataset
                    ),
                    destination_table_id=parts.file_tag,
                    destination_table_schema=self._create_raw_table_schema_from_columns(
                        columns
                    ),
                )
                logging.info("Load job [%s] for chunk [%d] started", load_job.job_id, i)

                temp_path_to_load_job[temp_output_path] = load_job
        except Exception as e:
            logging.error("Failed to start load jobs - cleaning up temp paths")
            self._delete_temp_output_paths(temp_output_paths)
            raise e

        try:
            self._wait_for_jobs(temp_path_to_load_job)
        finally:
            self._delete_temp_output_paths(temp_output_paths)

    @staticmethod
    def _wait_for_jobs(
        temp_path_to_load_job: Dict[GcsfsFilePath, bigquery.LoadJob]
    ) -> None:
        for temp_output_path, load_job in temp_path_to_load_job.items():
            try:
                logging.info(
                    "Waiting for load of [%s] into [%s]",
                    temp_output_path.abs_path(),
                    load_job.destination,
                )
                load_job.result()
                logging.info(
                    "BigQuery load of [%s] complete", temp_output_path.abs_path()
                )
            except BadRequest as e:
                logging.error(
                    "Insert job [%s] for path [%s] failed with errors: [%s]",
                    load_job.job_id,
                    temp_output_path,
                    load_job.errors,
                )
                raise e

    def _delete_temp_output_paths(self, temp_output_paths: List[GcsfsFilePath]) -> None:
        for temp_output_path in temp_output_paths:
            logging.info("Deleting temp file [%s].", temp_output_path.abs_path())
            self.fs.delete(temp_output_path)

    def _get_validated_columns(
        self, path: GcsfsFilePath, file_config: DirectIngestRawFileConfig
    ) -> List[str]:
        """Returns a list of normalized column names for the raw data file at the given path."""
        # TODO(#3807): We should not derive the columns from what we get in the uploaded raw data CSV - we should
        # instead define the set of columns we expect to see in each input CSV (with mandatory documentation) and update
        # this function to make sure that the columns in the CSV is a strict subset of expected columns. This will allow
        # to gracefully any raw data re-imports where a new column gets introduced in a later file.

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

        columns = [
            normalize_column_name_for_bq(column_name) for column_name in df.columns
        ]

        normalized_columns = set()
        for i, column_name in enumerate(columns):
            if not column_name:
                raise ValueError(f"Found empty column name in [{file_config.file_tag}]")

            # If the capitalization of the column name doesn't match the capitalization
            # listed in the file config, update the capitalization.
            if column_name not in file_config.columns:
                caps_normalized_col = file_config.caps_normalized_col(column_name)
                if caps_normalized_col:
                    column_name = caps_normalized_col

            if column_name in normalized_columns:
                raise ValueError(
                    f"Multiple columns with name [{column_name}] after normalization."
                )
            normalized_columns.add(column_name)
            columns[i] = column_name

        if len(normalized_columns) == 1:
            # A single-column file is almost always indicative of a parsing error. If
            # this column name is not registered in the file config, we throw.
            column = one(normalized_columns)
            if column not in file_config.columns:
                raise ValueError(
                    f"Found only one column: [{column}]. Columns likely did not "
                    f"parse properly. Are you using the correct separator and encoding "
                    f"for this file? If this file really has just one column, the "
                    f"column name must be registered in the raw file config before "
                    f"upload."
                )

        return columns

    @staticmethod
    def _create_raw_table_schema_from_columns(
        columns: List[str],
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
            schema.append(
                bigquery.SchemaField(name=name, field_type=typ_str, mode=mode)
            )
        return schema

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
    raw_data_df[UPDATE_DATETIME_COL_NAME] = utc_upload_datetime

    return raw_data_df


_RAW_TABLE_CONFIGS_BY_STATE = {}


def get_region_raw_file_config(region_code: str) -> DirectIngestRegionRawFileConfig:
    region_code_lower = region_code.lower()
    if region_code_lower not in _RAW_TABLE_CONFIGS_BY_STATE:
        _RAW_TABLE_CONFIGS_BY_STATE[
            region_code_lower
        ] = DirectIngestRegionRawFileConfig(region_code_lower)

    return _RAW_TABLE_CONFIGS_BY_STATE[region_code_lower]
