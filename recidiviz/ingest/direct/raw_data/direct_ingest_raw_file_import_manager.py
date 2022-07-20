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
import re
from enum import Enum
from types import ModuleType
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

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
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.common import attr_validators
from recidiviz.ingest.direct import regions
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
from recidiviz.ingest.direct.types.direct_ingest_constants import (
    FILE_ID_COL_NAME,
    UPDATE_DATETIME_COL_NAME,
)
from recidiviz.persistence.entity.operations.entities import DirectIngestRawFileMetadata
from recidiviz.utils.regions import Region
from recidiviz.utils.yaml_dict import YAMLDict

DATETIME_SQL_REGEX = re.compile(
    r"SAFE.PARSE_(TIMESTAMP|DATE|DATETIME)\(.*{col_name}.*\)"
)


class RawDataClassification(Enum):
    """Defines whether this is source or validation data.

    Used to keep the two sets of data separate. This prevents validation data from being
    ingested, or source data from being used to validate our metrics.
    """

    # Data to be ingested and used as the basis of our entities and calcs.
    SOURCE = "source"

    # Used to validate our entities and calcs.
    VALIDATION = "validation"


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
    is_datetime: bool = attr.ib(validator=attr_validators.is_bool)
    # True if a column contains Personal Identifiable Information (PII)
    is_pii: bool = attr.ib(validator=attr_validators.is_bool)
    # Describes the column contents - if None, this column cannot be used for ingest, nor will you be able to write a
    # raw data migration involving this column.
    description: Optional[str] = attr.ib(validator=attr_validators.is_opt_str)
    # Describes possible enum values for this column if known
    known_values: Optional[List[ColumnEnumValueInfo]] = attr.ib(
        default=None, validator=attr_validators.is_opt_list
    )
    # Describes the SQL parsers needed to parse the datetime string appropriately.
    # It should contain the string literal {col_name} and follow the format with the
    # SAFE.PARSE_TIMESTAMP('[insert your time format st]', [some expression w/ {col_name}]).
    # SAFE.PARSE_DATE or SAFE.PARSE_DATETIME can also be used.
    # See recidiviz.ingest.direct.views.direct_ingest_big_query_view_types.DATETIME_COL_NORMALIZATION_TEMPLATE
    datetime_sql_parsers: Optional[List[str]] = attr.ib(
        default=None, validator=attr_validators.is_opt_list
    )

    def __attrs_post_init__(self) -> None:
        self._validate_datetime_sql_parsers()

    def _validate_datetime_sql_parsers(self) -> None:
        """Validates the datetime_sql field by ensuring that is_datetime is set to True
        and the correct string literals are contained within the string."""
        if not self.is_datetime and self.datetime_sql_parsers:
            raise ValueError(
                f"Expected datetime_sql_parsers to be null if is_datetime is False for {self.name}"
            )
        # TODO(#12174) Enforce that is self.is_datetime is True, that datetime_sql_parsers exist.
        if self.datetime_sql_parsers:
            for parser in self.datetime_sql_parsers:
                if "{col_name}" not in parser:
                    raise ValueError(
                        "Expected datetime_sql_parser to have the string literal {col_name}"
                        f"for {self.name}: {parser}"
                    )
                if not re.match(
                    DATETIME_SQL_REGEX,
                    parser.strip(),
                ):
                    raise ValueError(
                        f"Expected datetime_sql_parser must match expected timestamp parsing formats for {self.name}. Current parser: {parser}"
                        "See recidiviz.ingest.direct.views.direct_ingest_big_query_view_types.DATETIME_COL_NORMALIZATION_TEMPLATE"
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


_DEFAULT_BQ_UPLOAD_CHUNK_SIZE = 250000


@attr.s(frozen=True)
class DirectIngestRawFileConfig:
    """Struct for storing any configuration for raw data imports for a certain file tag."""

    # The file tag / table name that this file will get written to
    file_tag: str = attr.ib(validator=attr_validators.is_non_empty_str)

    # The path to the config file
    file_path: str = attr.ib(validator=attr_validators.is_non_empty_str)

    # Description of the raw data file contents
    file_description: str = attr.ib(validator=attr_validators.is_non_empty_str)

    # Data classification of this raw file
    data_classification: RawDataClassification = attr.ib()

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

    # Defines the number of rows in each chunk we will read one at a time from the
    # original raw data file and write back to GCS files before loading into BQ.
    # Increasing this value may increase import speed, but should only be done carefully
    # - if the table has too much data in a row, increasing the number of rows per chunk
    # may push us over VM memory limits. Defaults to 250,000 rows per chunk.
    import_chunk_size_rows: int = attr.ib()

    # If true, means that we likely will receive a CSV that does not have a header row
    # and therefore, we will use the columns defined in the config, in the order they
    # are defined in, as the column names. By default, False.
    infer_columns_from_config: bool = attr.ib()

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
    def available_datetime_cols(self) -> List[Tuple[str, Optional[List[str]]]]:
        return [
            (column.name, column.datetime_sql_parsers)
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
    def datetime_cols(self) -> List[Tuple[str, Optional[List[str]]]]:
        return [
            (column.name, column.datetime_sql_parsers)
            for column in self.columns
            if column.is_datetime
        ]

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
        default_always_historical_export: bool,
        default_infer_columns_from_config: Optional[bool],
        file_config_dict: YAMLDict,
        yaml_filename: str,
    ) -> "DirectIngestRawFileConfig":
        """Returns a DirectIngestRawFileConfig built from a YAMLDict"""
        primary_key_cols = file_config_dict.pop("primary_key_cols", list)
        file_description = file_config_dict.pop("file_description", str)
        data_class = file_config_dict.pop("data_classification", str)
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
        import_chunk_size_rows = file_config_dict.pop_optional(
            "import_chunk_size_rows", int
        )
        infer_columns_from_config = file_config_dict.pop_optional(
            "infer_columns_from_config", bool
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
            data_classification=RawDataClassification(data_class),
            primary_key_cols=primary_key_cols,
            columns=[
                RawTableColumnInfo(
                    name=column["name"],
                    is_datetime=column.get("is_datetime", False),
                    is_pii=column.get("is_pii", False),
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
                    datetime_sql_parsers=list(column["datetime_sql_parsers"])
                    if "datetime_sql_parsers" in column
                    else None,
                )
                for column in columns
            ],
            supplemental_order_by_clause=supplemental_order_by_clause
            if supplemental_order_by_clause is not None
            else "",
            encoding=encoding if encoding is not None else default_encoding,
            separator=separator if separator is not None else default_separator,
            custom_line_terminator=custom_line_terminator
            if custom_line_terminator is not None
            else default_line_terminator,
            ignore_quotes=ignore_quotes
            if ignore_quotes is not None
            else default_ignore_quotes,
            always_historical_export=always_historical_export
            if always_historical_export is not None
            else default_always_historical_export,
            import_chunk_size_rows=import_chunk_size_rows
            if import_chunk_size_rows is not None
            else _DEFAULT_BQ_UPLOAD_CHUNK_SIZE,
            infer_columns_from_config=infer_columns_from_config
            if infer_columns_from_config is not None
            else (
                default_infer_columns_from_config
                if default_infer_columns_from_config is not None
                else False
            ),
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
    # The default setting for whether to always treat raw files as historical exports
    default_always_historical_export: bool = attr.ib(validator=attr_validators.is_bool)
    # The default line terminator for raw files from this region
    default_line_terminator: Optional[str] = attr.ib(
        default=None,
        validator=attr_validators.is_opt_str,
    )
    # The default setting of inferring columns from headers
    default_infer_columns_from_config: Optional[bool] = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
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
        default_always_historical_export = default_contents.pop(
            "default_always_historical_export", bool
        )
        default_infer_columns_from_config = default_contents.pop_optional(
            "default_infer_columns_from_config", bool
        )

        return DirectIngestRawFileDefaultConfig(
            filename=default_filename,
            default_encoding=default_encoding,
            default_separator=default_separator,
            default_line_terminator=default_line_terminator,
            default_ignore_quotes=default_ignore_quotes,
            default_infer_columns_from_config=default_infer_columns_from_config,
            default_always_historical_export=default_always_historical_export,
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
                    default_config.default_always_historical_export,
                    default_config.default_infer_columns_from_config,
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


class DirectIngestRawFileImportManager:
    """Class that stores raw data import configs for a region, with functionality for
    executing an import of a specific file.
    """

    def __init__(
        self,
        *,
        region: Region,
        fs: DirectIngestGCSFileSystem,
        temp_output_directory_path: GcsfsDirectoryPath,
        big_query_client: BigQueryClient,
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
        self.allow_incomplete_configs = allow_incomplete_configs
        self.csv_reader = GcsfsCsvReader(fs)
        self.raw_table_migrations = DirectIngestRawTableMigrationCollector(
            region_code=self.region.region_code,
            regions_module_override=self.region.region_module,
        ).collect_raw_table_migration_queries(sandbox_dataset_prefix)
        self.raw_tables_dataset = raw_tables_dataset_for_region(
            region_code=self.region.region_code,
            sandbox_dataset_prefix=sandbox_dataset_prefix,
        )

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
            query_job = self.big_query_client.run_query_async(query_str=migration_query)
            # Wait for the migration query to complete before running the next one
            query_job.result()

        logging.info("Completed BigQuery import of [%s]", path.abs_path())

    def _upload_contents_to_temp_gcs_paths(
        self, path: GcsfsFilePath, file_metadata: DirectIngestRawFileMetadata
    ) -> Tuple[List[GcsfsFilePath], Optional[List[str]]]:
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
            chunk_size=file_config.import_chunk_size_rows,
            encodings_to_try=file_config.encodings_to_try(),
            index_col=False,
            header=0 if not file_config.infer_columns_from_config else None,
            names=columns,
            keep_default_na=False,
            **self._common_read_csv_kwargs(file_config),
        )

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
        except BadRequest as e:
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
            self.check_found_columns_are_subset_of_config(
                raw_file_config=file_config, found_columns=normalized_csv_columns
            )

        return csv_columns

    @staticmethod
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
            extra_columns = found_columns_lower.difference(
                columns_from_file_config_lower
            )
            raise ValueError(
                f"Found columns in raw file {sorted(extra_columns)} that are not "
                f"defined in the raw data configuration for "
                f"[{raw_file_config.file_tag}]. Make sure that all columns from CSV "
                f"are defined in the raw data configuration."
            )

    @staticmethod
    def create_raw_table_schema_from_columns(
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
