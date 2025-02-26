#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2023 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Contains all classes related to raw file configs."""
import os
import re
from enum import Enum
from types import ModuleType
from typing import Dict, List, Optional, Set, Tuple

import attr
from more_itertools import one

from recidiviz.cloud_storage.gcsfs_csv_reader import COMMON_RAW_FILE_ENCODINGS
from recidiviz.common import attr_validators
from recidiviz.ingest.direct import regions
from recidiviz.ingest.direct.raw_data.raw_table_relationship_info import (
    RawTableRelationshipInfo,
)
from recidiviz.utils.yaml_dict import YAMLDict

_DEFAULT_BQ_UPLOAD_CHUNK_SIZE = 250000

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


class RawTableColumnFieldType(Enum):
    """Field type for a single raw data column"""

    # Contains string values (this is the default)
    STRING = "string"

    # Contains values representing dates or datetimes
    DATETIME = "datetime"

    # Contains external ids representing a justice-impacted individual
    # i.e. values that might hydrate state_person_external_id
    PERSON_EXTERNAL_ID = "person_external_id"

    # Contains external ids representing an agent / staff member
    # i.e. values that might hydrate state_staff_external_id
    STAFF_EXTERNAL_ID = "staff_external_id"


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
    # Designates the type of data that this column contains
    field_type: RawTableColumnFieldType = attr.ib()
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
    # See recidiviz.ingest.direct.views.raw_table_query_builder.DATETIME_COL_NORMALIZATION_TEMPLATE
    datetime_sql_parsers: Optional[List[str]] = attr.ib(
        default=None, validator=attr_validators.is_opt_list
    )

    def __attrs_post_init__(self) -> None:
        # Known values should not be present unless this is a string field
        if self.known_values and self.field_type != RawTableColumnFieldType.STRING:
            raise ValueError(
                f"Expected field type to be string if known values are present for {self.name}"
            )

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
                        "See recidiviz.ingest.direct.views.raw_table_query_builder.DATETIME_COL_NORMALIZATION_TEMPLATE"
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

    @property
    def is_datetime(self) -> bool:
        """
        Returns true if this column is a date/time
        """
        return self.field_type == RawTableColumnFieldType.DATETIME


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
    # The default value for whether tables in a region have valid primary keys
    default_no_valid_primary_keys: bool = attr.ib(validator=attr_validators.is_bool)
    # The default line terminator for raw files from this region
    default_line_terminator: Optional[str] = attr.ib(
        default=None,
        validator=attr_validators.is_opt_str,
    )
    # The default setting of inferring columns from headers
    default_infer_columns_from_config: Optional[bool] = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )


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

    # If true, that means that there are no valid primary key columns for this table.
    no_valid_primary_keys: bool = attr.ib()

    table_relationships: List[RawTableRelationshipInfo] = attr.ib(
        validator=attr_validators.is_list
    )

    def __attrs_post_init__(self) -> None:
        self._validate_primary_keys()

        column_names = [column.name for column in self.columns]
        if len(column_names) != len(set(column_names)):
            raise ValueError(f"Found duplicate columns in raw_file [{self.file_tag}]")

        missing_columns = set(self.primary_key_cols) - {
            column.name for column in self.columns
        }
        if missing_columns:
            raise ValueError(
                f"Column(s) marked as primary keys not listed in"
                f" columns list for file [{self.file_tag}]: {missing_columns}"
            )

        for i, relationship in enumerate(self.table_relationships):
            if relationship.file_tag != self.file_tag:
                raise ValueError(
                    f"Found table_relationship defined for [{self.file_tag}] with "
                    f"file_tag that does not match config file_tag: "
                    f"{relationship.file_tag}."
                )

            # Check for duplicate relationships defined on a single raw file
            for j, relationship_2 in enumerate(self.table_relationships):
                if i != j and relationship == relationship_2:
                    raise ValueError(
                        f"Found duplicate table relationships "
                        f"[{relationship.join_sql()}] and "
                        f"[{relationship_2.join_sql()}] defined in "
                        f"[{self.file_path}]"
                    )

    def _validate_primary_keys(self) -> None:
        """Confirm that the primary key configuration is valid for this config. If this
        check passes, it does NOT mean that the table is sufficently documented for use
        in an ingest view. To determine if this is a valid ingest view dependency, see
        is_undocumented().
        """
        if self.no_valid_primary_keys and self.primary_key_cols:
            raise ValueError(
                f"Incorrect primary key setup found for file_tag={self.file_tag}: "
                f"`no_valid_primary_keys`={self.no_valid_primary_keys} and `primary_key_cols` is not empty: "
                f"{self.primary_key_cols}."
            )

    @property
    def primary_key_str(self) -> str:
        """A comma-separated string representation of the primary keys"""
        return ", ".join(self.primary_key_cols)

    def encodings_to_try(self) -> List[str]:
        """Returns an ordered list of encodings we should try for this file."""
        return [self.encoding] + [
            encoding
            for encoding in COMMON_RAW_FILE_ENCODINGS
            if encoding.upper() != self.encoding.upper()
        ]

    def get_column_info(self, column_name: str) -> RawTableColumnInfo:
        """Returns information about the column with the provided |column_name|. Throws
        if that column does not exist in the table.
        """
        infos = [c for c in self.columns if c.name == column_name]
        if len(infos) != 1:
            raise ValueError(
                f"Expected to find exactly one entry for column [{column_name}], "
                f"found: {infos}"
            )
        return one(infos)

    @property
    def documented_columns(self) -> List[RawTableColumnInfo]:
        """Filters to only documented columns."""
        return [column for column in self.columns if column.description]

    @property
    def documented_datetime_cols(
        self,
    ) -> List[Tuple[str, Optional[List[str]]]]:
        return [
            (column.name, column.datetime_sql_parsers)
            for column in self.documented_columns
            if column.is_datetime
        ]

    @property
    def datetime_cols(self) -> List[Tuple[str, Optional[List[str]]]]:
        return [
            (column.name, column.datetime_sql_parsers)
            for column in self.columns
            if column.is_datetime
        ]

    @property
    def non_datetime_cols(self) -> List[str]:
        return [column.name for column in self.columns if not column.is_datetime]

    @property
    def documented_non_datetime_cols(self) -> List[str]:
        return [
            column.name for column in self.documented_columns if not column.is_datetime
        ]

    @property
    def has_enums(self) -> bool:
        """If true, columns with enum values exist within this raw file, and this config is eligible to be refreshed
        with the for the fetch_column_values_for_state script."""
        return bool([column.name for column in self.columns if column.is_enum])

    @property
    def is_undocumented(self) -> bool:
        """Returns true if the raw file config provides enough information for this
        table to be used in ingest views or *latest views.
        """
        return not self.documented_columns or (
            len(self.primary_key_cols) == 0 and not self.no_valid_primary_keys
        )

    def caps_normalized_col(self, col_name: str) -> Optional[str]:
        """If the provided column name has a case-insensitive match in the columns list,
        returns the proper capitalization of the column, as listed in the configuration
        file.
        """
        for registered_col in self.columns:
            if registered_col.name.lower() == col_name.lower():
                return registered_col.name
        return None

    def is_exempt_from_raw_data_pruning(self) -> bool:
        # TODO(#19528): remove gating once raw data pruning can be done on ContactNoteComment.
        if self.file_tag == "ContactNoteComment":
            return True

        if not self.always_historical_export:
            # We currently only conduct raw data pruning on raw files that are always historical.
            return True

        return self.no_valid_primary_keys

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
        default_no_valid_primary_keys: bool,
        default_infer_columns_from_config: Optional[bool],
        file_config_dict: YAMLDict,
    ) -> "DirectIngestRawFileConfig":
        """Returns a DirectIngestRawFileConfig built from a YAMLDict"""
        primary_key_cols = file_config_dict.pop("primary_key_cols", list)
        file_description = file_config_dict.pop("file_description", str)
        data_class = file_config_dict.pop("data_classification", str)
        column_infos: List[RawTableColumnInfo] = []
        for column in file_config_dict.pop_dicts("columns"):
            column_name = column.pop("name", str)

            known_value_infos = None
            if (known_values := column.pop_dicts_optional("known_values")) is not None:
                known_value_infos = []
                for known_value in known_values:
                    known_value_value = str(known_value.pop("value", object))
                    known_value_infos.append(
                        ColumnEnumValueInfo(
                            value=known_value_value,
                            description=known_value.pop_optional("description", str),
                        )
                    )
                    if len(known_value) > 0:
                        raise ValueError(
                            f"Found unexpected config values for raw file column "
                            f"[{column_name}] known_value [{known_value_value}] in "
                            f"[{file_tag}]: {repr(known_value.get())}"
                        )
            column_infos.append(
                RawTableColumnInfo(
                    name=column_name,
                    field_type=RawTableColumnFieldType(field_type_str)
                    if (field_type_str := column.pop_optional("field_type", str))
                    else RawTableColumnFieldType.STRING,
                    is_pii=column.pop_optional("is_pii", bool) or False,
                    description=column.pop_optional("description", str),
                    known_values=known_value_infos,
                    datetime_sql_parsers=column.pop_list_optional(
                        "datetime_sql_parsers", str
                    ),
                )
            )
            if len(column) > 0:
                raise ValueError(
                    f"Found unexpected config values for raw file column "
                    f"[{column_name}] in [{file_tag}]: {repr(column.get())}"
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
        no_valid_primary_keys = file_config_dict.pop_optional(
            "no_valid_primary_keys", bool
        )
        import_chunk_size_rows = file_config_dict.pop_optional(
            "import_chunk_size_rows", int
        )
        infer_columns_from_config = file_config_dict.pop_optional(
            "infer_columns_from_config", bool
        )
        table_relationships_yamls = file_config_dict.pop_dicts_optional(
            "table_relationships"
        )
        table_relationships = (
            [
                RawTableRelationshipInfo.build_from_table_relationship_yaml(
                    file_tag, table_relationship
                )
                for table_relationship in table_relationships_yamls
            ]
            if table_relationships_yamls
            else []
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
            columns=column_infos,
            supplemental_order_by_clause=supplemental_order_by_clause or "",
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
            no_valid_primary_keys=no_valid_primary_keys
            if no_valid_primary_keys is not None
            else default_no_valid_primary_keys,
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
            table_relationships=table_relationships,
        )


@attr.s
class DirectIngestRegionRawFileConfig:
    """Class that parses and stores raw data import configs for a region"""

    # TODO(#5262): Add documentation for the structure of the raw data yaml files
    region_code: str = attr.ib()
    region_module: ModuleType = attr.ib(default=regions)
    yaml_config_file_dir: str = attr.ib()
    raw_file_configs: Dict[str, DirectIngestRawFileConfig] = attr.ib()

    @property
    def default_config_filename(self) -> str:
        return f"{self.region_code.lower()}_default.yaml"

    def default_config(self) -> DirectIngestRawFileDefaultConfig:
        default_file_path = os.path.join(
            self.yaml_config_file_dir, self.default_config_filename
        )
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
        default_no_valid_primary_keys = default_contents.pop(
            "default_no_valid_primary_keys", bool
        )
        default_infer_columns_from_config = default_contents.pop_optional(
            "default_infer_columns_from_config", bool
        )

        return DirectIngestRawFileDefaultConfig(
            filename=self.default_config_filename,
            default_encoding=default_encoding,
            default_separator=default_separator,
            default_line_terminator=default_line_terminator,
            default_ignore_quotes=default_ignore_quotes,
            default_infer_columns_from_config=default_infer_columns_from_config,
            default_always_historical_export=default_always_historical_export,
            default_no_valid_primary_keys=default_no_valid_primary_keys,
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
        return self._generate_raw_data_file_configs()

    def get_raw_data_file_config_paths(self) -> List[str]:
        if not os.path.isdir(self.yaml_config_file_dir):
            raise ValueError(
                f"Missing raw data configs for region: {self.region_code}. "
                f"None found at path [{self.yaml_config_file_dir}]."
            )

        paths = []
        for filename in os.listdir(self.yaml_config_file_dir):
            if filename == self.default_config_filename or not filename.endswith(
                ".yaml"
            ):
                continue
            yaml_file_path = os.path.join(self.yaml_config_file_dir, filename)
            if os.path.isdir(yaml_file_path):
                continue

            paths.append(yaml_file_path)
        return paths

    def _read_configs_from_disk(self) -> Dict[str, DirectIngestRawFileConfig]:
        """Returns a dictionary of file tag to config for each raw data file tag we
        expect to see in this region.

        Does not do any validation or table relationships cleanup.
        """
        raw_data_yaml_paths = self.get_raw_data_file_config_paths()
        default_config = self.default_config()
        raw_data_configs = {}
        for yaml_file_path in raw_data_yaml_paths:
            yaml_contents = YAMLDict.from_path(yaml_file_path)
            filename = os.path.basename(yaml_file_path)

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
                default_config.default_no_valid_primary_keys,
                default_config.default_infer_columns_from_config,
                yaml_contents,
            )
        return raw_data_configs

    def _generate_raw_data_file_configs(self) -> Dict[str, DirectIngestRawFileConfig]:
        """Returns a dictionary of file tag to config for each raw data file tag we
        expect to see in this region.
        """
        raw_file_configs = self._read_configs_from_disk()

        # For any pair of tables, all relationships between those two tables must be
        # defined in only one of the YAMLs for those two tables. This dictionary tracks
        # the expected YAML where relationships between this (sorted) pair is defined.
        table_relationship_locations: Dict[Tuple[str, ...], str] = {}

        for file_tag, config in raw_file_configs.items():
            for table_relationship in config.table_relationships:
                # Check for references to columns that do not exist
                for join_clause in table_relationship.join_clauses:
                    for column in join_clause.get_referenced_columns():
                        column_config = raw_file_configs[column.file_tag]
                        if column.column not in [c.name for c in column_config.columns]:
                            raise ValueError(
                                f"Found column [{column}] referenced in join clause "
                                f"[{join_clause.to_sql()}] which is not defined in "
                                f"the config for [{column.file_tag}]"
                            )

                tables_key = table_relationship.get_referenced_tables()
                if tables_key not in table_relationship_locations:
                    table_relationship_locations[tables_key] = file_tag

                if table_relationship_locations[tables_key] != file_tag:
                    other_config = raw_file_configs[
                        table_relationship_locations[tables_key]
                    ]
                    raise ValueError(
                        f"Found table_relationship defined in [{config.file_path}] "
                        f"between tables {tables_key}. There is already a relationship "
                        f"between these tables defined in [{other_config.file_path}]. "
                        f"For any given pair of tables, all relationships between "
                        f"those two tables must be defined in only one YAML file."
                    )

        # Now that we are validated, we can hydrate reciprocal table relationships
        for file_tag, config in raw_file_configs.items():
            for table_relationship in config.table_relationships:
                src_file_tag = table_relationship_locations[
                    table_relationship.get_referenced_tables()
                ]
                if (
                    table_relationship.file_tag == table_relationship.foreign_table
                    or src_file_tag != file_tag
                ):
                    # If this is a self-join or this isn't the original config this
                    # relationship was defined in, we don't copy over to the other
                    # config.
                    continue
                other_file_tag = table_relationship.foreign_table
                other_config = raw_file_configs[other_file_tag]
                other_config.table_relationships.append(table_relationship.invert())

        return raw_file_configs

    raw_file_tags: Set[str] = attr.ib()

    @raw_file_tags.default
    def _raw_file_tags(self) -> Set[str]:
        return set(self.raw_file_configs.keys())
