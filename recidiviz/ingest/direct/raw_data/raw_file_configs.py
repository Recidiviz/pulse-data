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
"""Contains classes related to raw file configs."""
import csv
import os
import re
from collections import defaultdict
from datetime import datetime, timezone
from enum import Enum
from types import ModuleType
from typing import Any, Dict, List, Optional, Set, Tuple

import attr
from more_itertools import one

from recidiviz.common import attr_validators
from recidiviz.common.constants.csv import DEFAULT_CSV_LINE_TERMINATOR
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct import regions
from recidiviz.ingest.direct import regions as direct_ingest_regions_module
from recidiviz.ingest.direct.gating import FILES_EXEMPT_FROM_RAW_DATA_PRUNING_BY_STATE
from recidiviz.ingest.direct.raw_data.datetime_sql_parser_exemptions import (
    is_column_exempt_from_datetime_parsers,
)
from recidiviz.ingest.direct.raw_data.documentation_exemptions import (
    COLUMN_DOCUMENTATION_COLUMN_LEVEL_EXEMPTIONS,
    COLUMN_DOCUMENTATION_FILE_LEVEL_EXEMPTIONS,
    COLUMN_DOCUMENTATION_STATE_LEVEL_EXEMPTIONS,
    FILE_DOCUMENTATION_EXEMPTIONS,
)
from recidiviz.ingest.direct.raw_data.raw_table_relationship_info import (
    RawTableRelationshipInfo,
)
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation_type import (
    RawDataImportBlockingValidationType,
)
from recidiviz.utils.yaml_dict import YAMLDict

DATETIME_SQL_REGEX = re.compile(r"^SAFE.PARSE_DATETIME(.*{col_name}.*)$")
MAX_NUM_COLS = 300


class RawDataClassification(Enum):
    """Defines whether this is source or validation data.

    Used to keep the two sets of data separate. This prevents validation data from being
    ingested, or source data from being used to validate our metrics.
    """

    # Data to be ingested and used as the basis of our entities and calcs.
    SOURCE = "source"

    # Used to validate our entities and calcs.
    VALIDATION = "validation"


# TODO(#40717) Add BIRTHDATE
class RawTableColumnFieldType(Enum):
    """Field type for a single raw data column"""

    # Contains string values (this is the default)
    STRING = "string"

    # Contains values representing dates or datetimes
    DATETIME = "datetime"

    # Contains values representing integers
    INTEGER = "integer"

    # Contains external ids representing a justice-impacted individual
    # i.e. values that might hydrate state_person_external_id
    PERSON_EXTERNAL_ID = "person_external_id"

    # Contains external ids representing an agent / staff member
    # i.e. values that might hydrate state_staff_external_id
    STAFF_EXTERNAL_ID = "staff_external_id"

    @classmethod
    def external_id_types(cls) -> Set["RawTableColumnFieldType"]:
        return {cls.PERSON_EXTERNAL_ID, cls.STAFF_EXTERNAL_ID}


class RawDataFileUpdateCadence(Enum):
    """Defines an expected update cadence for a raw data file (i.e. how often we
    expect a state to transfer a raw data file to our ingest infrastructure). We do not
    necessarily expect for new data to flow through our system at this cadence (i.e.
    some states might send files that only contain header rows if there are sending
    incremental updates).
    """

    # There is no defined update cadence or the update cadence is expected to be irregular
    IRREGULAR = "IRREGULAR"

    # The file is expected to be updated once per month
    MONTHLY = "MONTHLY"

    # The file is expected to be updated once per week
    WEEKLY = "WEEKLY"

    # The file is expected to be updated once per day
    DAILY = "DAILY"

    @staticmethod
    def interval_from_cadence(cadence: "RawDataFileUpdateCadence") -> int:
        match cadence:
            case RawDataFileUpdateCadence.DAILY:
                return 1
            case RawDataFileUpdateCadence.WEEKLY:
                return 7
            case RawDataFileUpdateCadence.MONTHLY:
                return 31  # ¯\_(ツ)_/¯
            case _:
                raise ValueError(
                    f"Don't know how to get max days allowed stale for {cadence}"
                )


class RawDataExportLookbackWindow(Enum):
    """Defines the lookback window for each raw data file, or how much historical data we
    expect to be in a typical raw data file a state sends to us. Generally, we prefer
    states to send us full historical files. If a raw file tag is an incremental file,
    that likely means that either the file or underlying data table itself has an audit
    column that reliably allows the states to generate date-bounded diffs.
    """

    # We expect each raw data file to include a full table export, and will never change
    # to incremental exports.
    FULL_HISTORICAL_LOOKBACK = "FULL_HISTORICAL_LOOKBACK"

    # We expect each raw data file to ONLY include the last two months worth of data for this
    # file tag. This means that this file tag or table itself likely has some sort of
    # audit column that allows the state to generate date-bounded diffs.
    TWO_MONTH_INCREMENTAL_LOOKBACK = "TWO_MONTH_INCREMENTAL_LOOKBACK"

    # We expect each raw data file to ONLY include data for the last month for this
    # file tag. This means that this file tag or table itself likely has some sort of
    # audit column that allows the state to generate date-bounded diffs.
    ONE_MONTH_INCREMENTAL_LOOKBACK = "ONE_MONTH_INCREMENTAL_LOOKBACK"

    # We expect each raw data file to ONLY include the last two weeks worth of data for this
    # file tag. This means that this file tag or table itself likely has some sort of
    # audit column that allows the state to generate date-bounded diffs.
    TWO_WEEK_INCREMENTAL_LOOKBACK = "TWO_WEEK_INCREMENTAL_LOOKBACK"

    # We expect each raw data file to ONLY include the last weeks worth of data for this file
    # tag. This means that this file tag or table itself likely has some sort of audit
    # column that allows the state to generate date-bounded diffs.
    ONE_WEEK_INCREMENTAL_LOOKBACK = "ONE_WEEK_INCREMENTAL_LOOKBACK"

    # We expect each raw data file to only include recent data for this file tag, but
    # we're not exactly sure how much historical data is included with each file. This
    # means that this file tag or table itself likely has some sort of audit column that
    # allows the state to generate date-bounded diffs.
    UNKNOWN_INCREMENTAL_LOOKBACK = "UNKNOWN_INCREMENTAL_LOOKBACK"


def is_meaningful_docstring(docstring: str | None) -> bool:
    """Returns true if the provided docstring gives meaningful information, i.e. it is
    non-empty and does not start with an obvious placeholder.
    """
    if not docstring:
        return False

    stripped_docstring = docstring.strip()
    if not stripped_docstring:
        return False

    return (
        # Split up into TO and DO to avoid lint errors
        not stripped_docstring.startswith("TO" + "DO")
        and not stripped_docstring.startswith("XXX")
    )


@attr.s
class ColumnEnumValueInfo:
    # The literal enum value
    value: str = attr.ib(validator=attr_validators.is_non_empty_str)
    # The description that value maps to
    description: Optional[str] = attr.ib(validator=attr_validators.is_opt_str)


@attr.define
class ImportBlockingValidationExemption:
    validation_type: RawDataImportBlockingValidationType = attr.ib(
        validator=attr.validators.instance_of(RawDataImportBlockingValidationType)
    )
    exemption_reason: str = attr.ib(validator=attr_validators.is_non_empty_str)

    @staticmethod
    def list_includes_exemption_type(
        exemption_list: Optional[List["ImportBlockingValidationExemption"]],
        exemption_type: RawDataImportBlockingValidationType,
    ) -> bool:
        if not exemption_list:
            return False
        return any(
            exemption.validation_type == exemption_type for exemption in exemption_list
        )


class ColumnUpdateOperation(Enum):
    """Enum for column update operations"""

    ADDITION = "ADDITION"
    DELETION = "DELETION"
    RENAME = "RENAME"


@attr.define
class ColumnUpdateInfo:
    """Stores information about an update made to a column

    Attributes:
        update_type: The type of update made to the column
        previous_value: The previous name of the column if the update_type is RENAME
        update_datetime: The ISO formatted datetime the update was made
    """

    update_type: ColumnUpdateOperation = attr.ib(
        validator=attr.validators.instance_of(ColumnUpdateOperation)
    )
    update_datetime: datetime = attr.ib(validator=attr.validators.instance_of(datetime))
    previous_value: Optional[str] = attr.ib(
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(str)),
    )

    def __attrs_post_init__(self) -> None:
        if (self.update_type == ColumnUpdateOperation.RENAME) != bool(
            self.previous_value
        ):
            raise ValueError(
                f"ColumnUpdateInfo previous_value must be set if and only if update_type is {ColumnUpdateOperation.RENAME.value}"
            )
        if self.update_datetime.tzinfo is None:
            raise ValueError("Must include timezone in update_history update_datetime")


@attr.define(kw_only=True)
class RawTableColumnInfo:
    """Stores information about a single raw data table column."""

    # The state code is in the format of "US_XX" for each state
    state_code: StateCode = attr.ib(
        validator=attr.validators.instance_of(StateCode),
    )
    # The raw data file tag for the file this column belongs to
    file_tag: str = attr.ib(validator=attr_validators.is_str)

    # The column name in BigQuery-compatible, normalized form (e.g. punctuation stripped)
    name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    # Designates the type of data that this column contains
    field_type: RawTableColumnFieldType = attr.ib()
    # True if a column contains Personal Identifiable Information (PII)
    is_pii: bool = attr.ib(validator=attr_validators.is_bool)
    # Describes the column contents - if None, this column cannot be used for ingest, nor will you be able to write a
    # raw data migration involving this column.
    description: Optional[str] = attr.ib(validator=attr_validators.is_opt_str)

    @property
    def is_documented(self) -> bool:
        """Returns True if this column has meaningful documentation, i.e. a non-empty
        description that is not clearly a TO-DO to fill in the documentation later.
        """
        if not self.description:
            return False

        if self.state_code in COLUMN_DOCUMENTATION_STATE_LEVEL_EXEMPTIONS:
            return True

        if (
            exempt_file_tags := COLUMN_DOCUMENTATION_FILE_LEVEL_EXEMPTIONS.get(
                self.state_code
            )
        ) and self.file_tag in exempt_file_tags:
            return True

        if (
            (
                file_tags_with_exemptions := COLUMN_DOCUMENTATION_COLUMN_LEVEL_EXEMPTIONS.get(
                    self.state_code
                )
            )
            and (column_exemptions := file_tags_with_exemptions.get(self.file_tag))
            and self.name in column_exemptions
        ):
            return True

        return is_meaningful_docstring(self.description)

    # Describes possible enum values for this column if known
    known_values: Optional[List[ColumnEnumValueInfo]] = attr.ib(
        default=None, validator=attr_validators.is_opt_list
    )
    # Describes the SQL parsers needed to parse the datetime string appropriately.
    # It should contain the string literal {col_name} and follow the format with the
    # SAFE.PARSE_TIMESTAMP('[insert your time format st]', [some expression w/ {col_name}]).
    # SAFE.PARSE_DATE or SAFE.PARSE_DATETIME can also be used, but you must only use one type
    # within a list of parsers.
    # See recidiviz.ingest.direct.views.raw_table_query_builder.DATETIME_COL_NORMALIZATION_TEMPLATE
    datetime_sql_parsers: Optional[List[str]] = attr.ib(
        default=None, validator=attr_validators.is_opt_list
    )
    # If this column holds an external id type, designates which type it is
    # (out of the external types defined in common/constants/state/external_id_types.py)
    external_id_type: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    # True if this column holds external ID information in a primary person table.
    # Should only be set for a column of type PERSON_EXTERNAL_ID or STAFF_EXTERNAL_ID.
    # If true for a column of type PERSON_EXTERNAL_ID, then the table that this column
    # belongs to is an ID type root for that region (Note this does not necessarily
    # mean the table is the is_primary_person_table for the region, since there may be
    # multiple tables that are ID type roots for the region.)
    is_primary_for_external_id_type: bool = attr.ib(default=False)
    # Column-level import-blocking validation exemptions
    import_blocking_column_validation_exemptions: Optional[
        List[ImportBlockingValidationExemption]
    ] = attr.ib(default=None, validator=attr_validators.is_opt_list)
    # Stores a list of updates made to the column, sorted by datetime
    update_history: Optional[List[ColumnUpdateInfo]] = attr.ib(
        default=None, validator=attr_validators.is_opt_list
    )
    # Stores a list of values signifying null for this column
    null_values: Optional[List[str]] = attr.ib(
        default=None, validator=attr_validators.is_opt_list
    )

    # TODO(#40717) Check BIRTHDATE is PII and DATETIME is not
    def __attrs_post_init__(self) -> None:
        # Known values should not be present unless this is a string field
        if self.known_values and self.field_type != RawTableColumnFieldType.STRING:
            raise ValueError(
                f"Expected field type to be string if known values are present for {self.name}"
            )

        self._validate_datetime_sql_parsers()

        # Enforce that external_id_type and is_primary_for_external_id_type should only be set
        # if field type is an external id
        is_external_id_field = (
            self.field_type in RawTableColumnFieldType.external_id_types()
        )

        if (
            self.external_id_type or self.is_primary_for_external_id_type
        ) and not is_external_id_field:
            raise ValueError(
                f"Expected external_id_type to be None and "
                f"is_primary_for_external_id_type to be False when field_type is "
                f"{self.field_type.value} for {self.name}. If this field is an "
                f"external id, you must set the type to one of "
                f"{list(t.value for t in RawTableColumnFieldType.external_id_types())}"
            )
        if self.is_primary_for_external_id_type and not self.external_id_type:
            raise ValueError(
                f"Expected is_primary_for_external_id_type to be False when external "
                f"id type is None for {self.name}"
            )
        if is_external_id_field and not self.is_pii:
            raise ValueError(
                f"Found field {self.name} with external id type "
                f"{self.field_type.value} which is not labeled `is_pii: True`."
            )

        self._validate_update_history()

    def _validate_update_history(self) -> None:
        """Raises a ValueError if update_history is not sorted by update_datetime or if
        update_history contains invalid transitions --

        Invalid transitions:
        - DELETION -> RENAME|DELETION
        - ADDITION|RENAME -> ADDITION
        - RENAME -> RENAME with the same previous_value
        - Any two updates with the same update_datetime

        Valid transitions:
        - ADDITION|RENAME -> RENAME|DELETION
        - DELETION -> ADDITION
        """
        if self.update_history is None:
            return

        if self.update_history != sorted(
            self.update_history, key=lambda x: x.update_datetime
        ):
            raise ValueError(
                f"Expected update_history to be sorted by update_datetime for column [{self.name}]."
            )

        for i in range(1, len(self.update_history)):
            previous_update = self.update_history[i - 1]
            current_update = self.update_history[i]

            if previous_update.update_datetime == current_update.update_datetime:
                raise ValueError(
                    f"Invalid update_history sequence for column [{self.name}]. Found two updates with the same update_datetime [{current_update.update_datetime.isoformat()}]"
                )

            deletion_update_not_followed_by_addition = (
                previous_update.update_type == ColumnUpdateOperation.DELETION
                and current_update.update_type != ColumnUpdateOperation.ADDITION
            )
            addition_or_rename_followed_by_addition = (
                previous_update.update_type
                in {
                    ColumnUpdateOperation.ADDITION,
                    ColumnUpdateOperation.RENAME,
                }
                and current_update.update_type == ColumnUpdateOperation.ADDITION
            )

            if (
                deletion_update_not_followed_by_addition
                or addition_or_rename_followed_by_addition
            ):
                raise ValueError(
                    f"Invalid update_history sequence for column [{self.name}]. Found invalid transition from {previous_update.update_type.value} -> {current_update.update_type.value}"
                )

            consecutive_renames = (
                previous_update.update_type == ColumnUpdateOperation.RENAME
                and current_update.update_type == ColumnUpdateOperation.RENAME
            )
            same_previous_value = (
                previous_update.previous_value == current_update.previous_value
            )
            if consecutive_renames and same_previous_value:
                raise ValueError(
                    f"Invalid update_history sequence for column [{self.name}]. Found two consecutive RENAME updates with the same previous_value [{current_update.previous_value}]"
                )

    def _validate_datetime_sql_parsers(self) -> None:
        """Validates the datetime_sql field by ensuring that is_datetime is set to True,
        the correct string literals are contained within the string, and all parsers have
        type DATETIME."""
        if not self.is_datetime and self.datetime_sql_parsers:
            raise ValueError(
                f"Expected datetime_sql_parsers to be null if is_datetime is False for {self.name}"
            )

        if not self.is_datetime:
            # This isn't a datetime field and there are no parsers defined - nothing more to check.
            return

        if not self.datetime_sql_parsers:
            # TODO(#12174): Remove this if-check once all the exemptions in
            #  datetime_sql_parser_exemptions.py are gone.
            if is_column_exempt_from_datetime_parsers(
                self.state_code, self.file_tag, self.name
            ):
                return
            raise ValueError(
                f"State {self.state_code.value}: Expected datetime_sql_parsers to be "
                f"set for datetime field {self.name} in file {self.file_tag} with field"
                f"type: {self.field_type}"
            )

        # Now we know that the field is a datetime field AND there are parsers defined
        # - make sure each parser is valid.
        for parser in self.datetime_sql_parsers:
            if not (
                re.match(
                    DATETIME_SQL_REGEX,
                    parser.strip(),
                )
            ):
                raise ValueError(
                    f"Expected datetime_sql_parser must match expected datetime parsing format {DATETIME_SQL_REGEX} for [{self.name}]. Current parser: {parser}"
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

    def exists_at_datetime(self, dt: datetime) -> bool:
        """Determines if the column exists at the given datetime."""
        if self.update_history is None:
            return True

        for change in self.update_history:
            dt_precedes_addition = (
                change.update_type == ColumnUpdateOperation.ADDITION
                and dt < change.update_datetime
            )
            if dt_precedes_addition:
                return False

            dt_precedes_deletion = (
                change.update_type == ColumnUpdateOperation.DELETION
                and dt < change.update_datetime
            )
            if dt_precedes_deletion:
                return True

        final_update_is_not_deletion = (
            self.update_history[-1].update_type != ColumnUpdateOperation.DELETION
        )
        return final_update_is_not_deletion

    def name_at_datetime(self, dt: datetime) -> Optional[str]:
        """
        Based on the update_history for a column,
        returns the name of the column as it appeared at the given datetime
        or None if the column did not exist at that datetime.
        """
        if not self.exists_at_datetime(dt):
            return None

        if self.update_history is None:
            return self.name

        for change in self.update_history:
            if (
                change.update_datetime > dt
                and change.update_type == ColumnUpdateOperation.RENAME
            ):
                return change.previous_value
        return self.name


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
    # The default export lookback window for a raw data file, or how much historical
    # data we expect to be included in a typical raw data file a state sends to us
    default_export_lookback_window: RawDataExportLookbackWindow = attr.ib(
        validator=attr.validators.in_(RawDataExportLookbackWindow)
    )
    # The default value for whether tables in a region have valid primary keys
    default_no_valid_primary_keys: bool = attr.ib(validator=attr_validators.is_bool)
    # The default value for the expected ingest update cadence for files from this region
    default_update_cadence: RawDataFileUpdateCadence = attr.ib(
        validator=attr.validators.instance_of(RawDataFileUpdateCadence),
    )
    # The default setting of inferring columns from headers
    default_infer_columns_from_config: bool = attr.ib(validator=attr_validators.is_bool)
    # The default line terminator for raw files from this region
    default_line_terminator: Optional[str] = attr.ib(
        default=None,
        validator=attr_validators.is_opt_str,
    )
    # Import-blocking validation exemptions that are applied to all tables in this region
    # Can include table-level validation exemptions and/or column-level exemptions to apply
    # to all relevant columns in the region
    default_import_blocking_validation_exemptions: Optional[
        List[ImportBlockingValidationExemption]
    ] = attr.ib(default=None, validator=attr_validators.is_opt_list)


@attr.s(frozen=True)
class DirectIngestRawFileConfig:
    """Struct for storing any configuration for raw data imports for a certain file tag."""

    state_code: StateCode = attr.ib(validator=attr.validators.instance_of(StateCode))

    # The file tag / table name that this file will get written to
    file_tag: str = attr.ib(validator=attr_validators.is_non_empty_str)

    # The path to the config file
    file_path: str = attr.ib(validator=attr_validators.is_non_empty_str)

    # Description of the raw data file contents
    file_description: str = attr.ib(validator=attr_validators.is_non_empty_str)

    # The cadence at which we expect to receive this raw data file
    update_cadence: RawDataFileUpdateCadence = attr.ib()

    # Data classification of this raw file
    data_classification: RawDataClassification = attr.ib()

    # A list of columns that constitute the primary key for this file. If empty, this table cannot be used in an ingest
    # view query and a '*_latest' view will not be generated for this table. May be left empty for the purposes of
    # allowing us to quickly upload a new file into BQ and then determine the primary keys by querying BQ.
    primary_key_cols: List[str] = attr.ib(validator=attr.validators.instance_of(list))

    # A list of names and descriptions for each column in a file
    # because the list of columns may contain deleted columns, this information should be accessed
    # through the current_columns property, all_columns property, or columns_at_datetime method.
    # Adding alias "columns" to use in intialization ex DirectIngestRawFileConfig(columns=...)
    # while keeping access to the attribute private. this is the default behavior of attrs, but
    # I'm adding the redundant alias to make the intended use explicit.
    _columns: List[RawTableColumnInfo] = attr.ib(alias="columns")

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

    # The export lookback window for a raw data file, or how much historical data we
    # expect to be included in a typical file for this file tag
    export_lookback_window: RawDataExportLookbackWindow = attr.ib(
        validator=attr.validators.in_(RawDataExportLookbackWindow)
    )

    # If true, means that we likely will receive a CSV that does not have a header row
    # and therefore, we will use the columns defined in the config, in the order they
    # are defined in, as the column names. By default, False.
    infer_columns_from_config: bool = attr.ib()

    # If true, that means that there are no valid primary key columns for this table.
    no_valid_primary_keys: bool = attr.ib()

    # TODO(#40036): Consider generalizing relationship traversal.
    table_relationships: List[RawTableRelationshipInfo] = attr.ib(
        validator=attr_validators.is_list
    )
    # True if this is the overall table representing person (JII) information
    # for this region. All other raw data tables containing person-level information
    # should be able to be joined back to this table, either directly or indirectly,
    # via PERSON_EXTERNAL_ID type columns.
    # Each region may only have one raw file marked as is_primary_person_table; this
    # designation may be arbitrary if there are multiple primary tables representing
    # person information, such as if there are multiple source data systems.
    is_primary_person_table: bool = attr.ib(default=False)

    # TODO(#28561): add more nuance here to get at what we actually care about
    # (reasoning about the kind of data in our raw data files)
    # Boolean flag denoting if this file is a code table or not. If this file is a
    # code table, we expect this file to be relatively static overtime (without many
    # updates). If this file is not a code table, it likely contains person-level
    # information that we expect to change more frequently
    is_code_file: bool = attr.ib(default=False)

    # Boolean flag denoting whether the state sends us data for this file tag split into
    # multiple csv files in each transfer. If this is the case, we need to do some
    # extra handling to ensure that all csv file chunks get coalesced into the same
    # file id in the operations database.
    is_chunked_file: bool = attr.ib(default=False, validator=attr_validators.is_bool)

    # The maximum number of unparseable bytes we will allow this raw file to have in a single
    # 100 mb sample before we throw. In general, we expect this value to be None (i.e. we
    # don't allow unparseable bytes); however, in certain situations, states have not been able to
    # fix unparseable bytes in their dbs so we need to be able to clean them out of files
    # automatically during raw data import. It's per chunk as this is the most un-parseable
    # bytes that we'll allow a ~100 mB sample of the raw file have. In practice, we don't
    # need to worry about being super precise with the number of un-parseable bytes as
    # we are mainly concerned with enforcing a reasonable ceiling (like less than 10k)
    # than we are with strictly monitoring the number of bytes
    max_num_unparseable_bytes_per_chunk: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Can include table-level validation exemptions and/or column-level exemptions to apply
    # to all relevant columns in this table.
    # Values are applied in addition to any default_import_blocking_validation_exemptions
    import_blocking_validation_exemptions: Optional[
        List[ImportBlockingValidationExemption]
    ] = attr.ib(default=None, validator=attr_validators.is_opt_list)

    def __attrs_post_init__(self) -> None:
        for c in self._columns:
            if c.file_tag != self.file_tag:
                raise ValueError(
                    f"Found column [{c.name}] in raw data file [{self.file_tag}] which "
                    f"has mismatched file_tag [{c.file_tag}]."
                )

            if c.state_code != self.state_code:
                raise ValueError(
                    f"Found column [{c.name}] in raw data file [{self.file_tag}] which "
                    f"has state_code [{c.state_code}] which does not match the "
                    f"state_code for this file [{self.state_code}]."
                )

        self._validate_primary_keys()

        column_names = [column.name for column in self._columns]
        if len(column_names) != len(set(column_names)):
            raise ValueError(f"Found duplicate columns in raw_file [{self.file_tag}]")

        external_id_types = [
            column.external_id_type
            for column in self._columns
            if column.external_id_type
        ]
        if len(external_id_types) != len(set(external_id_types)):
            raise ValueError(
                f"Found duplicate external ID types in raw_file [{self.file_tag}]"
            )

        missing_columns = set(self.primary_key_cols) - {
            column.name for column in self.current_columns
        }
        if missing_columns:
            raise ValueError(
                "Column(s) marked as primary keys not listed in"
                " columns list or is marked as deleted for file"
                f" [{self.file_tag}]: {missing_columns}"
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

        if self.is_primary_person_table and not self.has_primary_external_id_col:
            raise ValueError(
                f"Table marked as primary person table, but no primary external ID"
                f" column was specified for file [{self.file_tag}]"
            )

    @property
    def has_primary_external_id_col(self) -> bool:
        return any(
            column.is_primary_for_external_id_type for column in self.current_columns
        )

    @property
    def has_primary_person_external_id_col(self) -> bool:
        return any(
            column.is_primary_for_external_id_type
            and column.field_type == RawTableColumnFieldType.PERSON_EXTERNAL_ID
            for column in self.current_columns
        )

    def get_external_id_col_with_type(
        self, external_id_type: str
    ) -> RawTableColumnInfo | None:
        cols = [
            column
            for column in self.current_columns
            if column.external_id_type == external_id_type
        ]
        if not cols:
            return None
        if len(cols) > 1:
            raise ValueError(
                f"Multiple external ID columns found with type {external_id_type} in file {self.file_tag}"
                + ",".join(col.name for col in cols)
            )
        return cols[0]

    def get_primary_external_id_cols(self) -> List[RawTableColumnInfo]:
        """Return a list of all the columns that are primary for some external id type"""
        return [
            column
            for column in self.current_columns
            if column.is_primary_for_external_id_type
        ]

    def _validate_primary_keys(self) -> None:
        """Confirm that the primary key configuration is valid for this config. If this
        check passes, it does NOT mean that the table is sufficiently documented for use
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

    @property
    def quoting_mode(self) -> int:
        return csv.QUOTE_NONE if self.ignore_quotes else csv.QUOTE_MINIMAL

    def get_column_info(self, column_name: str) -> RawTableColumnInfo:
        """Returns information about the column with the provided |column_name|. Throws
        if that column does not exist in the table.
        """
        infos = [c for c in self._columns if c.name == column_name]
        if len(infos) != 1:
            raise ValueError(
                f"Expected to find exactly one entry for column [{column_name}], "
                f"found: {infos}"
            )
        return one(infos)

    @property
    def all_columns(self) -> List[RawTableColumnInfo]:
        """Returns all columns in the raw file config, including those that have been
        marked as deleted.
        """
        return self._columns

    @property
    def current_columns(self) -> List[RawTableColumnInfo]:
        """Returns the columns that currently exist in the raw file config. This is
        necessary because a column in self.columns may have been marked as deleted.
        """
        now = datetime.now(timezone.utc)
        return [column for column in self._columns if column.exists_at_datetime(now)]

    @property
    def current_pii_columns(self) -> List[RawTableColumnInfo]:
        """Filters to only current columns that have PII."""
        return [column for column in self.current_columns if column.is_pii]

    @property
    def current_documented_columns(self) -> List[RawTableColumnInfo]:
        """Filters to only current columns with descriptions."""
        return [column for column in self.current_columns if column.is_documented]

    @property
    def current_datetime_cols(self) -> List[Tuple[str, Optional[List[str]]]]:
        return [
            (column.name, column.datetime_sql_parsers)
            for column in self.current_columns
            if column.is_datetime
        ]

    @property
    def has_enums(self) -> bool:
        """If true, columns with enum values exist within this raw file, and this config
        is eligible to be refreshed with the for the fetch_column_values_for_state
        script.
        """
        return bool([column.name for column in self.current_columns if column.is_enum])

    @property
    def is_undocumented(self) -> bool:
        """Returns true if the raw file config does not provide enough information for this
        table to be used in ingest views or *latest views.
        """

        if not self.current_documented_columns:
            return True

        if len(self.primary_key_cols) == 0 and not self.no_valid_primary_keys:
            return True

        if is_meaningful_docstring(self.file_description):
            return False

        files_with_file_docstring_exemptions = (
            FILE_DOCUMENTATION_EXEMPTIONS.get(self.state_code) or set()
        )

        if self.file_tag in files_with_file_docstring_exemptions:
            return False

        return True

    @property
    def line_terminator(self) -> str:
        if self.custom_line_terminator:
            return self.custom_line_terminator

        return DEFAULT_CSV_LINE_TERMINATOR

    @property
    def always_historical_export(self) -> bool:
        return (
            self.export_lookback_window
            == RawDataExportLookbackWindow.FULL_HISTORICAL_LOOKBACK
        )

    def is_exempt_from_raw_data_pruning(self) -> bool:
        """Returns True if this file is exempt from raw data pruning."""

        if self.file_tag in FILES_EXEMPT_FROM_RAW_DATA_PRUNING_BY_STATE.get(
            self.state_code, {}
        ):
            return True

        # We currently only conduct raw data pruning on raw files that are always historical.
        if not self.always_historical_export:
            return True

        return self.no_valid_primary_keys

    def has_regularly_updated_data(self) -> bool:
        """Returns whether or not we think the data in this file is regularly updated;
        that is, the file itself is transferred regularly (not IRREGULAR) and it is not
        a code file (typically few changes day over day).
        """
        return (
            self.update_cadence != RawDataFileUpdateCadence.IRREGULAR
            and not self.is_code_file
        )

    def get_update_interval_in_days(self) -> int:
        return RawDataFileUpdateCadence.interval_from_cadence(self.update_cadence)

    def max_hours_before_stale(self) -> int:
        """Returns the maximum number of hours we will go between receiving exports of
        this file before calling it "stale".

        In general, we want to allow some leniency between receiving files for the data
        to actually enter our system (~ 12 hours).
        """
        return self.get_update_interval_in_days() * 24 + 12

    def file_is_exempt_from_validation(
        self, validation_type: RawDataImportBlockingValidationType
    ) -> bool:
        """Returns True if the validation_type is found in the file_tag's import_blocking_validation_exemptions
        or the default_import_blocking_validation_exemptions for the file_tag's region.
        """
        return ImportBlockingValidationExemption.list_includes_exemption_type(
            self.import_blocking_validation_exemptions,
            validation_type,
        )

    def column_is_exempt_from_validation(
        self,
        column_name: str,
        validation_type: RawDataImportBlockingValidationType,
    ) -> bool:
        """Returns True if the validation_type is found in the column's import_blocking_column_validation_exemptions
        or the file_tag's import_blocking_validation_exemptions
        or the default_import_blocking_validation_exemptions for the file_tag's region.
        """
        column_info = self.get_column_info(column_name)

        return ImportBlockingValidationExemption.list_includes_exemption_type(
            column_info.import_blocking_column_validation_exemptions, validation_type
        ) or self.file_is_exempt_from_validation(validation_type)

    def for_admin_panel_api(self) -> Dict[str, Any]:
        """Constructs an abridged set of fields to send to the admin panel front end.
        If you are updating this object, please make sure to update constants::RawFileConfigSummary
        as well.
        """
        return {
            "fileTag": self.file_tag,
            "fileDescription": self.file_description,
            "updateCadence": self.update_cadence.value.title(),
            "encoding": self.encoding,
            "separator": self.separator,
            "lineTerminator": self.line_terminator,
            "exportLookbackWindow": self.export_lookback_window.value.replace(
                "_", " "
            ).title(),
            "isCodeFile": self.is_code_file,
            "isChunkedFile": self.is_chunked_file,
            "manuallyPruned": not self.is_exempt_from_raw_data_pruning(),
            "inferColumns": self.infer_columns_from_config,
        }

    def column_names_at_datetime(self, dt: datetime) -> List[str]:
        """Returns a list of column names as they appeared in the config at the given
        datetime.
        """
        return [
            col_name
            for column in self._columns
            if (col_name := column.name_at_datetime(dt)) is not None
        ]

    def columns_at_datetime(self, dt: datetime) -> List[RawTableColumnInfo]:
        """Returns a list of column info objects that existed at the given datetime."""
        return [
            column
            for column in self._columns
            if column.name_at_datetime(dt) is not None
        ]

    def column_mapping_from_datetime_to_current(self, dt: datetime) -> Dict[str, str]:
        """Returns a dictionary mapping with the key being the column name at the given
        datetime and the value being the column name now. Ignores columns that have been
        added or deleted between the provided datetime and now.
        """
        now = datetime.now(tz=timezone.utc)
        return {
            col_name_at_dt: current_col_name
            for column in self._columns
            if (col_name_at_dt := column.name_at_datetime(dt)) is not None
            and (current_col_name := column.name_at_datetime(now)) is not None
        }

    @property
    def is_recidiviz_generated(self) -> bool:
        """Returns True if this file was generated by/for Recidiviz
        and does not correspond to any state database table."""
        return self.file_tag.upper().startswith("RECIDIVIZ_REFERENCE_")

    @classmethod
    def from_yaml_dict(
        cls,
        *,
        state_code: StateCode,
        file_tag: str,
        file_path: str,
        default_encoding: str,
        default_separator: str,
        default_line_terminator: Optional[str],
        default_update_cadence: RawDataFileUpdateCadence,
        default_ignore_quotes: bool,
        default_export_lookback_window: RawDataExportLookbackWindow,
        default_no_valid_primary_keys: bool,
        default_infer_columns_from_config: bool,
        default_import_blocking_validation_exemptions: Optional[
            List[ImportBlockingValidationExemption]
        ],
        file_config_dict: YAMLDict,
    ) -> "DirectIngestRawFileConfig":
        """Returns a DirectIngestRawFileConfig built from a YAMLDict"""
        primary_key_cols = file_config_dict.pop("primary_key_cols", list)
        file_description = file_config_dict.pop("file_description", str)
        update_cadence = file_config_dict.pop_optional("update_cadence", str)
        data_class = file_config_dict.pop("data_classification", str)
        import_blocking_validation_exemptions = None
        if (
            import_blocking_table_validation_exemptions_yaml := file_config_dict.pop_dicts_optional(
                "import_blocking_validation_exemptions"
            )
        ) is not None:
            import_blocking_validation_exemptions = [
                ImportBlockingValidationExemption(
                    validation_type=RawDataImportBlockingValidationType(
                        exemption.pop("validation_type", str)
                    ),
                    exemption_reason=exemption.pop("exemption_reason", str),
                )
                for exemption in import_blocking_table_validation_exemptions_yaml
            ]
            if default_import_blocking_validation_exemptions is not None:
                import_blocking_validation_exemptions.extend(
                    exemption
                    for exemption in default_import_blocking_validation_exemptions
                    if not ImportBlockingValidationExemption.list_includes_exemption_type(
                        import_blocking_validation_exemptions, exemption.validation_type
                    )
                )

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
            import_blocking_column_validation_exemptions = None
            if (
                import_blocking_column_validation_exemptions_yaml := column.pop_dicts_optional(
                    "import_blocking_column_validation_exemptions"
                )
            ) is not None:
                import_blocking_column_validation_exemptions = [
                    ImportBlockingValidationExemption(
                        validation_type=RawDataImportBlockingValidationType(
                            exemption.pop("validation_type", str)
                        ),
                        exemption_reason=exemption.pop("exemption_reason", str),
                    )
                    for exemption in import_blocking_column_validation_exemptions_yaml
                ]
            update_history = None
            if (
                update_history_yaml := column.pop_dicts_optional("update_history")
            ) is not None:
                update_history = [
                    ColumnUpdateInfo(
                        update_datetime=change.pop("update_datetime", datetime),
                        update_type=ColumnUpdateOperation(
                            change.pop("update_type", str)
                        ),
                        previous_value=change.pop_optional("previous_value", str),
                    )
                    for change in update_history_yaml
                ]
            column_infos.append(
                RawTableColumnInfo(
                    state_code=state_code,
                    file_tag=file_tag,
                    name=column_name,
                    field_type=(
                        RawTableColumnFieldType(field_type_str)
                        if (field_type_str := column.pop_optional("field_type", str))
                        else RawTableColumnFieldType.STRING
                    ),
                    is_pii=column.pop_optional("is_pii", bool) or False,
                    description=column.pop_optional("description", str),
                    known_values=known_value_infos,
                    datetime_sql_parsers=column.pop_list_optional(
                        "datetime_sql_parsers", str
                    ),
                    external_id_type=column.pop_optional("external_id_type", str),
                    is_primary_for_external_id_type=column.pop_optional(
                        "is_primary_for_external_id_type", bool
                    )
                    or False,
                    import_blocking_column_validation_exemptions=import_blocking_column_validation_exemptions,
                    update_history=update_history,
                    null_values=column.pop_list_optional("null_values", str),
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
        export_lookback_window = None
        if export_lookback_window_yaml := file_config_dict.pop_optional(
            "export_lookback_window", str
        ):
            export_lookback_window = RawDataExportLookbackWindow(
                export_lookback_window_yaml
            )

        is_code_file = file_config_dict.pop_optional("is_code_file", bool) or False
        no_valid_primary_keys = file_config_dict.pop_optional(
            "no_valid_primary_keys", bool
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
        is_primary_person_table = (
            file_config_dict.pop_optional("is_primary_person_table", bool) or False
        )
        is_chunked_file = (
            file_config_dict.pop_optional("is_chunked_file", bool) or False
        )

        max_num_unparseable_bytes_per_chunk = file_config_dict.pop_optional(
            "max_num_unparseable_bytes_per_chunk", int
        )

        if len(file_config_dict) > 0:
            raise ValueError(
                f"Found unexpected config values for raw file"
                f"[{file_tag}]: {repr(file_config_dict.get())}"
            )

        return DirectIngestRawFileConfig(
            state_code=state_code,
            file_tag=file_tag,
            file_path=file_path,
            file_description=file_description,
            update_cadence=RawDataFileUpdateCadence(
                update_cadence if update_cadence is not None else default_update_cadence
            ),
            data_classification=RawDataClassification(data_class),
            primary_key_cols=primary_key_cols,
            columns=column_infos,
            supplemental_order_by_clause=supplemental_order_by_clause or "",
            encoding=encoding if encoding is not None else default_encoding,
            separator=separator if separator is not None else default_separator,
            custom_line_terminator=(
                custom_line_terminator
                if custom_line_terminator is not None
                else default_line_terminator
            ),
            ignore_quotes=(
                ignore_quotes if ignore_quotes is not None else default_ignore_quotes
            ),
            export_lookback_window=(
                export_lookback_window
                if export_lookback_window is not None
                else default_export_lookback_window
            ),
            no_valid_primary_keys=(
                no_valid_primary_keys
                if no_valid_primary_keys is not None
                else default_no_valid_primary_keys
            ),
            infer_columns_from_config=(
                infer_columns_from_config
                if infer_columns_from_config is not None
                else default_infer_columns_from_config
            ),
            table_relationships=table_relationships,
            is_primary_person_table=is_primary_person_table,
            is_code_file=is_code_file,
            is_chunked_file=is_chunked_file,
            import_blocking_validation_exemptions=(
                import_blocking_validation_exemptions
                if import_blocking_validation_exemptions is not None
                else default_import_blocking_validation_exemptions
            ),
            max_num_unparseable_bytes_per_chunk=max_num_unparseable_bytes_per_chunk,
        )


@attr.s
class DirectIngestRegionRawFileConfig:
    """Class that parses and stores raw data import configs for a region"""

    # TODO(#5262): Add documentation for the structure of the raw data yaml files
    region_code: str = attr.ib()
    region_module: ModuleType = attr.ib(default=regions)
    yaml_config_file_dir: str = attr.ib()
    raw_file_configs: Dict[str, DirectIngestRawFileConfig] = attr.ib()

    def __attrs_post_init__(self) -> None:
        # Verify that all configs are in the dictionary with the right tags
        for labeled_file_tag, config in self.raw_file_configs.items():
            if config.file_tag != labeled_file_tag:
                raise ValueError(
                    f"The file tagged {config.file_tag} was labeled in code as"
                    f" {labeled_file_tag} in region: {self.region_code}."
                )

        configs = self.raw_file_configs.values()

        # Verify that only one file is marked as the is_primary_person_table
        is_primary_person_tables = list(
            filter(lambda config: config.is_primary_person_table, configs)
        )
        if len(is_primary_person_tables) >= 2:
            raise ValueError(
                f"The following tables in region: {self.region_code} are marked"
                f" as primary person tables, but only one primary person table is"
                f" allowed per region: {[c.file_tag for c in is_primary_person_tables]}"
            )

        # Verify that all columns marked as primary for some external ID within this region
        # are distinct ID types
        external_id_type_primaries = set()
        external_ids_present = set()
        columns_with_external_id_types = [
            column
            for config in configs
            for column in config.current_columns
            if column.external_id_type
        ]
        for col in columns_with_external_id_types:
            if col.is_primary_for_external_id_type:
                if col.external_id_type in external_id_type_primaries:
                    raise ValueError(
                        f"Duplicate columns marked as primary for external id type "
                        f"{col.external_id_type} in region: {self.region_code}"
                    )
                external_id_type_primaries.add(col.external_id_type)
            if col.external_id_type:
                external_ids_present.add(col.external_id_type)

        # Verify that every non-null external ID type on any column has
        # a corresponding external ID primary column
        id_types_without_primaries = external_ids_present - external_id_type_primaries
        if id_types_without_primaries:
            raise ValueError(
                f"These external ID types are present on columns, without a"
                f" corresponding column marked as the primary for that"
                f" external id type, in region {self.region_code}:"
                f" {id_types_without_primaries}"
            )

    @property
    def default_config_filename(self) -> str:
        return f"{self.region_code.lower()}_default.yaml"

    def default_config(self) -> DirectIngestRawFileDefaultConfig:
        """Return the default raw data config for this region."""
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
        default_update_cadence_str = default_contents.pop("default_update_cadence", str)
        default_update_cadence = RawDataFileUpdateCadence(default_update_cadence_str)
        default_ignore_quotes = default_contents.pop("default_ignore_quotes", bool)

        default_export_lookback_window = RawDataExportLookbackWindow(
            default_contents.pop("default_export_lookback_window", str)
        )
        default_no_valid_primary_keys = default_contents.pop(
            "default_no_valid_primary_keys", bool
        )
        default_infer_columns_from_config = default_contents.pop(
            "default_infer_columns_from_config", bool
        )
        default_import_blocking_validation_exemptions = None
        if (
            import_blocking_validation_exemptions_yaml := default_contents.pop_dicts_optional(
                "default_import_blocking_validation_exemptions"
            )
        ) is not None:
            default_import_blocking_validation_exemptions = [
                ImportBlockingValidationExemption(
                    validation_type=RawDataImportBlockingValidationType(
                        exemption.pop("validation_type", str)
                    ),
                    exemption_reason=exemption.pop("exemption_reason", str),
                )
                for exemption in import_blocking_validation_exemptions_yaml
            ]

        return DirectIngestRawFileDefaultConfig(
            filename=self.default_config_filename,
            default_encoding=default_encoding,
            default_separator=default_separator,
            default_line_terminator=default_line_terminator,
            default_ignore_quotes=default_ignore_quotes,
            default_infer_columns_from_config=default_infer_columns_from_config,
            default_export_lookback_window=default_export_lookback_window,
            default_no_valid_primary_keys=default_no_valid_primary_keys,
            default_update_cadence=default_update_cadence,
            default_import_blocking_validation_exemptions=default_import_blocking_validation_exemptions,
        )

    def get_datetime_parsers(self) -> Set[str]:
        """Returns the set of every datetime parser that exists for this raw file config
        config.
        """
        all_parsers = set()
        for config in self.raw_file_configs.values():
            for _, parsers in config.current_datetime_cols:
                if parsers:
                    for parser in parsers:
                        all_parsers.add(parser)
        return all_parsers

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

    def get_primary_person_table(self) -> Optional[DirectIngestRawFileConfig]:
        primary_person_table = [
            config
            for config in self.raw_file_configs.values()
            if config.is_primary_person_table
        ]
        return one(primary_person_table) if primary_person_table else None

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
                state_code=StateCode(self.region_code.upper()),
                file_tag=file_tag,
                file_path=yaml_file_path,
                default_encoding=default_config.default_encoding,
                default_separator=default_config.default_separator,
                default_line_terminator=default_config.default_line_terminator,
                default_update_cadence=default_config.default_update_cadence,
                default_ignore_quotes=default_config.default_ignore_quotes,
                default_export_lookback_window=default_config.default_export_lookback_window,
                default_no_valid_primary_keys=default_config.default_no_valid_primary_keys,
                default_infer_columns_from_config=default_config.default_infer_columns_from_config,
                default_import_blocking_validation_exemptions=default_config.default_import_blocking_validation_exemptions,
                file_config_dict=yaml_contents,
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
                        if column.column not in [
                            c.name for c in column_config.current_columns
                        ]:
                            raise ValueError(
                                f"Found column [{column}] referenced in join clause "
                                f"[{join_clause.to_sql()}] which is not defined in "
                                f"or is marked as deleted in the config for [{column.file_tag}]"
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
        for config in sorted(raw_file_configs.values(), key=lambda c: c.file_tag):
            for table_relationship in config.table_relationships:
                src_file_tag = table_relationship_locations[
                    table_relationship.get_referenced_tables()
                ]
                if (
                    table_relationship.file_tag == table_relationship.foreign_table
                    or src_file_tag != config.file_tag
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

    def get_configs_with_regularly_updated_data(
        self,
    ) -> List[DirectIngestRawFileConfig]:
        """List of configs in this region that have regularly updated data"""
        return [
            config
            for config in self.raw_file_configs.values()
            if config.has_regularly_updated_data()
        ]


_RAW_TABLE_CONFIGS: Dict[
    str, Dict[str, "DirectIngestRegionRawFileConfig"]
] = defaultdict(dict)


def get_region_raw_file_config(
    region_code: str, region_module: Optional[ModuleType] = None
) -> DirectIngestRegionRawFileConfig:
    region_code_lower = region_code.lower()
    if not region_module:
        region_module = direct_ingest_regions_module

    region_module_str = region_module.__name__
    if region_code_lower not in _RAW_TABLE_CONFIGS[region_module_str]:
        _RAW_TABLE_CONFIGS[region_module_str][
            region_code_lower
        ] = DirectIngestRegionRawFileConfig(region_code_lower, region_module)

    return _RAW_TABLE_CONFIGS[region_module_str][region_code_lower]
