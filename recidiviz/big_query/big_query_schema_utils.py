# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Utilities for comparing declared view schemas against deployed BigQuery schemas."""

from typing import Sequence, cast

import sqlalchemy
from google.cloud import bigquery

from recidiviz.big_query.big_query_utils import schema_for_sqlalchemy_table
from recidiviz.big_query.big_query_view_column import (
    COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
    BigQueryViewColumn,
    Bool,
    ConcreteBigQueryColumnType,
    Date,
    DateTime,
    Float,
    Integer,
    Json,
    Numeric,
    Record,
    SqlFieldMode,
    String,
    Time,
    Timestamp,
)
from recidiviz.big_query.constants import (
    BQ_TABLE_COLUMN_DESCRIPTION_MAX_LENGTH,
    EXTERNAL_DATA_FILE_NAME_PSEUDOCOLUMN,
    PARTITION_DATE_PSEUDOCOLUMN,
    PARTITION_TIME_PSEUDOCOLUMN,
)
from recidiviz.utils.string_formatting import truncate_string_if_necessary

# Pseudocolumns that real BigQuery exposes but does not include in
# a table's schema. The BQ emulator (when seeded by our test fixtures)
# returns these as real schema fields, which produces false schema mismatches
# when comparing emulator-returned schemas against real BigQuery.
_BQ_PSEUDOCOLUMN_NAMES = frozenset(
    {
        EXTERNAL_DATA_FILE_NAME_PSEUDOCOLUMN,
        PARTITION_TIME_PSEUDOCOLUMN,
        PARTITION_DATE_PSEUDOCOLUMN,
    }
)


def strip_pseudocolumns_from_schema(
    schema: Sequence[bigquery.SchemaField],
) -> list[bigquery.SchemaField]:
    """Returns |schema| with any BigQuery pseudocolumn fields removed.

    Real BigQuery does not include pseudocolumns (`_FILE_NAME`, `_PARTITIONTIME`,
    `_PARTITIONDATE`) in a table's schema, but our emulator source-table fixtures
    seed these fields explicitly. Stripping them lets emulator-derived schemas
    match real BigQuery.
    """
    return [field for field in schema if field.name not in _BQ_PSEUDOCOLUMN_NAMES]


_FIELD_TYPE_TO_COLUMN_CLASS: dict[str, ConcreteBigQueryColumnType] = {
    "STRING": String,
    "INTEGER": Integer,
    "INT64": Integer,
    "DATE": Date,
    "FLOAT": Float,
    "FLOAT64": Float,
    "BOOLEAN": Bool,
    "BOOL": Bool,
    "DATETIME": DateTime,
    "TIMESTAMP": Timestamp,
    "TIME": Time,
    "JSON": Json,
    "NUMERIC": Numeric,
}


def bq_field_type_to_column_class(
    field_type: str | bigquery.SqlTypeNames,
) -> ConcreteBigQueryColumnType:
    """Returns the BigQueryViewColumn subclass for the given BigQuery field type."""
    field_type_str = (
        field_type.value
        if isinstance(field_type, bigquery.SqlTypeNames)
        else field_type
    )
    column_cls = _FIELD_TYPE_TO_COLUMN_CLASS.get(field_type_str)
    if column_cls is None:
        raise ValueError(f"Unsupported BigQuery field type {field_type!r}")
    return column_cls


def schema_field_to_view_column(field: bigquery.SchemaField) -> BigQueryViewColumn:
    """Converts a bigquery.SchemaField to a BigQueryViewColumn instance."""
    column_cls = bq_field_type_to_column_class(field.field_type)
    description = field.description or COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT
    mode = cast(SqlFieldMode, field.mode)
    return column_cls(
        name=field.name,
        description=description,
        mode=mode,
    )


def declared_schema_for_sqlalchemy_table(
    table: sqlalchemy.Table,
) -> list[BigQueryViewColumn]:
    """Returns the declared BigQueryViewColumn schema for the given SQLAlchemy
    table. Wraps schema_for_sqlalchemy_table + schema_field_to_view_column so
    the SQLAlchemy -> BigQuery type mapping has a single source of truth.
    """
    return [
        schema_field_to_view_column(field)
        for field in schema_for_sqlalchemy_table(table)
    ]


def truncate_column_description_for_big_query(description: str) -> str:
    """Truncates |description| to fit within BigQuery's column description
    character limit (1024 chars). Returns the original string if it is already
    within the limit.
    """
    return truncate_string_if_necessary(
        description, max_length=BQ_TABLE_COLUMN_DESCRIPTION_MAX_LENGTH
    )


def diff_declared_schema_to_bq_schema(
    declared_schema: Sequence[BigQueryViewColumn],
    deployed_schema: Sequence[bigquery.SchemaField],
) -> list[tuple[str, bigquery.SchemaField]]:
    """Compare a declared view schema (list of BigQueryViewColumns) against a native
    BigQuery schema (list of bigquery.SchemaFields). Fields must appear in the same
    order in both schemas.

    Returns:
        A list of (indicator, field) tuples in positional order.
        The indicator is "+" for deployed fields not matching declared schema, and
        "-" for declared fields not matching deployed schema.
    """
    diff: list[tuple[str, bigquery.SchemaField]] = []
    for i in range(max(len(declared_schema), len(deployed_schema))):
        declared_field = declared_schema[i] if i < len(declared_schema) else None
        deployed_field = deployed_schema[i] if i < len(deployed_schema) else None

        if deployed_field is not None and declared_field is None:
            diff.append(("+", deployed_field))
        elif declared_field is not None and deployed_field is None:
            diff.append(("-", declared_field.as_schema_field()))
        elif declared_field is not None and deployed_field is not None:
            if (
                isinstance(declared_field, Record)
                and deployed_field.field_type == bigquery.SqlTypeNames.RECORD
                and declared_field.name == deployed_field.name
            ):
                # Record subfield diff entries have names of the form
                # `top_level_field.subfield_name`.
                subdiff = diff_declared_schema_to_bq_schema(
                    declared_field.fields, deployed_field.fields
                )
                for line in subdiff:
                    sub_field = line[1]
                    sub_field = bigquery.SchemaField(
                        declared_field.name + "." + sub_field.name,
                        sub_field.field_type,
                    )
                    diff.append((line[0], sub_field))
            elif not declared_field.matches_bq_field(deployed_field):
                diff.append(("-", declared_field.as_schema_field()))
                diff.append(("+", deployed_field))

    return diff


def format_schema_diffs(
    diffs_by_address: dict[str, list[tuple[str, bigquery.SchemaField]]],
) -> str:
    """Format a dictionary of schema diffs (keyed by view address)."""
    parts = []
    for address, schema_diff in diffs_by_address.items():
        # TODO(#54941) include description when it is loaded
        diff_lines = "\n".join(
            f"  {indicator} {field.name} ({field.mode} {field.field_type})"
            for indicator, field in schema_diff
        )
        parts.append(f"{address}:\n{diff_lines}")
    return "\n".join(parts)
