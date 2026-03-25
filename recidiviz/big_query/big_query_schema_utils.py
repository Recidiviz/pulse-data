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

from google.cloud import bigquery

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
from recidiviz.big_query.constants import BQ_TABLE_COLUMN_DESCRIPTION_MAX_LENGTH
from recidiviz.utils.string_formatting import truncate_string_if_necessary

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
