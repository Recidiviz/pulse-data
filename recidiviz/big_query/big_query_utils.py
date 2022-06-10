# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Helper functions for creating BigQuery views and manipulating BigQuery contents"""
import datetime
import logging
import string
from enum import Enum
from typing import Any, Dict, List, Type

import attr
import sqlalchemy
from google.cloud import bigquery
from sqlalchemy import Column
from sqlalchemy.dialects import postgresql

from recidiviz.common.attr_utils import (
    is_bool,
    is_date,
    is_enum,
    is_float,
    is_int,
    is_list,
    is_str,
)

# Maximum value of an integer stored in BigQuery
MAX_BQ_INT = (2**63) - 1


def _schema_column_type_for_attribute(attribute: attr.Attribute) -> str:
    """Returns the schema column type that should be used to store the value of the
    provided |attribute| in a BigQuery table."""
    if is_enum(attribute) or is_list(attribute) or is_str(attribute):
        return bigquery.enums.SqlTypeNames.STRING.value
    if is_int(attribute):
        return bigquery.enums.SqlTypeNames.INTEGER.value
    if is_float(attribute):
        return bigquery.enums.SqlTypeNames.FLOAT.value
    if is_date(attribute):
        return bigquery.enums.SqlTypeNames.DATE.value
    if is_bool(attribute):
        return bigquery.enums.SqlTypeNames.BOOLEAN.value
    raise ValueError(f"Unhandled attribute type for attribute: {attribute}")


def schema_field_for_attribute(field_name: str, attribute: attr.Attribute) -> bigquery:
    """Returns a BigQuery SchemaField object with the information needed for a column
    with the name of |field_name| storing the values in the |attribute|."""
    return bigquery.SchemaField(
        field_name, _schema_column_type_for_attribute(attribute), mode="NULLABLE"
    )


def _schema_column_type_for_type(field_type: Type) -> str:
    """Returns the schema column type that should be used to store the value of the
    provided |field_type| in a BigQuery table."""
    if field_type is Enum or field_type is str or field_type is List:
        return bigquery.enums.SqlTypeNames.STRING.value
    if field_type is int:
        return bigquery.enums.SqlTypeNames.INTEGER.value
    if field_type is float:
        return bigquery.enums.SqlTypeNames.FLOAT.value
    if field_type is datetime.date:
        return bigquery.enums.SqlTypeNames.DATE.value
    if field_type is datetime.datetime:
        return bigquery.enums.SqlTypeNames.DATETIME.value
    if field_type is bool:
        return bigquery.enums.SqlTypeNames.BOOLEAN.value
    # TODO(#7285): Add support for ARRAY types when we turn on the regular
    #  CloudSQL to BQ refresh for the JUSTICE_COUNTS schema
    raise ValueError(f"Unhandled field type for field_type: {field_type}")


def schema_field_for_type(field_name: str, field_type: Type) -> bigquery.SchemaField:
    """Returns a BigQuery SchemaField object with the information needed for a column
    with the name of |field_name| storing the values in the |field_type|."""
    return bigquery.SchemaField(
        field_name, _schema_column_type_for_type(field_type), mode="NULLABLE"
    )


def _schema_column_type_for_sqlalchemy_column(column: Column) -> str:
    """Returns the schema column type that should be used to store the value of the
    provided |column| in a BigQuery table."""
    col_postgres_type = column.type

    if isinstance(col_postgres_type, postgresql.UUID):
        # UUID types don't have a python_type implemented, but we cast to string
        # when we migrate to BQ
        return bigquery.enums.SqlTypeNames.STRING.value

    col_python_type = col_postgres_type.python_type

    if col_python_type == datetime.datetime and (
        isinstance(col_postgres_type, postgresql.TIMESTAMP)
        or col_postgres_type.timezone
    ):
        return bigquery.enums.SqlTypeNames.TIMESTAMP.value
    if col_python_type == dict and isinstance(
        col_postgres_type, (postgresql.JSON, postgresql.JSONB)
    ):
        return bigquery.enums.SqlTypeNames.STRING.value

    return _schema_column_type_for_type(col_python_type)


def schema_for_sqlalchemy_table(
    table: sqlalchemy.Table, add_state_code_field: bool = False
) -> List[bigquery.SchemaField]:
    """Returns the necessary BigQuery schema for storing the contents of the
    table in BigQuery, which is a list of SchemaField objects containing the
    column name and value type for each column in the table."""
    columns_for_table = [
        bigquery.SchemaField(
            col.name, _schema_column_type_for_sqlalchemy_column(col), mode="NULLABLE"
        )
        for col in table.columns
    ]

    if add_state_code_field:
        columns_for_table.append(
            bigquery.SchemaField(
                "state_code",
                bigquery.enums.SqlTypeNames.STRING.value,
                mode="NULLABLE",
            )
        )

    return columns_for_table


def normalize_column_name_for_bq(column_name: str) -> str:
    column_name = _remove_non_printable_characters(column_name)
    # Strip whitespace from head/tail of column names
    column_name = column_name.strip()
    column_name = _make_bq_compatible_column_name(column_name)

    # BQ doesn't allow column names to begin with a number, so we prepend an underscore in that case
    if column_name[0] in string.digits:
        column_name = "_" + column_name

    return column_name


def _make_bq_compatible_column_name(column_name: str) -> str:
    """Replaces all non-allowable BigQuery characters with an underscore."""

    def is_bq_allowable_column_char(x: str) -> bool:
        return x in string.ascii_letters or x in string.digits or x == "_"

    column_name = "".join(
        [c if is_bq_allowable_column_char(c) else "_" for c in column_name]
    )
    return column_name


def _remove_non_printable_characters(column_name: str) -> str:
    """Removes all non-printable characters that occasionally show up in column names. This is known to happen in
    random columns"""
    fixed_column = "".join([x for x in column_name if x in string.printable])
    if fixed_column != column_name:
        logging.info(
            "Found non-printable characters in column [%s]. Original: [%s]",
            fixed_column,
            repr(column_name),
        )
    return fixed_column


def transform_dict_to_bigquery_row(data_point: Dict[str, Any]) -> bigquery.table.Row:
    """Transforms a dictionary back to a BigQuery row."""
    values = []
    indices = {}
    for idx, (key, value) in enumerate(data_point.items()):
        values.append(value)
        indices[key] = idx
    return bigquery.table.Row(values, indices)
