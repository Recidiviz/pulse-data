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
import enum
import logging
import string
from enum import Enum
from typing import Any, Dict, List, Optional, Type

import attr
import sqlalchemy
from google.cloud import bigquery
from sqlalchemy import Column
from sqlalchemy.dialects import postgresql

from recidiviz.big_query.constants import BQ_TABLE_COLUMN_DESCRIPTION_MAX_LENGTH
from recidiviz.common.attr_utils import (
    is_bool,
    is_date,
    is_datetime,
    is_enum,
    is_float,
    is_int,
    is_list,
    is_str,
)
from recidiviz.common.constants.csv import DEFAULT_CSV_LINE_TERMINATOR
from recidiviz.common.constants.encoding import (
    BIG_QUERY_FIXED_LENGTH_ENCODINGS,
    BIG_QUERY_VARIABLE_LENGTH_ENCODINGS,
    PYTHON_STANDARD_ENCODINGS_TO_BIG_QUERY_ENCODING,
)
from recidiviz.persistence.database.reserved_words import BIGQUERY_RESERVED_WORDS
from recidiviz.utils.encoding import to_python_standard

# Maximum value of an integer stored in BigQuery
MAX_BQ_INT = (2**63) - 1


class BigQueryDateInterval(enum.Enum):
    DAY = "DAY"
    WEEK = "WEEK"
    MONTH = "MONTH"
    QUARTER = "QUARTER"
    YEAR = "YEAR"


def _schema_column_type_for_attribute(attribute: attr.Attribute) -> str:
    """Returns the schema column type that should be used to store the value of the
    provided |attribute| in a BigQuery table."""
    if is_enum(attribute) or is_list(attribute) or is_str(attribute):
        return bigquery.enums.SqlTypeNames.STRING.value
    if is_int(attribute):
        return bigquery.enums.SqlTypeNames.INTEGER.value
    if is_float(attribute):
        return bigquery.enums.SqlTypeNames.FLOAT.value
    if is_datetime(attribute):
        return bigquery.enums.SqlTypeNames.DATETIME.value
    if is_date(attribute):
        return bigquery.enums.SqlTypeNames.DATE.value
    if is_bool(attribute):
        return bigquery.enums.SqlTypeNames.BOOLEAN.value
    raise ValueError(f"Unhandled attribute type for attribute: {attribute}")


def schema_field_for_attribute(
    field_name: str, attribute: attr.Attribute
) -> bigquery.SchemaField:
    """Returns a BigQuery SchemaField object with the information needed for a column
    with the name of |field_name| storing the values in the |attribute|."""
    return bigquery.SchemaField(
        field_name, _schema_column_type_for_attribute(attribute), mode="NULLABLE"
    )


def _bq_schema_column_type_for_type(
    field_type: Type,
) -> bigquery.enums.SqlTypeNames:
    """Returns the schema column type that should be used to store the value of the
    provided |field_type| in a BigQuery table."""
    if field_type is Enum or field_type is str or field_type is list:
        return bigquery.enums.SqlTypeNames.STRING
    if field_type is int:
        return bigquery.enums.SqlTypeNames.INTEGER
    if field_type is float:
        return bigquery.enums.SqlTypeNames.FLOAT
    if field_type is datetime.date:
        return bigquery.enums.SqlTypeNames.DATE
    if field_type is datetime.datetime:
        return bigquery.enums.SqlTypeNames.DATETIME
    if field_type is bool:
        return bigquery.enums.SqlTypeNames.BOOLEAN
    raise ValueError(f"Unhandled field type for field_type: {field_type}")


def schema_field_for_type(field_name: str, field_type: Type) -> bigquery.SchemaField:
    """Returns a BigQuery SchemaField object with the information needed for a column
    with the name of |field_name| storing the values in the |field_type|."""
    return bigquery.SchemaField(
        field_name,
        _bq_schema_column_type_for_type(field_type).value,
        mode="REPEATED" if field_type is list else "NULLABLE",
    )


def bq_schema_column_type_for_sqlalchemy_column(
    column: Column,
) -> bigquery.enums.SqlTypeNames:
    """Returns the schema column type that should be used to store the value of the
    provided |column| in a BigQuery table."""
    col_postgres_type = column.type

    if isinstance(col_postgres_type, postgresql.UUID):
        # UUID types don't have a python_type implemented, but we cast to string
        # when we migrate to BQ
        return bigquery.enums.SqlTypeNames.STRING

    col_python_type = col_postgres_type.python_type

    if col_python_type == datetime.datetime and (
        isinstance(col_postgres_type, postgresql.TIMESTAMP)
        or col_postgres_type.timezone
    ):
        return bigquery.enums.SqlTypeNames.TIMESTAMP
    if col_python_type == dict and isinstance(
        col_postgres_type, (postgresql.JSON, postgresql.JSONB)
    ):
        return bigquery.enums.SqlTypeNames.STRING
    if col_python_type == list:
        if isinstance(col_postgres_type.item_type, sqlalchemy.sql.sqltypes.String):
            return bigquery.enums.SqlTypeNames.STRING
        if isinstance(col_postgres_type.item_type, sqlalchemy.sql.sqltypes.Numeric):
            return bigquery.enums.SqlTypeNames.NUMERIC
        raise ValueError(
            "Syncing non-string/numeric array item types has not yet been implemented"
        )

    return _bq_schema_column_type_for_type(col_python_type)


def schema_for_sqlalchemy_table(
    table: sqlalchemy.Table,
    add_state_code_field: bool = False,
) -> List[bigquery.SchemaField]:
    """Returns the necessary BigQuery schema for storing the contents of the
    table in BigQuery, which is a list of SchemaField objects containing the
    column name and value type for each column in the table."""
    columns_for_table = [
        bigquery.SchemaField(
            col.name,
            bq_schema_column_type_for_sqlalchemy_column(col).value,
            mode="REPEATED" if isinstance(col.type, postgresql.ARRAY) else "NULLABLE",
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

    # BQ doesn't allow column names to begin with a number.
    # Also doesn't allow for column names to be reserved words.
    # So we prepend an underscore in that case
    if (
        column_name[0] in string.digits
        or column_name.upper() in BIGQUERY_RESERVED_WORDS
    ):
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


def format_description_for_big_query(description: Optional[str]) -> str:
    """If |description| is longer than BQ allows, truncate down to size with suffix
    indicating it is truncated. THe full versions of the description should be in
    Gitbook.
    """
    if not description:
        return ""

    if len(description) > BQ_TABLE_COLUMN_DESCRIPTION_MAX_LENGTH:
        truncated_str = " ... (truncated)"
        description = (
            description[: BQ_TABLE_COLUMN_DESCRIPTION_MAX_LENGTH - len(truncated_str)]
            + truncated_str
        )
    return description


def transform_dict_to_bigquery_row(data_point: Dict[str, Any]) -> bigquery.table.Row:
    """Transforms a dictionary back to a BigQuery row."""
    values = []
    indices = {}
    for idx, (key, value) in enumerate(data_point.items()):
        values.append(value)
        indices[key] = idx
    return bigquery.table.Row(values, indices)


def datetime_clause(dt: datetime.datetime) -> str:
    """Returns a datetime formatted as a BigQuery DATETIME() function."""
    return f'DATETIME "{dt.isoformat()}"'


def is_big_query_valid_encoding(encoding: str) -> bool:
    """Given a string encoding, returns whether or not this encoding is supported
    by BigQuery. Also standardizes the encoding to account for variations in naming
    equivalent encodings, like `latin-1` -> `iso8859-1`, `windows-1252` -> `cp1252`
    so we dont need to care about equivalent aliases, only the "standards" that
    python has pre-deteremined
    """
    return (
        to_python_standard(encoding) in PYTHON_STANDARD_ENCODINGS_TO_BIG_QUERY_ENCODING
    )


def to_big_query_valid_encoding(encoding: str) -> str:
    """Given a string encoding, returns the BigQuery equivalent."""
    return PYTHON_STANDARD_ENCODINGS_TO_BIG_QUERY_ENCODING[to_python_standard(encoding)]


def is_big_query_valid_line_terminator(line_terminator: str) -> bool:
    """Boolean return for if the provided |line_terminator| will be accepted by BigQuery"""
    return line_terminator == DEFAULT_CSV_LINE_TERMINATOR


def is_big_query_valid_delimiter(delimiter: str, encoding: str) -> bool:
    """Boolean return for if the provided |delimiter| will be accepted by BigQuery. If
    it uses a variable length encoding (utf-8) must be a single byte (code points 1-127)
    otherwise, it is a valid delimiter if it is a single string char.
    """
    big_query_encoding = to_big_query_valid_encoding(encoding)
    if big_query_encoding in BIG_QUERY_VARIABLE_LENGTH_ENCODINGS:
        return len(delimiter.encode(encoding)) == 1

    if big_query_encoding in BIG_QUERY_FIXED_LENGTH_ENCODINGS:
        return len(delimiter) == 1

    raise ValueError(f"Unrecognized encoding: {big_query_encoding}")


def bq_query_job_result_to_list_of_row_dicts(
    rows: bigquery.table.RowIterator,
) -> List[Dict[str, Any]]:
    """Converts a BigQuery query job result to a list of dictionaries, each dictionary
    representing a row with key=column name, value=value."""
    row_tuples: List[Dict[str, Any]] = [row.items() for row in rows]

    return [dict(item) for item in row_tuples]
