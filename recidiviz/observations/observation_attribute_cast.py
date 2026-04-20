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
"""Helpers for emitting SQL CAST clauses for observation attribute columns."""

from google.cloud import bigquery

from recidiviz.big_query.big_query_view_column import BigQueryViewColumn

# Mapping from BQ schema field type to the BigQuery Standard SQL type name used in
# CAST(... AS <type>) clauses.
_FIELD_TYPE_TO_SQL_CAST_TYPE: dict[
    bigquery.SqlTypeNames | bigquery.StandardSqlTypeNames, str
] = {
    bigquery.SqlTypeNames.STRING: "STRING",
    bigquery.SqlTypeNames.INTEGER: "INT64",
    bigquery.SqlTypeNames.BOOLEAN: "BOOL",
    bigquery.SqlTypeNames.FLOAT: "FLOAT64",
    bigquery.SqlTypeNames.DATE: "DATE",
    bigquery.SqlTypeNames.DATETIME: "DATETIME",
    bigquery.SqlTypeNames.TIMESTAMP: "TIMESTAMP",
    bigquery.SqlTypeNames.TIME: "TIME",
    bigquery.StandardSqlTypeNames.NUMERIC: "NUMERIC",
}


def sql_cast_clause_for_attribute_column(column: BigQueryViewColumn) -> str:
    """Returns a `CAST(<col> AS <type>) AS <col>` clause for the given attribute column,
    based on its declared BigQuery type."""
    if column.field_type not in _FIELD_TYPE_TO_SQL_CAST_TYPE:
        raise ValueError(
            f"No SQL CAST mapping defined for column [{column.name}] with field type "
            f"[{column.field_type}]."
        )
    cast_type = _FIELD_TYPE_TO_SQL_CAST_TYPE[column.field_type]
    return f"CAST({column.name} AS {cast_type}) AS {column.name}"
