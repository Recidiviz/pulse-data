# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Defines the table schema to be used when mocking a table in Postgres, with
helpers to construct from various sources.
"""
from typing import Dict, List

import attr
import sqlalchemy
from google.cloud import bigquery
from sqlalchemy.sql import sqltypes


# TODO(#9717): Rename to PostgresTableSchema?
@attr.s(frozen=True)
class MockTableSchema:
    """Defines the table schema to be used when mocking a table in Postgres, with
    helpers to construct from various sources.
    """

    data_types: Dict[str, sqltypes.SchemaType] = attr.ib()

    @classmethod
    def from_sqlalchemy_table(cls, table: sqlalchemy.Table) -> "MockTableSchema":
        data_types = {}
        for column in table.columns:
            if isinstance(column.type, sqltypes.Enum):
                data_types[column.name] = sqltypes.String(255)
            else:
                data_types[column.name] = column.type
        return cls(data_types)

    @classmethod
    def from_big_query_schema_fields(
        cls, bq_schema: List[bigquery.SchemaField]
    ) -> "MockTableSchema":
        data_types = {}
        for field in bq_schema:
            field_type = bigquery.enums.SqlTypeNames(field.field_type)
            if field_type is bigquery.enums.SqlTypeNames.STRING:
                data_type = sqltypes.String(255)
            elif field_type is bigquery.enums.SqlTypeNames.INTEGER:
                data_type = sqltypes.Integer
            elif field_type is bigquery.enums.SqlTypeNames.FLOAT:
                data_type = sqltypes.Float
            elif field_type is bigquery.enums.SqlTypeNames.DATE:
                data_type = sqltypes.Date
            elif field_type is bigquery.enums.SqlTypeNames.BOOLEAN:
                data_type = sqltypes.Boolean
            else:
                raise ValueError(
                    f"Unhandled big query field type '{field_type}' for attribute '{field.name}'"
                )
            data_types[field.name] = data_type
        return cls(data_types)
