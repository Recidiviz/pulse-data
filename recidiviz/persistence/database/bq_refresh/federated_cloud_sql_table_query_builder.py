# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Defines and object that builds BigQuery-compatible queries that can be used to query
a table in CloudSQL using a BigQuery `EXTERNAL_QUERY` federated query.
"""
from typing import Dict, List, Optional

import sqlalchemy
from sqlalchemy import Table
from sqlalchemy.dialects import postgresql

from recidiviz.persistence.database.schema_type import SchemaType


class FederatedCloudSQLTableQueryBuilder:
    """Builds BigQuery-compatible queries that can be used to query a table in CloudSQL
    using a BigQuery `EXTERNAL_QUERY` federated query. BigQuery places some restrictions
    on the output columns that we must handle when doing this type of query.
    """

    def __init__(
        self, *, schema_type: SchemaType, table: Table, columns_to_include: List[str]
    ):
        if schema_type.is_multi_db_schema:
            raise ValueError(
                f"No support for loading multi-DB schema [{schema_type.value}] - since multi-DB schems"
                f"may have conflicting primary keys in each of the databses, they need to be loaded into BigQuery with"
                f"caution."
            )
        self.table = table
        self.columns_to_include = columns_to_include

    def select_clause(self) -> str:
        formatted_columns = self._formatted_columns_for_select_clause()
        return f"SELECT {formatted_columns}"

    def full_query(self) -> str:
        return " ".join(
            filter(
                None,
                [
                    self.select_clause(),
                    self.from_clause(),
                ],
            )
        )

    def from_clause(self) -> str:
        """The FROM clause that should be used to query from the table."""
        return f"FROM {self.table_name}"

    @property
    def table_name(self) -> str:
        return self.table.name

    @staticmethod
    def qualified_column_names_map(
        columns: List[str], table_prefix: Optional[str] = None
    ) -> Dict[str, str]:
        if table_prefix:
            return {col: f"{table_prefix}.{col}" for col in columns}
        return {col: col for col in columns}

    def _formatted_columns_for_select_clause(self) -> str:
        qualified_names_map = self.qualified_column_names_map(
            self.columns_to_include, table_prefix=self.table_name
        )
        select_columns = []
        for column in self.table.columns:
            if column.name not in self.columns_to_include:
                continue
            qualified_name = qualified_names_map[column.name]
            if isinstance(column.type, sqlalchemy.Enum):
                select_columns.append(f"CAST({qualified_name} as VARCHAR)")
            elif isinstance(column.type, postgresql.UUID):
                select_columns.append(f"CAST({qualified_name} as VARCHAR)")
            elif isinstance(column.type, sqlalchemy.ARRAY) and isinstance(
                column.type.item_type, sqlalchemy.String
            ):
                # BigQuery, while claiming to support NULL values in an array, actually
                # does not. For strings, we instead replace NULL with the empty string.
                # Arrays of other types are not modified, so if they include NULL values
                # they will fail.
                select_columns.append(
                    f"ARRAY_REPLACE({qualified_name}, NULL, '') as {column.name}"
                )
            else:
                select_columns.append(qualified_name)
        return ",".join(select_columns)
