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

"""Query builder to filter data from CloudSQL export and to select filtered regions from existing
    BigQuery tables. For association tables, a join clause is added to filter for region codes via their associated
    table.
"""
from abc import abstractmethod
from typing import List, Optional

from sqlalchemy import Table, ForeignKeyConstraint
from sqlalchemy.ext.declarative import DeclarativeMeta

from recidiviz.persistence.database.schema_utils import get_foreign_key_constraints, get_region_code_col, \
    get_table_class_by_name, schema_has_region_code_query_support, is_association_table


class SchemaTableRegionFilteredQueryBuilder:
    """Query builder for querying data from tables whose schema match our database schema, with additional functionality
    for filtering by state code(s) and hydrating a state code column on an association table by joining to a foreign key
    table.

        Usage:
            # Returns a query that selects all columns to include from table.
            QueryBuilder(metadata_base, table, columns_to_include).full_query()
            QueryBuilder(metadata_base, table, columns_to_include, region_codes_to_exclude=[]).full_query()

            # Returns a query that will return zero rows.
            QueryBuilder(metadata_base, table, columns_to_include, region_codes_to_include=[]).full_query()

            # Returns a query that will return rows matching the provided region_codes
            QueryBuilder(metadata_base, table, columns_to_include, region_codes_to_include=['US_ND']).full_query()

            # Returns a query that will return rows that do NOT match the provided region codes
            QueryBuilder(metadata_base, table, columns_to_include, region_codes_to_exclude=['US_ID']).full_query()
    """
    def __init__(self,
                 metadata_base: DeclarativeMeta,
                 table: Table,
                 columns_to_include: List[str],
                 region_codes_to_include: Optional[List[str]] = None,
                 region_codes_to_exclude: Optional[List[str]] = None):
        if region_codes_to_include is not None and region_codes_to_exclude is not None:
            raise ValueError(f'Expected only one of region_codes_to_include ([{region_codes_to_include}])'
                             f'and region_codes_to_exclude ([{region_codes_to_exclude}]) to be not None')
        self.metadata_base = metadata_base
        self.sorted_tables: List[Table] = metadata_base.metadata.sorted_tables
        self.table = table
        self.columns_to_include = columns_to_include
        self.region_codes_to_include = region_codes_to_include
        self.region_codes_to_exclude = region_codes_to_exclude

    @property
    def table_name(self) -> str:
        return self.table.name

    @property
    def excludes_all_rows(self) -> bool:
        if self.region_codes_to_include is not None and len(self.region_codes_to_include) == 0:
            return True
        return False

    @property
    def filters_by_region_codes(self) -> bool:
        return bool(self.region_codes_to_include or self.region_codes_to_exclude)

    def _get_region_code_col(self) -> Optional[str]:
        if not schema_has_region_code_query_support(self.metadata_base):
            return None
        table = self._get_region_code_table()
        return get_region_code_col(self.metadata_base, table)

    def _get_region_code_table(self) -> Table:
        if self._join_to_get_region_code():
            return self._get_association_join_table()
        return self.table

    def _get_association_foreign_key_constraint(self) -> ForeignKeyConstraint:
        foreign_key_constraints = get_foreign_key_constraints(self.table)
        constraint = foreign_key_constraints[0]
        return constraint

    def _get_association_join_table(self) -> Table:
        constraint = self._get_association_foreign_key_constraint()
        join_table = get_table_class_by_name(constraint.referred_table.name, self.sorted_tables)
        return join_table

    def _get_association_foreign_key_col(self) -> str:
        constraint = self._get_association_foreign_key_constraint()
        return constraint.column_keys[0]

    def select_clause(self) -> str:
        formatted_columns = self.format_columns_for_sql(self.columns_to_include, table_prefix=self.table_name)

        if schema_has_region_code_query_support(self.metadata_base):
            region_code_col = self._get_region_code_col()
            if region_code_col not in self.columns_to_include:
                region_code_table = self._get_region_code_table()
                formatted_columns = \
                    formatted_columns + f',{region_code_table.name}.{region_code_col} AS {region_code_col}'

        return 'SELECT {columns}'.format(columns=formatted_columns)

    @abstractmethod
    def from_clause(self) -> str:
        """The FROM clause that should be used to query from the table."""

    def join_clause(self) -> Optional[str]:
        if not self._join_to_get_region_code():
            return None
        join_table = self._get_association_join_table()
        foreign_key_col = self._get_association_foreign_key_col()
        return f'JOIN {join_table.name} ON {join_table.name}.{foreign_key_col} = {self.table_name}.{foreign_key_col}'

    @abstractmethod
    def _join_to_get_region_code(self) -> bool:
        """Returns true if this query should join against a foreign key table to get a region_code."""

    def filter_clause(self) -> Optional[str]:
        if self.excludes_all_rows:
            return "WHERE FALSE"
        if not self.filters_by_region_codes:
            return None

        region_code_col = self._get_region_code_col()
        operator = 'NOT IN' if self.region_codes_to_exclude else 'IN'
        region_codes = self.region_codes_to_exclude if self.region_codes_to_exclude else self.region_codes_to_include
        if not region_codes:
            return None
        return f'WHERE {region_code_col} {operator} ({self.format_region_codes_for_sql(region_codes)})'

    def full_query(self) -> str:
        return ' '.join(filter(None,
                               [self.select_clause(), self.from_clause(), self.join_clause(), self.filter_clause()]))

    @staticmethod
    def format_region_codes_for_sql(region_codes: List[str]) -> str:
        """Format a list of region codes to use in a SQL string
            format_region_codes_for_sql(['US_ND']) --> "'US_ND'"
            format_region_codes_for_sql(['US_ND', 'US_PA']) --> "'US_ND', 'US_PA'"
        """
        return ','.join([f"\'{region_code.upper()}\'" for region_code in region_codes])

    @staticmethod
    def format_columns_for_sql(columns: List[str], table_prefix: Optional[str] = None) -> str:
        if table_prefix:
            return ','.join(map(lambda col: f'{table_prefix}.{col}', columns))
        return ','.join(columns)


class CloudSqlSchemaTableRegionFilteredQueryBuilder(SchemaTableRegionFilteredQueryBuilder):
    """Implementation of the SchemaTableRegionFilteredQueryBuilder for querying tables in CloudSQL (i.e. Postgres)."""

    def from_clause(self) -> str:
        return f'FROM {self.table_name}'

    def _join_to_get_region_code(self) -> bool:
        return schema_has_region_code_query_support(self.metadata_base) and is_association_table(self.table_name)


class BigQuerySchemaTableRegionFilteredQueryBuilder(SchemaTableRegionFilteredQueryBuilder):
    """Implementation of the SchemaTableRegionFilteredQueryBuilder for querying schema tables in BigQuery that have been
    exported from CloudSQL.
    """

    def __init__(self,
                 project_id: str,
                 dataset_id: str,
                 metadata_base: DeclarativeMeta,
                 table: Table,
                 columns_to_include: List[str],
                 region_codes_to_include: Optional[List[str]] = None,
                 region_codes_to_exclude: Optional[List[str]] = None):
        super().__init__(metadata_base, table, columns_to_include, region_codes_to_include, region_codes_to_exclude)
        self.project_id = project_id
        self.dataset_id = dataset_id

    def from_clause(self) -> str:
        return f'FROM `{self.project_id}.{self.dataset_id}.{self.table_name}` {self.table_name}'

    def _join_to_get_region_code(self) -> bool:
        # All schema tables the in schemas that support region code fields should already have that column in the BQ
        # copy of the table.
        return False
