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
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Set

import sqlalchemy
from more_itertools import one
from sqlalchemy import ForeignKeyConstraint, Table
from sqlalchemy.dialects import postgresql

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema_utils import (
    SchemaType,
    get_foreign_key_constraints,
    get_region_code_col,
    get_table_class_by_name,
    is_association_table,
    schema_has_region_code_query_support,
    schema_type_to_schema_base,
)


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

    def __init__(
        self,
        schema_type: SchemaType,
        table: Table,
        columns_to_include: List[str],
        region_codes_to_include: Optional[List[str]] = None,
        region_codes_to_exclude: Optional[List[str]] = None,
    ):
        if region_codes_to_include is not None and region_codes_to_exclude is not None:
            raise ValueError(
                f"Expected only one of region_codes_to_include ([{region_codes_to_include}])"
                f"and region_codes_to_exclude ([{region_codes_to_exclude}]) to be not None"
            )
        self.schema_type = schema_type
        self.metadata_base = schema_type_to_schema_base(schema_type)
        self.sorted_tables: List[Table] = self.metadata_base.metadata.sorted_tables
        self.table = table
        self.columns_to_include = columns_to_include
        self.region_codes_to_include = region_codes_to_include
        self.region_codes_to_exclude = region_codes_to_exclude

    @property
    def table_name(self) -> str:
        return self.table.name

    @property
    def excludes_all_rows(self) -> bool:
        if (
            self.region_codes_to_include is not None
            and len(self.region_codes_to_include) == 0
        ):
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
        # It doesn't actually matter which one we pick, but we sort for determinism
        # across calls and in tests.
        foreign_key_constraints = sorted(
            get_foreign_key_constraints(self.table), key=lambda c: c.referred_table.name
        )
        constraint = foreign_key_constraints[0]
        return constraint

    def _get_association_join_table(self) -> Table:
        constraint = self._get_association_foreign_key_constraint()
        join_table = get_table_class_by_name(
            constraint.referred_table.name, self.sorted_tables
        )
        return join_table

    def _get_association_join_table_col(self) -> str:
        constraint = self._get_association_foreign_key_constraint()
        return one(constraint.elements).column.name

    def _get_association_foreign_key_col(self) -> str:
        constraint = self._get_association_foreign_key_constraint()
        return constraint.column_keys[0]

    def select_clause(self) -> str:
        formatted_columns = self._formatted_columns_for_select_clause()

        if schema_has_region_code_query_support(self.metadata_base):
            region_code_col = self._get_region_code_col()
            if region_code_col not in self.columns_to_include:
                region_code_table = self._get_region_code_table()
                formatted_columns = (
                    formatted_columns
                    + f",{region_code_table.name}.{region_code_col} AS {region_code_col}"
                )

        return f"SELECT {formatted_columns}"

    @abstractmethod
    def _formatted_columns_for_select_clause(self) -> str:
        """A string columns list for use in the select clause. Should be overidden by
        subclasses.
        """

    def _unmodified_qualified_columns_for_select_clause(self) -> str:
        """Returns a comma-separated string of columns for use in the select clause with
        no modifications other than qualifying column names by the table name.

        Example:
            "state_person.full_name,state_person.birthdate,state_person.person_id"
        """
        return ",".join(
            self.qualified_column_names_map(
                self.columns_to_include, table_prefix=self.table_name
            ).values()
        )

    @abstractmethod
    def from_clause(self) -> str:
        """The FROM clause that should be used to query from the table."""

    def join_clause(self) -> Optional[str]:
        if not self._join_to_get_region_code():
            return None
        join_table = self._get_association_join_table()
        join_table_col = self._get_association_join_table_col()
        foreign_key_col = self._get_association_foreign_key_col()
        return f"JOIN {join_table.name} ON {join_table.name}.{join_table_col} = {self.table_name}.{foreign_key_col}"

    @abstractmethod
    def _join_to_get_region_code(self) -> bool:
        """Returns true if this query should join against a foreign key table to get a region_code."""

    def filter_clause(self) -> Optional[str]:
        if self.excludes_all_rows:
            return "WHERE FALSE"
        if not self.filters_by_region_codes:
            return None

        region_code_col = self._get_region_code_col()
        operator = "NOT IN" if self.region_codes_to_exclude else "IN"
        region_codes = (
            self.region_codes_to_exclude
            if self.region_codes_to_exclude
            else self.region_codes_to_include
        )
        if not region_codes:
            return None
        return f"WHERE {region_code_col} {operator} ({self.format_region_codes_for_sql(region_codes)})"

    def full_query(self) -> str:
        return " ".join(
            filter(
                None,
                [
                    self.select_clause(),
                    self.from_clause(),
                    self.join_clause(),
                    self.filter_clause(),
                ],
            )
        )

    @staticmethod
    def format_region_codes_for_sql(region_codes: List[str]) -> str:
        """Format a list of region codes to use in a SQL string
        format_region_codes_for_sql(['US_ND']) --> "'US_ND'"
        format_region_codes_for_sql(['US_ND', 'US_PA']) --> "'US_ND', 'US_PA'"
        """
        return ",".join([f"'{region_code.upper()}'" for region_code in region_codes])

    @staticmethod
    def qualified_column_names_map(
        columns: List[str], table_prefix: Optional[str] = None
    ) -> Dict[str, str]:
        if table_prefix:
            return {col: f"{table_prefix}.{col}" for col in columns}
        return {col: col for col in columns}


class BaseCloudSqlSchemaTableRegionFilteredQueryBuilder(
    SchemaTableRegionFilteredQueryBuilder, ABC
):
    """Base class for building queries that will both be run directly in CloudSQL
    (Postgres) and against CloudSQL using BigQuery federated queries.
    """

    def from_clause(self) -> str:
        return f"FROM {self.table_name}"

    def _join_to_get_region_code(self) -> bool:
        return schema_has_region_code_query_support(
            self.metadata_base
        ) and is_association_table(self.table_name)


class CloudSqlSchemaTableRegionFilteredQueryBuilder(
    BaseCloudSqlSchemaTableRegionFilteredQueryBuilder
):
    """Implementation of the SchemaTableRegionFilteredQueryBuilder for querying tables
    directly in CloudSQL (i.e. Postgres).
    """

    def _formatted_columns_for_select_clause(self) -> str:
        return self._unmodified_qualified_columns_for_select_clause()


class FederatedSchemaTableRegionFilteredQueryBuilder(
    BaseCloudSqlSchemaTableRegionFilteredQueryBuilder
):
    """Implementation of the SchemaTableRegionFilteredQueryBuilder for querying tables
    in CloudSQL using a BigQuery `EXTERNAL_QUERY` federated query. BigQuery places some
    restrictions on the output columns that we must handle when doing this type of
    query. This query also handles primary/foreign key translation in the case where
    we're querying a multi-DB schema.
    """

    def __init__(
        self,
        *,
        schema_type: SchemaType,
        table: Table,
        columns_to_include: List[str],
        region_code: Optional[str],
    ):
        super().__init__(
            schema_type=schema_type,
            table=table,
            columns_to_include=columns_to_include,
            region_codes_to_include=[region_code] if region_code else None,
        )
        should_translate_key_columns = schema_type.is_multi_db_schema
        if should_translate_key_columns and not region_code:
            raise ValueError(
                f"Can only do primary/foreign key translation for single-region queries."
                f"Schema [{schema_type}] requires translation."
            )
        self.should_translate_key_columns = should_translate_key_columns
        self.region_code = region_code

    def _key_columns_to_translate(self) -> Set[str]:
        """Returns a list of column names corresponding to columns in this table that
        are primary/foreign keys and should have a region mask applied to prevent
        conflicts when results from multiple databases are merged.
        """
        if not self.should_translate_key_columns:
            return set()

        foreign_key_columns = [fk.parent for fk in self.table.foreign_keys]
        primary_key_columns = self.table.primary_key.columns

        columns_to_translate = {*foreign_key_columns, *primary_key_columns}

        for c in columns_to_translate:
            if c.type.python_type != int:
                raise ValueError(
                    f"Can only translate integer columns! "
                    f"Found integer key column [{c.name}] in table [{self.table_name}]"
                )
            if c.autoincrement != "auto":
                raise ValueError(
                    f"Expected to only translate auto-increment key columns! "
                    f"Found non-increment column [{c.name}] in table [{self.table_name}]"
                )
        return {c.name for c in columns_to_translate}

    def _get_translated_key_column_mask(self) -> int:
        """Returns an integer mask to add to every primary/foreign key column in this
        query. The mask is stable across all tables and derived from the region code.

        Example: 46000000000000

        For the above mask, if a primary key is 123456 in Postgres, then the translated
        primary key would be 46000000123456.
        """
        if not self.region_code:
            raise ValueError(
                "Must have set region code to do primary/foreign key translation."
            )
        if not StateCode.is_state_code(self.region_code):
            raise ValueError(
                "No support yet for doing primary/foreign key translation on non-state "
                "regions."
            )
        # The FIPS code is always a two-digit code for states
        fips = int(StateCode(self.region_code).get_state().fips)
        return fips * pow(10, 12)

    def _translate_key_colunm_clause(
        self, column_name: str, qualified_column_name: str
    ) -> str:
        """Returns a string column select clause that applies the key translation
        mask to the given column.

        Example: "(46000000000000 + state_charge.person_id) AS person_id"
        """
        mask = self._get_translated_key_column_mask()
        return f"({mask} + {qualified_column_name}) AS {column_name}"

    def _formatted_columns_for_select_clause(self) -> str:
        qualified_names_map = self.qualified_column_names_map(
            self.columns_to_include, table_prefix=self.table_name
        )
        select_columns = []
        key_columns_to_translate = self._key_columns_to_translate()
        for column in self.table.columns:
            if column.name not in self.columns_to_include:
                continue
            qualified_name = qualified_names_map[column.name]
            if column.name in key_columns_to_translate:
                select_columns.append(
                    self._translate_key_colunm_clause(column.name, qualified_name)
                )
            elif isinstance(column.type, sqlalchemy.Enum):
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


class BigQuerySchemaTableRegionFilteredQueryBuilder(
    SchemaTableRegionFilteredQueryBuilder
):
    """Implementation of the SchemaTableRegionFilteredQueryBuilder for querying schema tables in BigQuery that have been
    exported from CloudSQL.
    """

    def __init__(
        self,
        project_id: str,
        dataset_id: str,
        schema_type: SchemaType,
        table: Table,
        columns_to_include: List[str],
        region_codes_to_include: Optional[List[str]] = None,
        region_codes_to_exclude: Optional[List[str]] = None,
    ):
        super().__init__(
            schema_type,
            table,
            columns_to_include,
            region_codes_to_include,
            region_codes_to_exclude,
        )
        self.project_id = project_id
        self.dataset_id = dataset_id

    def _formatted_columns_for_select_clause(self) -> str:
        return self._unmodified_qualified_columns_for_select_clause()

    def from_clause(self) -> str:
        return f"FROM `{self.project_id}.{self.dataset_id}.{self.table_name}` {self.table_name}"

    def _join_to_get_region_code(self) -> bool:
        # All schema tables the in schemas that support region code fields should already have that column in the BQ
        # copy of the table.
        return False
