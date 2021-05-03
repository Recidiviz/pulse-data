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

"""Tests for schema_table_region_filtered_query_builder.py."""

import unittest
import collections
from typing import List
from unittest import mock

import sqlalchemy

from recidiviz.persistence.database.base_schema import StateBase, JailsBase
from recidiviz.persistence.database.schema_table_region_filtered_query_builder import (
    CloudSqlSchemaTableRegionFilteredQueryBuilder,
    SchemaTableRegionFilteredQueryBuilder,
    BigQuerySchemaTableRegionFilteredQueryBuilder,
    FederatedSchemaTableRegionFilteredQueryBuilder,
)


class SchemaTableRegionFilteredQueryBuilderTest(unittest.TestCase):
    """General tests for the SchemaTableRegionFilteredQueryBuilder."""

    def setUp(self) -> None:
        self.single_state_code = ["us_va"]
        self.multiple_states = ["us_nd", "US_ID"]

    def test_format_region_codes_for_sql(self) -> None:
        """Assert that a list of region codes are formatted to a comma delimited string
        to be used for a SQL clause
        """
        self.assertEqual(
            SchemaTableRegionFilteredQueryBuilder.format_region_codes_for_sql(
                self.single_state_code
            ),
            "'US_VA'",
        )
        self.assertEqual(
            SchemaTableRegionFilteredQueryBuilder.format_region_codes_for_sql(
                self.multiple_states
            ),
            "'US_ND','US_ID'",
        )

    def test_qualified_column_names_map(self) -> None:
        self.assertEqual(
            SchemaTableRegionFilteredQueryBuilder.qualified_column_names_map(
                ["name", "state_code", "person_id"], table_prefix=None
            ),
            {"name": "name", "person_id": "person_id", "state_code": "state_code"},
        )
        self.assertEqual(
            SchemaTableRegionFilteredQueryBuilder.qualified_column_names_map(
                [], table_prefix=None
            ),
            {},
        )

    def test_qualified_column_names_map_with_prefix(self) -> None:
        table_prefix = "table_prefix"
        expected = {
            "name": "table_prefix.name",
            "person_id": "table_prefix.person_id",
            "state_code": "table_prefix.state_code",
        }
        self.assertEqual(
            SchemaTableRegionFilteredQueryBuilder.qualified_column_names_map(
                ["name", "state_code", "person_id"], table_prefix
            ),
            expected,
        )
        self.assertEqual(
            SchemaTableRegionFilteredQueryBuilder.qualified_column_names_map(
                [], table_prefix
            ),
            {},
        )


class CloudSqlSchemaTableRegionFilteredQueryBuilderTest(unittest.TestCase):
    """Tests for the CloudSqlSchemaTableRegionFilteredQueryBuilder class."""

    @staticmethod
    def sqlalchemy_columns(column_names: List[str]) -> List[sqlalchemy.Column]:
        return [
            sqlalchemy.Column(col_name, sqlalchemy.String(length=255))
            for col_name in column_names
        ]

    def setUp(self) -> None:

        self.mock_columns_to_include = ["column1", "state_code"]
        self.mock_association_table_columns_to_include = ["column1", "column2"]
        ForeignKeyConstraint = collections.namedtuple(
            "ForeignKeyConstraint", ["referred_table", "column_keys"]
        )
        Table = collections.namedtuple("Table", ["name", "columns"])
        self.fake_jails_table = Table(
            "fake_jails_table",
            self.sqlalchemy_columns([*self.mock_columns_to_include, "excluded_col"]),
        )
        self.fake_state_table = Table(
            "fake_state_table",
            self.sqlalchemy_columns([*self.mock_columns_to_include, "excluded_col"]),
        )
        self.fake_association_table = Table(
            "fake_state_table_association",
            self.sqlalchemy_columns([*self.mock_association_table_columns_to_include]),
        )

        self.fake_table_complex_schema = Table(
            "fake_complex_schema_table",
            [
                sqlalchemy.Column("state_code", sqlalchemy.String(length=255)),
                sqlalchemy.Column(
                    "column1", sqlalchemy.ARRAY(sqlalchemy.String(length=255))
                ),
                sqlalchemy.Column(
                    "column2", sqlalchemy.Enum("VAL1", "VAL2", name="my_enum")
                ),
            ],
        )

        self.foreign_key = "fake_foreign_key"
        self.get_foreign_key_constraints_patcher = mock.patch(
            "recidiviz.persistence.database.schema_table_region_filtered_query_builder"
            ".get_foreign_key_constraints"
        )
        self.get_table_class_by_name_patcher = mock.patch(
            "recidiviz.persistence.database.schema_table_region_filtered_query_builder.get_table_class_by_name"
        )
        self.get_region_code_col_patcher = mock.patch(
            "recidiviz.persistence.database.schema_table_region_filtered_query_builder.get_region_code_col"
        )
        self.get_table_class_by_name_patcher.start().return_value = (
            self.fake_state_table
        )
        self.get_region_code_col_patcher.start().return_value = "state_code"
        self.get_foreign_key_constraints_patcher.start().return_value = [
            ForeignKeyConstraint(self.fake_state_table, [self.foreign_key])
        ]

    def tearDown(self) -> None:
        self.get_table_class_by_name_patcher.stop()
        self.get_foreign_key_constraints_patcher.stop()
        self.get_region_code_col_patcher.stop()

    def test___init__(self) -> None:
        """Test that an assertion is raised if both region_codes_to_include and region_codes_to_exclude are set"""
        with self.assertRaises(ValueError):
            CloudSqlSchemaTableRegionFilteredQueryBuilder(
                StateBase,
                self.fake_jails_table,
                self.mock_columns_to_include,
                region_codes_to_include=[],
                region_codes_to_exclude=["US_ID"],
            )

    def test___init__region_codes_none(self) -> None:
        """Test that no assertion is raised if both region_codes_to_include and region_codes_to_exclude are None"""
        CloudSqlSchemaTableRegionFilteredQueryBuilder(
            StateBase,
            self.fake_jails_table,
            self.mock_columns_to_include,
            region_codes_to_include=None,
            region_codes_to_exclude=None,
        )

    def test_select_clause_jails_schema(self) -> None:
        """Given a JailsBase schema, it returns the basic select query for a table."""
        query_builder = CloudSqlSchemaTableRegionFilteredQueryBuilder(
            JailsBase, self.fake_jails_table, self.mock_columns_to_include
        )
        expected_select = f"SELECT {self.fake_jails_table.name}.column1,{self.fake_jails_table.name}.state_code"
        self.assertEqual(expected_select, query_builder.select_clause())

    def test_from_clause_jails_schema(self) -> None:
        """Given a JailsBase schema, it returns the basic FROM query for a table."""
        query_builder = CloudSqlSchemaTableRegionFilteredQueryBuilder(
            JailsBase, self.fake_jails_table, self.mock_columns_to_include
        )
        expected_select = f"FROM {self.fake_jails_table.name}"
        self.assertEqual(expected_select, query_builder.from_clause())

    def test_select_clause_state_schema(self) -> None:
        """Given a StateBase schema, it returns the basic select query for a table."""
        query_builder = CloudSqlSchemaTableRegionFilteredQueryBuilder(
            StateBase, self.fake_state_table, self.mock_columns_to_include
        )
        expected_select = f"SELECT {self.fake_state_table.name}.column1,{self.fake_state_table.name}.state_code"
        self.assertEqual(expected_select, query_builder.select_clause())

    def test_select_clause_state_association_table(self) -> None:
        """Given a StateBase schema and an association table, it includes the state_code in the select statement."""
        query_builder = CloudSqlSchemaTableRegionFilteredQueryBuilder(
            StateBase,
            self.fake_association_table,
            self.mock_association_table_columns_to_include,
        )
        expected_select = (
            f"SELECT {self.fake_association_table.name}.column1,"
            f"{self.fake_association_table.name}.column2,"
            f"{self.fake_state_table.name}.state_code AS state_code"
        )
        self.assertEqual(expected_select, query_builder.select_clause())

    def test_join_clause(self) -> None:
        """Given a StateBase schema and an association table, it includes a join with the foreign key."""
        query_builder = CloudSqlSchemaTableRegionFilteredQueryBuilder(
            StateBase,
            self.fake_association_table,
            self.mock_association_table_columns_to_include,
        )
        expected_join = (
            f"JOIN {self.fake_state_table.name} ON "
            f"{self.fake_state_table.name}.{self.foreign_key} = "
            f"{self.fake_association_table.name}.{self.foreign_key}"
        )
        self.assertEqual(expected_join, query_builder.join_clause())

    def test_join_clause_not_association_table(self) -> None:
        """No join clause for non-association table."""
        query_builder = CloudSqlSchemaTableRegionFilteredQueryBuilder(
            StateBase, self.fake_state_table, self.mock_columns_to_include
        )
        self.assertEqual(None, query_builder.join_clause())

    def test_join_clause_no_region_codes_in_schema(self) -> None:
        """Given a JailsBase schema it returns None."""
        query_builder = CloudSqlSchemaTableRegionFilteredQueryBuilder(
            JailsBase, self.fake_jails_table, self.mock_columns_to_include
        )
        self.assertEqual(None, query_builder.join_clause())

    def test_filter_clause_region_codes_to_exclude(self) -> None:
        """Given a StateBase schema and excluded region codes, it returns a
        filter clause to exclude the region codes."""
        query_builder = CloudSqlSchemaTableRegionFilteredQueryBuilder(
            StateBase,
            self.fake_state_table,
            self.mock_columns_to_include,
            region_codes_to_exclude=["US_ND", "US_ID"],
        )
        expected_filter = "WHERE state_code NOT IN ('US_ND','US_ID')"
        self.assertEqual(expected_filter, query_builder.filter_clause())

    def test_filter_clause_region_codes_to_exclude_empty(self) -> None:
        """Given an empty list for excluded region codes, it returns None."""
        query_builder = CloudSqlSchemaTableRegionFilteredQueryBuilder(
            StateBase,
            self.fake_state_table,
            self.mock_columns_to_include,
            region_codes_to_exclude=[],
        )
        self.assertEqual(None, query_builder.filter_clause())

    def test_filter_clause_region_codes_to_include(self) -> None:
        """Given a StateBase schema and included region codes, it returns a filter clause to include
        the region codes.
        """
        query_builder = CloudSqlSchemaTableRegionFilteredQueryBuilder(
            StateBase,
            self.fake_state_table,
            self.mock_columns_to_include,
            region_codes_to_include=["US_ND", "US_ID"],
        )
        expected_filter = "WHERE state_code IN ('US_ND','US_ID')"
        self.assertEqual(expected_filter, query_builder.filter_clause())

    def test_filter_clause_no_region_codes(self) -> None:
        """Given no region codes to include or exclude, it returns None."""
        query_builder = CloudSqlSchemaTableRegionFilteredQueryBuilder(
            StateBase,
            self.fake_state_table,
            self.mock_columns_to_include,
            region_codes_to_include=None,
            region_codes_to_exclude=None,
        )
        self.assertEqual(None, query_builder.filter_clause())

    def test_filter_clause_region_codes_to_include_empty(self) -> None:
        """Given and empty list for region_codes_to_include, it returns a filter to exclude all rows."""
        query_builder = CloudSqlSchemaTableRegionFilteredQueryBuilder(
            StateBase,
            self.fake_state_table,
            self.mock_columns_to_include,
            region_codes_to_include=[],
            region_codes_to_exclude=None,
        )
        expected_filter = "WHERE FALSE"
        self.assertEqual(expected_filter, query_builder.filter_clause())

    def test_full_query(self) -> None:
        """Given a table it returns a full query string."""
        query_builder = CloudSqlSchemaTableRegionFilteredQueryBuilder(
            JailsBase, self.fake_jails_table, self.mock_columns_to_include
        )
        expected_query = (
            f"SELECT {self.fake_jails_table.name}.column1,{self.fake_jails_table.name}.state_code "
            f"FROM {self.fake_jails_table.name}"
        )
        self.assertEqual(expected_query, query_builder.full_query())

    def test_full_query_federated(self) -> None:
        """Given a table it returns a full query string."""
        query_builder = FederatedSchemaTableRegionFilteredQueryBuilder(
            JailsBase, self.fake_jails_table, self.mock_columns_to_include
        )
        expected_query = (
            f"SELECT {self.fake_jails_table.name}.column1,{self.fake_jails_table.name}.state_code "
            f"FROM {self.fake_jails_table.name}"
        )
        self.assertEqual(expected_query, query_builder.full_query())

    def test_full_query_federated_complex_schema(self) -> None:
        """Given a table it returns a full query string."""
        query_builder = FederatedSchemaTableRegionFilteredQueryBuilder(
            JailsBase,
            self.fake_table_complex_schema,
            [c.name for c in self.fake_table_complex_schema.columns],
        )
        expected_query = (
            f"SELECT {self.fake_table_complex_schema.name}.state_code,"
            f"ARRAY_REPLACE({self.fake_table_complex_schema.name}.column1, NULL, '') "
            f"as column1,"
            f"CAST({self.fake_table_complex_schema.name}.column2 as VARCHAR) "
            f"FROM {self.fake_table_complex_schema.name}"
        )
        self.assertEqual(expected_query, query_builder.full_query())

    def test_full_query_region_codes_to_exclude(self) -> None:
        """Given a list of region_codes_to_excludes, it returns a full query string that filters out the excluded
        region codes.
        """
        query_builder = CloudSqlSchemaTableRegionFilteredQueryBuilder(
            StateBase,
            self.fake_state_table,
            self.mock_columns_to_include,
            region_codes_to_exclude=["US_nd"],
        )
        expected_query = (
            f"SELECT {self.fake_state_table.name}.column1,{self.fake_state_table.name}.state_code "
            f"FROM {self.fake_state_table.name} "
            "WHERE state_code NOT IN ('US_ND')"
        )
        self.assertEqual(expected_query, query_builder.full_query())

    def test_full_query_region_codes_to_exclude_is_empty(self) -> None:
        """Given an empty list for region_codes_to_exclude, it returns a full query string that does not
        filter out anything.
        """
        query_builder = CloudSqlSchemaTableRegionFilteredQueryBuilder(
            StateBase,
            self.fake_state_table,
            self.mock_columns_to_include,
            region_codes_to_exclude=[],
        )
        expected_query = (
            f"SELECT {self.fake_state_table.name}.column1,{self.fake_state_table.name}.state_code "
            f"FROM {self.fake_state_table.name}"
        )
        self.assertEqual(expected_query, query_builder.full_query())

    def test_full_query_region_codes_to_exclude_is_none(self) -> None:
        """Given the value None for region_codes_to_exclude, it returns a full query string that does not
        filter out anything.
        """
        query_builder = CloudSqlSchemaTableRegionFilteredQueryBuilder(
            StateBase,
            self.fake_state_table,
            self.mock_columns_to_include,
            region_codes_to_exclude=None,
        )
        expected_query = (
            f"SELECT {self.fake_state_table.name}.column1,{self.fake_state_table.name}.state_code "
            f"FROM {self.fake_state_table.name}"
        )
        self.assertEqual(expected_query, query_builder.full_query())

    def test_full_query_region_codes_to_include(self) -> None:
        """Given a list of region_codes_to_include, it returns a full query string that filters for rows matching the
        the region codes to include.
        """
        query_builder = CloudSqlSchemaTableRegionFilteredQueryBuilder(
            StateBase,
            self.fake_state_table,
            self.mock_columns_to_include,
            region_codes_to_include=["US_ND"],
        )
        expected_query = (
            f"SELECT {self.fake_state_table.name}.column1,{self.fake_state_table.name}.state_code "
            f"FROM {self.fake_state_table.name} WHERE state_code IN ('US_ND')"
        )
        self.assertEqual(expected_query, query_builder.full_query())

    def test_full_query_region_codes_to_include_is_empty(self) -> None:
        """Given an empty list of region_codes_to_include, it returns a full query string that returns zero rows."""
        query_builder = CloudSqlSchemaTableRegionFilteredQueryBuilder(
            StateBase,
            self.fake_state_table,
            self.mock_columns_to_include,
            region_codes_to_include=[],
        )
        expected_query = (
            f"SELECT {self.fake_state_table.name}.column1,{self.fake_state_table.name}.state_code "
            f"FROM {self.fake_state_table.name} WHERE FALSE"
        )
        self.assertEqual(expected_query, query_builder.full_query())

    def test_full_query_region_codes_to_include_is_none(self) -> None:
        """Given the value None for region_codes_to_include, it returns a full query string that does not filter out
        any rows.
        """
        query_builder = CloudSqlSchemaTableRegionFilteredQueryBuilder(
            StateBase,
            self.fake_state_table,
            self.mock_columns_to_include,
            region_codes_to_include=None,
        )
        expected_query = (
            f"SELECT {self.fake_state_table.name}.column1,{self.fake_state_table.name}.state_code "
            f"FROM {self.fake_state_table.name}"
        )
        self.assertEqual(expected_query, query_builder.full_query())

    def test_full_query_association_table(self) -> None:
        """Given an association table with excluded region codes, it returns a full query string."""
        query_builder = CloudSqlSchemaTableRegionFilteredQueryBuilder(
            StateBase,
            self.fake_association_table,
            self.mock_association_table_columns_to_include,
            region_codes_to_exclude=["US_nd"],
        )
        expected_query = (
            f"SELECT {self.fake_association_table.name}.column1,"
            f"{self.fake_association_table.name}.column2,"
            f"{self.fake_state_table.name}.state_code AS state_code "
            f"FROM {self.fake_association_table.name} "
            f"JOIN {self.fake_state_table.name} ON "
            f"{self.fake_state_table.name}.{self.foreign_key} = "
            f"{self.fake_association_table.name}.{self.foreign_key} "
            "WHERE state_code NOT IN ('US_ND')"
        )
        self.assertEqual(expected_query, query_builder.full_query())


class BigQuerySchemaTableRegionFilteredQueryBuilderTest(unittest.TestCase):
    """Tests for the BigQuerySchemaTableRegionFilteredQueryBuilder class."""

    def setUp(self) -> None:

        self.mock_columns_to_include = ["column1", "state_code"]
        self.mock_association_table_columns_to_include = ["column1", "column2"]
        ForeignKeyConstraint = collections.namedtuple(
            "ForeignKeyConstraint", ["referred_table", "column_keys"]
        )
        Table = collections.namedtuple("Table", ["name"])
        self.fake_jails_table = Table("fake_jails_table")
        self.fake_state_table = Table("fake_state_table")
        self.fake_association_table = Table("fake_state_table_association")
        self.foreign_key = "fake_foreign_key"
        self.get_foreign_key_constraints_patcher = mock.patch(
            "recidiviz.persistence.database.schema_table_region_filtered_query_builder"
            ".get_foreign_key_constraints"
        )
        self.get_table_class_by_name_patcher = mock.patch(
            "recidiviz.persistence.database.schema_table_region_filtered_query_builder.get_table_class_by_name"
        )
        self.get_region_code_col_patcher = mock.patch(
            "recidiviz.persistence.database.schema_table_region_filtered_query_builder.get_region_code_col"
        )
        self.get_table_class_by_name_patcher.start().return_value = (
            self.fake_state_table
        )
        self.get_region_code_col_patcher.start().return_value = "state_code"
        self.get_foreign_key_constraints_patcher.start().return_value = [
            ForeignKeyConstraint(Table(self.fake_state_table), [self.foreign_key])
        ]

    def tearDown(self) -> None:
        self.get_table_class_by_name_patcher.stop()
        self.get_foreign_key_constraints_patcher.stop()
        self.get_region_code_col_patcher.stop()

    def test___init__(self) -> None:
        """Test that an assertion is raised if both region_codes_to_include and region_codes_to_exclude are set"""
        with self.assertRaises(ValueError):
            BigQuerySchemaTableRegionFilteredQueryBuilder(
                "recidiviz-456",
                "my_dataset",
                StateBase,
                self.fake_jails_table,
                self.mock_columns_to_include,
                region_codes_to_include=[],
                region_codes_to_exclude=["US_ID"],
            )

    def test___init__region_codes_none(self) -> None:
        """Test that no assertion is raised if both region_codes_to_include and region_codes_to_exclude are None"""
        BigQuerySchemaTableRegionFilteredQueryBuilder(
            "recidiviz-456",
            "my_dataset",
            StateBase,
            self.fake_jails_table,
            self.mock_columns_to_include,
            region_codes_to_include=None,
            region_codes_to_exclude=None,
        )

    def test_select_clause_jails_schema(self) -> None:
        """Given a JailsBase schema, it returns the basic select query for a table."""
        query_builder = BigQuerySchemaTableRegionFilteredQueryBuilder(
            "recidiviz-456",
            "my_dataset",
            JailsBase,
            self.fake_jails_table,
            self.mock_columns_to_include,
        )
        expected_select = f"SELECT {self.fake_jails_table.name}.column1,{self.fake_jails_table.name}.state_code"
        self.assertEqual(expected_select, query_builder.select_clause())

    def test_from_clause_jails_schema(self) -> None:
        """Given a JailsBase schema, it returns the basic FROM query for a table."""
        query_builder = BigQuerySchemaTableRegionFilteredQueryBuilder(
            "recidiviz-456",
            "my_dataset",
            JailsBase,
            self.fake_jails_table,
            self.mock_columns_to_include,
        )
        expected_select = f"FROM `recidiviz-456.my_dataset.{self.fake_jails_table.name}` {self.fake_jails_table.name}"
        self.assertEqual(expected_select, query_builder.from_clause())

    def test_select_clause_state_schema(self) -> None:
        """Given a StateBase schema, it returns the basic select query for a table."""
        query_builder = BigQuerySchemaTableRegionFilteredQueryBuilder(
            "recidiviz-456",
            "my_dataset",
            StateBase,
            self.fake_state_table,
            self.mock_columns_to_include,
        )
        expected_select = f"SELECT {self.fake_state_table.name}.column1,{self.fake_state_table.name}.state_code"
        self.assertEqual(expected_select, query_builder.select_clause())

    def test_select_clause_state_association_table(self) -> None:
        """Given a StateBase schema and an association table, it includes the state_code in the select statement."""
        query_builder = BigQuerySchemaTableRegionFilteredQueryBuilder(
            "recidiviz-456",
            "my_dataset",
            StateBase,
            self.fake_association_table,
            self.mock_association_table_columns_to_include,
        )
        expected_select = (
            f"SELECT {self.fake_association_table.name}.column1,"
            f"{self.fake_association_table.name}.column2,"
            f"{self.fake_association_table.name}.state_code AS state_code"
        )
        self.assertEqual(expected_select, query_builder.select_clause())

    def test_join_clause(self) -> None:
        """Given a StateBase schema and an association table, it does not join to get the region code, since the region
        code should already be hydrated in all BQ tables.
        """
        query_builder = BigQuerySchemaTableRegionFilteredQueryBuilder(
            "recidiviz-456",
            "my_dataset",
            StateBase,
            self.fake_association_table,
            self.mock_association_table_columns_to_include,
        )
        self.assertEqual(None, query_builder.join_clause())

    def test_join_clause_not_association_table(self) -> None:
        """No join clause for non-association table."""
        query_builder = BigQuerySchemaTableRegionFilteredQueryBuilder(
            "recidiviz-456",
            "my_dataset",
            StateBase,
            self.fake_state_table,
            self.mock_columns_to_include,
        )
        self.assertEqual(None, query_builder.join_clause())

    def test_join_clause_no_region_codes_in_schema(self) -> None:
        """Given a JailsBase schema it returns None."""
        query_builder = BigQuerySchemaTableRegionFilteredQueryBuilder(
            "recidiviz-456",
            "my_dataset",
            JailsBase,
            self.fake_jails_table,
            self.mock_columns_to_include,
        )
        self.assertEqual(None, query_builder.join_clause())

    def test_filter_clause_region_codes_to_exclude(self) -> None:
        """Given a StateBase schema and excluded region codes, it returns a
        filter clause to exclude the region codes."""
        query_builder = BigQuerySchemaTableRegionFilteredQueryBuilder(
            "recidiviz-456",
            "my_dataset",
            StateBase,
            self.fake_state_table,
            self.mock_columns_to_include,
            region_codes_to_exclude=["US_ND", "US_ID"],
        )
        expected_filter = "WHERE state_code NOT IN ('US_ND','US_ID')"
        self.assertEqual(expected_filter, query_builder.filter_clause())

    def test_filter_clause_region_codes_to_exclude_empty(self) -> None:
        """Given an empty list for excluded region codes, it returns None."""
        query_builder = BigQuerySchemaTableRegionFilteredQueryBuilder(
            "recidiviz-456",
            "my_dataset",
            StateBase,
            self.fake_state_table,
            self.mock_columns_to_include,
            region_codes_to_exclude=[],
        )
        self.assertEqual(None, query_builder.filter_clause())

    def test_filter_clause_region_codes_to_include(self) -> None:
        """Given a StateBase schema and included region codes, it returns a filter clause to include
        the region codes.
        """
        query_builder = BigQuerySchemaTableRegionFilteredQueryBuilder(
            "recidiviz-456",
            "my_dataset",
            StateBase,
            self.fake_state_table,
            self.mock_columns_to_include,
            region_codes_to_include=["US_ND", "US_ID"],
        )
        expected_filter = "WHERE state_code IN ('US_ND','US_ID')"
        self.assertEqual(expected_filter, query_builder.filter_clause())

    def test_filter_clause_no_region_codes(self) -> None:
        """Given no region codes to include or exclude, it returns None."""
        query_builder = BigQuerySchemaTableRegionFilteredQueryBuilder(
            "recidiviz-456",
            "my_dataset",
            StateBase,
            self.fake_state_table,
            self.mock_columns_to_include,
            region_codes_to_include=None,
            region_codes_to_exclude=None,
        )
        self.assertEqual(None, query_builder.filter_clause())

    def test_filter_clause_region_codes_to_include_empty(self) -> None:
        """Given and empty list for region_codes_to_include, it returns a filter to exclude all rows."""
        query_builder = BigQuerySchemaTableRegionFilteredQueryBuilder(
            "recidiviz-456",
            "my_dataset",
            StateBase,
            self.fake_state_table,
            self.mock_columns_to_include,
            region_codes_to_include=[],
            region_codes_to_exclude=None,
        )
        expected_filter = "WHERE FALSE"
        self.assertEqual(expected_filter, query_builder.filter_clause())

    def test_full_query(self) -> None:
        """Given a table it returns a full query string."""
        query_builder = BigQuerySchemaTableRegionFilteredQueryBuilder(
            "recidiviz-456",
            "my_dataset",
            JailsBase,
            self.fake_jails_table,
            self.mock_columns_to_include,
        )
        expected_query = (
            f"SELECT {self.fake_jails_table.name}.column1,{self.fake_jails_table.name}.state_code "
            f"FROM `recidiviz-456.my_dataset.{self.fake_jails_table.name}` {self.fake_jails_table.name}"
        )
        self.assertEqual(expected_query, query_builder.full_query())

    def test_full_query_region_codes_to_exclude(self) -> None:
        """Given a list of region_codes_to_excludes, it returns a full query string that filters out the excluded
        region codes.
        """
        query_builder = BigQuerySchemaTableRegionFilteredQueryBuilder(
            "recidiviz-456",
            "my_dataset",
            StateBase,
            self.fake_state_table,
            self.mock_columns_to_include,
            region_codes_to_exclude=["US_nd"],
        )
        expected_query = (
            f"SELECT {self.fake_state_table.name}.column1,{self.fake_state_table.name}.state_code "
            f"FROM `recidiviz-456.my_dataset.{self.fake_state_table.name}` {self.fake_state_table.name} "
            "WHERE state_code NOT IN ('US_ND')"
        )
        self.assertEqual(expected_query, query_builder.full_query())

    def test_full_query_region_codes_to_exclude_is_empty(self) -> None:
        """Given an empty list for region_codes_to_exclude, it returns a full query string that does not
        filter out anything.
        """
        query_builder = BigQuerySchemaTableRegionFilteredQueryBuilder(
            "recidiviz-456",
            "my_dataset",
            StateBase,
            self.fake_state_table,
            self.mock_columns_to_include,
            region_codes_to_exclude=[],
        )
        expected_query = (
            f"SELECT {self.fake_state_table.name}.column1,{self.fake_state_table.name}.state_code "
            f"FROM `recidiviz-456.my_dataset.{self.fake_state_table.name}` {self.fake_state_table.name}"
        )
        self.assertEqual(expected_query, query_builder.full_query())

    def test_full_query_region_codes_to_exclude_is_none(self) -> None:
        """Given the value None for region_codes_to_exclude, it returns a full query string that does not
        filter out anything.
        """
        query_builder = BigQuerySchemaTableRegionFilteredQueryBuilder(
            "recidiviz-456",
            "my_dataset",
            StateBase,
            self.fake_state_table,
            self.mock_columns_to_include,
            region_codes_to_exclude=None,
        )
        expected_query = (
            f"SELECT {self.fake_state_table.name}.column1,{self.fake_state_table.name}.state_code "
            f"FROM `recidiviz-456.my_dataset.{self.fake_state_table.name}` {self.fake_state_table.name}"
        )
        self.assertEqual(expected_query, query_builder.full_query())

    def test_full_query_region_codes_to_include(self) -> None:
        """Given a list of region_codes_to_include, it returns a full query string that filters for rows matching the
        the region codes to include.
        """
        query_builder = BigQuerySchemaTableRegionFilteredQueryBuilder(
            "recidiviz-456",
            "my_dataset",
            StateBase,
            self.fake_state_table,
            self.mock_columns_to_include,
            region_codes_to_include=["US_ND"],
        )
        expected_query = (
            f"SELECT {self.fake_state_table.name}.column1,{self.fake_state_table.name}.state_code "
            f"FROM `recidiviz-456.my_dataset.{self.fake_state_table.name}` {self.fake_state_table.name}"
            f" WHERE state_code IN ('US_ND')"
        )
        self.assertEqual(expected_query, query_builder.full_query())

    def test_full_query_region_codes_to_include_is_empty(self) -> None:
        """Given an empty list of region_codes_to_include, it returns a full query string that returns zero rows."""
        query_builder = BigQuerySchemaTableRegionFilteredQueryBuilder(
            "recidiviz-456",
            "my_dataset",
            StateBase,
            self.fake_state_table,
            self.mock_columns_to_include,
            region_codes_to_include=[],
        )
        expected_query = (
            f"SELECT {self.fake_state_table.name}.column1,{self.fake_state_table.name}.state_code "
            f"FROM `recidiviz-456.my_dataset.{self.fake_state_table.name}` {self.fake_state_table.name} "
            f"WHERE FALSE"
        )
        self.assertEqual(expected_query, query_builder.full_query())

    def test_full_query_region_codes_to_include_is_none(self) -> None:
        """Given the value None for region_codes_to_include, it returns a full query string that does not filter out
        any rows.
        """
        query_builder = BigQuerySchemaTableRegionFilteredQueryBuilder(
            "recidiviz-456",
            "my_dataset",
            StateBase,
            self.fake_state_table,
            self.mock_columns_to_include,
            region_codes_to_include=None,
        )
        expected_query = (
            f"SELECT {self.fake_state_table.name}.column1,{self.fake_state_table.name}.state_code "
            f"FROM `recidiviz-456.my_dataset.{self.fake_state_table.name}` {self.fake_state_table.name}"
        )
        self.assertEqual(expected_query, query_builder.full_query())

    def test_full_query_association_table(self) -> None:
        """Given an association table with excluded region codes, it returns a full query string."""
        query_builder = BigQuerySchemaTableRegionFilteredQueryBuilder(
            "recidiviz-456",
            "my_dataset",
            StateBase,
            self.fake_association_table,
            self.mock_association_table_columns_to_include,
            region_codes_to_exclude=["US_nd"],
        )
        expected_query = (
            f"SELECT {self.fake_association_table.name}.column1,"
            f"{self.fake_association_table.name}.column2,"
            f"{self.fake_association_table.name}.state_code AS state_code "
            f"FROM `recidiviz-456.my_dataset.{self.fake_association_table.name}` {self.fake_association_table.name} "
            "WHERE state_code NOT IN ('US_ND')"
        )
        self.assertEqual(expected_query, query_builder.full_query())
