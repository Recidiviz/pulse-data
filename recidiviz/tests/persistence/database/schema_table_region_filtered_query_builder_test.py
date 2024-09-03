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
from typing import List

import sqlalchemy
from sqlalchemy.dialects import postgresql

from recidiviz.persistence.database.base_schema import StateBase
from recidiviz.persistence.database.schema.operations.schema import OperationsBase
from recidiviz.persistence.database.schema_table_region_filtered_query_builder import (
    BigQuerySchemaTableRegionFilteredQueryBuilder,
    FederatedSchemaTableRegionFilteredQueryBuilder,
    SchemaTableRegionFilteredQueryBuilder,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.schema_utils import get_all_table_classes_in_schema


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


class BaseSchemaTableRegionFilteredQueryBuilderTest(unittest.TestCase):
    """Base test class for SchemaTableRegionFilteredQueryBuilder subclass tests."""

    def setUp(self) -> None:
        get_all_table_classes_in_schema.cache_clear()
        self.fake_operations_table = sqlalchemy.Table(
            "fake_operations_table",
            OperationsBase.metadata,
            sqlalchemy.Column("column1", sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column("region_code", sqlalchemy.String, index=True),
            sqlalchemy.Column("excluded_col", sqlalchemy.String),
        )
        self.mock_columns_to_include = ["column1", "state_code"]
        self.mock_operations_columns_to_include = ["column1", "region_code"]
        self.fake_operations_table_complex_schema = sqlalchemy.Table(
            "fake_operations_table_complex_schema",
            OperationsBase.metadata,
            sqlalchemy.Column("column1", sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column("state_code", sqlalchemy.String(length=255)),
            sqlalchemy.Column(
                "column2", sqlalchemy.ARRAY(sqlalchemy.String(length=255))
            ),
            sqlalchemy.Column(
                "column3", sqlalchemy.Enum("VAL1", "VAL2", name="my_enum")
            ),
            sqlalchemy.Column("column4", postgresql.UUID),
        )

        self.fake_operations_association_table = sqlalchemy.Table(
            "fake_operations_table_association",
            OperationsBase.metadata,
            sqlalchemy.Column(
                "column1_simple",
                sqlalchemy.Integer,
                sqlalchemy.ForeignKey("fake_operations_table.column1"),
                index=True,
            ),
            sqlalchemy.Column(
                "column1_complex",
                sqlalchemy.Integer,
                sqlalchemy.ForeignKey("fake_operations_table_complex_schema.column1"),
                index=True,
            ),
        )
        self.mock_association_table_columns_to_include = [
            c.name for c in self.fake_operations_association_table.columns
        ]

    def tearDown(self) -> None:
        OperationsBase.metadata.remove(self.fake_operations_table)
        OperationsBase.metadata.remove(self.fake_operations_table_complex_schema)
        OperationsBase.metadata.remove(self.fake_operations_association_table)
        get_all_table_classes_in_schema.cache_clear()


class CloudSqlSchemaTableRegionFilteredQueryBuilderTest(
    BaseSchemaTableRegionFilteredQueryBuilderTest
):
    """Tests for the CloudSqlSchemaTableRegionFilteredQueryBuilder class."""

    @staticmethod
    def sqlalchemy_columns(column_names: List[str]) -> List[sqlalchemy.Column]:
        return [
            sqlalchemy.Column(col_name, sqlalchemy.String(length=255))
            for col_name in column_names
        ]

    def test_select_clause_state_schema(self) -> None:
        """Given a SchemaType.STATE schema, it returns the basic select query for a table."""
        query_builder = FederatedSchemaTableRegionFilteredQueryBuilder(
            schema_type=SchemaType.OPERATIONS,
            table=self.fake_operations_table,
            columns_to_include=self.mock_operations_columns_to_include,
        )
        expected_select = f"SELECT {self.fake_operations_table.name}.column1,{self.fake_operations_table.name}.region_code"
        self.assertEqual(expected_select, query_builder.select_clause())

    def test_select_clause_state_association_table(self) -> None:
        """Given a schema with an association table, it includes the state code in the select statement."""
        query_builder = FederatedSchemaTableRegionFilteredQueryBuilder(
            schema_type=SchemaType.OPERATIONS,
            table=self.fake_operations_association_table,
            columns_to_include=self.mock_association_table_columns_to_include,
        )
        expected_select = (
            f"SELECT {self.fake_operations_association_table.name}.column1_simple,"
            f"{self.fake_operations_association_table.name}.column1_complex,"
            f"{self.fake_operations_table.name}.region_code AS region_code"
        )
        self.assertEqual(expected_select, query_builder.select_clause())

    def test_join_clause(self) -> None:
        """Given a schema with an association table, it includes a join with the foreign key."""
        query_builder = FederatedSchemaTableRegionFilteredQueryBuilder(
            schema_type=SchemaType.OPERATIONS,
            table=self.fake_operations_association_table,
            columns_to_include=self.mock_association_table_columns_to_include,
        )
        expected_join = (
            f"JOIN {self.fake_operations_table.name} ON "
            f"{self.fake_operations_table.name}.column1 = "
            f"{self.fake_operations_association_table.name}.column1_simple"
        )
        self.assertEqual(expected_join, query_builder.join_clause())

    def test_join_clause_not_association_table(self) -> None:
        """No join clause for non-association table."""
        query_builder = FederatedSchemaTableRegionFilteredQueryBuilder(
            schema_type=SchemaType.OPERATIONS,
            table=self.fake_operations_table,
            columns_to_include=self.mock_operations_columns_to_include,
        )
        self.assertEqual(None, query_builder.join_clause())

    def test_join_clause_no_region_codes_in_schema(self) -> None:
        """Given a SchemaType.OPERATIONS schema it returns None."""
        query_builder = FederatedSchemaTableRegionFilteredQueryBuilder(
            schema_type=SchemaType.OPERATIONS,
            table=self.fake_operations_table,
            columns_to_include=self.mock_operations_columns_to_include,
        )
        self.assertEqual(None, query_builder.join_clause())

    def test_filter_clause(self) -> None:
        """Given no region codes to include, it returns None."""
        query_builder = FederatedSchemaTableRegionFilteredQueryBuilder(
            schema_type=SchemaType.OPERATIONS,
            table=self.fake_operations_table,
            columns_to_include=self.mock_operations_columns_to_include,
        )
        self.assertEqual(None, query_builder.filter_clause())

    def test_full_query(self) -> None:
        """Given a table it returns a full query string."""
        query_builder = FederatedSchemaTableRegionFilteredQueryBuilder(
            schema_type=SchemaType.OPERATIONS,
            table=self.fake_operations_table,
            columns_to_include=self.mock_operations_columns_to_include,
        )
        expected_query = (
            f"SELECT {self.fake_operations_table.name}.column1,{self.fake_operations_table.name}.region_code "
            f"FROM {self.fake_operations_table.name}"
        )
        self.assertEqual(expected_query, query_builder.full_query())

    def test_full_query_region_codes_to_include_is_none(self) -> None:
        """Given the value None for region_codes_to_include, it returns a full query string that does not filter out
        any rows.
        """
        query_builder = FederatedSchemaTableRegionFilteredQueryBuilder(
            schema_type=SchemaType.OPERATIONS,
            table=self.fake_operations_table,
            columns_to_include=self.mock_operations_columns_to_include,
        )
        expected_query = (
            f"SELECT {self.fake_operations_table.name}.column1,{self.fake_operations_table.name}.region_code "
            f"FROM {self.fake_operations_table.name}"
        )
        self.assertEqual(expected_query, query_builder.full_query())

    def test_full_query_association_table(self) -> None:
        """Given an association table, it returns a full query string."""
        query_builder = FederatedSchemaTableRegionFilteredQueryBuilder(
            schema_type=SchemaType.OPERATIONS,
            table=self.fake_operations_association_table,
            columns_to_include=self.mock_association_table_columns_to_include,
        )
        expected_query = (
            f"SELECT {self.fake_operations_association_table.name}.column1_simple,"
            f"{self.fake_operations_association_table.name}.column1_complex,"
            f"{self.fake_operations_table.name}.region_code AS region_code "
            f"FROM {self.fake_operations_association_table.name} "
            f"JOIN {self.fake_operations_table.name} ON "
            f"{self.fake_operations_table.name}.column1 = "
            f"{self.fake_operations_association_table.name}.column1_simple"
        )
        self.assertEqual(expected_query, query_builder.full_query())


class BigQuerySchemaTableRegionFilteredQueryBuilderTest(
    BaseSchemaTableRegionFilteredQueryBuilderTest
):
    """Tests for the BigQuerySchemaTableRegionFilteredQueryBuilder class."""

    def setUp(self) -> None:
        super().setUp()
        self.mock_columns_to_include = ["column1", "state_code"]
        self.fake_state_table = sqlalchemy.Table(
            "fake_state_table",
            StateBase.metadata,
            sqlalchemy.Column("column1", sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column("state_code", sqlalchemy.String, index=True),
            sqlalchemy.Column("excluded_col", sqlalchemy.String),
        )
        self.fake_table_complex_schema = sqlalchemy.Table(
            "fake_state_table_complex_schema",
            StateBase.metadata,
            sqlalchemy.Column("column1", sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column("state_code", sqlalchemy.String(length=255)),
            sqlalchemy.Column(
                "column2", sqlalchemy.ARRAY(sqlalchemy.String(length=255))
            ),
            sqlalchemy.Column(
                "column3", sqlalchemy.Enum("VAL1", "VAL2", name="my_enum")
            ),
            sqlalchemy.Column("column4", postgresql.UUID),
        )

        self.fake_association_table = sqlalchemy.Table(
            "fake_state_table_association",
            StateBase.metadata,
            sqlalchemy.Column(
                "column1_simple",
                sqlalchemy.Integer,
                sqlalchemy.ForeignKey("fake_state_table.column1"),
                index=True,
            ),
            sqlalchemy.Column(
                "column1_complex",
                sqlalchemy.Integer,
                sqlalchemy.ForeignKey("fake_state_table_complex_schema.column1"),
                index=True,
            ),
        )
        self.mock_association_table_columns_to_include = [
            c.name for c in self.fake_association_table.columns
        ]

    def tearDown(self) -> None:
        StateBase.metadata.remove(self.fake_state_table)
        StateBase.metadata.remove(self.fake_table_complex_schema)
        StateBase.metadata.remove(self.fake_association_table)
        super().tearDown()

    def test_select_clause_operations_schema(self) -> None:
        """Given a SchemaType.OPERATIONS schema, it returns the basic select query for a table."""
        query_builder = BigQuerySchemaTableRegionFilteredQueryBuilder(
            "recidiviz-456",
            "my_dataset",
            SchemaType.OPERATIONS,
            self.fake_operations_table,
            self.mock_operations_columns_to_include,
        )
        expected_select = f"SELECT {self.fake_operations_table.name}.column1,{self.fake_operations_table.name}.region_code"
        self.assertEqual(expected_select, query_builder.select_clause())

    def test_from_clause_operations_schema(self) -> None:
        """Given a SchemaType.OPERATIONS schema, it returns the basic FROM query for a table."""
        query_builder = BigQuerySchemaTableRegionFilteredQueryBuilder(
            "recidiviz-456",
            "my_dataset",
            SchemaType.OPERATIONS,
            self.fake_operations_table,
            self.mock_columns_to_include,
        )
        expected_select = f"FROM `recidiviz-456.my_dataset.{self.fake_operations_table.name}` {self.fake_operations_table.name}"
        self.assertEqual(expected_select, query_builder.from_clause())

    def test_select_clause_state_schema(self) -> None:
        """Given a SchemaType.STATE schema, it returns the basic select query for a table."""
        query_builder = BigQuerySchemaTableRegionFilteredQueryBuilder(
            "recidiviz-456",
            "my_dataset",
            SchemaType.STATE,
            self.fake_state_table,
            self.mock_columns_to_include,
        )
        expected_select = f"SELECT {self.fake_state_table.name}.column1,{self.fake_state_table.name}.state_code"
        self.assertEqual(expected_select, query_builder.select_clause())

    def test_select_clause_state_association_table(self) -> None:
        """Given a schema with an association table, it includes the state_code in the select statement."""
        query_builder = BigQuerySchemaTableRegionFilteredQueryBuilder(
            "recidiviz-456",
            "my_dataset",
            SchemaType.STATE,
            self.fake_association_table,
            self.mock_association_table_columns_to_include,
        )
        expected_select = (
            f"SELECT {self.fake_association_table.name}.column1_simple,"
            f"{self.fake_association_table.name}.column1_complex,"
            f"{self.fake_association_table.name}.state_code AS state_code"
        )
        self.assertEqual(expected_select, query_builder.select_clause())

    def test_join_clause(self) -> None:
        """Given a schema with an association table, it does not join to get the region code, since the region
        code should already be hydrated in all BQ tables.
        """
        query_builder = BigQuerySchemaTableRegionFilteredQueryBuilder(
            "recidiviz-456",
            "my_dataset",
            SchemaType.STATE,
            self.fake_association_table,
            self.mock_association_table_columns_to_include,
        )
        self.assertEqual(None, query_builder.join_clause())

    def test_join_clause_not_association_table(self) -> None:
        """No join clause for non-association table."""
        query_builder = BigQuerySchemaTableRegionFilteredQueryBuilder(
            "recidiviz-456",
            "my_dataset",
            SchemaType.STATE,
            self.fake_state_table,
            self.mock_columns_to_include,
        )
        self.assertEqual(None, query_builder.join_clause())

    def test_join_clause_no_region_codes_in_schema(self) -> None:
        """Given a SchemaType.OPERATIONS schema it returns None."""
        query_builder = BigQuerySchemaTableRegionFilteredQueryBuilder(
            "recidiviz-456",
            "my_dataset",
            SchemaType.OPERATIONS,
            self.fake_operations_table,
            self.mock_columns_to_include,
        )
        self.assertEqual(None, query_builder.join_clause())

    def test_filter_clause_region_codes_to_include(self) -> None:
        """Given a SchemaType.STATE schema and included region codes, it returns a filter clause to include
        the region codes.
        """
        query_builder = BigQuerySchemaTableRegionFilteredQueryBuilder(
            "recidiviz-456",
            "my_dataset",
            SchemaType.STATE,
            self.fake_state_table,
            self.mock_columns_to_include,
            region_codes_to_include=["US_ND", "US_ID"],
        )
        expected_filter = "WHERE state_code IN ('US_ND','US_ID')"
        self.assertEqual(expected_filter, query_builder.filter_clause())

    def test_filter_clause_no_region_codes(self) -> None:
        """Given no region codes to include, it returns None."""
        query_builder = BigQuerySchemaTableRegionFilteredQueryBuilder(
            "recidiviz-456",
            "my_dataset",
            SchemaType.STATE,
            self.fake_state_table,
            self.mock_columns_to_include,
            region_codes_to_include=None,
        )
        self.assertEqual(None, query_builder.filter_clause())

    def test_filter_clause_region_codes_to_include_empty(self) -> None:
        """Given and empty list for region_codes_to_include, it returns a filter to exclude all rows."""
        query_builder = BigQuerySchemaTableRegionFilteredQueryBuilder(
            "recidiviz-456",
            "my_dataset",
            SchemaType.STATE,
            self.fake_state_table,
            self.mock_columns_to_include,
            region_codes_to_include=[],
        )
        expected_filter = "WHERE FALSE"
        self.assertEqual(expected_filter, query_builder.filter_clause())

    def test_full_query(self) -> None:
        """Given a table it returns a full query string."""
        query_builder = BigQuerySchemaTableRegionFilteredQueryBuilder(
            "recidiviz-456",
            "my_dataset",
            SchemaType.OPERATIONS,
            self.fake_operations_table,
            self.mock_operations_columns_to_include,
        )
        expected_query = (
            f"SELECT {self.fake_operations_table.name}.column1,{self.fake_operations_table.name}.region_code "
            f"FROM `recidiviz-456.my_dataset.{self.fake_operations_table.name}` {self.fake_operations_table.name}"
        )
        self.assertEqual(expected_query, query_builder.full_query())

    def test_full_query_region_codes_to_include(self) -> None:
        """Given a list of region_codes_to_include, it returns a full query string that filters for rows matching the
        the region codes to include.
        """
        query_builder = BigQuerySchemaTableRegionFilteredQueryBuilder(
            "recidiviz-456",
            "my_dataset",
            SchemaType.STATE,
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
            SchemaType.STATE,
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
            SchemaType.STATE,
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
        """Given an association table with filtered region codes, it returns a full query string."""
        query_builder = BigQuerySchemaTableRegionFilteredQueryBuilder(
            "recidiviz-456",
            "my_dataset",
            SchemaType.STATE,
            self.fake_association_table,
            self.mock_association_table_columns_to_include,
            region_codes_to_include=["US_nd"],
        )
        expected_query = (
            f"SELECT {self.fake_association_table.name}.column1_simple,"
            f"{self.fake_association_table.name}.column1_complex,"
            f"{self.fake_association_table.name}.state_code AS state_code "
            f"FROM `recidiviz-456.my_dataset.{self.fake_association_table.name}` {self.fake_association_table.name} "
            "WHERE state_code IN ('US_ND')"
        )
        self.assertEqual(expected_query, query_builder.full_query())
