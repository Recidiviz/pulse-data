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

"""Tests for the FederatedCloudSQLTableQueryBuilder class."""

import unittest
from typing import List

import sqlalchemy
from sqlalchemy.dialects import postgresql

from recidiviz.persistence.database.bq_refresh.federated_cloud_sql_table_query_builder import (
    FederatedCloudSQLTableQueryBuilder,
)
from recidiviz.persistence.database.schema.operations.schema import OperationsBase
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.schema_utils import get_all_table_classes_in_schema


class FederatedCloudSQLTableQueryBuilderTest(unittest.TestCase):
    """Tests for the FederatedCloudSQLTableQueryBuilder class."""

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

    @staticmethod
    def sqlalchemy_columns(column_names: List[str]) -> List[sqlalchemy.Column]:
        return [
            sqlalchemy.Column(col_name, sqlalchemy.String(length=255))
            for col_name in column_names
        ]

    def test_qualified_column_names_map(self) -> None:
        self.assertEqual(
            FederatedCloudSQLTableQueryBuilder.qualified_column_names_map(
                ["name", "state_code", "person_id"], table_prefix=None
            ),
            {"name": "name", "person_id": "person_id", "state_code": "state_code"},
        )
        self.assertEqual(
            FederatedCloudSQLTableQueryBuilder.qualified_column_names_map(
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
            FederatedCloudSQLTableQueryBuilder.qualified_column_names_map(
                ["name", "state_code", "person_id"], table_prefix
            ),
            expected,
        )
        self.assertEqual(
            FederatedCloudSQLTableQueryBuilder.qualified_column_names_map(
                [], table_prefix
            ),
            {},
        )

    def test_select_clause_state_schema(self) -> None:
        """Given a SchemaType.OPERATIONS schema, it returns the basic select query for a table."""
        query_builder = FederatedCloudSQLTableQueryBuilder(
            schema_type=SchemaType.OPERATIONS,
            table=self.fake_operations_table,
            columns_to_include=self.mock_operations_columns_to_include,
        )
        expected_select = f"SELECT {self.fake_operations_table.name}.column1,{self.fake_operations_table.name}.region_code"
        self.assertEqual(expected_select, query_builder.select_clause())

    def test_full_query(self) -> None:
        """Given a table it returns a full query string."""
        query_builder = FederatedCloudSQLTableQueryBuilder(
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
        query_builder = FederatedCloudSQLTableQueryBuilder(
            schema_type=SchemaType.OPERATIONS,
            table=self.fake_operations_table,
            columns_to_include=self.mock_operations_columns_to_include,
        )
        expected_query = (
            f"SELECT {self.fake_operations_table.name}.column1,{self.fake_operations_table.name}.region_code "
            f"FROM {self.fake_operations_table.name}"
        )
        self.assertEqual(expected_query, query_builder.full_query())
