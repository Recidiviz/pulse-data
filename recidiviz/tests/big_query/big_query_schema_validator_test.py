# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Tests for BigQuerySchemaValidator."""
import unittest

from google.cloud import bigquery

from recidiviz.big_query.big_query_schema_validator import BigQuerySchemaValidator


class TestBigQuerySchemaValidator(unittest.TestCase):
    """Tests for BigQuerySchemaValidator."""

    def setUp(self) -> None:
        self.schema_map = {
            "table1": [
                bigquery.SchemaField("field1", "STRING"),
                bigquery.SchemaField("field2", "INTEGER"),
            ],
            "table2": [
                bigquery.SchemaField("field3", "FLOAT"),
                bigquery.SchemaField("field4", "BOOLEAN"),
            ],
        }
        self.validator = BigQuerySchemaValidator(self.schema_map)

    def test_validate_table_contains_field_valid(self) -> None:
        self.assertEqual(
            self.validator.validate_table_contains_field("table1", "field1"), "field1"
        )

    def test_validate_table_contains_field_invalid_table(self) -> None:
        with self.assertRaises(AttributeError) as context:
            self.validator.validate_table_contains_field("invalid_table", "field1")
        self.assertIn(
            "Table [invalid_table] not found in schema.", str(context.exception)
        )

    def test_validate_table_contains_field_invalid_field(self) -> None:
        with self.assertRaises(AttributeError) as context:
            self.validator.validate_table_contains_field("table1", "invalid_field")
        self.assertIn(
            "Field [invalid_field] not found in [table1].",
            str(context.exception),
        )

    def test_validate_table_contains_fields_valid(self) -> None:
        self.assertEqual(
            self.validator.validate_table_contains_fields(
                "table1", ["field1", "field2"]
            ),
            ["field1", "field2"],
        )

    def test_validate_table_contains_fields_invalid(self) -> None:
        with self.assertRaises(AttributeError) as context:
            self.validator.validate_table_contains_fields(
                "table1", ["field1", "invalid_field", "invalid_field2"]
            )
        self.assertIn(
            "Field [invalid_field] not found in [table1].",
            str(context.exception),
        )
        self.assertIn(
            "Field [invalid_field2] not found in [table1].",
            str(context.exception),
        )
