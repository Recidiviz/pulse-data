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
"""Tests for big_query_schema_utils"""

import unittest

from google.cloud import bigquery

from recidiviz.big_query.big_query_schema_utils import (
    diff_declared_schema_to_bq_schema,
    format_schema_diffs,
)
from recidiviz.big_query.big_query_view_column import Date, Integer, Record, String


class DiffDeclaredSchemaToBqSchemaTest(unittest.TestCase):
    """Tests for diff_declared_schema_to_bq_schema"""

    def test_no_differences(self) -> None:
        declared_schema = [
            String(name="col1", description="Column 1", mode="NULLABLE"),
            Integer(name="col2", description="Column 2", mode="NULLABLE"),
            Date(name="col3", description="Column 3", mode="NULLABLE"),
        ]

        deployed_schema = [
            bigquery.SchemaField(name="col1", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="col2", field_type="INTEGER", mode="NULLABLE"),
            bigquery.SchemaField(name="col3", field_type="DATE", mode="NULLABLE"),
        ]

        diff = diff_declared_schema_to_bq_schema(declared_schema, deployed_schema)
        self.assertEqual([], diff)

    def test_required_vs_nullable_treated_equal(self) -> None:
        """REQUIRED and NULLABLE modes should be treated as equivalent."""
        declared_schema = [
            String(name="col1", description="Column 1", mode="REQUIRED"),
        ]

        deployed_schema = [
            bigquery.SchemaField(name="col1", field_type="STRING", mode="NULLABLE"),
        ]

        diff = diff_declared_schema_to_bq_schema(declared_schema, deployed_schema)
        self.assertEqual([], diff)

    def test_missing(self) -> None:
        declared_schema = [
            String(name="col1", description="Column 1", mode="NULLABLE"),
            String(name="col2", description="Column 2", mode="NULLABLE"),
        ]

        deployed_schema = [
            bigquery.SchemaField(name="col1", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="col3", field_type="INTEGER", mode="NULLABLE"),
        ]

        diff = diff_declared_schema_to_bq_schema(declared_schema, deployed_schema)
        self.assertEqual(2, len(diff))
        self.assertEqual(
            diff,
            [
                ("-", declared_schema[1].as_schema_field()),
                ("+", deployed_schema[1]),
            ],
        )

    def test_type_mismatch(self) -> None:
        declared_schema = [
            String(name="col1", description="Column 1", mode="NULLABLE"),
        ]

        deployed_schema = [
            bigquery.SchemaField(name="col1", field_type="INTEGER", mode="NULLABLE"),
        ]

        diff = diff_declared_schema_to_bq_schema(declared_schema, deployed_schema)
        self.assertEqual(2, len(diff))
        self.assertEqual(
            diff,
            [
                ("-", declared_schema[0].as_schema_field()),
                ("+", deployed_schema[0]),
            ],
        )

    def test_record_matching_subfields(self) -> None:
        declared_schema = [
            Record(
                name="address",
                description="An address",
                mode="NULLABLE",
                fields=[
                    String(name="street", description="Street", mode="NULLABLE"),
                    Integer(name="zip_code", description="Zip", mode="NULLABLE"),
                ],
            ),
        ]

        deployed_schema = [
            bigquery.SchemaField(
                name="address",
                field_type="RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField(
                        name="street", field_type="STRING", mode="NULLABLE"
                    ),
                    bigquery.SchemaField(
                        name="zip_code", field_type="INTEGER", mode="NULLABLE"
                    ),
                ],
            ),
        ]

        diff = diff_declared_schema_to_bq_schema(declared_schema, deployed_schema)
        self.assertEqual([], diff)

    def test_record_missing_declared_subfield(self) -> None:
        declared_schema = [
            Record(
                name="address",
                description="An address",
                mode="NULLABLE",
                fields=[
                    String(name="street", description="Street", mode="NULLABLE"),
                    Integer(name="zip_code", description="Zip", mode="NULLABLE"),
                ],
            ),
        ]

        deployed_schema = [
            bigquery.SchemaField(
                name="address",
                field_type="RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField(
                        name="street", field_type="STRING", mode="NULLABLE"
                    ),
                ],
            ),
        ]

        diff = diff_declared_schema_to_bq_schema(declared_schema, deployed_schema)
        self.assertEqual(
            [("-", bigquery.SchemaField("address.zip_code", "INTEGER"))],
            diff,
        )

    def test_record_extra_deployed_subfield(self) -> None:
        declared_schema = [
            Record(
                name="address",
                description="An address",
                mode="NULLABLE",
                fields=[
                    String(name="street", description="Street", mode="NULLABLE"),
                ],
            ),
        ]

        deployed_schema = [
            bigquery.SchemaField(
                name="address",
                field_type="RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField(
                        name="street", field_type="STRING", mode="NULLABLE"
                    ),
                    bigquery.SchemaField(
                        name="zip_code", field_type="INTEGER", mode="NULLABLE"
                    ),
                ],
            ),
        ]

        diff = diff_declared_schema_to_bq_schema(declared_schema, deployed_schema)
        self.assertEqual(
            [("+", bigquery.SchemaField("address.zip_code", "INTEGER"))],
            diff,
        )

    def test_record_subfield_type_mismatch(self) -> None:
        declared_schema = [
            Record(
                name="address",
                description="An address",
                mode="NULLABLE",
                fields=[
                    String(name="zip_code", description="Zip", mode="NULLABLE"),
                ],
            ),
        ]

        deployed_schema = [
            bigquery.SchemaField(
                name="address",
                field_type="RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField(
                        name="zip_code", field_type="INTEGER", mode="NULLABLE"
                    ),
                ],
            ),
        ]

        diff = diff_declared_schema_to_bq_schema(declared_schema, deployed_schema)
        self.assertEqual(
            [
                ("-", bigquery.SchemaField("address.zip_code", "STRING")),
                ("+", bigquery.SchemaField("address.zip_code", "INTEGER")),
            ],
            diff,
        )


class FormatSchemaDiffsTest(unittest.TestCase):
    """Tests for format_schema_diffs"""

    def test_format_schema_diffs_empty(self) -> None:
        result = format_schema_diffs({})
        self.assertEqual("", result)

    def test_format_multiple_diffs(self) -> None:
        diffs = {
            "dataset.view": [
                (
                    "-",
                    bigquery.SchemaField(
                        name="col1", field_type="INTEGER", mode="NULLABLE"
                    ),
                ),
                (
                    "+",
                    bigquery.SchemaField(
                        name="col2", field_type="DATE", mode="REQUIRED"
                    ),
                ),
            ]
        }
        result = format_schema_diffs(diffs)
        expected = """
dataset.view:
  - col1 (NULLABLE INTEGER)
  + col2 (REQUIRED DATE)
""".strip()
        self.assertEqual(expected, result)
