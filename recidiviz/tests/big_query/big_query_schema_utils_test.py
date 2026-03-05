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
from parameterized import parameterized

from recidiviz.big_query.big_query_schema_utils import (
    diff_declared_schema_to_bq_schema,
    format_schema_diffs,
    schema_field_to_view_column,
    truncate_column_description_for_big_query,
)
from recidiviz.big_query.big_query_view_column import (
    COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
    BigQueryViewColumn,
    Bool,
    Date,
    DateTime,
    Float,
    Integer,
    Json,
    Record,
    String,
    Time,
    Timestamp,
)


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

    def test_different_field_order(self) -> None:
        """Fields in different order should produce a diff."""
        declared_schema = [
            String(name="col1", description="Column 1", mode="NULLABLE"),
            Integer(name="col2", description="Column 2", mode="NULLABLE"),
        ]

        deployed_schema = [
            bigquery.SchemaField(name="col2", field_type="INTEGER", mode="NULLABLE"),
            bigquery.SchemaField(name="col1", field_type="STRING", mode="NULLABLE"),
        ]

        diff = diff_declared_schema_to_bq_schema(declared_schema, deployed_schema)
        self.assertEqual(
            [
                ("-", declared_schema[0].as_schema_field()),
                ("+", deployed_schema[0]),
                ("-", declared_schema[1].as_schema_field()),
                ("+", deployed_schema[1]),
            ],
            diff,
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


class TruncateColumnDescriptionForBigQueryTest(unittest.TestCase):
    """Tests for truncate_column_description_for_big_query"""

    def test_short_description_unchanged(self) -> None:
        self.assertEqual("short", truncate_column_description_for_big_query("short"))

    def test_exactly_1024_chars_unchanged(self) -> None:
        desc = "x" * 1024
        self.assertEqual(desc, truncate_column_description_for_big_query(desc))

    def test_over_1024_chars_truncated(self) -> None:
        desc = "x" * 2000
        result = truncate_column_description_for_big_query(desc)
        self.assertEqual(1024, len(result))
        self.assertTrue(result.endswith(" ... (truncated)"))


class SchemaFieldToViewColumnTest(unittest.TestCase):
    """Tests for schema_field_to_view_column"""

    @parameterized.expand(
        [
            ("STRING", String, bigquery.SqlTypeNames.STRING),
            ("INTEGER", Integer, bigquery.SqlTypeNames.INTEGER),
            ("DATE", Date, bigquery.SqlTypeNames.DATE),
            ("FLOAT", Float, bigquery.SqlTypeNames.FLOAT),
            ("BOOLEAN", Bool, bigquery.SqlTypeNames.BOOLEAN),
            ("DATETIME", DateTime, bigquery.SqlTypeNames.DATETIME),
            ("TIMESTAMP", Timestamp, bigquery.SqlTypeNames.TIMESTAMP),
            ("TIME", Time, bigquery.SqlTypeNames.TIME),
            ("JSON", Json, bigquery.StandardSqlTypeNames.JSON),
            # Aliases
            ("INT64", Integer, bigquery.SqlTypeNames.INTEGER),
            ("FLOAT64", Float, bigquery.SqlTypeNames.FLOAT),
            ("BOOL", Bool, bigquery.SqlTypeNames.BOOLEAN),
            ("NUMERIC", Float, bigquery.SqlTypeNames.FLOAT),
        ]
    )
    def test_type_mapping(
        self,
        bq_type: str,
        expected_class: type[BigQueryViewColumn],
        expected_field_type: bigquery.SqlTypeNames | bigquery.StandardSqlTypeNames,
    ) -> None:
        col = schema_field_to_view_column(
            bigquery.SchemaField("x", bq_type, mode="NULLABLE", description="desc")
        )
        self.assertIsInstance(col, expected_class)
        self.assertEqual(col.field_type, expected_field_type)

    def test_mode_preserved(self) -> None:
        col = schema_field_to_view_column(
            bigquery.SchemaField("x", "STRING", mode="REQUIRED", description="desc")
        )
        self.assertEqual(col.mode, "REQUIRED")

    def test_empty_description_uses_placeholder(self) -> None:
        col = schema_field_to_view_column(
            bigquery.SchemaField("col", "STRING", mode="NULLABLE")
        )
        self.assertEqual(col.description, COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT)

    def test_description_preserved(self) -> None:
        col = schema_field_to_view_column(
            bigquery.SchemaField(
                "col", "STRING", mode="NULLABLE", description="my description"
            )
        )
        self.assertEqual(col.description, "my description")

    def test_unsupported_type_raises(self) -> None:
        with self.assertRaises(ValueError):
            schema_field_to_view_column(
                bigquery.SchemaField("x", "RECORD", description="d")
            )
