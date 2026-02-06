# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Tests for BigQueryViewColumn"""

import unittest

from google.cloud import bigquery

from recidiviz.big_query.big_query_view_column import Integer, Json, Record, String


class BigQueryViewColumnTest(unittest.TestCase):
    """Tests for BigQueryViewColumn"""

    def test_as_schema_field(self) -> None:
        """Tests as_schema_field method of BigQueryViewColumn"""

        column = String(
            name="test_column",
            description="A test column",
            mode="REQUIRED",
        )

        schema_field = column.as_schema_field()

        self.assertEqual(schema_field.name, "test_column")
        self.assertEqual(schema_field.description, "A test column")
        self.assertEqual(schema_field.field_type, "STRING")
        self.assertEqual(schema_field.mode, "REQUIRED")

    def test_matches_bq_field_exact_match(self) -> None:
        column = String(
            name="test_column",
            description="A test column",
            mode="NULLABLE",
        )

        schema_field = bigquery.SchemaField(
            name="test_column",
            field_type="STRING",
            mode="NULLABLE",
        )

        self.assertTrue(column.matches_bq_field(schema_field))

    def test_matches_deployed_field_type(self) -> None:
        column = Integer(
            name="test_column",
            description="A test column",
            mode="NULLABLE",
        )
        schema_field = bigquery.SchemaField(
            name="test_column",
            field_type="INTEGER",
            mode="NULLABLE",
        )
        self.assertTrue(column.matches_bq_field(schema_field))

    def test_matches_bq_field_required_vs_nullable(self) -> None:
        """REQUIRED and NULLABLE are treated as equivalent since views produce NULLABLE."""
        column = String(
            name="test_column",
            description="A test column",
            mode="REQUIRED",
        )

        schema_field = bigquery.SchemaField(
            name="test_column",
            field_type="STRING",
            mode="NULLABLE",
        )

        self.assertTrue(column.matches_bq_field(schema_field))

    def test_matches_bq_field_different_name(self) -> None:
        column = String(
            name="test_column",
            description="A test column",
            mode="NULLABLE",
        )

        schema_field = bigquery.SchemaField(
            name="other_column",
            field_type="STRING",
            mode="NULLABLE",
        )

        self.assertFalse(column.matches_bq_field(schema_field))

    def test_matches_bq_field_different_type(self) -> None:
        column = String(
            name="test_column",
            description="A test column",
            mode="NULLABLE",
        )

        schema_field = bigquery.SchemaField(
            name="test_column",
            field_type="INTEGER",
            mode="NULLABLE",
        )

        self.assertFalse(column.matches_bq_field(schema_field))

    def test_matches_bq_field_different_mode_repeated(self) -> None:
        """REPEATED mode should not match NULLABLE."""
        column = String(
            name="test_column",
            description="A test column",
            mode="REPEATED",
        )

        schema_field = bigquery.SchemaField(
            name="test_column",
            field_type="STRING",
            mode="NULLABLE",
        )

        self.assertFalse(column.matches_bq_field(schema_field))

    def test_json_as_schema_field(self) -> None:
        column = Json(
            name="metadata",
            description="JSON metadata",
            mode="NULLABLE",
        )
        schema_field = column.as_schema_field()
        self.assertEqual(schema_field.name, "metadata")
        self.assertEqual(schema_field.field_type, "JSON")

    def test_json_matches_bq_field(self) -> None:
        column = Json(
            name="metadata",
            description="JSON metadata",
            mode="NULLABLE",
        )
        schema_field = bigquery.SchemaField(
            name="metadata",
            field_type="JSON",
            mode="NULLABLE",
        )
        self.assertTrue(column.matches_bq_field(schema_field))


class RecordColumnTest(unittest.TestCase):
    """Tests for Record BigQueryViewColumn"""

    def setUp(self) -> None:
        self.street_field = String(name="street", description="Street", mode="NULLABLE")
        self.zip_field = Integer(name="zip_code", description="Zip", mode="NULLABLE")
        self.record = Record(
            name="address",
            description="An address record",
            mode="NULLABLE",
            fields=[self.street_field, self.zip_field],
        )

    def test_as_schema_field(self) -> None:
        schema_field = self.record.as_schema_field()
        self.assertEqual(schema_field.field_type, "RECORD")
        self.assertEqual(
            schema_field.fields,
            (self.street_field.as_schema_field(), self.zip_field.as_schema_field()),
        )

    def test_matches_bq_field(self) -> None:
        schema_field = bigquery.SchemaField(
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
        )
        self.assertTrue(self.record.matches_bq_field(schema_field))

    def test_does_not_match_missing_subfield(self) -> None:
        schema_field = bigquery.SchemaField(
            name="address",
            field_type="RECORD",
            mode="NULLABLE",
            fields=[
                bigquery.SchemaField(
                    name="street", field_type="STRING", mode="NULLABLE"
                ),
            ],
        )
        self.assertFalse(self.record.matches_bq_field(schema_field))

    def test_does_not_match_different_subfield_type(self) -> None:
        record = Record(
            name="address",
            description="An address record",
            mode="NULLABLE",
            fields=[self.street_field],
        )
        schema_field = bigquery.SchemaField(
            name="address",
            field_type="RECORD",
            mode="NULLABLE",
            fields=[
                bigquery.SchemaField(
                    name="street", field_type="INTEGER", mode="NULLABLE"
                ),
            ],
        )
        self.assertFalse(record.matches_bq_field(schema_field))
