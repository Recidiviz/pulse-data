# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests for big_query_utils.py"""
import unittest

from google.cloud import bigquery

from recidiviz.big_query.big_query_utils import (
    format_description_for_big_query,
    is_big_query_valid_delimiter,
    is_big_query_valid_encoding,
    is_big_query_valid_line_terminator,
    normalize_column_name_for_bq,
    to_big_query_valid_encoding,
)
from recidiviz.big_query.big_query_view_column import (
    Date,
    Integer,
    String,
    diff_declared_schema_to_bq_schema,
)
from recidiviz.big_query.constants import BQ_TABLE_COLUMN_DESCRIPTION_MAX_LENGTH


class BigQueryUtilsTest(unittest.TestCase):
    """TestCase for BigQuery utils"""

    def test_normalize_column_name_for_bq(self) -> None:
        valid_column_name = "FIELD_NAME_532"
        column_names = [
            "  FIELD_NAME_532",
            "FIELD_NAME_532  ",
            "FIELD_\x16NAME_532",
            "FIELD NAME 532",
            "FIELD?NAME*532",
            valid_column_name,
        ]

        for column_name in column_names:
            normalized = normalize_column_name_for_bq(column_name)
            self.assertEqual(normalized, valid_column_name)

        # Handles reserved words correctly
        self.assertEqual(
            "_ASSERT_ROWS_MODIFIED",
            normalize_column_name_for_bq("ASSERT_ROWS_MODIFIED"),
        )
        self.assertEqual(
            "_TableSAmple",
            normalize_column_name_for_bq("TableSAmple"),
        )

        # Handles digits correctly
        self.assertEqual("_123_COLUMN", normalize_column_name_for_bq("123_COLUMN"))

        self.assertRaisesRegex(
            ValueError, "Column name cannot be empty", normalize_column_name_for_bq, ""
        )
        self.assertRaisesRegex(
            ValueError,
            "Column name cannot contain only whitespace and/or unprintable characters",
            normalize_column_name_for_bq,
            " ",
        )
        self.assertRaisesRegex(
            ValueError,
            "Column name cannot contain only whitespace and/or unprintable characters",
            normalize_column_name_for_bq,
            "  αα",
        )

    def test_is_big_query_valid_encoding(self) -> None:
        for valid_encoding in ["utf_8", "latin", "utf-16-be"]:
            assert is_big_query_valid_encoding(valid_encoding) is True

        for invalid_encoding in ["windows-1252", "latin2", "utf16"]:
            assert is_big_query_valid_encoding(invalid_encoding) is False

    def test_to_big_query_valid_encoding(self) -> None:
        for valid_encoding, big_query_encoding in [
            ("utf_8", "UTF-8"),
            ("latin", "ISO-8859-1"),
            ("utf-16-be", "UTF_16BE"),
        ]:
            assert to_big_query_valid_encoding(valid_encoding) == big_query_encoding

        for invalid_encoding in ["windows-1252", "latin2", "utf16"]:
            with self.assertRaises(KeyError):
                _ = to_big_query_valid_encoding(invalid_encoding)

    def test_is_big_query_valid_line_terminator(self) -> None:
        for valid_line_terminator in ["\n"]:
            assert is_big_query_valid_line_terminator(valid_line_terminator) is True

        for invalid_line_terminator in ["‡\n", "‡", "†\n", "†"]:
            assert is_big_query_valid_line_terminator(invalid_line_terminator) is False

    def is_big_query_valid_delimiter(self) -> None:
        for valid_delimiter, encoding in [
            (",", "utf-8"),
            ("|", "windows-1252"),
            ("‡", "windows-1252"),
        ]:
            assert is_big_query_valid_delimiter(valid_delimiter, encoding) is True

        for invalid_delimiter, encoding in [
            ("‡", "utf-8"),
            ("†", "utf-8"),
            ("", "utf-8"),
            ("aaaaaa", "utf-8"),
            ("delim", "latin-1"),
            ("||", "latin-1"),
        ]:
            assert is_big_query_valid_delimiter(invalid_delimiter, encoding) is False

    def test_format_description_for_big_query_empty(self) -> None:
        assert format_description_for_big_query(None) == ""
        assert format_description_for_big_query("") == ""
        assert format_description_for_big_query("a") == "a"

    def test_format_description_for_big_query_too_long(self) -> None:
        for length in range(
            BQ_TABLE_COLUMN_DESCRIPTION_MAX_LENGTH,
            BQ_TABLE_COLUMN_DESCRIPTION_MAX_LENGTH + 2,
        ):
            description = format_description_for_big_query("?" * length)
            assert len(description) == BQ_TABLE_COLUMN_DESCRIPTION_MAX_LENGTH
            if length > BQ_TABLE_COLUMN_DESCRIPTION_MAX_LENGTH:
                assert description.endswith("(truncated)")
            else:
                assert description == "?" * length


# TODO(#54941): move to big_query_view_column_test.py
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
