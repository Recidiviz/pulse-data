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

import attr
import sqlglot
from google.cloud import bigquery

from recidiviz.big_query.big_query_utils import (
    BigQueryFieldMode,
    are_bq_schemas_same,
    format_description_for_big_query,
    get_reserved_bq_column_name_prefix,
    is_big_query_valid_delimiter,
    is_big_query_valid_encoding,
    is_big_query_valid_line_terminator,
    make_bq_compatible_identifier,
    normalize_column_name_for_bq,
    schema_field_for_attribute,
    sql_type_name_for_schema_field,
    to_big_query_valid_encoding,
    to_validated_schema_field,
    typed_null_expression_for_field,
    validate_unquoted_bq_identifier,
)
from recidiviz.big_query.constants import BQ_TABLE_COLUMN_DESCRIPTION_MAX_LENGTH
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)


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

    def test_make_bq_compatible_identifier(self) -> None:
        # Already-compatible identifiers pass through unchanged.
        self.assertEqual("my_field_1", make_bq_compatible_identifier("my_field_1"))
        self.assertEqual("ABC_123", make_bq_compatible_identifier("ABC_123"))
        self.assertEqual("_leading", make_bq_compatible_identifier("_leading"))

        # Disallowed characters get replaced with underscores.
        self.assertEqual(
            "field_with_spaces", make_bq_compatible_identifier("field with spaces")
        )
        self.assertEqual("a_b_c", make_bq_compatible_identifier("a-b.c"))
        self.assertEqual("q__", make_bq_compatible_identifier("q?*"))
        self.assertEqual("z__", make_bq_compatible_identifier("zαα"))
        self.assertEqual("a_b", make_bq_compatible_identifier("a/b"))

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


class AreBqSchemasSameTest(unittest.TestCase):
    """Tests for are_bq_schemas_same."""

    def test_same_schemas(self) -> None:
        schema = [
            bigquery.SchemaField("col1", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("col2", "INTEGER", mode="NULLABLE"),
        ]
        self.assertTrue(are_bq_schemas_same(schema, list(schema)))

    def test_different_field_order(self) -> None:
        """Fields in different order should not be considered the same."""
        schema1 = [
            bigquery.SchemaField("col1", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("col2", "INTEGER", mode="NULLABLE"),
        ]
        schema2 = [
            bigquery.SchemaField("col2", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("col1", "STRING", mode="NULLABLE"),
        ]
        self.assertFalse(are_bq_schemas_same(schema1, schema2))

    def test_different_record_subfield_order(self) -> None:
        """Record subfields in different order should not be considered the same."""
        schema1 = [
            bigquery.SchemaField(
                "record",
                "RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField("a", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("b", "INTEGER", mode="NULLABLE"),
                ],
            ),
        ]
        schema2 = [
            bigquery.SchemaField(
                "record",
                "RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField("b", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("a", "STRING", mode="NULLABLE"),
                ],
            ),
        ]
        self.assertFalse(are_bq_schemas_same(schema1, schema2))


class ToValidatedSchemaFieldTest(unittest.TestCase):
    """Tests for to_validated_schema_field."""

    def test_invalid_field_name_raises(self) -> None:
        with self.assertRaisesRegex(ValueError, "contains invalid characters"):
            to_validated_schema_field(
                field_name="bad-name",
                description=None,
                field_type=bigquery.enums.SqlTypeNames.STRING,
                mode=BigQueryFieldMode.NULLABLE,
            )

    def test_long_description_is_truncated(self) -> None:
        long_description = "x" * (BQ_TABLE_COLUMN_DESCRIPTION_MAX_LENGTH + 100)
        field = to_validated_schema_field(
            field_name="my_column",
            description=long_description,
            field_type=bigquery.enums.SqlTypeNames.STRING,
            mode=BigQueryFieldMode.NULLABLE,
        )
        self.assertEqual(len(field.description), BQ_TABLE_COLUMN_DESCRIPTION_MAX_LENGTH)
        self.assertTrue(field.description.endswith("(truncated)"))

    def test_field_name_exceeds_length_limit_raises(self) -> None:
        with self.assertRaisesRegex(ValueError, "exceeds the 300-character limit"):
            to_validated_schema_field(
                field_name="a" * 301,
                description=None,
                field_type=bigquery.enums.SqlTypeNames.STRING,
                mode=BigQueryFieldMode.NULLABLE,
            )


class ValidateBqIdentifierTest(unittest.TestCase):
    """Tests for validate_bq_identifier."""

    def test_valid_identifiers(self) -> None:
        validate_unquoted_bq_identifier("my_table")
        validate_unquoted_bq_identifier("col_123")
        validate_unquoted_bq_identifier("_leading_underscore")
        validate_unquoted_bq_identifier("A")

    def test_empty(self) -> None:
        with self.assertRaisesRegex(ValueError, "cannot be empty"):
            validate_unquoted_bq_identifier("")

    def test_invalid_characters(self) -> None:
        with self.assertRaisesRegex(ValueError, "contains invalid characters"):
            validate_unquoted_bq_identifier("my-column")
        with self.assertRaisesRegex(ValueError, "contains invalid characters"):
            validate_unquoted_bq_identifier("has spaces")
        with self.assertRaisesRegex(ValueError, "contains invalid characters"):
            validate_unquoted_bq_identifier("col.name")

    def test_starts_with_digit(self) -> None:
        with self.assertRaisesRegex(ValueError, "cannot start with a digit"):
            validate_unquoted_bq_identifier("1_bad_name")

    def test_reserved_word(self) -> None:
        with self.assertRaisesRegex(ValueError, "is a reserved word"):
            validate_unquoted_bq_identifier("SELECT")
        with self.assertRaisesRegex(ValueError, "is a reserved word"):
            validate_unquoted_bq_identifier("select")


class GetReservedBqColumnNamePrefixTest(unittest.TestCase):
    """Tests for get_reserved_bq_column_name_prefix."""

    def test_no_reserved_prefix(self) -> None:
        self.assertIsNone(get_reserved_bq_column_name_prefix("state_code"))
        self.assertIsNone(get_reserved_bq_column_name_prefix("person_id"))
        # Underscore-prefixed but not reserved
        self.assertIsNone(get_reserved_bq_column_name_prefix("_internal_id"))
        self.assertIsNone(get_reserved_bq_column_name_prefix("_file"))

    def test_reserved_prefixes(self) -> None:
        self.assertEqual("_FILE_", get_reserved_bq_column_name_prefix("_FILE_NAME"))
        self.assertEqual("_TABLE_", get_reserved_bq_column_name_prefix("_TABLE_SUFFIX"))
        self.assertEqual(
            "_PARTITION", get_reserved_bq_column_name_prefix("_PARTITIONTIME")
        )
        self.assertEqual(
            "_PARTITION", get_reserved_bq_column_name_prefix("_PARTITIONDATE")
        )
        self.assertEqual("__ROOT__", get_reserved_bq_column_name_prefix("__ROOT__"))
        self.assertEqual(
            "_CHANGE_TYPE", get_reserved_bq_column_name_prefix("_CHANGE_TYPE")
        )

    def test_case_insensitive(self) -> None:
        self.assertEqual("_FILE_", get_reserved_bq_column_name_prefix("_file_name"))
        self.assertEqual(
            "_PARTITION", get_reserved_bq_column_name_prefix("_partitiondate")
        )


@attr.define
class _CollectionFieldsAttrClass:
    list_field: list[str] = attr.ib(factory=list)
    tuple_field: tuple[str, ...] = attr.ib(factory=tuple)


class SchemaFieldForAttributeTest(unittest.TestCase):
    """Tests for schema_field_for_attribute."""

    def test_list_and_tuple_attributes_map_to_string_columns(self) -> None:
        fields = attr.fields_dict(_CollectionFieldsAttrClass)
        self.assertEqual(
            "STRING",
            schema_field_for_attribute("list_field", fields["list_field"]).field_type,
        )
        self.assertEqual(
            "STRING",
            schema_field_for_attribute("tuple_field", fields["tuple_field"]).field_type,
        )


def _repeated_record_field(mode: str = "REPEATED") -> bigquery.SchemaField:
    """A RECORD field with a couple of scalar sub-fields, for exercising struct
    and array-of-struct type rendering."""
    return bigquery.SchemaField(
        "my_record",
        "RECORD",
        mode=mode,
        fields=[
            bigquery.SchemaField("a", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("b", "STRING"),
        ],
    )


class SqlTypeNameForSchemaFieldTest(unittest.TestCase):
    """Tests for sql_type_name_for_schema_field."""

    def test_scalar_types_echo_field_type(self) -> None:
        self.assertEqual(
            "STRING",
            sql_type_name_for_schema_field(bigquery.SchemaField("x", "STRING")),
        )
        self.assertEqual(
            "INT64",
            sql_type_name_for_schema_field(
                bigquery.SchemaField("x", "INT64", mode="REQUIRED")
            ),
        )
        self.assertEqual(
            "TIMESTAMP",
            sql_type_name_for_schema_field(bigquery.SchemaField("x", "TIMESTAMP")),
        )

    def test_repeated_scalar_wraps_in_array(self) -> None:
        self.assertEqual(
            "ARRAY<STRING>",
            sql_type_name_for_schema_field(
                bigquery.SchemaField("x", "STRING", mode="REPEATED")
            ),
        )

    def test_record_renders_struct(self) -> None:
        field = bigquery.SchemaField(
            "m",
            "RECORD",
            fields=[
                bigquery.SchemaField("n", "INT64", mode="REQUIRED"),
                bigquery.SchemaField("s", "STRING"),
            ],
        )
        self.assertEqual(
            "STRUCT<n INT64, s STRING>", sql_type_name_for_schema_field(field)
        )

    def test_repeated_record_renders_array_of_struct(self) -> None:
        self.assertEqual(
            "ARRAY<STRUCT<a INT64, b STRING>>",
            sql_type_name_for_schema_field(_repeated_record_field()),
        )

    def test_struct_with_repeated_sub_field_recurses(self) -> None:
        field = bigquery.SchemaField(
            "m",
            "RECORD",
            fields=[bigquery.SchemaField("x", "INT64", mode="REPEATED")],
        )
        self.assertEqual(
            "STRUCT<x ARRAY<INT64>>", sql_type_name_for_schema_field(field)
        )

    def test_nested_struct_recurses(self) -> None:
        field = bigquery.SchemaField(
            "m",
            "RECORD",
            fields=[
                bigquery.SchemaField(
                    "a", "RECORD", fields=[bigquery.SchemaField("b", "STRING")]
                )
            ],
        )
        self.assertEqual(
            "STRUCT<a STRUCT<b STRING>>", sql_type_name_for_schema_field(field)
        )

    def test_struct_spelling_matches_record_spelling(self) -> None:
        sub_fields = [bigquery.SchemaField("n", "INT64", mode="REQUIRED")]
        self.assertEqual(
            sql_type_name_for_schema_field(
                bigquery.SchemaField("m", "RECORD", fields=sub_fields)
            ),
            sql_type_name_for_schema_field(
                bigquery.SchemaField("m", "STRUCT", fields=sub_fields)
            ),
        )


class TypedNullExpressionForFieldTest(unittest.TestCase):
    """Tests for typed_null_expression_for_field."""

    def test_scalar_casts_null(self) -> None:
        self.assertEqual(
            "CAST(NULL AS STRING)",
            typed_null_expression_for_field(bigquery.SchemaField("x", "STRING")),
        )
        self.assertEqual(
            "CAST(NULL AS INT64)",
            typed_null_expression_for_field(
                bigquery.SchemaField("x", "INT64", mode="REQUIRED")
            ),
        )

    def test_record_casts_null_to_struct(self) -> None:
        self.assertEqual(
            "CAST(NULL AS STRUCT<a INT64, b STRING>)",
            typed_null_expression_for_field(_repeated_record_field(mode="NULLABLE")),
        )

    def test_repeated_scalar_is_empty_typed_array(self) -> None:
        self.assertEqual(
            "ARRAY<STRING>[]",
            typed_null_expression_for_field(
                bigquery.SchemaField("x", "STRING", mode="REPEATED")
            ),
        )

    def test_repeated_record_is_empty_typed_array(self) -> None:
        self.assertEqual(
            "ARRAY<STRUCT<a INT64, b STRING>>[]",
            typed_null_expression_for_field(_repeated_record_field()),
        )

    def test_expressions_parse_as_valid_bigquery(self) -> None:
        for field in [
            bigquery.SchemaField("x", "STRING"),
            bigquery.SchemaField("x", "INT64", mode="REPEATED"),
            _repeated_record_field(mode="NULLABLE"),
            _repeated_record_field(),
        ]:
            with self.subTest(field=field.name, mode=field.mode):
                # Raises if the expression is not parseable BigQuery SQL.
                sqlglot.parse_one(
                    f"SELECT {typed_null_expression_for_field(field)} AS x",
                    dialect="bigquery",
                )


class TypedNullExpressionForFieldEmulatorTest(BigQueryEmulatorTestCase):
    """Validates that the expressions produced by typed_null_expression_for_field
    are accepted and evaluated by BigQuery — in particular the empty typed-array
    literal for REPEATED fields, which a plain CAST(NULL AS ...) cannot express."""

    def test_repeated_record_selects_empty_array(self) -> None:
        expression = typed_null_expression_for_field(_repeated_record_field())
        result = self.query(f"SELECT {expression} AS value")
        self.assertEqual(1, len(result))
        self.assertEqual(0, len(result.iloc[0]["value"]))

    def test_repeated_scalar_selects_empty_array(self) -> None:
        expression = typed_null_expression_for_field(
            bigquery.SchemaField("x", "STRING", mode="REPEATED")
        )
        result = self.query(f"SELECT {expression} AS value")
        self.assertEqual(1, len(result))
        self.assertEqual(0, len(result.iloc[0]["value"]))

    def test_scalar_selects_null(self) -> None:
        expression = typed_null_expression_for_field(
            bigquery.SchemaField("x", "STRING")
        )
        result = self.query(f"SELECT {expression} AS value")
        self.assertEqual(1, len(result))
        self.assertTrue(bool(result["value"].isna().all()))
