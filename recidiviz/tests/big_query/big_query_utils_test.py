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

import datetime
import decimal
import unittest

import attr
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery

from recidiviz.big_query.big_query_utils import (
    BigQueryFieldMode,
    JsonExtractionType,
    are_bq_schemas_same,
    format_description_for_big_query,
    get_reserved_bq_column_name_prefix,
    is_big_query_valid_delimiter,
    is_big_query_valid_encoding,
    is_big_query_valid_line_terminator,
    json_extraction_clause,
    make_bq_compatible_identifier,
    normalize_column_name_for_bq,
    null_sql_cast_clause_for_schema_field,
    schema_field_for_attribute,
    sql_cast_clause_for_schema_field,
    sql_type_name_for_schema_field,
    to_big_query_valid_encoding,
    to_validated_schema_field,
    validate_unquoted_bq_identifier,
)
from recidiviz.big_query.constants import BQ_TABLE_COLUMN_DESCRIPTION_MAX_LENGTH
from recidiviz.common import attr_validators
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

    def test_legacy_scalar_names_map_to_standard_sql(self) -> None:
        """Legacy schema type names are rewritten to the standard-SQL names that
        BigQuery accepts as CAST targets (only these are legal — e.g. FLOAT is not).
        """
        self.assertEqual(
            "FLOAT64",
            sql_type_name_for_schema_field(bigquery.SchemaField("x", "FLOAT")),
        )
        self.assertEqual(
            "INT64",
            sql_type_name_for_schema_field(bigquery.SchemaField("x", "INTEGER")),
        )
        self.assertEqual(
            "BOOL",
            sql_type_name_for_schema_field(bigquery.SchemaField("x", "BOOLEAN")),
        )

    def test_unknown_field_type_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^No mapping in _STANDARD_SQL_SCALAR_TYPE_BY_FIELD_TYPE for field "
            r"\[x\] with field type \[RANGE\]\.$",
        ):
            sql_type_name_for_schema_field(bigquery.SchemaField("x", "RANGE"))

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


class JsonExtractionClauseTest(unittest.TestCase):
    """Tests for json_extraction_clause / JsonExtractionType."""

    def test_each_extraction_type_produces_its_function(self) -> None:
        # Each type names its BigQuery extraction function, wrapping the source
        # expression and JSON path.
        cases = [
            (
                JsonExtractionType.SCALAR,
                "$.employer.value",
                "JSON_VALUE(result_json, '$.employer.value')",
            ),
            (
                JsonExtractionType.JSON,
                "$.citations",
                "JSON_QUERY(result_json, '$.citations')",
            ),
            (
                JsonExtractionType.ARRAY,
                "$.jobs",
                "JSON_QUERY_ARRAY(result_json, '$.jobs')",
            ),
        ]
        for extraction_type, json_path, expected in cases:
            with self.subTest(extraction_type=extraction_type):
                self.assertEqual(
                    expected,
                    json_extraction_clause(extraction_type, "result_json", json_path),
                )

    def test_source_expression_interpolated_verbatim(self) -> None:
        # A non-bare-column source (e.g. the UNNEST element alias used when
        # extracting from an unnested array) passes through unchanged.
        self.assertEqual(
            "JSON_QUERY_ARRAY(elem, '$.titles')",
            json_extraction_clause(JsonExtractionType.ARRAY, "elem", "$.titles"),
        )

    def test_double_quoted_path_segment_renders(self) -> None:
        # JSONPath double-quote syntax for special-character segments nests fine
        # inside the single-quoted SQL string literal.
        self.assertEqual(
            "JSON_VALUE(result_json, '$.\"my field\"')",
            json_extraction_clause(
                JsonExtractionType.SCALAR, "result_json", '$."my field"'
            ),
        )

    def test_single_quote_in_path_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^JSON path \[\$\.it's\] contains a single quote, which would break "
            r"the single-quoted SQL string literal it is rendered into\. Use "
            r'JSONPath double-quote syntax \(e\.g\. \$\."my field"\) for path '
            r"segments with special characters\.$",
        ):
            json_extraction_clause(JsonExtractionType.SCALAR, "result_json", "$.it's")


class SqlCastClauseForSchemaFieldTest(unittest.TestCase):
    """Tests for sql_cast_clause_for_schema_field."""

    def test_default_value_expression_self_casts(self) -> None:
        self.assertEqual(
            "CAST(wage AS FLOAT64) AS wage",
            sql_cast_clause_for_schema_field(bigquery.SchemaField("wage", "FLOAT")),
        )

    def test_explicit_value_expression_keeps_field_alias(self) -> None:
        """An explicit value expression is cast to the field's type but the alias
        stays the field name (the scalar_value_column_sql_clause / JSON_VALUE path)."""
        self.assertEqual(
            "CAST(JSON_VALUE(j, '$.wage') AS FLOAT64) AS wage",
            sql_cast_clause_for_schema_field(
                bigquery.SchemaField("wage", "FLOAT"),
                value_expression="JSON_VALUE(j, '$.wage')",
            ),
        )

    def test_repeated_and_struct_render_in_clause(self) -> None:
        self.assertEqual(
            "CAST(tags AS ARRAY<STRING>) AS tags",
            sql_cast_clause_for_schema_field(
                bigquery.SchemaField("tags", "STRING", mode="REPEATED")
            ),
        )
        self.assertEqual(
            "CAST(my_record AS ARRAY<STRUCT<a INT64, b STRING>>) AS my_record",
            sql_cast_clause_for_schema_field(_repeated_record_field()),
        )


class NullSqlCastClauseForSchemaFieldTest(unittest.TestCase):
    """Tests for null_sql_cast_clause_for_schema_field."""

    def test_scalar_casts_null(self) -> None:
        self.assertEqual(
            "CAST(NULL AS STRING) AS x",
            null_sql_cast_clause_for_schema_field(bigquery.SchemaField("x", "STRING")),
        )

    def test_struct_casts_null(self) -> None:
        self.assertEqual(
            "CAST(NULL AS STRUCT<a INT64, b STRING>) AS my_record",
            null_sql_cast_clause_for_schema_field(
                _repeated_record_field(mode="NULLABLE")
            ),
        )

    def test_repeated_casts_null_array(self) -> None:
        self.assertEqual(
            "CAST(NULL AS ARRAY<STRING>) AS x",
            null_sql_cast_clause_for_schema_field(
                bigquery.SchemaField("x", "STRING", mode="REPEATED")
            ),
        )


@attr.define(frozen=True, kw_only=True)
class _CastTargetCase:
    """Bundles information about CASTing logic test cases we want to run against a given
    field of a given type.
    """

    field: bigquery.SchemaField = attr.ib(
        validator=attr.validators.instance_of(bigquery.SchemaField)
    )
    """Field whose sql_type_name_for_schema_field token is the CAST target under test."""

    castable_source_to_expected: dict[str, object] = attr.ib(
        validator=attr_validators.is_dict
    )
    """Source SQL expression -> the value CASTing it to the field's type should yield.

    Expected values are the plain-Python forms returned by the BigQuery results
    iterator (Decimal for NUMERIC, datetime types for temporals, dict for STRUCT,
    list for ARRAY).
    """

    uncastable_source_expressions: list[str] = attr.ib(
        validator=attr_validators.is_list_of(str)
    )
    """Source SQL expressions BigQuery rejects casting to the field's type."""

    expected_null_cast_value: object = attr.ib(
        validator=attr.validators.instance_of((type(None), list))
    )
    """Value CAST(NULL AS <type>) yields for this field: None for scalar and struct
    types, and an empty list for REPEATED (a NULL array becomes an empty array,
    since BigQuery arrays cannot be NULL)."""


_CAST_TARGET_MATRIX: list[_CastTargetCase] = [
    _CastTargetCase(
        field=bigquery.SchemaField("v", "STRING"),
        castable_source_to_expected={
            "'a'": "a",
            "1": "1",
            "1.5": "1.5",
            # This is casting the string 'TRUE' to a string
            "'TRUE'": "TRUE",
            # This is casting the boolean value TRUE to a string
            "TRUE": "true",
        },
        # A JSON-typed value cannot be CAST to STRING (or any scalar) — it must go
        # through JSON_VALUE / STRING(). This means only JsonExtractionType.SCALAR
        # output can be fed into a scalar cast clause; JSON_QUERY output cannot.
        uncastable_source_expressions=["JSON '\"a\"'"],
        expected_null_cast_value=None,
    ),
    _CastTargetCase(
        field=bigquery.SchemaField("v", "INTEGER"),
        castable_source_to_expected={
            "'1'": 1,
            "1": 1,
            # Lossy but legal: CAST to INT64 rounds half away from zero.
            "1.5": 2,
            "2.5": 3,
        },
        # JSON_VALUE always returns a STRING, so a JSON number like "1.5" extracted
        # for an INT64 column fails at query runtime — string sources must be exact
        # integers.
        uncastable_source_expressions=["'abc'", "'1.5'"],
        expected_null_cast_value=None,
    ),
    _CastTargetCase(
        field=bigquery.SchemaField("v", "FLOAT"),
        castable_source_to_expected={
            "'1.5'": 1.5,
            "1.5": 1.5,
            "1": 1.0,
            # A more complex expression - JSON_VALUE returns a STRING that we should
            # still be able to cast.
            json_extraction_clause(
                JsonExtractionType.SCALAR, 'JSON \'{"v": "1.5"}\'', "$.v"
            ): 1.5,
        },
        uncastable_source_expressions=["'abc'"],
        expected_null_cast_value=None,
    ),
    _CastTargetCase(
        field=bigquery.SchemaField("v", "BOOLEAN"),
        castable_source_to_expected={
            "'true'": True,
            # String parsing is case-insensitive.
            "'TRUE'": True,
            "TRUE": True,
            "1": True,
            "0": False,
        },
        # Only 'true'/'false' strings parse — numeric strings do not.
        uncastable_source_expressions=["'1'", "'abc'"],
        expected_null_cast_value=None,
    ),
    _CastTargetCase(
        field=bigquery.SchemaField("v", "NUMERIC"),
        castable_source_to_expected={
            "'1.50'": decimal.Decimal("1.5"),
            "1.5": decimal.Decimal("1.5"),
            "1": decimal.Decimal("1"),
            # Lossy but legal: NUMERIC holds 9 decimal digits; further digits round.
            "'1.1234567891'": decimal.Decimal("1.123456789"),
        },
        uncastable_source_expressions=["'abc'"],
        expected_null_cast_value=None,
    ),
    _CastTargetCase(
        field=bigquery.SchemaField("v", "BIGNUMERIC"),
        castable_source_to_expected={
            "'1.5'": decimal.Decimal("1.5"),
            "1.5": decimal.Decimal("1.5"),
        },
        uncastable_source_expressions=["'abc'"],
        expected_null_cast_value=None,
    ),
    _CastTargetCase(
        field=bigquery.SchemaField("v", "BYTES"),
        castable_source_to_expected={"b'abc'": b"abc", "'abc'": b"abc"},
        uncastable_source_expressions=["1"],
        expected_null_cast_value=None,
    ),
    # GEOGRAPHY (also in the type map) has no emulator coverage here: the emulator
    # does not support geography functions (e.g. ST_GEOGPOINT), and GEOGRAPHY has
    # no CAST conversions from other types anyway.
    _CastTargetCase(
        field=bigquery.SchemaField("v", "INTERVAL"),
        castable_source_to_expected={
            "'P1Y'": relativedelta(years=+1),
            "INTERVAL 1 YEAR": relativedelta(years=+1),
        },
        uncastable_source_expressions=["'abc'"],
        expected_null_cast_value=None,
    ),
    _CastTargetCase(
        field=bigquery.SchemaField("v", "DATE"),
        castable_source_to_expected={
            "'2020-01-01'": datetime.date(2020, 1, 1),
            "DATE '2020-01-01'": datetime.date(2020, 1, 1),
        },
        # Only canonical (ISO) date strings cast — other formats need PARSE_DATE.
        uncastable_source_expressions=["'01/02/2020'", "'2020-13-01'"],
        expected_null_cast_value=None,
    ),
    _CastTargetCase(
        field=bigquery.SchemaField("v", "DATETIME"),
        castable_source_to_expected={
            "'2020-01-01T09:30:00'": datetime.datetime(2020, 1, 1, 9, 30),
            "DATETIME '2020-01-01 09:30:00'": datetime.datetime(2020, 1, 1, 9, 30),
        },
        uncastable_source_expressions=["'abc'"],
        expected_null_cast_value=None,
    ),
    _CastTargetCase(
        field=bigquery.SchemaField("v", "TIMESTAMP"),
        castable_source_to_expected={
            "'2020-01-01 09:30:00+00'": datetime.datetime(
                2020, 1, 1, 9, 30, tzinfo=datetime.timezone.utc
            ),
            "TIMESTAMP '2020-01-01 09:30:00+00'": datetime.datetime(
                2020, 1, 1, 9, 30, tzinfo=datetime.timezone.utc
            ),
        },
        uncastable_source_expressions=["'abc'"],
        expected_null_cast_value=None,
    ),
    _CastTargetCase(
        field=bigquery.SchemaField("v", "TIME"),
        castable_source_to_expected={
            "'09:30:00'": datetime.time(9, 30),
            "TIME '09:30:00'": datetime.time(9, 30),
        },
        uncastable_source_expressions=["'abc'"],
        expected_null_cast_value=None,
    ),
    # STRING -> JSON is rejected; JSON must be built with a JSON literal / PARSE_JSON.
    _CastTargetCase(
        field=bigquery.SchemaField("v", "JSON"),
        castable_source_to_expected={"JSON '{}'": {}, "PARSE_JSON('{}')": {}},
        uncastable_source_expressions=["'{}'"],
        expected_null_cast_value=None,
    ),
    # Fields declared with the standard-SQL spellings (INT64/FLOAT64/BOOL), which
    # pass through the type map unchanged.
    _CastTargetCase(
        field=bigquery.SchemaField("v", "INT64"),
        castable_source_to_expected={"'1'": 1},
        uncastable_source_expressions=[],
        expected_null_cast_value=None,
    ),
    _CastTargetCase(
        field=bigquery.SchemaField("v", "FLOAT64"),
        castable_source_to_expected={"'1.5'": 1.5},
        uncastable_source_expressions=[],
        expected_null_cast_value=None,
    ),
    _CastTargetCase(
        field=bigquery.SchemaField("v", "BOOL"),
        castable_source_to_expected={"'true'": True},
        uncastable_source_expressions=[],
        expected_null_cast_value=None,
    ),
    # A scalar cannot be cast to an array/struct type regardless of value.
    _CastTargetCase(
        field=bigquery.SchemaField("v", "STRING", mode="REPEATED"),
        castable_source_to_expected={"['a', 'b']": ["a", "b"]},
        uncastable_source_expressions=[
            "'a'",
            # Array casts require the exact same array type — element types are not
            # coerced.
            "[1, 2]",
            # JSON_QUERY_ARRAY returns ARRAY<JSON>, which cannot be CAST to
            # ARRAY<STRING>. A REPEATED column can NOT be populated by composing
            # JsonExtractionType.ARRAY output with a cast clause — it needs
            # UNNEST + per-element casts (or JSON_VALUE_ARRAY for scalar arrays).
            "JSON_QUERY_ARRAY(JSON '[\"a\", \"b\"]', '$')",
        ],
        # A NULL array casts to an empty array, not NULL.
        expected_null_cast_value=[],
    ),
    _CastTargetCase(
        field=_repeated_record_field(mode="NULLABLE"),
        castable_source_to_expected={
            "STRUCT(1 AS a, 'b' AS b)": {"a": 1, "b": "b"},
            # Struct casts coerce field-by-field ('1' -> INT64, 2 -> STRING).
            "STRUCT('1' AS a, 2 AS b)": {"a": 1, "b": "2"},
        },
        uncastable_source_expressions=["'x'", "STRUCT('abc' AS a, 'b' AS b)"],
        expected_null_cast_value=None,
    ),
    _CastTargetCase(
        field=_repeated_record_field(),
        castable_source_to_expected={
            "[STRUCT(1 AS a, 'b' AS b)]": [{"a": 1, "b": "b"}],
        },
        # A bare struct cannot be cast to an array of structs.
        uncastable_source_expressions=["STRUCT(1 AS a, 'b' AS b)"],
        expected_null_cast_value=[],
    ),
]


class SqlCastEmulatorTest(BigQueryEmulatorTestCase):
    """End-to-end proof against BigQuery that the CAST SQL emitted by
    sql_type_name_for_schema_field, sql_cast_clause_for_schema_field, and
    null_sql_cast_clause_for_schema_field is accepted and evaluates to the expected
    value, for every source/target pairing in _CAST_TARGET_MATRIX, and that the
    casts BigQuery rejects do in fact raise.
    """

    # These tests create no tables — the only datasets in the emulator are the
    # anonymous query-cache datasets it materializes for each distinct query (one
    # per matrix entry, ~100+ per test). Wiping fans out parallel delete_dataset
    # calls over all of them, which crashes the emulator and hangs teardown
    # (see: OBT-40787).
    wipe_emulator_data_on_teardown = False

    def test_valid_casts_produce_expected_values(self) -> None:
        for case in _CAST_TARGET_MATRIX:
            for source, expected in case.castable_source_to_expected.items():
                field_cast_type = sql_type_name_for_schema_field(case.field)
                full_cast_clause = sql_cast_clause_for_schema_field(
                    case.field, value_expression=source
                )
                with self.subTest(clause=full_cast_clause):
                    self.run_query_test(
                        f"SELECT CAST({source} AS {field_cast_type}) AS x",
                        [{"x": expected}],
                    )
                    self.run_query_test(
                        f"SELECT {full_cast_clause}", [{case.field.name: expected}]
                    )

            null_cast_clause = null_sql_cast_clause_for_schema_field(case.field)
            with self.subTest(clause=null_cast_clause):
                self.run_query_test(
                    f"SELECT {null_cast_clause}",
                    [{case.field.name: case.expected_null_cast_value}],
                )

    def test_rejected_casts_raise(self) -> None:
        for case in _CAST_TARGET_MATRIX:
            token = sql_type_name_for_schema_field(case.field)
            for source in case.uncastable_source_expressions:
                with self.subTest(token=token, source=source):
                    with self.assertRaises(Exception):
                        self.query(f"SELECT CAST({source} AS {token}) AS v")

    def test_self_cast_uses_field_name_as_source(self) -> None:
        # With no value_expression the clause casts the column named by the field.
        clause = sql_cast_clause_for_schema_field(bigquery.SchemaField("c", "FLOAT"))
        self.run_query_test(
            f"WITH t AS (SELECT 1.5 AS c) SELECT {clause} FROM t", [{"c": 1.5}]
        )

    def test_null_casts_union_with_real_values_across_type_shapes(self) -> None:
        # The document diff-query 'deleted row' scenario: a branch of typed-null
        # columns must be type-compatible with a real-values branch across scalar,
        # REPEATED, and STRUCT columns — something per-column null casts can't prove.
        fields = [
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("tags", "STRING", mode="REPEATED"),
            bigquery.SchemaField(
                "addr", "RECORD", fields=[bigquery.SchemaField("street", "STRING")]
            ),
        ]
        null_columns = ", ".join(
            null_sql_cast_clause_for_schema_field(f) for f in fields
        )
        query = f"""
SELECT 'a' AS id, ['x', 'y'] AS tags, STRUCT('main st' AS street) AS addr
UNION ALL
SELECT {null_columns}
ORDER BY id NULLS LAST
"""
        self.run_query_test(
            query,
            [
                {"id": "a", "tags": ["x", "y"], "addr": {"street": "main st"}},
                {"id": None, "tags": [], "addr": None},
            ],
        )


class JsonExtractionClauseEmulatorTest(BigQueryEmulatorTestCase):
    """Proves the shape-selection semantics of each JsonExtractionType against
    BigQuery: what each function returns when the path matches its expected
    shape, and that a shape mismatch or a missing path yields NULL rather than
    an error.
    """

    # No tables are created; skip the teardown wipe of query-cache datasets (see
    # SqlCastEmulatorTest).
    wipe_emulator_data_on_teardown = False

    _SOURCE_JSON_EXPRESSION = (
        """JSON '{"name": "ann", "job": {"title": "dev"}, "tags": ["a", "b"]}'"""
    )

    def _select_extraction(
        self, extraction_type: JsonExtractionType, json_path: str
    ) -> str:
        clause = json_extraction_clause(
            extraction_type, self._SOURCE_JSON_EXPRESSION, json_path
        )
        return f"SELECT {clause} AS v"

    def test_scalar_extracts_leaf_value(self) -> None:
        self.run_query_test(
            self._select_extraction(JsonExtractionType.SCALAR, "$.name"),
            [{"v": "ann"}],
        )

    def test_scalar_on_object_is_null(self) -> None:
        # JSON_VALUE only returns scalar leaves — pointing it at an object (or
        # array) yields NULL, not an error.
        self.run_query_test(
            self._select_extraction(JsonExtractionType.SCALAR, "$.job"),
            [{"v": None}],
        )

    def test_json_extracts_nested_object(self) -> None:
        self.run_query_test(
            self._select_extraction(JsonExtractionType.JSON, "$.job"),
            [{"v": {"title": "dev"}}],
        )

    def test_json_on_missing_path_is_null(self) -> None:
        self.run_query_test(
            self._select_extraction(JsonExtractionType.JSON, "$.nope"),
            [{"v": None}],
        )

    def test_array_extracts_elements(self) -> None:
        self.run_query_test(
            self._select_extraction(JsonExtractionType.ARRAY, "$.tags"),
            [{"v": ["a", "b"]}],
        )

    def test_array_on_non_array_is_null(self) -> None:
        # JSON_QUERY_ARRAY on a non-array yields a NULL array, which surfaces as
        # an empty array in query results (arrays cannot be NULL in output).
        self.run_query_test(
            self._select_extraction(JsonExtractionType.ARRAY, "$.name"),
            [{"v": []}],
        )

    def test_double_quoted_path_segment_extracts(self) -> None:
        clause = json_extraction_clause(
            JsonExtractionType.SCALAR, """JSON '{"my field": "x"}'""", '$."my field"'
        )
        self.run_query_test(f"SELECT {clause} AS v", [{"v": "x"}])
