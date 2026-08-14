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
"""Tests for LabelStudioTaskDataField, and in particular which values it will accept for
each BigQuery type the parsed annotations view casts to.
"""
import unittest

from google.cloud import bigquery

from recidiviz.llm_eval.label_studio.models.label_studio_task_data_field import (
    LabelStudioTaskDataField,
)


def _field(
    bq_type: bigquery.StandardSqlTypeNames, *, extract_as_json: bool = False
) -> LabelStudioTaskDataField:
    return LabelStudioTaskDataField(
        column_name="some_field",
        description="A field.",
        bq_type=bq_type,
        extract_as_json=extract_as_json,
    )


class ValidateValueTest(unittest.TestCase):
    """Tests for LabelStudioTaskDataField.validate_value."""

    def test_null_always_accepted(self) -> None:
        for bq_type in [
            bigquery.StandardSqlTypeNames.STRING,
            bigquery.StandardSqlTypeNames.INT64,
            bigquery.StandardSqlTypeNames.FLOAT64,
            bigquery.StandardSqlTypeNames.BOOL,
        ]:
            with self.subTest(bq_type=bq_type.value):
                _field(bq_type).validate_value(None)

    def test_string_accepts_str(self) -> None:
        _field(bigquery.StandardSqlTypeNames.STRING).validate_value("employed")

    def test_string_rejects_int(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"declares bq_type \[STRING\], so its value must be one of \['str'\] or "
            r"None, but found \[7\] of type \[int\]",
        ):
            _field(bigquery.StandardSqlTypeNames.STRING).validate_value(7)

    def test_int64_accepts_int(self) -> None:
        _field(bigquery.StandardSqlTypeNames.INT64).validate_value(7)

    def test_int64_rejects_bool(self) -> None:
        # Python's bool is an int subclass, but true in JSON is not a number to BigQuery.
        with self.assertRaisesRegex(
            ValueError, r"declares bq_type \[INT64\].*found \[True\] of type \[bool\]"
        ):
            _field(bigquery.StandardSqlTypeNames.INT64).validate_value(True)

    def test_int64_rejects_str(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"declares bq_type \[INT64\].*found \[7\] of type \[str\]"
        ):
            _field(bigquery.StandardSqlTypeNames.INT64).validate_value("7")

    def test_float64_accepts_float_and_whole_number(self) -> None:
        # JSON has one number type, so a whole number is a legitimate FLOAT64.
        _field(bigquery.StandardSqlTypeNames.FLOAT64).validate_value(15.5)
        _field(bigquery.StandardSqlTypeNames.FLOAT64).validate_value(15)

    def test_float64_rejects_bool(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"declares bq_type \[FLOAT64\].*of type \[bool\]"
        ):
            _field(bigquery.StandardSqlTypeNames.FLOAT64).validate_value(False)

    def test_bool_accepts_bool(self) -> None:
        _field(bigquery.StandardSqlTypeNames.BOOL).validate_value(True)

    def test_bool_rejects_int(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"declares bq_type \[BOOL\].*found \[1\] of type \[int\]"
        ):
            _field(bigquery.StandardSqlTypeNames.BOOL).validate_value(1)

    def test_timestamp_accepts_str(self) -> None:
        # JSON has no timestamp literal, so these travel as strings and are cast on the
        # way out.
        _field(bigquery.StandardSqlTypeNames.TIMESTAMP).validate_value(
            "2026-01-15T09:12:00Z"
        )

    def test_extract_as_json_accepts_object_and_array(self) -> None:
        json_field = _field(bigquery.StandardSqlTypeNames.STRING, extract_as_json=True)
        json_field.validate_value({"employer_name": "Walmart"})
        json_field.validate_value([1, 2, 3])

    def test_extract_as_json_rejects_scalar(self) -> None:
        # TO_JSON_STRING on a scalar would come back quoted, so a container is required
        # rather than merely permitted.
        with self.assertRaisesRegex(
            ValueError,
            r"declares bq_type \[STRING\] with extract_as_json, so its value must be one "
            r"of \['dict', 'list'\] or None, but found \[Walmart\] of type \[str\]",
        ):
            _field(
                bigquery.StandardSqlTypeNames.STRING, extract_as_json=True
            ).validate_value("Walmart")


class AllowedValueTypesTest(unittest.TestCase):
    """Tests for LabelStudioTaskDataField.allowed_value_types."""

    def test_types_by_bq_type(self) -> None:
        self.assertEqual(
            (str,), _field(bigquery.StandardSqlTypeNames.STRING).allowed_value_types
        )
        self.assertEqual(
            (int,), _field(bigquery.StandardSqlTypeNames.INT64).allowed_value_types
        )
        self.assertEqual(
            (int, float),
            _field(bigquery.StandardSqlTypeNames.FLOAT64).allowed_value_types,
        )
        self.assertEqual(
            (bool,), _field(bigquery.StandardSqlTypeNames.BOOL).allowed_value_types
        )

    def test_unmapped_bq_type_falls_back_to_str(self) -> None:
        self.assertEqual(
            (str,), _field(bigquery.StandardSqlTypeNames.DATE).allowed_value_types
        )
        self.assertEqual(
            (str,), _field(bigquery.StandardSqlTypeNames.NUMERIC).allowed_value_types
        )

    def test_extract_as_json_requires_container(self) -> None:
        self.assertEqual(
            (dict, list),
            _field(
                bigquery.StandardSqlTypeNames.STRING, extract_as_json=True
            ).allowed_value_types,
        )


if __name__ == "__main__":
    unittest.main()
