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
"""Tests for lookml_bq_utils.py"""
import unittest

from google.cloud import bigquery

from recidiviz.looker.lookml_bq_utils import (
    lookml_field_type_for_bq_type,
    lookml_view_field_for_schema_field,
)
from recidiviz.looker.lookml_view_field import (
    DimensionGroupLookMLViewField,
    DimensionLookMLViewField,
)
from recidiviz.looker.lookml_view_field_parameter import LookMLFieldType


class TestLookMLBQUtils(unittest.TestCase):
    """Tests for lookml_bq_utils.py"""

    def test_lookml_field_type_for_bq_type_date(self) -> None:
        self.assertEqual(
            lookml_field_type_for_bq_type(bigquery.enums.SqlTypeNames.DATE),
            LookMLFieldType.TIME,
        )

    def test_lookml_field_type_for_bq_type_datetime(self) -> None:
        self.assertEqual(
            lookml_field_type_for_bq_type(bigquery.enums.SqlTypeNames.DATETIME),
            LookMLFieldType.TIME,
        )

    def test_lookml_field_type_for_bq_type_string(self) -> None:
        self.assertEqual(
            lookml_field_type_for_bq_type(bigquery.enums.SqlTypeNames.STRING),
            LookMLFieldType.STRING,
        )

    def test_lookml_field_type_for_bq_type_boolean(self) -> None:
        self.assertEqual(
            lookml_field_type_for_bq_type(bigquery.enums.SqlTypeNames.BOOLEAN),
            LookMLFieldType.YESNO,
        )

    def test_lookml_field_type_for_bq_type_integer(self) -> None:
        self.assertEqual(
            lookml_field_type_for_bq_type(bigquery.enums.SqlTypeNames.INTEGER),
            LookMLFieldType.NUMBER,
        )

    def test_lookml_field_type_for_bq_type_not_implemented(self) -> None:
        with self.assertRaises(NotImplementedError):
            lookml_field_type_for_bq_type(bigquery.enums.SqlTypeNames.FLOAT64)

    def test_lookml_dimension_view_field_for_schema_field_datetime(self) -> None:
        schema_field = bigquery.SchemaField("test_field", "DATETIME")
        result = lookml_view_field_for_schema_field(schema_field)
        self.assertIsInstance(result, DimensionGroupLookMLViewField)
        self.assertEqual(result.field_name, "test_field")

    def test_lookml_dimension_view_field_for_schema_field_date(self) -> None:
        schema_field = bigquery.SchemaField("test_field", "DATE")
        result = lookml_view_field_for_schema_field(schema_field)
        self.assertIsInstance(result, DimensionGroupLookMLViewField)
        self.assertEqual(result.field_name, "test_field")

    def test_lookml_dimension_view_field_for_schema_field_integer(self) -> None:
        schema_field = bigquery.SchemaField("test_field", "INTEGER")
        result = lookml_view_field_for_schema_field(schema_field)
        self.assertIsInstance(result, DimensionLookMLViewField)
        self.assertEqual(result.field_name, "test_field")
