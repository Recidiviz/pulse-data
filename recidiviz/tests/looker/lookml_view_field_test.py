# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests functionality of LookMLViewField functions"""

import unittest

from recidiviz.looker.lookml_view_field import (
    DimensionGroupLookMLViewField,
    DimensionLookMLViewField,
    MeasureLookMLViewField,
    ParameterLookMLViewField,
)
from recidiviz.looker.lookml_view_field_parameter import (
    LookMLFieldParameter,
    LookMLFieldType,
    LookMLTimeframesOption,
)


class LookMLViewTest(unittest.TestCase):
    """Tests correctness of LookML view field generation"""

    def test_view_field_disallowed_parameters_sql_throw(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"The following parameter types are not allowed for "
            r"\[parameter\] fields: \['sql'\]",
        ):
            _ = ParameterLookMLViewField(
                field_name="my_parameter",
                parameters=[
                    LookMLFieldParameter.allowed_value("First Value", "value1"),
                    LookMLFieldParameter.default_value("value1"),
                    LookMLFieldParameter.sql("${TABLE}.value1"),
                ],
            )

    def test_view_field_disallowed_parameters_precision_throw(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"The following parameter types are not allowed for "
            r"\[dimension\] fields: \['allowed_value', 'default_value'\]",
        ):
            _ = DimensionLookMLViewField(
                field_name="my_dimension",
                parameters=[
                    LookMLFieldParameter.allowed_value("First Value", "value1"),
                    LookMLFieldParameter.allowed_value("Second Value", "value2"),
                    LookMLFieldParameter.default_value("value1"),
                    LookMLFieldParameter.sql("${TABLE}.value1"),
                ],
            )

    def test_disallowed_timeframes_throw(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"The following parameter types are not allowed for "
            r"\[dimension\] fields: \['timeframes'\]",
        ):
            _ = DimensionLookMLViewField(
                field_name="my_dimension",
                parameters=[
                    LookMLFieldParameter.timeframes(
                        [LookMLTimeframesOption.DATE, LookMLTimeframesOption.MONTH]
                    )
                ],
            )

    def test_view_field_empty_timeframes_throw(self) -> None:
        with self.assertRaises(ValueError):
            _ = DimensionGroupLookMLViewField(
                field_name="my_dimension_group",
                parameters=[
                    LookMLFieldParameter.timeframes([]),
                ],
            )

    def test_view_field_repeated_parameters_throw(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Defined field parameters contain repeated key: \['label', 'label', 'sql'\]",
        ):
            _ = MeasureLookMLViewField(
                field_name="my_measure",
                parameters=[
                    LookMLFieldParameter.label("my_param"),
                    LookMLFieldParameter.label("jk_my_param"),
                    LookMLFieldParameter.sql("${TABLE}.value1"),
                ],
            )

    def test_view_field_repeated_parameters_allowed_value(self) -> None:
        view_field = ParameterLookMLViewField(
            field_name="my_parameter",
            parameters=[
                LookMLFieldParameter.allowed_value("First Value", "value1"),
                LookMLFieldParameter.allowed_value("Second Value", "value2"),
                LookMLFieldParameter.default_value("value1"),
            ],
        ).build()
        expected_view_field = """
  parameter: my_parameter {
    allowed_value: {
      label: "First Value"
      value: "value1"
    }
    allowed_value: {
      label: "Second Value"
      value: "value2"
    }
    default_value: "value1"
  }"""
        self.assertEqual(view_field, expected_view_field)

    def test_view_field_date_type_datatype_throw(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Datatype parameter must be supplied when type parameter is `date`.",
        ):
            _ = DimensionLookMLViewField(
                field_name="my_date_dimension",
                parameters=[
                    LookMLFieldParameter.label("my_date_dimension"),
                    LookMLFieldParameter.type(LookMLFieldType.DATE),
                    LookMLFieldParameter.sql("${TABLE}.value1"),
                ],
            )

    def test_view_field_date_type_datatype(self) -> None:
        view_field = DimensionLookMLViewField.for_column(
            column_name="date_dim",
            field_type=LookMLFieldType.DATE,
        ).build()
        expected_view_field = """
  dimension: date_dim {
    type: date
    datatype: date
    sql: ${TABLE}.date_dim ;;
  }"""
        self.assertEqual(view_field, expected_view_field)

    def test_view_field_dimension_group_timeframes_one_option(self) -> None:
        view_field = DimensionGroupLookMLViewField(
            field_name="my_dimension_group",
            parameters=[
                LookMLFieldParameter.type(LookMLFieldType.TIME),
                LookMLFieldParameter.timeframes([LookMLTimeframesOption.DATE]),
            ],
        ).build()
        expected_view_field = """
  dimension_group: my_dimension_group {
    type: time
    timeframes: [
      date
    ]
  }"""
        self.assertEqual(view_field, expected_view_field)

    def test_view_field_dimension_group_timeframes(self) -> None:
        view_field = DimensionGroupLookMLViewField(
            field_name="my_dimension_group",
            parameters=[
                LookMLFieldParameter.type(LookMLFieldType.TIME),
                LookMLFieldParameter.timeframes(
                    [LookMLTimeframesOption.DATE, LookMLTimeframesOption.MONTH]
                ),
                LookMLFieldParameter.sql("${TABLE}.time"),
            ],
        ).build()
        expected_view_field = """
  dimension_group: my_dimension_group {
    type: time
    timeframes: [
      date,
      month
    ]
    sql: ${TABLE}.time ;;
  }"""
        self.assertEqual(view_field, expected_view_field)

    def test_view_field_dimension_group_missing_type_throw(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Type parameter must be `duration` or `time` for a `dimension_group`.",
        ):
            _ = DimensionGroupLookMLViewField(
                field_name="my_dimension_group",
                parameters=[
                    LookMLFieldParameter.timeframes(
                        [LookMLTimeframesOption.DATE, LookMLTimeframesOption.MONTH]
                    ),
                ],
            )

    def test_view_field_timeframes_wrong_type_throw(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"`timeframes` may only be used when type parameter is `time`.",
        ):
            _ = DimensionGroupLookMLViewField(
                field_name="my_dimension_group",
                parameters=[
                    LookMLFieldParameter.type(LookMLFieldType.DURATION),
                    LookMLFieldParameter.timeframes(
                        [LookMLTimeframesOption.DATE, LookMLTimeframesOption.MONTH]
                    ),
                ],
            )

    def test_view_field_primary_key(self) -> None:
        view_field = DimensionLookMLViewField(
            field_name="my_dim",
            parameters=[LookMLFieldParameter.primary_key(True)],
        ).build()
        expected_view_field = """
  dimension: my_dim {
    primary_key: yes
  }"""
        self.assertEqual(view_field, expected_view_field)

    def test_view_field_drill_fields(self) -> None:
        view_field = DimensionLookMLViewField(
            field_name="my_dim",
            parameters=[
                LookMLFieldParameter.drill_fields(["field1", "field2", "field3"])
            ],
        ).build()
        expected_view_field = """
  dimension: my_dim {
    drill_fields: [field1, field2, field3]
  }"""
        self.assertEqual(view_field, expected_view_field)

    def test_view_field_suggestions(self) -> None:
        view_field = DimensionLookMLViewField(
            field_name="my_dim",
            parameters=[
                LookMLFieldParameter.suggestions(["option1", "option2", "option3"])
            ],
        ).build()
        expected_view_field = """
  dimension: my_dim {
    suggestions: ["option1", "option2", "option3"]
  }"""
        self.assertEqual(view_field, expected_view_field)

    def test_datetime_view_field(self) -> None:
        view_field = DimensionGroupLookMLViewField.for_datetime_column(
            column_name="my_datetime"
        ).build()
        expected_view_field = """
  dimension_group: my_datetime {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    convert_tz: no
    datatype: datetime
    sql: ${TABLE}.my_datetime ;;
  }"""
        self.assertEqual(view_field, expected_view_field)

    def test_date_view_field(self) -> None:
        view_field = DimensionGroupLookMLViewField.for_date_column(
            column_name="my_date"
        ).build()
        expected_view_field = """
  dimension_group: my_date {
    type: time
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year
    ]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.my_date ;;
  }"""
        self.assertEqual(view_field, expected_view_field)
