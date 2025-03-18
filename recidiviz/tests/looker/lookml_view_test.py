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
"""Tests functionality of LookMLView functions"""

import unittest

from google.cloud.bigquery import SchemaField

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.calculator.query.state.views.sessions.sessions_views import (
    COMPARTMENT_SUB_SESSIONS_VIEW_BUILDER,
)
from recidiviz.looker.lookml_view import LookMLView
from recidiviz.looker.lookml_view_field import (
    DimensionLookMLViewField,
    MeasureLookMLViewField,
    ParameterLookMLViewField,
)
from recidiviz.looker.lookml_view_field_parameter import (
    LookMLFieldParameter,
    LookMLFieldType,
)
from recidiviz.looker.lookml_view_source_table import LookMLViewSourceTable


class LookMLViewTest(unittest.TestCase):
    """Tests correctness of LookML view generation"""

    # test empty view
    def test_empty_lookml_view(self) -> None:
        bq_view_builder = COMPARTMENT_SUB_SESSIONS_VIEW_BUILDER
        view = LookMLView(
            view_name="my_sessions_view",
            table=LookMLViewSourceTable.sql_table_address(
                bq_view_builder.table_for_query
            ),
        ).build()
        expected_view = """view: my_sessions_view {
  sql_table_name: sessions.compartment_sub_sessions_materialized ;;
}"""
        self.assertEqual(view, expected_view)

    def test_extension_required_lookml_view(self) -> None:
        view = LookMLView(
            view_name="my_extension_required_view",
            extension_required=True,
        ).build()
        expected_view = """view: my_extension_required_view {
  extension: required

}"""
        self.assertEqual(view, expected_view)

    def test_derived_table_lookml_view(self) -> None:
        derived_table_query = """
    SELECT col1, col2
    FROM `my_dataset.my_table_materialized`"""
        view = LookMLView(
            view_name="my_sessions_view",
            table=LookMLViewSourceTable.derived_table(derived_table_query),
            fields=[],
        ).build()
        expected_view = """view: my_sessions_view {
  derived_table: {
    sql: 
    SELECT col1, col2
    FROM `my_dataset.my_table_materialized` ;;
  }
}"""
        self.assertEqual(view, expected_view)

    def test_derived_table_with_measure(self) -> None:
        derived_table_query = """
    SELECT my_metric
    FROM `my_dataset.my_table_materialized`"""
        view = LookMLView(
            view_name="my_sessions_view_2",
            table=LookMLViewSourceTable.derived_table(derived_table_query),
            fields=[
                MeasureLookMLViewField(
                    field_name="metric_value",
                    parameters=[
                        LookMLFieldParameter.description(
                            "This is a description of a measure"
                        ),
                        LookMLFieldParameter.type(LookMLFieldType.NUMBER),
                        LookMLFieldParameter.view_label("All Measures"),
                        LookMLFieldParameter.precision(3),
                        LookMLFieldParameter.sql("SUM(${TABLE}.my_metric)"),
                    ],
                )
            ],
        ).build()
        expected_view = """view: my_sessions_view_2 {
  derived_table: {
    sql: 
    SELECT my_metric
    FROM `my_dataset.my_table_materialized` ;;
  }

  measure: metric_value {
    description: "This is a description of a measure"
    type: number
    view_label: "All Measures"
    precision: 3
    sql: SUM(${TABLE}.my_metric) ;;
  }
}"""
        self.assertEqual(view, expected_view)

    def test_derived_table_with_dimensions(self) -> None:
        derived_table_query = """
    SELECT my_metric
    FROM `my_dataset.my_table_materialized`"""
        view = LookMLView(
            view_name="my_sessions_view_3",
            table=LookMLViewSourceTable.derived_table(derived_table_query),
            fields=[
                DimensionLookMLViewField(
                    field_name="days_since_event_metric",
                    parameters=[
                        LookMLFieldParameter.description("Metric in days"),
                        LookMLFieldParameter.type(LookMLFieldType.NUMBER),
                        LookMLFieldParameter.view_label("All Metrics"),
                        LookMLFieldParameter.group_label("Days"),
                        LookMLFieldParameter.sql("${TABLE}.my_metric"),
                    ],
                ),
                DimensionLookMLViewField(
                    field_name="years_since_event_metric",
                    parameters=[
                        LookMLFieldParameter.description("Metric in years"),
                        LookMLFieldParameter.type(LookMLFieldType.NUMBER),
                        LookMLFieldParameter.view_label("All Metrics"),
                        LookMLFieldParameter.group_label("Years"),
                        LookMLFieldParameter.sql("${TABLE}.my_metric / 365"),
                    ],
                ),
                ParameterLookMLViewField(
                    field_name="time_granularity",
                    parameters=[
                        LookMLFieldParameter.allowed_value("Years", "years"),
                        LookMLFieldParameter.allowed_value("Days", "days"),
                        LookMLFieldParameter.default_value("days"),
                    ],
                ),
            ],
        ).build()
        expected_view = """view: my_sessions_view_3 {
  derived_table: {
    sql: 
    SELECT my_metric
    FROM `my_dataset.my_table_materialized` ;;
  }

  dimension: days_since_event_metric {
    description: "Metric in days"
    type: number
    view_label: "All Metrics"
    group_label: "Days"
    sql: ${TABLE}.my_metric ;;
  }

  dimension: years_since_event_metric {
    description: "Metric in years"
    type: number
    view_label: "All Metrics"
    group_label: "Years"
    sql: ${TABLE}.my_metric / 365 ;;
  }

  parameter: time_granularity {
    allowed_value: {
      label: "Years"
      value: "years"
    }
    allowed_value: {
      label: "Days"
      value: "days"
    }
    default_value: "days"
  }
}"""
        self.assertEqual(view, expected_view)

    def test_view_repeated_fields_throw(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"Duplicate field names found in \['my_field', 'my_field'\]"
        ):
            _ = LookMLView(
                view_name="my_view",
                table=LookMLViewSourceTable.sql_table_address(
                    BigQueryAddress(dataset_id="my_dataset", table_id="my_table")
                ),
                fields=[
                    DimensionLookMLViewField(
                        field_name="my_field",
                        parameters=[
                            LookMLFieldParameter.description("Field description"),
                        ],
                    ),
                    DimensionLookMLViewField(
                        field_name="my_field",
                        parameters=[
                            LookMLFieldParameter.description("Field description 2"),
                        ],
                    ),
                ],
            )

    def test_view_for_big_query_table(self) -> None:
        view = LookMLView.for_big_query_table(
            dataset_id="my_dataset",
            table_id="my_table",
            fields=[
                DimensionLookMLViewField(
                    field_name="my_field",
                    parameters=[
                        LookMLFieldParameter.description("Field description"),
                    ],
                )
            ],
        ).build()
        expected_view = """view: my_table {
  sql_table_name: my_dataset.my_table ;;

  dimension: my_field {
    description: "Field description"
  }
}"""
        self.assertEqual(view, expected_view)

    def test_referenced_view_fields_exist(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Fields \{'full_name'\} referenced in \[my_field\] do not exist in view fields \{'my_field'\}",
        ):
            _ = LookMLView(
                view_name="my_view",
                table=LookMLViewSourceTable.sql_table_address(
                    BigQueryAddress(dataset_id="my_dataset", table_id="my_table")
                ),
                fields=[
                    DimensionLookMLViewField(
                        field_name="my_field",
                        parameters=[
                            LookMLFieldParameter.description("Field description"),
                            LookMLFieldParameter.sql(
                                "CONCAT("
                                'INITCAP(JSON_EXTRACT_SCALAR(${full_name}, "$.given_names")),'
                                '" ",'
                                'INITCAP(JSON_EXTRACT_SCALAR(${full_name}, "$.surname"))'
                                ")"
                            ),
                        ],
                    ),
                ],
            )

        # Since we don't have context for all the views that exist
        # we can't check if the referenced field from another view exists
        _ = LookMLView(
            view_name="my_view",
            table=LookMLViewSourceTable.sql_table_address(
                BigQueryAddress(dataset_id="my_dataset", table_id="my_table")
            ),
            fields=[
                DimensionLookMLViewField(
                    field_name="my_field",
                    parameters=[
                        LookMLFieldParameter.description("Field description"),
                        LookMLFieldParameter.sql(
                            "CONCAT("
                            'INITCAP(JSON_EXTRACT_SCALAR(another_view.full_name, "$.given_names")),'
                            '" ",'
                            'INITCAP(JSON_EXTRACT_SCALAR(another_view.full_name, "$.surname"))'
                            ")"
                        ),
                    ],
                ),
            ],
        )

    def test_table_fields_exist(self) -> None:
        view = LookMLView(
            view_name="my_view",
            table=LookMLViewSourceTable.sql_table_address(
                BigQueryAddress(dataset_id="my_dataset", table_id="my_table")
            ),
            fields=[
                MeasureLookMLViewField(
                    field_name="referrals_array",
                    parameters=[
                        LookMLFieldParameter.type(LookMLFieldType.STRING),
                        LookMLFieldParameter.description(
                            "List in string form of all program referral dates and parenthesized program_id's"
                        ),
                        LookMLFieldParameter.sql(
                            r"""ARRAY_TO_STRING(ARRAY_AGG(
      DISTINCT CONCAT(CAST(${TABLE}.referral_date AS STRING), " (", ${TABLE}.program_id, ")")
      ORDER BY CONCAT(CAST(${TABLE}.referral_date AS STRING), " (", ${TABLE}.program_id, ")")
    ), ";\r\n")"""
                        ),
                    ],
                ),
            ],
        )

        # Shouldn't crash
        view.validate_referenced_fields_exist_in_schema(
            schema_fields=[
                SchemaField(name="referral_date", field_type="STRING"),
                SchemaField(name="program_id", field_type="STRING"),
            ]
        )

        with self.assertRaisesRegex(
            ValueError,
            r"Fields \{'program_id'\} referenced in \[referrals_array\] do not exist in schema fields \{'referral_date'\}",
        ):
            view.validate_referenced_fields_exist_in_schema(
                schema_fields=[SchemaField(name="referral_date", field_type="STRING")]
            )
