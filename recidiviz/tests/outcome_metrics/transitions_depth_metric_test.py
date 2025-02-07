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
"""Tests functionality of transitions baseline and depth view builder generation functions"""
from datetime import date

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import schema_field_for_type
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.observations.views.events.person.impact_transition import (
    VIEW_BUILDER as impact_transition_view_builder,
)
from recidiviz.outcome_metrics.views.transitions_breadth_metric import (
    get_transitions_breadth_metric_for_year,
)
from recidiviz.outcome_metrics.views.transitions_depth_metric import (
    get_transitions_baseline_metric_for_year,
    get_transitions_depth_metric_for_year,
)
from recidiviz.tests.big_query.simple_big_query_view_builder_test_case import (
    SimpleBigQueryViewBuilderTestCase,
)


class TransitionsBaselineMetricForYearTest(SimpleBigQueryViewBuilderTestCase):
    """Tests `get_transitions_baseline_metric_for_year` function"""

    METRIC_YEAR = 2020
    impact_transition_address = impact_transition_view_builder.table_for_query

    @property
    def view_builder(self) -> SimpleBigQueryViewBuilder:
        return get_transitions_baseline_metric_for_year(metric_year=self.METRIC_YEAR)

    @property
    def parent_schemas(self) -> dict[BigQueryAddress, list[bigquery.SchemaField]]:
        return {
            self.impact_transition_address: [
                schema_field_for_type("person_id", int),
                schema_field_for_type("state_code", str),
                schema_field_for_type("event_date", date),
                schema_field_for_type("decarceral_impact_type", str),
                schema_field_for_type(
                    "is_within_one_year_before_full_state_launch_month", str
                ),
                schema_field_for_type("full_state_launch_date", str),
                schema_field_for_type("product_transition_type", str),
                schema_field_for_type("has_mandatory_due_date", str),
                schema_field_for_type("is_jii_transition", str),
                schema_field_for_type("weight_factor", str),
                schema_field_for_type("delta_direction_factor", str),
            ],
        }

    def test_baseline_for_tool_launched_prior_to_metric_year(self) -> None:
        impact_transition_observations = [
            {
                "person_id": 1,
                "state_code": "US_OZ",
                "event_date": date(2019, 1, 15),
                "decarceral_impact_type": "SENTENCE_LENGTH_REDUCTION",
                "is_within_one_year_before_full_state_launch_month": "true",
                "full_state_launch_date": "2019-06-18",
                "product_transition_type": "courtroom_1",
                "has_mandatory_due_date": "false",
                "is_jii_transition": "true",
                "weight_factor": "1.0",
                "delta_direction_factor": "1",
            },
            {
                "person_id": 2,
                "state_code": "US_OZ",
                "event_date": date(2019, 12, 5),
                "decarceral_impact_type": "SENTENCE_LENGTH_REDUCTION",
                "is_within_one_year_before_full_state_launch_month": "true",
                "full_state_launch_date": "2019-06-18",
                "product_transition_type": "courtroom_1",
                "has_mandatory_due_date": "false",
                "is_jii_transition": "true",
                "weight_factor": "1.0",
                "delta_direction_factor": "1",
            },
        ]

        # Baseline is the same every month because the tool was launched prior to metric year
        expected_output = [
            {
                "metric_month": date(2020, 1, 1),
                "weight_factor": "1.0",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.1667,
                "monthly_baseline_new_tools": 0,
                "total_baseline": 0.1667,
            },
            {
                "metric_month": date(2020, 2, 1),
                "weight_factor": "1.0",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.1667,
                "monthly_baseline_new_tools": 0,
                "total_baseline": 0.1667,
            },
            {
                "metric_month": date(2020, 3, 1),
                "weight_factor": "1.0",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.1667,
                "monthly_baseline_new_tools": 0,
                "total_baseline": 0.1667,
            },
            {
                "metric_month": date(2020, 4, 1),
                "weight_factor": "1.0",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.1667,
                "monthly_baseline_new_tools": 0,
                "total_baseline": 0.1667,
            },
            {
                "metric_month": date(2020, 5, 1),
                "weight_factor": "1.0",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.1667,
                "monthly_baseline_new_tools": 0,
                "total_baseline": 0.1667,
            },
            {
                "metric_month": date(2020, 6, 1),
                "weight_factor": "1.0",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.1667,
                "monthly_baseline_new_tools": 0,
                "total_baseline": 0.1667,
            },
            {
                "metric_month": date(2020, 7, 1),
                "weight_factor": "1.0",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.1667,
                "monthly_baseline_new_tools": 0,
                "total_baseline": 0.1667,
            },
            {
                "metric_month": date(2020, 8, 1),
                "weight_factor": "1.0",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.1667,
                "monthly_baseline_new_tools": 0,
                "total_baseline": 0.1667,
            },
            {
                "metric_month": date(2020, 9, 1),
                "weight_factor": "1.0",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.1667,
                "monthly_baseline_new_tools": 0,
                "total_baseline": 0.1667,
            },
            {
                "metric_month": date(2020, 10, 1),
                "weight_factor": "1.0",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.1667,
                "monthly_baseline_new_tools": 0,
                "total_baseline": 0.1667,
            },
            {
                "metric_month": date(2020, 11, 1),
                "weight_factor": "1.0",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.1667,
                "monthly_baseline_new_tools": 0,
                "total_baseline": 0.1667,
            },
            {
                "metric_month": date(2020, 12, 1),
                "weight_factor": "1.0",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.1667,
                "monthly_baseline_new_tools": 0,
                "total_baseline": 0.1667,
            },
        ]

        self.maxDiff = None

        self.run_simple_view_builder_query_test_from_data(
            {
                self.impact_transition_address: impact_transition_observations,
            },
            expected_output,
        )

    def test_baseline_for_tool_launched_during_metric_year(self) -> None:
        impact_transition_observations = [
            {
                "person_id": 3,
                "state_code": "US_OZ",
                "event_date": date(2019, 5, 10),
                "decarceral_impact_type": "NEW_IMPACT_TYPE",
                "is_within_one_year_before_full_state_launch_month": "true",
                "full_state_launch_date": "2020-04-26",
                "product_transition_type": "workflows_discretionary",
                "has_mandatory_due_date": "false",
                "is_jii_transition": "true",
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
            {
                "person_id": 4,
                "state_code": "US_OZ",
                "event_date": date(2019, 9, 1),
                "decarceral_impact_type": "NEW_IMPACT_TYPE",
                "is_within_one_year_before_full_state_launch_month": "true",
                "full_state_launch_date": "2020-04-26",
                "product_transition_type": "workflows_discretionary",
                "has_mandatory_due_date": "false",
                "is_jii_transition": "true",
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
            {
                "person_id": 5,
                "state_code": "US_OZ",
                "event_date": date(2019, 9, 3),
                "decarceral_impact_type": "NEW_IMPACT_TYPE",
                "is_within_one_year_before_full_state_launch_month": "true",
                "full_state_launch_date": "2020-04-26",
                "product_transition_type": "workflows_discretionary",
                "has_mandatory_due_date": "false",
                "is_jii_transition": "true",
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
        ]

        expected_output = [
            {
                "metric_month": date(2020, 1, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.0,
                "monthly_baseline_new_tools": 0.0,
                "total_baseline": 0,
            },
            {
                "metric_month": date(2020, 2, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.0,
                "monthly_baseline_new_tools": 0.0,
                "total_baseline": 0,
            },
            {
                "metric_month": date(2020, 3, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.0,
                "monthly_baseline_new_tools": 0.0,
                "total_baseline": 0,
            },
            {
                "metric_month": date(2020, 4, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 1,
                "monthly_baseline_old_tools": 0.0,
                "monthly_baseline_new_tools": 0.25,
                "total_baseline": 0.25,
            },
            {
                "metric_month": date(2020, 5, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.0,
                "monthly_baseline_new_tools": 0.25,
                "total_baseline": 0.25,
            },
            {
                "metric_month": date(2020, 6, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.0,
                "monthly_baseline_new_tools": 0.25,
                "total_baseline": 0.25,
            },
            {
                "metric_month": date(2020, 7, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.0,
                "monthly_baseline_new_tools": 0.25,
                "total_baseline": 0.25,
            },
            {
                "metric_month": date(2020, 8, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.0,
                "monthly_baseline_new_tools": 0.25,
                "total_baseline": 0.25,
            },
            {
                "metric_month": date(2020, 9, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.0,
                "monthly_baseline_new_tools": 0.25,
                "total_baseline": 0.25,
            },
            {
                "metric_month": date(2020, 10, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.0,
                "monthly_baseline_new_tools": 0.25,
                "total_baseline": 0.25,
            },
            {
                "metric_month": date(2020, 11, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.0,
                "monthly_baseline_new_tools": 0.25,
                "total_baseline": 0.25,
            },
            {
                "metric_month": date(2020, 12, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.0,
                "monthly_baseline_new_tools": 0.25,
                "total_baseline": 0.25,
            },
        ]

        self.maxDiff = None

        self.run_simple_view_builder_query_test_from_data(
            {
                self.impact_transition_address: impact_transition_observations,
            },
            expected_output,
        )

    def test_baseline_for_tool_launched_during_metric_year_no_transitions(self) -> None:
        """Test that only transitions within a year of full-state launch date
        are included for tools launched within the metric year"""
        impact_transition_observations = [
            {
                "person_id": 6,
                "state_code": "US_OZ",
                "event_date": date(2019, 1, 10),
                "decarceral_impact_type": "NEW_IMPACT_TYPE",
                "is_within_one_year_before_full_state_launch_month": "false",
                "full_state_launch_date": "2020-04-26",
                "product_transition_type": "workflows_discretionary",
                "has_mandatory_due_date": "false",
                "is_jii_transition": "true",
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
            {
                "person_id": 7,
                "state_code": "US_OZ",
                "event_date": date(2019, 2, 1),
                "decarceral_impact_type": "NEW_IMPACT_TYPE",
                "is_within_one_year_before_full_state_launch_month": "false",
                "full_state_launch_date": "2020-04-26",
                "product_transition_type": "workflows_discretionary",
                "has_mandatory_due_date": "false",
                "is_jii_transition": "true",
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
            {
                "person_id": 8,
                "state_code": "US_OZ",
                "event_date": date(2019, 9, 3),
                "decarceral_impact_type": "NEW_IMPACT_TYPE",
                "is_within_one_year_before_full_state_launch_month": "true",
                "full_state_launch_date": "2020-04-26",
                "product_transition_type": "workflows_discretionary",
                "has_mandatory_due_date": "false",
                "is_jii_transition": "true",
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
        ]

        expected_output = [
            {
                "metric_month": date(2020, 1, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0,
                "monthly_baseline_new_tools": 0,
                "total_baseline": 0,
            },
            {
                "metric_month": date(2020, 2, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0,
                "monthly_baseline_new_tools": 0,
                "total_baseline": 0,
            },
            {
                "metric_month": date(2020, 3, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0,
                "monthly_baseline_new_tools": 0,
                "total_baseline": 0,
            },
            {
                "metric_month": date(2020, 4, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 1,
                "monthly_baseline_old_tools": 0,
                "monthly_baseline_new_tools": 0.0833,
                "total_baseline": 0.0833,
            },
            {
                "metric_month": date(2020, 5, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0,
                "monthly_baseline_new_tools": 0.0833,
                "total_baseline": 0.0833,
            },
            {
                "metric_month": date(2020, 6, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0,
                "monthly_baseline_new_tools": 0.0833,
                "total_baseline": 0.0833,
            },
            {
                "metric_month": date(2020, 7, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0,
                "monthly_baseline_new_tools": 0.0833,
                "total_baseline": 0.0833,
            },
            {
                "metric_month": date(2020, 8, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0,
                "monthly_baseline_new_tools": 0.0833,
                "total_baseline": 0.0833,
            },
            {
                "metric_month": date(2020, 9, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0,
                "monthly_baseline_new_tools": 0.0833,
                "total_baseline": 0.0833,
            },
            {
                "metric_month": date(2020, 10, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0,
                "monthly_baseline_new_tools": 0.0833,
                "total_baseline": 0.0833,
            },
            {
                "metric_month": date(2020, 11, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0,
                "monthly_baseline_new_tools": 0.0833,
                "total_baseline": 0.0833,
            },
            {
                "metric_month": date(2020, 12, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0,
                "monthly_baseline_new_tools": 0.0833,
                "total_baseline": 0.0833,
            },
        ]

        self.run_simple_view_builder_query_test_from_data(
            {
                self.impact_transition_address: impact_transition_observations,
            },
            expected_output,
        )

    def test_baseline_for_tool_launched_prior_and_during_metric_year(self) -> None:
        impact_transition_observations = [
            {
                "person_id": 1,
                "state_code": "US_OZ",
                "event_date": date(2019, 1, 15),
                "decarceral_impact_type": "SENTENCE_LENGTH_REDUCTION",
                "is_within_one_year_before_full_state_launch_month": "true",
                "full_state_launch_date": "2019-06-18",
                "product_transition_type": "courtroom_1",
                "has_mandatory_due_date": "false",
                "is_jii_transition": "true",
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
            {
                "person_id": 2,
                "state_code": "US_OZ",
                "event_date": date(2019, 12, 5),
                "decarceral_impact_type": "SENTENCE_LENGTH_REDUCTION",
                "is_within_one_year_before_full_state_launch_month": "true",
                "full_state_launch_date": "2019-06-18",
                "product_transition_type": "courtroom_1",
                "has_mandatory_due_date": "false",
                "is_jii_transition": "true",
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
            {
                "person_id": 3,
                "state_code": "US_OZ",
                "event_date": date(2019, 5, 10),
                "decarceral_impact_type": "NEW_IMPACT_TYPE",
                "is_within_one_year_before_full_state_launch_month": "true",
                "full_state_launch_date": "2020-04-26",
                "product_transition_type": "workflows_discretionary",
                "has_mandatory_due_date": "false",
                "is_jii_transition": "true",
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
            {
                "person_id": 4,
                "state_code": "US_OZ",
                "event_date": date(2019, 9, 1),
                "decarceral_impact_type": "NEW_IMPACT_TYPE",
                "is_within_one_year_before_full_state_launch_month": "true",
                "full_state_launch_date": "2020-04-26",
                "product_transition_type": "workflows_discretionary",
                "has_mandatory_due_date": "false",
                "is_jii_transition": "true",
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
            {
                "person_id": 5,
                "state_code": "US_OZ",
                "event_date": date(2019, 9, 3),
                "decarceral_impact_type": "NEW_IMPACT_TYPE",
                "is_within_one_year_before_full_state_launch_month": "true",
                "full_state_launch_date": "2020-04-26",
                "product_transition_type": "workflows_discretionary",
                "has_mandatory_due_date": "false",
                "is_jii_transition": "true",
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
            {
                "person_id": 6,
                "state_code": "US_OZ",
                "event_date": date(2019, 6, 1),
                "decarceral_impact_type": "OTHER_NEW_IMPACT_TYPE",
                "is_within_one_year_before_full_state_launch_month": "false",
                "full_state_launch_date": "2020-11-01",
                "product_transition_type": "workflows_mandatory",
                "has_mandatory_due_date": "true",
                "is_jii_transition": "true",
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
            {
                "person_id": 7,
                "state_code": "US_OZ",
                "event_date": date(2019, 12, 14),
                "decarceral_impact_type": "OTHER_NEW_IMPACT_TYPE",
                "is_within_one_year_before_full_state_launch_month": "true",
                "full_state_launch_date": "2020-11-01",
                "product_transition_type": "workflows_mandatory",
                "has_mandatory_due_date": "true",
                "is_jii_transition": "true",
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
        ]

        expected_output = [
            {
                "metric_month": date(2020, 1, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.1667,
                "monthly_baseline_new_tools": 0,
                "total_baseline": 0.1667,
            },
            {
                "metric_month": date(2020, 2, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.1667,
                "monthly_baseline_new_tools": 0,
                "total_baseline": 0.1667,
            },
            {
                "metric_month": date(2020, 3, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.1667,
                "monthly_baseline_new_tools": 0,
                "total_baseline": 0.1667,
            },
            {
                "metric_month": date(2020, 4, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 1,
                "monthly_baseline_old_tools": 0.1667,
                "monthly_baseline_new_tools": 0.25,
                "total_baseline": 0.4167,
            },
            {
                "metric_month": date(2020, 5, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.1667,
                "monthly_baseline_new_tools": 0.25,
                "total_baseline": 0.4167,
            },
            {
                "metric_month": date(2020, 6, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.1667,
                "monthly_baseline_new_tools": 0.25,
                "total_baseline": 0.4167,
            },
            {
                "metric_month": date(2020, 7, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.1667,
                "monthly_baseline_new_tools": 0.25,
                "total_baseline": 0.4167,
            },
            {
                "metric_month": date(2020, 8, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.1667,
                "monthly_baseline_new_tools": 0.25,
                "total_baseline": 0.4167,
            },
            {
                "metric_month": date(2020, 9, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.1667,
                "monthly_baseline_new_tools": 0.25,
                "total_baseline": 0.4167,
            },
            {
                "metric_month": date(2020, 10, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.1667,
                "monthly_baseline_new_tools": 0.25,
                "total_baseline": 0.4167,
            },
            {
                "metric_month": date(2020, 11, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 1,
                "monthly_baseline_old_tools": 0.1667,
                "monthly_baseline_new_tools": 0.3333,
                "total_baseline": 0.5,
            },
            {
                "metric_month": date(2020, 12, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.1667,
                "monthly_baseline_new_tools": 0.3333,
                "total_baseline": 0.5,
            },
        ]

        self.run_simple_view_builder_query_test_from_data(
            {
                self.impact_transition_address: impact_transition_observations,
            },
            expected_output,
        )

    def test_baseline_for_tool_launched_during_metric_year_dedup(self) -> None:
        """Test that events are deduplicated by decarceral impact type, has_mandatory_due_date, is_jii_transition, person, and date"""
        impact_transition_observations = [
            {
                "person_id": 10,
                "state_code": "US_OZ",
                "event_date": date(2019, 1, 10),
                "decarceral_impact_type": "FIRST_IMPACT_TYPE",
                "is_within_one_year_before_full_state_launch_month": "true",
                "full_state_launch_date": "2019-08-05",
                "product_transition_type": "workflows_discretionary",
                "has_mandatory_due_date": "true",
                "is_jii_transition": "true",
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
            {
                "person_id": 10,
                "state_code": "US_OZ",
                "event_date": date(2019, 1, 10),
                "decarceral_impact_type": "SECOND_IMPACT_TYPE",
                "is_within_one_year_before_full_state_launch_month": "true",
                "full_state_launch_date": "2019-08-05",
                "product_transition_type": "workflows_discretionary",
                "has_mandatory_due_date": "true",
                "is_jii_transition": "true",
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
            {
                "person_id": 10,
                "state_code": "US_OZ",
                "event_date": date(2019, 1, 11),
                "decarceral_impact_type": "SECOND_IMPACT_TYPE",
                "is_within_one_year_before_full_state_launch_month": "true",
                "full_state_launch_date": "2019-08-05",
                "product_transition_type": "workflows_discretionary",
                "has_mandatory_due_date": "true",
                "is_jii_transition": "true",
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
            {
                "person_id": 11,
                "state_code": "US_OZ",
                "event_date": date(2019, 12, 1),
                "decarceral_impact_type": "SECOND_IMPACT_TYPE",
                "is_within_one_year_before_full_state_launch_month": "true",
                "full_state_launch_date": "2020-04-26",
                "product_transition_type": "workflows_discretionary",
                "has_mandatory_due_date": "true",
                "is_jii_transition": "true",
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
            {
                "person_id": 11,
                "state_code": "US_OZ",
                "event_date": date(2019, 12, 1),
                "decarceral_impact_type": "SECOND_IMPACT_TYPE",
                "is_within_one_year_before_full_state_launch_month": "true",
                "full_state_launch_date": "2020-04-26",
                "product_transition_type": "workflows_discretionary",
                "has_mandatory_due_date": "true",
                "is_jii_transition": "true",
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
            {
                "person_id": 11,
                "state_code": "US_OZ",
                "event_date": date(2019, 12, 1),
                "decarceral_impact_type": "SECOND_IMPACT_TYPE",
                "is_within_one_year_before_full_state_launch_month": "true",
                "full_state_launch_date": "2020-04-26",
                "product_transition_type": "insights",
                "has_mandatory_due_date": "true",
                "is_jii_transition": "true",
                "weight_factor": "0.9",
                "delta_direction_factor": "1",
            },
        ]

        expected_output = [
            {
                "metric_month": date(2020, 1, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.25,
                "monthly_baseline_new_tools": 0,
                "total_baseline": 0.25,
            },
            {
                "metric_month": date(2020, 2, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.25,
                "monthly_baseline_new_tools": 0,
                "total_baseline": 0.25,
            },
            {
                "metric_month": date(2020, 3, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.25,
                "monthly_baseline_new_tools": 0,
                "total_baseline": 0.25,
            },
            {
                "metric_month": date(2020, 4, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 1,
                "monthly_baseline_old_tools": 0.25,
                "monthly_baseline_new_tools": 0.0833,
                "total_baseline": 0.3333,
            },
            {
                "metric_month": date(2020, 5, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.25,
                "monthly_baseline_new_tools": 0.0833,
                "total_baseline": 0.3333,
            },
            {
                "metric_month": date(2020, 6, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.25,
                "monthly_baseline_new_tools": 0.0833,
                "total_baseline": 0.3333,
            },
            {
                "metric_month": date(2020, 7, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.25,
                "monthly_baseline_new_tools": 0.0833,
                "total_baseline": 0.3333,
            },
            {
                "metric_month": date(2020, 8, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.25,
                "monthly_baseline_new_tools": 0.0833,
                "total_baseline": 0.3333,
            },
            {
                "metric_month": date(2020, 9, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.25,
                "monthly_baseline_new_tools": 0.0833,
                "total_baseline": 0.3333,
            },
            {
                "metric_month": date(2020, 10, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.25,
                "monthly_baseline_new_tools": 0.0833,
                "total_baseline": 0.3333,
            },
            {
                "metric_month": date(2020, 11, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.25,
                "monthly_baseline_new_tools": 0.0833,
                "total_baseline": 0.3333,
            },
            {
                "metric_month": date(2020, 12, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.25,
                "monthly_baseline_new_tools": 0.0833,
                "total_baseline": 0.3333,
            },
            {
                "metric_month": date(2020, 1, 1),
                "weight_factor": "0.9",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.0,
                "monthly_baseline_new_tools": 0.0,
                "total_baseline": 0.0,
            },
            {
                "metric_month": date(2020, 2, 1),
                "weight_factor": "0.9",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.0,
                "monthly_baseline_new_tools": 0.0,
                "total_baseline": 0.0,
            },
            {
                "metric_month": date(2020, 3, 1),
                "weight_factor": "0.9",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.0,
                "monthly_baseline_new_tools": 0.0,
                "total_baseline": 0.0,
            },
            {
                "metric_month": date(2020, 4, 1),
                "weight_factor": "0.9",
                "delta_direction_factor": "1",
                "num_new_launches": 1,
                "monthly_baseline_old_tools": 0.0,
                "monthly_baseline_new_tools": 0.0833,
                "total_baseline": 0.0833,
            },
            {
                "metric_month": date(2020, 5, 1),
                "weight_factor": "0.9",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.0,
                "monthly_baseline_new_tools": 0.0833,
                "total_baseline": 0.0833,
            },
            {
                "metric_month": date(2020, 6, 1),
                "weight_factor": "0.9",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.0,
                "monthly_baseline_new_tools": 0.0833,
                "total_baseline": 0.0833,
            },
            {
                "metric_month": date(2020, 7, 1),
                "weight_factor": "0.9",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.0,
                "monthly_baseline_new_tools": 0.0833,
                "total_baseline": 0.0833,
            },
            {
                "metric_month": date(2020, 8, 1),
                "weight_factor": "0.9",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.0,
                "monthly_baseline_new_tools": 0.0833,
                "total_baseline": 0.0833,
            },
            {
                "metric_month": date(2020, 9, 1),
                "weight_factor": "0.9",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.0,
                "monthly_baseline_new_tools": 0.0833,
                "total_baseline": 0.0833,
            },
            {
                "metric_month": date(2020, 10, 1),
                "weight_factor": "0.9",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.0,
                "monthly_baseline_new_tools": 0.0833,
                "total_baseline": 0.0833,
            },
            {
                "metric_month": date(2020, 11, 1),
                "weight_factor": "0.9",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.0,
                "monthly_baseline_new_tools": 0.0833,
                "total_baseline": 0.0833,
            },
            {
                "metric_month": date(2020, 12, 1),
                "weight_factor": "0.9",
                "delta_direction_factor": "1",
                "num_new_launches": 0,
                "monthly_baseline_old_tools": 0.0,
                "monthly_baseline_new_tools": 0.0833,
                "total_baseline": 0.0833,
            },
        ]

        self.maxDiff = None

        self.run_simple_view_builder_query_test_from_data(
            {
                self.impact_transition_address: impact_transition_observations,
            },
            expected_output,
        )


class TransitionsDepthMetricForYearTest(SimpleBigQueryViewBuilderTestCase):
    """Tests `get_transitions_depth_metric_for_year` function"""

    METRIC_YEAR = 2020
    transitions_breadth_address = get_transitions_breadth_metric_for_year(
        METRIC_YEAR
    ).table_for_query
    transitions_baseline_address = get_transitions_baseline_metric_for_year(
        METRIC_YEAR
    ).table_for_query

    @property
    def view_builder(self) -> SimpleBigQueryViewBuilder:
        return get_transitions_depth_metric_for_year(metric_year=self.METRIC_YEAR)

    @property
    def parent_schemas(self) -> dict[BigQueryAddress, list[bigquery.SchemaField]]:
        return {
            self.transitions_breadth_address: [
                schema_field_for_type("metric_month", date),
                schema_field_for_type("transitions", int),
                schema_field_for_type("weight_factor", str),
                schema_field_for_type("delta_direction_factor", str),
            ],
            self.transitions_baseline_address: [
                schema_field_for_type("metric_month", date),
                schema_field_for_type("total_baseline", float),
                schema_field_for_type("weight_factor", str),
                schema_field_for_type("delta_direction_factor", str),
            ],
        }

    def test_depth_metric_(self) -> None:
        transitions_breadth_data = [
            {
                "metric_month": date(2020, 1, 1),
                "transitions": 45,
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
            {
                "metric_month": date(2020, 2, 1),
                "transitions": 50,
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
            {
                "metric_month": date(2020, 3, 1),
                "transitions": 48,
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
            {
                "metric_month": date(2020, 4, 1),
                "transitions": 58,
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
            {
                "metric_month": date(2020, 5, 1),
                "transitions": 49,
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
            {
                "metric_month": date(2020, 6, 1),
                "transitions": 62,
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
            {
                "metric_month": date(2020, 7, 1),
                "transitions": 68,
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
            {
                "metric_month": date(2020, 8, 1),
                "transitions": 80,
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
            {
                "metric_month": date(2020, 9, 1),
                "transitions": 72,
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
            {
                "metric_month": date(2020, 10, 1),
                "transitions": 79,
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
            {
                "metric_month": date(2020, 11, 1),
                "transitions": 64,
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
            {
                "metric_month": date(2020, 12, 1),
                "transitions": 94,
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
        ]

        transitions_baseline_data = [
            {
                "metric_month": date(2020, 1, 1),
                "total_baseline": 40,
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
            {
                "metric_month": date(2020, 2, 1),
                "total_baseline": 40,
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
            {
                "metric_month": date(2020, 3, 1),
                "total_baseline": 40,
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
            {
                "metric_month": date(2020, 4, 1),
                "total_baseline": 50,
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
            {
                "metric_month": date(2020, 5, 1),
                "total_baseline": 50,
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
            {
                "metric_month": date(2020, 6, 1),
                "total_baseline": 50,
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
            {
                "metric_month": date(2020, 7, 1),
                "total_baseline": 50,
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
            {
                "metric_month": date(2020, 8, 1),
                "total_baseline": 75,
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
            {
                "metric_month": date(2020, 9, 1),
                "total_baseline": 75,
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
            {
                "metric_month": date(2020, 10, 1),
                "total_baseline": 75,
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
            {
                "metric_month": date(2020, 11, 1),
                "total_baseline": 75,
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
            {
                "metric_month": date(2020, 12, 1),
                "total_baseline": 75,
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
            },
        ]

        expected_output = [
            {
                "metric_month": date(2020, 1, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "transitions_month": 45,
                "transitions_cumulative": 45,
                "total_baseline": 40.0,
                "total_baseline_cumulative": 40.0,
                "transitions_delta_month": 0.125,
                "transitions_delta_cumulative": 0.125,
            },
            {
                "metric_month": date(2020, 2, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "transitions_month": 50,
                "transitions_cumulative": 95,
                "total_baseline": 40.0,
                "total_baseline_cumulative": 80.0,
                "transitions_delta_month": 0.25,
                "transitions_delta_cumulative": 0.1875,
            },
            {
                "metric_month": date(2020, 3, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "transitions_month": 48,
                "transitions_cumulative": 143,
                "total_baseline": 40.0,
                "total_baseline_cumulative": 120.0,
                "transitions_delta_month": 0.2,
                "transitions_delta_cumulative": 0.1917,
            },
            {
                "metric_month": date(2020, 4, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "transitions_month": 58,
                "transitions_cumulative": 201,
                "total_baseline": 50.0,
                "total_baseline_cumulative": 170.0,
                "transitions_delta_month": 0.16,
                "transitions_delta_cumulative": 0.1824,
            },
            {
                "metric_month": date(2020, 5, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "transitions_month": 49,
                "transitions_cumulative": 250,
                "total_baseline": 50.0,
                "total_baseline_cumulative": 220.0,
                "transitions_delta_month": -0.02,
                "transitions_delta_cumulative": 0.1364,
            },
            {
                "metric_month": date(2020, 6, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "transitions_month": 62,
                "transitions_cumulative": 312,
                "total_baseline": 50.0,
                "total_baseline_cumulative": 270.0,
                "transitions_delta_month": 0.24,
                "transitions_delta_cumulative": 0.1556,
            },
            {
                "metric_month": date(2020, 7, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "transitions_month": 68,
                "transitions_cumulative": 380,
                "total_baseline": 50.0,
                "total_baseline_cumulative": 320.0,
                "transitions_delta_month": 0.36,
                "transitions_delta_cumulative": 0.1875,
            },
            {
                "metric_month": date(2020, 8, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "transitions_month": 80,
                "transitions_cumulative": 460,
                "total_baseline": 75.0,
                "total_baseline_cumulative": 395.0,
                "transitions_delta_month": 0.0667,
                "transitions_delta_cumulative": 0.1646,
            },
            {
                "metric_month": date(2020, 9, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "transitions_month": 72,
                "transitions_cumulative": 532,
                "total_baseline": 75.0,
                "total_baseline_cumulative": 470.0,
                "transitions_delta_month": -0.04,
                "transitions_delta_cumulative": 0.1319,
            },
            {
                "metric_month": date(2020, 10, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "transitions_month": 79,
                "transitions_cumulative": 611,
                "total_baseline": 75.0,
                "total_baseline_cumulative": 545.0,
                "transitions_delta_month": 0.0533,
                "transitions_delta_cumulative": 0.1211,
            },
            {
                "metric_month": date(2020, 11, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "transitions_month": 64,
                "transitions_cumulative": 675,
                "total_baseline": 75.0,
                "total_baseline_cumulative": 620.0,
                "transitions_delta_month": -0.1467,
                "transitions_delta_cumulative": 0.0887,
            },
            {
                "metric_month": date(2020, 12, 1),
                "weight_factor": "0.6",
                "delta_direction_factor": "1",
                "transitions_month": 94,
                "transitions_cumulative": 769,
                "total_baseline": 75.0,
                "total_baseline_cumulative": 695.0,
                "transitions_delta_month": 0.2533,
                "transitions_delta_cumulative": 0.1065,
            },
        ]

        self.run_simple_view_builder_query_test_from_data(
            {
                self.transitions_breadth_address: transitions_breadth_data,
                self.transitions_baseline_address: transitions_baseline_data,
            },
            expected_output,
        )
