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
"""Tests functionality of transitions breadth view builder generation functions"""
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
from recidiviz.tests.big_query.simple_big_query_view_builder_test_case import (
    SimpleBigQueryViewBuilderTestCase,
)


class TransitionsBreadthMetricForYearTest(SimpleBigQueryViewBuilderTestCase):
    """Tests `get_transitions_breadth_metric_for_year` function"""

    METRIC_YEAR = 2023
    impact_transition_address = impact_transition_view_builder.table_for_query

    @property
    def view_builder(self) -> SimpleBigQueryViewBuilder:
        return get_transitions_breadth_metric_for_year(
            metric_year=self.METRIC_YEAR, attribute_cols=["system_type"]
        )

    @property
    def parent_schemas(self) -> dict[BigQueryAddress, list[bigquery.SchemaField]]:
        return {
            self.impact_transition_address: [
                schema_field_for_type("person_id", int),
                schema_field_for_type("state_code", str),
                schema_field_for_type("event_date", date),
                schema_field_for_type("decarceral_impact_type", str),
                schema_field_for_type("system_type", str),
                schema_field_for_type(
                    "is_during_or_after_full_state_launch_month", str
                ),
                schema_field_for_type("full_state_launch_date", str),
                schema_field_for_type("product_transition_type", str),
                schema_field_for_type("has_mandatory_due_date", str),
                schema_field_for_type("is_jii_transition", str),
                schema_field_for_type("weight_factor", str),
                schema_field_for_type("delta_direction_factor", str),
            ],
        }

    def test_breadth_for_tool_launched_prior_to_metric_year(self) -> None:
        impact_transition_observations = [
            {
                "person_id": 1,
                "state_code": "US_OZ",
                "event_date": date(2023, 1, 15),
                "decarceral_impact_type": "CUSTODY_LEVEL_DOWNGRADE",
                "system_type": "INCARCERATION",
                "is_during_or_after_full_state_launch_month": "true",
                "full_state_launch_date": "2022-06-18",
                "product_transition_type": "workflows_discretionary",
                "has_mandatory_due_date": "false",
                "is_jii_transition": "true",
                "weight_factor": "1.0",
                "delta_direction_factor": "1",
            },
            {
                "person_id": 2,
                "state_code": "US_OZ",
                "event_date": date(2023, 7, 5),
                "decarceral_impact_type": "CUSTODY_LEVEL_DOWNGRADE",
                "system_type": "INCARCERATION",
                "is_during_or_after_full_state_launch_month": "true",
                "full_state_launch_date": "2022-06-18",
                "product_transition_type": "workflows_discretionary",
                "has_mandatory_due_date": "false",
                "is_jii_transition": "true",
                "weight_factor": "1.0",
                "delta_direction_factor": "1",
            },
        ]

        expected_output = [
            {
                "metric_month": date(2023, 1, 1),
                "weight_factor": "1.0",
                "delta_direction_factor": "1",
                "system_type": "INCARCERATION",
                "transitions": 1,
                "new_transitions_added_via_launch": 0,
            },
            {
                "metric_month": date(2023, 7, 1),
                "weight_factor": "1.0",
                "delta_direction_factor": "1",
                "system_type": "INCARCERATION",
                "transitions": 1,
                "new_transitions_added_via_launch": 0,
            },
        ]

        self.run_simple_view_builder_query_test_from_data(
            {
                self.impact_transition_address: impact_transition_observations,
            },
            expected_output,
        )

    def test_breadth_for_tool_launched_during_metric_year(self) -> None:
        impact_transition_observations = [
            {
                "person_id": 3,
                "state_code": "US_OZ",
                "event_date": date(2023, 2, 5),
                "decarceral_impact_type": "NEW_IMPACT_TYPE",
                "system_type": "INCARCERATION",
                "is_during_or_after_full_state_launch_month": "false",
                "full_state_launch_date": "2023-06-18",
                "product_transition_type": "workflows_discretionary",
                "has_mandatory_due_date": "false",
                "is_jii_transition": "true",
                "weight_factor": "1.0",
                "delta_direction_factor": "1",
            },
            {
                "person_id": 7,
                "state_code": "US_OZ",
                "event_date": date(2023, 6, 10),
                "decarceral_impact_type": "NEW_IMPACT_TYPE",
                "system_type": "INCARCERATION",
                "is_during_or_after_full_state_launch_month": "true",
                "full_state_launch_date": "2023-06-18",
                "product_transition_type": "workflows_discretionary",
                "has_mandatory_due_date": "false",
                "is_jii_transition": "true",
                "weight_factor": "1.0",
                "delta_direction_factor": "1",
            },
            {
                "person_id": 7,
                "state_code": "US_OZ",
                "event_date": date(2023, 6, 21),
                "decarceral_impact_type": "NEW_IMPACT_TYPE",
                "system_type": "INCARCERATION",
                "is_during_or_after_full_state_launch_month": "true",
                "full_state_launch_date": "2023-06-18",
                "product_transition_type": "workflows_discretionary",
                "has_mandatory_due_date": "false",
                "is_jii_transition": "true",
                "weight_factor": "1.0",
                "delta_direction_factor": "1",
            },
            {
                "person_id": 4,
                "state_code": "US_OZ",
                "event_date": date(2023, 7, 5),
                "decarceral_impact_type": "NEW_IMPACT_TYPE",
                "system_type": "INCARCERATION",
                "is_during_or_after_full_state_launch_month": "true",
                "full_state_launch_date": "2023-06-18",
                "product_transition_type": "workflows_discretionary",
                "has_mandatory_due_date": "false",
                "is_jii_transition": "true",
                "weight_factor": "1.0",
                "delta_direction_factor": "1",
            },
            {
                "person_id": 5,
                "state_code": "US_OZ",
                "event_date": date(2023, 12, 20),
                "decarceral_impact_type": "NEW_IMPACT_TYPE",
                "system_type": "INCARCERATION",
                "is_during_or_after_full_state_launch_month": "true",
                "full_state_launch_date": "2023-06-18",
                "product_transition_type": "workflows_discretionary",
                "has_mandatory_due_date": "false",
                "is_jii_transition": "true",
                "weight_factor": "1.0",
                "delta_direction_factor": "1",
            },
            {
                "person_id": 4,
                "state_code": "US_OZ",
                "event_date": date(2023, 12, 5),
                "decarceral_impact_type": "SECOND_NEW_IMPACT_TYPE",
                "system_type": "SUPERVISION",
                "is_during_or_after_full_state_launch_month": "true",
                "full_state_launch_date": "2023-12-01",
                "product_transition_type": "workflows_mandatory",
                "has_mandatory_due_date": "false",
                "is_jii_transition": "true",
                "weight_factor": "1.0",
                "delta_direction_factor": "-1",
            },
        ]

        expected_output = [
            {
                "metric_month": date(2023, 6, 1),
                "weight_factor": "1.0",
                "delta_direction_factor": "1",
                "system_type": "INCARCERATION",
                "transitions": 2,
                "new_transitions_added_via_launch": 2,
            },
            {
                "metric_month": date(2023, 7, 1),
                "weight_factor": "1.0",
                "delta_direction_factor": "1",
                "system_type": "INCARCERATION",
                "transitions": 1,
                "new_transitions_added_via_launch": 0,
            },
            {
                "metric_month": date(2023, 12, 1),
                "weight_factor": "1.0",
                "delta_direction_factor": "1",
                "system_type": "INCARCERATION",
                "transitions": 1,
                "new_transitions_added_via_launch": 0,
            },
            {
                "metric_month": date(2023, 12, 1),
                "weight_factor": "1.0",
                "delta_direction_factor": "-1",
                "system_type": "SUPERVISION",
                "transitions": 1,
                "new_transitions_added_via_launch": 1,
            },
        ]

        self.run_simple_view_builder_query_test_from_data(
            {
                self.impact_transition_address: impact_transition_observations,
            },
            expected_output,
        )

    def test_breadth_with_dedup(self) -> None:
        impact_transition_observations = [
            {
                "person_id": 3,
                "state_code": "US_OZ",
                "event_date": date(2023, 2, 5),
                "decarceral_impact_type": "NEW_IMPACT_TYPE",
                "system_type": "INCARCERATION",
                "is_during_or_after_full_state_launch_month": "false",
                "full_state_launch_date": "2023-06-18",
                "product_transition_type": "workflows_discretionary",
                "has_mandatory_due_date": "false",
                "is_jii_transition": "true",
                "weight_factor": "1.0",
                "delta_direction_factor": "1",
            },
            {
                "person_id": 7,
                "state_code": "US_OZ",
                "event_date": date(2023, 8, 11),
                "decarceral_impact_type": "NEW_IMPACT_TYPE",
                "system_type": "INCARCERATION",
                "is_during_or_after_full_state_launch_month": "true",
                "full_state_launch_date": "2023-06-18",
                "product_transition_type": "workflows_discretionary",
                "has_mandatory_due_date": "false",
                "is_jii_transition": "true",
                "weight_factor": "1.0",
                "delta_direction_factor": "1",
            },
            {
                "person_id": 7,
                "state_code": "US_OZ",
                "event_date": date(2023, 8, 11),
                "decarceral_impact_type": "NEW_IMPACT_TYPE",
                "system_type": "INCARCERATION",
                "is_during_or_after_full_state_launch_month": "true",
                "full_state_launch_date": "2023-06-18",
                "product_transition_type": "workflows_discretionary",
                "has_mandatory_due_date": "false",
                "is_jii_transition": "false",
                "weight_factor": "1.0",
                "delta_direction_factor": "1",
            },
        ]

        expected_output = [
            {
                "metric_month": date(2023, 8, 1),
                "weight_factor": "1.0",
                "delta_direction_factor": "1",
                "system_type": "INCARCERATION",
                "transitions": 2,
                "new_transitions_added_via_launch": 0,
            },
        ]

        self.run_simple_view_builder_query_test_from_data(
            {
                self.impact_transition_address: impact_transition_observations,
            },
            expected_output,
        )
