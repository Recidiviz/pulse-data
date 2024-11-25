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
"""A script for building and writing a set of LookML views that support custom aggregated metrics
in Looker.

Run the following to write views to the specified directory DIR:
python -m recidiviz.tools.looker.aggregated_metrics.custom_aggregated_metrics_lookml_generator --save_views_to_dir [DIR]

"""

import argparse
import os

from recidiviz.aggregated_metrics.models.aggregated_metric import (
    AssignmentEventAggregatedMetric,
    AssignmentSpanAggregatedMetric,
    EventMetricConditionsMixin,
    PeriodEventAggregatedMetric,
    PeriodSpanAggregatedMetric,
    SpanMetricConditionsMixin,
)
from recidiviz.aggregated_metrics.standard_deployed_metrics_by_population import (
    METRICS_BY_POPULATION_TYPE,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.common.constants.state.state_person import StateGender, StateRace
from recidiviz.common.str_field_utils import snake_to_title
from recidiviz.observations.metric_unit_of_observation import MetricUnitOfObservation
from recidiviz.observations.metric_unit_of_observation_type import (
    MetricUnitOfObservationType,
)
from recidiviz.tools.looker.aggregated_metrics.custom_metrics_lookml_utils import (
    ASSIGNMENT_NAME_TO_TYPES,
    generate_assignment_event_metric_view,
    generate_assignment_span_metric_view,
    generate_assignments_view,
    generate_assignments_with_attributes_and_time_periods_view,
    generate_custom_metrics_view,
    generate_period_event_metric_view,
    generate_period_span_metric_view,
    generate_person_assignments_with_attributes_view,
)


def parse_arguments() -> argparse.Namespace:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--save_views_to_dir",
        dest="save_dir",
        help="Specifies name of directory where to save view files",
        type=str,
        required=True,
    )

    return parser.parse_args()


def main(output_directory: str, view_name: str) -> None:
    """Builds and writes views required to build person-level metrics in Looker"""
    metrics = METRICS_BY_POPULATION_TYPE[MetricPopulationType.SUPERVISION]
    if output_directory:
        output_directory = os.path.join(output_directory, view_name)
        # Create subdirectory for all subquery views
        output_subdirectory = os.path.join(output_directory, "subqueries")

        generate_custom_metrics_view(
            [
                metric
                for metric in metrics
                if hasattr(metric, "unit_of_observation_type")
            ],
            view_name,
            additional_view_fields=[],
            json_field_filters_with_suggestions={},
        ).write(output_directory, source_script_path=__file__)

        for unit_of_observation_type in set(
            metric.unit_of_observation_type
            for metric in metrics
            if isinstance(
                metric, (EventMetricConditionsMixin, SpanMetricConditionsMixin)
            )
        ):
            unit_of_observation = MetricUnitOfObservation(type=unit_of_observation_type)
            generate_assignments_view(
                view_name,
                ASSIGNMENT_NAME_TO_TYPES,
                unit_of_observation=unit_of_observation,
            ).write(output_subdirectory, source_script_path=__file__)

            if unit_of_observation_type == MetricUnitOfObservationType.PERSON_ID:
                generate_person_assignments_with_attributes_view(
                    view_name=view_name,
                    time_dependent_person_attribute_query="SELECT * FROM sessions.compartment_sub_sessions_materialized",
                    time_dependent_person_attribute_fields=[
                        "age",
                        "assessment_score",
                        "case_type",
                        "compartment_level_1",
                        "compartment_level_2",
                        "correctional_level",
                        "correctional_level_raw_text",
                        "housing_unit",
                        "housing_unit_type",
                        "supervision_district",
                        "supervision_district_name",
                        "supervision_office",
                        "supervision_office_name",
                    ],
                    demographic_attribute_field_filters_with_suggestions={
                        "gender": sorted(
                            [snake_to_title(member.value) for member in StateGender]
                        ),
                        "race": sorted(
                            [snake_to_title(member.value) for member in StateRace]
                        ),
                        "is_female": [],
                        "is_nonwhite": [],
                    },
                ).write(output_subdirectory, source_script_path=__file__)

            generate_assignments_with_attributes_and_time_periods_view(
                view_name, unit_of_observation=unit_of_observation
            ).write(output_subdirectory, source_script_path=__file__)

            generate_period_span_metric_view(
                [
                    metric
                    for metric in metrics
                    if isinstance(metric, PeriodSpanAggregatedMetric)
                    and metric.unit_of_observation_type == unit_of_observation_type
                ],
                view_name,
                unit_of_observation=unit_of_observation,
                json_field_filters=[],
            ).write(output_subdirectory, source_script_path=__file__)

            generate_period_event_metric_view(
                [
                    metric
                    for metric in metrics
                    if isinstance(metric, PeriodEventAggregatedMetric)
                    and metric.unit_of_observation_type == unit_of_observation_type
                ],
                view_name,
                unit_of_observation=unit_of_observation,
                json_field_filters=[],
            ).write(output_subdirectory, source_script_path=__file__)

            generate_assignment_span_metric_view(
                [
                    metric
                    for metric in metrics
                    if isinstance(metric, AssignmentSpanAggregatedMetric)
                    and metric.unit_of_observation_type == unit_of_observation_type
                ],
                view_name,
                unit_of_observation=unit_of_observation,
                json_field_filters=[],
            ).write(output_subdirectory, source_script_path=__file__)

            generate_assignment_event_metric_view(
                [
                    metric
                    for metric in metrics
                    if isinstance(metric, AssignmentEventAggregatedMetric)
                    and metric.unit_of_observation_type == unit_of_observation_type
                ],
                view_name,
                unit_of_observation=unit_of_observation,
                json_field_filters=[],
            ).write(output_subdirectory, source_script_path=__file__)


if __name__ == "__main__":
    args = parse_arguments()
    main(args.save_dir, "custom_metrics")
