# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""A script for building and writing a set of LookML views that support custom metrics
in Looker relevant to Insights impact measurement.

Run the following to write views to the specified directory DIR:
python -m recidiviz.tools.looker.aggregated_metrics.custom_insights_metrics_lookml_generator --save_views_to_dir [DIR]

"""

import argparse
import os

import recidiviz.tools.looker.aggregated_metrics.custom_insights_metrics_configurations as insights_metrics
from recidiviz.aggregated_metrics.models.aggregated_metric import (
    AggregatedMetric,
    AssignmentEventAggregatedMetric,
    AssignmentSpanAggregatedMetric,
    MiscAggregatedMetric,
    PeriodEventAggregatedMetric,
    PeriodSpanAggregatedMetric,
)
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.common.constants.state.state_person import StateGender, StateRace
from recidiviz.common.str_field_utils import snake_to_title
from recidiviz.observations.metric_unit_of_observation import MetricUnitOfObservation
from recidiviz.observations.metric_unit_of_observation_type import (
    MetricUnitOfObservationType,
)
from recidiviz.tools.looker.aggregated_metrics.custom_metrics_lookml_utils import (
    generate_assignment_event_metric_view,
    generate_assignment_span_metric_view,
    generate_assignments_view,
    generate_assignments_with_attributes_and_time_periods_view,
    generate_custom_metrics_view,
    generate_period_event_metric_view,
    generate_period_span_metric_view,
    generate_person_assignments_with_attributes_view,
)

INSIGHTS_ASSIGNMENT_NAMES_TO_TYPES = {
    "SUPERVISION_STATE": (
        MetricPopulationType.SUPERVISION,
        MetricUnitOfAnalysisType.STATE_CODE,
    ),
    "SUPERVISION_DISTRICT": (
        MetricPopulationType.SUPERVISION,
        MetricUnitOfAnalysisType.SUPERVISION_DISTRICT,
    ),
    "SUPERVISION_OFFICE": (
        MetricPopulationType.SUPERVISION,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICE,
    ),
    "SUPERVISION_UNIT_SUPERVISOR": (
        MetricPopulationType.SUPERVISION,
        MetricUnitOfAnalysisType.SUPERVISION_UNIT,
    ),
    "SUPERVISION_OFFICER": (
        MetricPopulationType.SUPERVISION,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICER,
    ),
}


INSIGHTS_IMPACT_LOOKER_METRICS: list[AggregatedMetric] = [
    insights_metrics.ABSCONSIONS_AND_BENCH_WARRANTS_LOOKER,
    insights_metrics.AVG_DAILY_POPULATION_LOOKER,
    insights_metrics.DISTINCT_PROVISIONED_INSIGHTS_USERS_LOOKER,
    insights_metrics.DISTINCT_REGISTERED_PROVISIONED_INSIGHTS_USERS_LOOKER,
    insights_metrics.DISTINCT_PROVISIONED_PRIMARY_INSIGHTS_USERS_LOOKER,
    insights_metrics.DISTINCT_REGISTERED_PRIMARY_INSIGHTS_USERS_LOOKER,
    insights_metrics.DISTINCT_LOGGED_IN_PRIMARY_INSIGHTS_USERS_LOOKER,
    insights_metrics.DISTINCT_ACTIVE_PRIMARY_INSIGHTS_USERS_LOOKER,
    insights_metrics.DISTINCT_ACTIVE_PRIMARY_INSIGHTS_USERS_WITH_OUTLIERS_VISIBLE_IN_TOOL_LOOKER,
    insights_metrics.DISTINCT_ACTIVE_PRIMARY_INSIGHTS_USERS_WITHOUT_OUTLIERS_VISIBLE_IN_TOOL_LOOKER,
    insights_metrics.LOGINS_PRIMARY_INSIGHTS_USERS_LOOKER,
    insights_metrics.INCARCERATION_STARTS_LOOKER,
    insights_metrics.INCARCERATION_STARTS_AND_INFERRED_LOOKER,
    insights_metrics.VIOLATIONS_ABSCONSION,
]


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


def main(
    output_directory: str,
    view_name: str,
) -> None:
    """Builds and writes views required to build Insights impact metrics in Looker"""
    metrics = INSIGHTS_IMPACT_LOOKER_METRICS
    if output_directory:
        output_directory = os.path.join(output_directory, view_name)
        # Create subdirectory for all subquery views
        output_subdirectory = os.path.join(output_directory, "subqueries")

        generate_custom_metrics_view(
            [
                metric
                for metric in metrics
                if not isinstance(metric, MiscAggregatedMetric)
            ],
            view_name,
            additional_view_fields=[],
            assignment_types_dict=INSIGHTS_ASSIGNMENT_NAMES_TO_TYPES,
            json_field_filters_with_suggestions={},
        ).write(output_directory, source_script_path=__file__)

        for unit_of_observation_type in set(
            metric.unit_of_observation_type
            for metric in metrics
            if not isinstance(metric, MiscAggregatedMetric)
        ):
            unit_of_observation = MetricUnitOfObservation(type=unit_of_observation_type)
            generate_assignments_view(
                view_name,
                INSIGHTS_ASSIGNMENT_NAMES_TO_TYPES,
                unit_of_observation=unit_of_observation,
            ).write(output_subdirectory, source_script_path=__file__)

            if unit_of_observation_type == MetricUnitOfObservationType.PERSON_ID:
                # This is just a dummy view right now that doesn't provide any additional attributes
                generate_person_assignments_with_attributes_view(
                    view_name=view_name,
                    time_dependent_person_attribute_query="SELECT *, NULL AS dummy_attribute, FROM sessions.system_sessions_materialized",
                    time_dependent_person_attribute_fields=["dummy_attribute"],
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
    main(args.save_dir, view_name="insights_impact_metrics")
