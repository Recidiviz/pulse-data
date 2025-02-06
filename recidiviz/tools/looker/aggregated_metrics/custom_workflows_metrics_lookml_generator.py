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
"""A script for building and writing a set of LookML views that support custom metrics
in Looker relevant to workflows impact measurement.

Run the following to write views to the specified directory DIR:
python -m recidiviz.tools.looker.aggregated_metrics.custom_workflows_metrics_lookml_generator --save_views_to_dir [DIR]

"""

import argparse
import os

import recidiviz.tools.looker.aggregated_metrics.custom_workflows_metrics_configurations as workflows_metrics
from recidiviz.aggregated_metrics.models.aggregated_metric import (
    AggregatedMetric,
    AssignmentEventAggregatedMetric,
    AssignmentSpanAggregatedMetric,
    MiscAggregatedMetric,
    PeriodEventAggregatedMetric,
    PeriodSpanAggregatedMetric,
)
from recidiviz.aggregated_metrics.models.aggregated_metric_configurations import (
    DEDUPED_TASK_COMPLETION_EVENT_VB,
)
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.common.constants.state.state_person import StateGender, StateRace
from recidiviz.common.decarceral_impact_type import DecarceralImpactType
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

WORKFLOWS_IMPACT_LOOKER_METRICS: list[AggregatedMetric] = [
    workflows_metrics.AVG_DAILY_POPULATION_TASK_ELIGIBLE_LOOKER,
    *workflows_metrics.AVG_DAILY_POPULATION_TASK_ELIGIBLE_LOOKER_FUNNEL_METRICS,
    workflows_metrics.AVG_DAILY_POPULATION_TASK_ALMOST_ELIGIBLE_LOOKER,
    *workflows_metrics.AVG_DAILY_POPULATION_TASK_ALMOST_ELIGIBLE_LOOKER_FUNNEL_METRICS,
    workflows_metrics.DISTINCT_PROVISIONED_WORKFLOWS_USERS_LOOKER,
    workflows_metrics.DISTINCT_REGISTERED_PROVISIONED_WORKFLOWS_USERS_LOOKER,
    workflows_metrics.DISTINCT_PROVISIONED_PRIMARY_WORKFLOWS_USERS_LOOKER,
    workflows_metrics.DISTINCT_REGISTERED_PRIMARY_WORKFLOWS_USERS_LOOKER,
    workflows_metrics.DISTINCT_LOGGED_IN_PRIMARY_WORKFLOWS_USERS_LOOKER,
    workflows_metrics.DISTINCT_ACTIVE_PRIMARY_WORKFLOWS_USERS_LOOKER,
    workflows_metrics.LOGINS_BY_PRIMARY_WORKFLOWS_USER_LOOKER,
    workflows_metrics.PERSON_DAYS_TASK_ELIGIBLE_LOOKER,
    workflows_metrics.TASK_COMPLETIONS_LOOKER,
    workflows_metrics.TASK_COMPLETIONS_WHILE_ALMOST_ELIGIBLE_LOOKER,
    workflows_metrics.TASK_COMPLETIONS_WHILE_ELIGIBLE_LOOKER,
    workflows_metrics.TASK_COMPLETIONS_AFTER_TOOL_ACTION_LOOKER,
    workflows_metrics.TASK_COMPLETIONS_WHILE_ALMOST_ELIGIBLE_AFTER_TOOL_ACTION_LOOKER,
    workflows_metrics.DAYS_ELIGIBLE_AT_TASK_COMPLETION_LOOKER,
    workflows_metrics.TASK_ELIGIBILITY_STARTS_WHILE_ALMOST_ELIGIBLE_AFTER_TOOL_ACTION_LOOKER,
    workflows_metrics.FIRST_TOOL_ACTIONS_LOOKER,
    workflows_metrics.DAYS_ELIGIBLE_AT_FIRST_TOOL_ACTION_LOOKER,
]

WORKFLOWS_ASSIGNMENT_NAMES_TO_TYPES = {
    "ALL_JUSTICE_INVOLVED_STATES": (
        MetricPopulationType.JUSTICE_INVOLVED,
        MetricUnitOfAnalysisType.ALL_STATES,
    ),
    "JUSTICE_INVOLVED_STATE": (
        MetricPopulationType.JUSTICE_INVOLVED,
        MetricUnitOfAnalysisType.STATE_CODE,
    ),
    "JUSTICE_INVOLVED_LOCATION": (
        MetricPopulationType.JUSTICE_INVOLVED,
        MetricUnitOfAnalysisType.LOCATION,
    ),
    "JUSTICE_INVOLVED_LOCATION_DETAIL": (
        MetricPopulationType.JUSTICE_INVOLVED,
        MetricUnitOfAnalysisType.LOCATION_DETAIL,
    ),
    "ALL_SUPERVISION_STATES": (
        MetricPopulationType.SUPERVISION,
        MetricUnitOfAnalysisType.ALL_STATES,
    ),
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
        MetricUnitOfAnalysisType.SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL,
    ),
    "ALL_INCARCERATION_STATES": (
        MetricPopulationType.INCARCERATION,
        MetricUnitOfAnalysisType.ALL_STATES,
    ),
    "INCARCERATION_FACILITY": (
        MetricPopulationType.INCARCERATION,
        MetricUnitOfAnalysisType.FACILITY,
    ),
    "INCARCERATION_FACILITY_COUNSELOR": (
        MetricPopulationType.INCARCERATION,
        MetricUnitOfAnalysisType.FACILITY_COUNSELOR,
    ),
}


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
    """Builds and writes views required to build Workflows impact metrics in Looker"""
    metrics = WORKFLOWS_IMPACT_LOOKER_METRICS
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
            assignment_types_dict=WORKFLOWS_ASSIGNMENT_NAMES_TO_TYPES,
            json_field_filters_with_suggestions={
                "task_type": sorted(
                    snake_to_title(builder.task_type_name)
                    for builder in DEDUPED_TASK_COMPLETION_EVENT_VB
                ),
                "system_type": ["Incarceration", "Supervision"],
                "decarceral_impact_type": sorted(
                    snake_to_title(decarceral_impact_type.name)
                    for decarceral_impact_type in DecarceralImpactType
                ),
                "is_jii_decarceral_transition": ["True", "False"],
                "has_mandatory_due_date": ["True", "False"],
                "task_type_is_live": ["True", "False"],
                "task_type_is_fully_launched": ["True", "False"],
                "denial_reasons": [],
            },
        ).write(output_directory, source_script_path=__file__)

        for unit_of_observation_type in set(
            metric.unit_of_observation_type
            for metric in metrics
            if not isinstance(metric, MiscAggregatedMetric)
        ):
            unit_of_observation = MetricUnitOfObservation(type=unit_of_observation_type)
            generate_assignments_view(
                view_name,
                WORKFLOWS_ASSIGNMENT_NAMES_TO_TYPES,
                unit_of_observation=unit_of_observation,
            ).write(output_subdirectory, source_script_path=__file__)

            if unit_of_observation_type == MetricUnitOfObservationType.PERSON_ID:
                generate_person_assignments_with_attributes_view(
                    view_name=view_name,
                    # Renaming caseload and location allows them to be properly parsed as Dynamic Attributes in Looker
                    time_dependent_person_attribute_query="SELECT *, caseload_id as workflows_caseload FROM sessions.person_caseload_location_sessions_materialized",
                    time_dependent_person_attribute_fields=[],
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
                json_field_filters=[
                    "task_type",
                    "system_type",
                    "decarceral_impact_type",
                    "is_jii_decarceral_transition",
                    "has_mandatory_due_date",
                    "task_type_is_live",
                    "task_type_is_fully_launched",
                    "denial_reasons",
                ],
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
                json_field_filters=[
                    "task_type",
                    "system_type",
                    "decarceral_impact_type",
                    "is_jii_decarceral_transition",
                    "has_mandatory_due_date",
                    "task_type_is_live",
                    "task_type_is_fully_launched",
                    "denial_reasons",
                ],
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
                json_field_filters=[
                    "task_type",
                    "system_type",
                    "decarceral_impact_type",
                    "is_jii_decarceral_transition",
                    "has_mandatory_due_date",
                    "task_type_is_live",
                    "task_type_is_fully_launched",
                    "denial_reasons",
                ],
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
                json_field_filters=[
                    "task_type",
                    "system_type",
                    "decarceral_impact_type",
                    "is_jii_decarceral_transition",
                    "has_mandatory_due_date",
                    "task_type_is_live",
                    "task_type_is_fully_launched",
                    "denial_reasons",
                ],
            ).write(output_subdirectory, source_script_path=__file__)


if __name__ == "__main__":
    args = parse_arguments()
    main(args.save_dir, view_name="workflows_impact_metrics")
