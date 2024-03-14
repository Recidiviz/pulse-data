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

from recidiviz.aggregated_metrics.models.aggregated_metric import (
    AssignmentEventAggregatedMetric,
    AssignmentSpanAggregatedMetric,
    MiscAggregatedMetric,
    PeriodEventAggregatedMetric,
    PeriodSpanAggregatedMetric,
)
from recidiviz.aggregated_metrics.models.aggregated_metric_configurations import (
    DEDUPED_TASK_COMPLETION_EVENT_VB,
)
from recidiviz.common.str_field_utils import snake_to_title
from recidiviz.looker.lookml_view_field import (
    LookMLFieldParameter,
    ParameterLookMLViewField,
)
from recidiviz.looker.lookml_view_field_parameter import LookMLFieldType
from recidiviz.tools.looker.aggregated_metrics.custom_metrics_lookml_utils import (
    ASSIGNMENT_NAME_TO_TYPES,
    generate_assignment_event_metric_view,
    generate_assignment_span_metric_view,
    generate_assignments_view,
    generate_assignments_with_attributes_and_time_periods_view,
    generate_assignments_with_attributes_view,
    generate_custom_metrics_view,
    generate_period_event_metric_view,
    generate_period_span_metric_view,
)
from recidiviz.tools.looker.aggregated_metrics.custom_workflows_metrics_configurations import (
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_LOOKER,
    DAYS_ELIGIBLE_AT_TASK_COMPLETION_LOOKER,
    PERSON_DAYS_TASK_ELIGIBLE_LOOKER,
    TASK_COMPLETIONS_LOOKER,
    TASK_COMPLETIONS_WHILE_ELIGIBLE_LOOKER,
)

WORKFLOWS_IMPACT_LOOKER_METRICS = [
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_LOOKER,
    DAYS_ELIGIBLE_AT_TASK_COMPLETION_LOOKER,
    PERSON_DAYS_TASK_ELIGIBLE_LOOKER,
    TASK_COMPLETIONS_LOOKER,
    TASK_COMPLETIONS_WHILE_ELIGIBLE_LOOKER,
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
    """Builds and writes views required to build person-level metrics in Looker"""
    metrics = WORKFLOWS_IMPACT_LOOKER_METRICS
    if output_directory:
        output_directory = os.path.join(output_directory, view_name)
        # Create subdirectory for all subquery views
        output_subdirectory = os.path.join(output_directory, "subqueries")

        task_type_parameter_field = ParameterLookMLViewField(
            field_name="task_type",
            parameters=[
                LookMLFieldParameter.type(LookMLFieldType.STRING),
                LookMLFieldParameter.description(
                    "Used to select the workflows task type"
                ),
                LookMLFieldParameter.view_label("Workflows Filters"),
                *[
                    LookMLFieldParameter.allowed_value(
                        snake_to_title(builder.task_type_name),
                        builder.task_type_name,
                    )
                    for builder in DEDUPED_TASK_COMPLETION_EVENT_VB
                ],
                LookMLFieldParameter.default_value(
                    DEDUPED_TASK_COMPLETION_EVENT_VB[0].task_type_name
                ),
            ],
        )

        generate_custom_metrics_view(
            [
                metric
                for metric in metrics
                if not isinstance(metric, MiscAggregatedMetric)
            ],
            view_name,
            additional_view_fields=[task_type_parameter_field],
        ).write(output_directory, source_script_path=__file__)

        generate_assignments_view(
            view_name,
            ASSIGNMENT_NAME_TO_TYPES,
        ).write(output_subdirectory, source_script_path=__file__)

        generate_assignments_with_attributes_view(
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
        ).write(output_subdirectory, source_script_path=__file__)

        generate_assignments_with_attributes_and_time_periods_view(
            view_name,
        ).write(output_subdirectory, source_script_path=__file__)

        generate_period_span_metric_view(
            [
                metric
                for metric in metrics
                if isinstance(metric, PeriodSpanAggregatedMetric)
            ],
            view_name,
        ).write(output_subdirectory, source_script_path=__file__)

        generate_period_event_metric_view(
            [
                metric
                for metric in metrics
                if isinstance(metric, PeriodEventAggregatedMetric)
            ],
            view_name,
        ).write(output_subdirectory, source_script_path=__file__)

        generate_assignment_span_metric_view(
            [
                metric
                for metric in metrics
                if isinstance(metric, AssignmentSpanAggregatedMetric)
            ],
            view_name,
        ).write(output_subdirectory, source_script_path=__file__)

        generate_assignment_event_metric_view(
            [
                metric
                for metric in metrics
                if isinstance(metric, AssignmentEventAggregatedMetric)
            ],
            view_name,
        ).write(output_subdirectory, source_script_path=__file__)


if __name__ == "__main__":
    args = parse_arguments()
    main(args.save_dir, view_name="workflows_impact_metrics")
