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
"""A script for building and writing a set of LookML views that support custom metrics
in Looker for all configured packages

Example usage:
python -m recidiviz.tools.looker.top_level_generators.custom_metrics_lookml_generator
"""

import os
from typing import Dict, Optional, Tuple

from recidiviz.aggregated_metrics.aggregated_metrics_view_collector import (
    METRIC_CLASSES,
)
from recidiviz.aggregated_metrics.configuration.collections.standard import (
    METRICS_BY_POPULATION_TYPE,
)
from recidiviz.aggregated_metrics.models.aggregated_metric import AggregatedMetric
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.case_triage.util import MessageType
from recidiviz.observations.metric_unit_of_observation import MetricUnitOfObservation
from recidiviz.tools.looker.aggregated_metrics.custom_cpa_metrics_configurations import (
    CPA_ASSIGNMENT_NAMES_TO_TYPES,
    CPA_IMPACT_LOOKER_METRICS,
)
from recidiviz.tools.looker.aggregated_metrics.custom_global_metrics_configurations import (
    GLOBAL_ASSIGNMENT_NAMES_TO_TYPES,
    GLOBAL_IMPACT_LOOKER_METRICS,
)
from recidiviz.tools.looker.aggregated_metrics.custom_incarceration_system_health_metrics_configurations import (
    INCARCERATION_SYSTEM_HEALTH_ASSIGNMENT_NAMES_TO_TYPES,
)
from recidiviz.tools.looker.aggregated_metrics.custom_insights_metrics_configurations import (
    INSIGHTS_ASSIGNMENT_NAMES_TO_TYPES,
    INSIGHTS_IMPACT_LOOKER_METRICS,
)
from recidiviz.tools.looker.aggregated_metrics.custom_jii_tablet_app_metrics_configurations import (
    JII_TABLET_APP_ASSIGNMENT_NAMES_TO_TYPES,
    JII_TABLET_APP_IMPACT_LOOKER_METRICS,
)
from recidiviz.tools.looker.aggregated_metrics.custom_jii_texts_metrics_configurations import (
    JII_TEXTS_ASSIGNMENT_NAMES_TO_TYPES,
    JII_TEXTS_IMPACT_LOOKER_METRICS,
)
from recidiviz.tools.looker.aggregated_metrics.custom_metrics_lookml_utils import (
    build_assignments_by_time_period_lookml_view,
    build_assignments_lookml_view,
    build_custom_metrics_lookml_view,
    build_time_periods_lookml_view,
)
from recidiviz.tools.looker.aggregated_metrics.custom_supervision_system_health_metrics_configurations import (
    SUPERVISION_SYSTEM_HEALTH_ASSIGNMENT_NAMES_TO_TYPES,
)
from recidiviz.tools.looker.aggregated_metrics.custom_system_metrics_configurations import (
    ASSIGNMENT_NAME_TO_TYPES,
    CUSTOM_SYSTEM_IMPACT_LOOKER_METRICS,
)
from recidiviz.tools.looker.aggregated_metrics.custom_tasks_metrics_configurations import (
    TASKS_ASSIGNMENT_NAMES_TO_TYPES,
    TASKS_IMPACT_LOOKER_METRICS,
    TASKS_JSON_FIELD_FILTERS_WITH_SUGGESTIONS,
)
from recidiviz.tools.looker.aggregated_metrics.custom_workflows_metrics_configurations import (
    WORKFLOWS_ASSIGNMENT_NAMES_TO_TYPES,
    WORKFLOWS_IMPACT_LOOKER_METRICS,
    WORKFLOWS_JSON_FIELD_FILTERS_WITH_SUGGESTIONS,
)
from recidiviz.tools.looker.constants import GENERATED_LOOKML_ROOT_PATH
from recidiviz.tools.looker.script_helpers import get_generated_views_path
from recidiviz.tools.looker.top_level_generators.base_lookml_generator import (
    LookMLGenerator,
)


def collect_and_build_custom_metrics_views_for_package(
    lookml_views_package_name: str,
    output_directory: str,
    metrics: list[AggregatedMetric],
    assignment_types_dict: Dict[
        str, Tuple[MetricPopulationType, MetricUnitOfAnalysisType]
    ],
    json_field_filters_with_suggestions: Optional[dict[str, list[str]]] = None,
) -> None:
    """Builds and writes views required to build Workflows impact metrics in Looker"""
    if not output_directory:
        raise ValueError("Must supply a non-empty output_directory")

    output_directory = os.path.join(output_directory, lookml_views_package_name)
    # Create subdirectory for all subquery views
    output_subdirectory = os.path.join(output_directory, "subqueries")

    for unit_of_observation_type in sorted(
        set(metric.unit_of_observation_type for metric in metrics),
        key=lambda t: t.value,
    ):
        unit_of_observation = MetricUnitOfObservation(type=unit_of_observation_type)
        build_assignments_lookml_view(
            lookml_views_package_name,
            assignment_types_dict,
            unit_of_observation=unit_of_observation,
        ).write(output_subdirectory, source_script_path=__file__)

        build_time_periods_lookml_view(lookml_views_package_name).write(
            output_subdirectory, source_script_path=__file__
        )

        for metric_class in METRIC_CLASSES:
            build_assignments_by_time_period_lookml_view(
                lookml_views_package_name,
                unit_of_observation_type=unit_of_observation_type,
                metric_time_period_to_assignment_join_type=metric_class.metric_time_period_to_assignment_join_type(),
            ).write(output_subdirectory, source_script_path=__file__)

    build_custom_metrics_lookml_view(
        lookml_views_package_name,
        metrics,
        assignment_types_dict,
        json_field_filters_with_suggestions,
    ).write(output_directory, source_script_path=__file__)


class CustomMetricsLookMLGenerator(LookMLGenerator):
    """Generates a set of LookML views that support custom metrics in Looker for all configured packages."""

    @staticmethod
    def generate_lookml(output_dir: str) -> None:
        """
        Write custom metrics LookML views to the given directory,
        which should be a path to the local copy of the looker repo
        """
        output_subdir = get_generated_views_path(
            output_dir=output_dir, module_name="aggregated_metrics"
        )

        # TODO(#53131): Deprecate this explore in favor of `experiment_kpi_metrics`
        collect_and_build_custom_metrics_views_for_package(
            lookml_views_package_name="custom_metrics",
            output_directory=output_subdir,
            metrics=CUSTOM_SYSTEM_IMPACT_LOOKER_METRICS,
            assignment_types_dict=ASSIGNMENT_NAME_TO_TYPES,
            json_field_filters_with_suggestions={},
        )

        ## Supervision System Health
        collect_and_build_custom_metrics_views_for_package(
            lookml_views_package_name="supervision_system_health_metrics",
            output_directory=output_subdir,
            metrics=METRICS_BY_POPULATION_TYPE[MetricPopulationType.SUPERVISION],
            assignment_types_dict=SUPERVISION_SYSTEM_HEALTH_ASSIGNMENT_NAMES_TO_TYPES,
            json_field_filters_with_suggestions={},
        )

        ## Incarceration System Health
        collect_and_build_custom_metrics_views_for_package(
            lookml_views_package_name="incarceration_system_health_metrics",
            output_directory=output_subdir,
            metrics=METRICS_BY_POPULATION_TYPE[MetricPopulationType.INCARCERATION],
            assignment_types_dict=INCARCERATION_SYSTEM_HEALTH_ASSIGNMENT_NAMES_TO_TYPES,
            json_field_filters_with_suggestions={},
        )

        # Workflows
        collect_and_build_custom_metrics_views_for_package(
            lookml_views_package_name="workflows_impact_metrics",
            output_directory=output_subdir,
            metrics=WORKFLOWS_IMPACT_LOOKER_METRICS,
            assignment_types_dict=WORKFLOWS_ASSIGNMENT_NAMES_TO_TYPES,
            json_field_filters_with_suggestions=WORKFLOWS_JSON_FIELD_FILTERS_WITH_SUGGESTIONS,
        )

        # Insights
        collect_and_build_custom_metrics_views_for_package(
            lookml_views_package_name="insights_impact_metrics",
            output_directory=output_subdir,
            metrics=INSIGHTS_IMPACT_LOOKER_METRICS,
            assignment_types_dict=INSIGHTS_ASSIGNMENT_NAMES_TO_TYPES,
            json_field_filters_with_suggestions={},
        )

        # Tasks
        collect_and_build_custom_metrics_views_for_package(
            lookml_views_package_name="tasks_impact_metrics",
            output_directory=output_subdir,
            metrics=TASKS_IMPACT_LOOKER_METRICS,
            assignment_types_dict=TASKS_ASSIGNMENT_NAMES_TO_TYPES,
            json_field_filters_with_suggestions=TASKS_JSON_FIELD_FILTERS_WITH_SUGGESTIONS,
        )

        # Global usage
        collect_and_build_custom_metrics_views_for_package(
            lookml_views_package_name="global_impact_metrics",
            output_directory=output_subdir,
            metrics=GLOBAL_IMPACT_LOOKER_METRICS,
            assignment_types_dict=GLOBAL_ASSIGNMENT_NAMES_TO_TYPES,
            json_field_filters_with_suggestions={},
        )

        # JII tablet app
        collect_and_build_custom_metrics_views_for_package(
            lookml_views_package_name="jii_tablet_app_metrics",
            output_directory=output_subdir,
            metrics=JII_TABLET_APP_IMPACT_LOOKER_METRICS,
            assignment_types_dict=JII_TABLET_APP_ASSIGNMENT_NAMES_TO_TYPES,
            json_field_filters_with_suggestions={},
        )

        # CPA
        collect_and_build_custom_metrics_views_for_package(
            lookml_views_package_name="case_planning_assistant_metrics",
            output_directory=output_subdir,
            metrics=CPA_IMPACT_LOOKER_METRICS,
            assignment_types_dict=CPA_ASSIGNMENT_NAMES_TO_TYPES,
            json_field_filters_with_suggestions={},
        )

        # JII Texts
        collect_and_build_custom_metrics_views_for_package(
            lookml_views_package_name="jii_texts_metrics",
            output_directory=output_subdir,
            metrics=JII_TEXTS_IMPACT_LOOKER_METRICS,
            assignment_types_dict=JII_TEXTS_ASSIGNMENT_NAMES_TO_TYPES,
            json_field_filters_with_suggestions={"message_type": list(MessageType)},
        )


if __name__ == "__main__":
    CustomMetricsLookMLGenerator.generate_lookml(output_dir=GENERATED_LOOKML_ROOT_PATH)
