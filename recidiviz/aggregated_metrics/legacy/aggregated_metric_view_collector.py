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
"""Returns all aggregated metric view builders for specified populations and units of analysis"""
from typing import Dict, List

from recidiviz.aggregated_metrics.legacy.assignment_event_aggregated_metrics import (
    generate_assignment_event_aggregated_metrics_view_builder,
)
from recidiviz.aggregated_metrics.legacy.assignment_span_aggregated_metrics import (
    generate_assignment_span_aggregated_metrics_view_builder,
)
from recidiviz.aggregated_metrics.legacy.misc_aggregated_metrics import (
    generate_misc_aggregated_metrics_view_builder,
)
from recidiviz.aggregated_metrics.legacy.period_span_aggregated_metrics import (
    generate_period_span_aggregated_metrics_view_builder,
)
from recidiviz.aggregated_metrics.models.aggregated_metric import (
    AggregatedMetric,
    AssignmentEventAggregatedMetric,
    AssignmentSpanAggregatedMetric,
    EventValueMetric,
    MiscAggregatedMetric,
    PeriodEventAggregatedMetric,
    PeriodSpanAggregatedMetric,
)
from recidiviz.aggregated_metrics.models.aggregated_metric_configurations import (
    ASSIGNMENTS,
)
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysis,
    MetricUnitOfAnalysisType,
)
from recidiviz.aggregated_metrics.query_building.aggregated_metric_query_utils import (
    is_metric_class_supported_by_optimized_format,
)
from recidiviz.aggregated_metrics.standard_deployed_metrics_by_population import (
    METRICS_BY_POPULATION_TYPE,
)
from recidiviz.aggregated_metrics.standard_deployed_unit_of_analysis_types_by_population_type import (
    UNIT_OF_ANALYSIS_TYPES_BY_POPULATION_TYPE,
)
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


# TODO(#35914): Delete this once all callers use a collector that builds optimized
#  queries instead.
def collect_legacy_aggregated_metrics_view_builders(
    *,
    metrics_by_population_dict: Dict[MetricPopulationType, List[AggregatedMetric]],
    units_of_analysis_by_population_dict: Dict[
        MetricPopulationType, List[MetricUnitOfAnalysisType]
    ],
) -> List[SimpleBigQueryViewBuilder]:
    """
    Collects all aggregated metrics view builders at all available units of analysis and populations
    """
    view_builders = []

    for population_type, all_metrics in metrics_by_population_dict.items():
        if not all_metrics:
            continue

        # Check that all EventValueMetrics have the configured EventCountMetric included for the same population
        event_value_metric_list = [
            m for m in all_metrics if isinstance(m, EventValueMetric)
        ]
        for metric in event_value_metric_list:
            if metric.event_count_metric not in all_metrics:
                raise ValueError(
                    f"`{metric.event_count_metric.name}` EventCountMetric "
                    f"not found in configured `metrics_by_population_dict` for "
                    f"{population_type.name} population, although this is a required "
                    f"dependency for `{metric.name}` EventValueMetric."
                )

        for unit_of_analysis_type in units_of_analysis_by_population_dict[
            population_type
        ]:
            unit_of_analysis = MetricUnitOfAnalysis.for_type(unit_of_analysis_type)

            # Build metric builder views by type
            # PeriodSpanAggregatedMetric
            period_span_metric_list = [
                m for m in all_metrics if isinstance(m, PeriodSpanAggregatedMetric)
            ]
            if period_span_metric_list:
                view_builders.append(
                    generate_period_span_aggregated_metrics_view_builder(
                        unit_of_analysis=unit_of_analysis,
                        population_type=population_type,
                        metrics=period_span_metric_list,
                    )
                )

            if not is_metric_class_supported_by_optimized_format(
                PeriodEventAggregatedMetric
            ):
                raise ValueError(
                    "Expected PeriodEventAggregatedMetric to be generated by new "
                    "optimized aggregated metric view collection."
                )

            # AssignmentSpanAggregatedMetric
            assignment_span_metric_list = [
                m for m in all_metrics if isinstance(m, AssignmentSpanAggregatedMetric)
            ]
            if assignment_span_metric_list:
                view_builders.append(
                    generate_assignment_span_aggregated_metrics_view_builder(
                        unit_of_analysis=unit_of_analysis,
                        population_type=population_type,
                        metrics=assignment_span_metric_list,
                    )
                )

            # AssignmentEventAggregatedMetric
            assignment_event_metric_list = [
                m for m in all_metrics if isinstance(m, AssignmentEventAggregatedMetric)
            ]
            if assignment_event_metric_list:
                view_builders.append(
                    generate_assignment_event_aggregated_metrics_view_builder(
                        unit_of_analysis=unit_of_analysis,
                        population_type=population_type,
                        metrics=assignment_event_metric_list,
                    )
                )

            # verify that ASSIGNMENTS is present if any assignment span/event metrics
            # are present
            if assignment_span_metric_list or assignment_event_metric_list:
                if not ASSIGNMENTS in all_metrics:
                    raise ValueError(
                        "Assignment span/event metrics are present but ASSIGNMENTS is not"
                    )

            # MiscAggregatedMetric
            misc_metric_list = [
                m
                for m in all_metrics
                if isinstance(m, MiscAggregatedMetric)
                and population_type in m.populations
                and unit_of_analysis_type in m.unit_of_analysis_types
            ]
            if misc_metric_list:
                # Even if there are misc metrics, generate_misc_aggregated_metrics_view_builder
                # may return None if there are no metrics for the given population,
                # so first generate the view builder and then check if it is None
                misc_metric_view_builder = (
                    generate_misc_aggregated_metrics_view_builder(
                        unit_of_analysis=unit_of_analysis,
                        population_type=population_type,
                        metrics=misc_metric_list,
                    )
                )
                if misc_metric_view_builder:
                    view_builders.append(misc_metric_view_builder)

    return view_builders


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        for view_builder in collect_legacy_aggregated_metrics_view_builders(
            metrics_by_population_dict=METRICS_BY_POPULATION_TYPE,
            units_of_analysis_by_population_dict=UNIT_OF_ANALYSIS_TYPES_BY_POPULATION_TYPE,
        ):
            view_builder.build_and_print()
