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
"""Functions for collecting aggregated metrics views"""
import itertools
from collections import defaultdict

from recidiviz.aggregated_metrics.aggregated_metric_collection_config import (
    AggregatedMetricsCollection,
)
from recidiviz.aggregated_metrics.aggregated_metrics_view_builder import (
    AggregatedMetricsBigQueryViewBuilder,
    aggregated_metric_view_description,
)
from recidiviz.aggregated_metrics.assignments_by_time_period_view_builder import (
    AssignmentsByTimePeriodViewBuilder,
)
from recidiviz.aggregated_metrics.models.aggregated_metric import (
    AssignmentEventAggregatedMetric,
    AssignmentSpanAggregatedMetric,
    MetricTimePeriodJoinType,
    PeriodEventAggregatedMetric,
    PeriodSpanAggregatedMetric,
)
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysis,
    MetricUnitOfAnalysisType,
)
from recidiviz.aggregated_metrics.query_building.aggregated_metric_query_utils import (
    AggregatedMetricClassType,
)
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.union_all_big_query_view_builder import (
    UnionAllBigQueryViewBuilder,
)
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.observations.metric_unit_of_observation_type import (
    MetricUnitOfObservationType,
)

METRIC_CLASSES: list[AggregatedMetricClassType] = [
    PeriodEventAggregatedMetric,
    PeriodSpanAggregatedMetric,
    AssignmentEventAggregatedMetric,
    AssignmentSpanAggregatedMetric,
    # TODO(#29291): Figure out what to do with MiscAggregatedMetric
]


def _build_time_periods_unioned_view_address(
    dataset_id: str,
    population_type: MetricPopulationType,
    unit_of_analysis_type: MetricUnitOfAnalysisType,
    metric_class: AggregatedMetricClassType,
) -> BigQueryAddress:
    population_name = population_type.population_name_short
    unit_of_analysis_name = unit_of_analysis_type.short_name
    metric_class_name = metric_class.metric_class_name_lower()
    view_id = f"{population_name}_{unit_of_analysis_name}_{metric_class_name}_aggregated_metrics"

    return BigQueryAddress(dataset_id=dataset_id, table_id=view_id)


def _build_time_periods_unioned_view_builder(
    dataset_id: str,
    population_type: MetricPopulationType,
    unit_of_analysis_type: MetricUnitOfAnalysisType,
    metric_class: AggregatedMetricClassType,
    parents: list[AggregatedMetricsBigQueryViewBuilder],
) -> UnionAllBigQueryViewBuilder:
    """Returns a view builder for a view that unions together multiple aggregated
    metrics views that produce metrics for different time periods but otherwise the same
    set of metrics and other parameters.
    """
    view_address = _build_time_periods_unioned_view_address(
        dataset_id=dataset_id,
        population_type=population_type,
        unit_of_analysis_type=unit_of_analysis_type,
        metric_class=metric_class,
    )

    if not parents:
        raise ValueError(
            f"Expected at least one parent for UnionAllBigQueryViewBuilder "
            f"[{view_address.to_str()}]"
        )

    unit_of_analysis = MetricUnitOfAnalysis.for_type(unit_of_analysis_type)

    def _parent_to_select_statement(vb: AggregatedMetricsBigQueryViewBuilder) -> str:
        columns_str = list_to_query_string(vb.output_columns)
        return f"SELECT {columns_str}"

    description = aggregated_metric_view_description(
        population_type=population_type,
        unit_of_analysis_type=unit_of_analysis_type,
        metric_class=metric_class,
        metrics=parents[0].metrics,
        time_period=None,
    )
    bq_description = aggregated_metric_view_description(
        population_type=population_type,
        unit_of_analysis_type=unit_of_analysis_type,
        metric_class=metric_class,
        metrics=None,
        time_period=None,
    )

    return UnionAllBigQueryViewBuilder(
        dataset_id=view_address.dataset_id,
        view_id=view_address.table_id,
        description=description,
        bq_description=bq_description,
        parents=parents,
        parent_to_select_statement=_parent_to_select_statement,
        clustering_fields=unit_of_analysis.primary_key_columns,
    )


# TODO(#29291): Remove this function once aggregated metrics ship for all metric classes
def is_metric_class_supported_by_optimized_format(
    metric_class: AggregatedMetricClassType,
) -> bool:
    if metric_class in {
        # TODO(#29291): Add support for PeriodSpanAggregatedMetric
        PeriodSpanAggregatedMetric,
        # TODO(#29291): Add support for AssignmentEventAggregatedMetric
        AssignmentEventAggregatedMetric,
        # TODO(#29291): Add support for AssignmentSpanAggregatedMetric
        AssignmentSpanAggregatedMetric,
    }:
        return False
    return True


def collect_assignments_by_time_period_builders_for_collections(
    collection_configs: list[AggregatedMetricsCollection],
) -> list[AssignmentsByTimePeriodViewBuilder]:
    """For the given aggregated metrics collections, returns the set of assigments by
    time period view builders for views that will be referenced by any of those
    collections. Some returned views may be referenced by multiple collections.
    """
    builders_by_address = {}
    for collection_config in collection_configs:
        for (
            population_type,
            population_config,
        ) in collection_config.population_configs.items():
            observation_types_by_join_type: dict[
                MetricTimePeriodJoinType, set[MetricUnitOfObservationType]
            ] = defaultdict(set)
            for metric_class in METRIC_CLASSES:
                if not is_metric_class_supported_by_optimized_format(metric_class):
                    continue

                join_type: MetricTimePeriodJoinType = (
                    metric_class.metric_time_period_join_type()
                )
                metrics = population_config.metrics_of_class(metric_class)
                if not metrics:
                    continue

                for m in metrics:
                    observation_types_by_join_type[join_type].add(
                        m.unit_of_observation_type
                    )

            for (
                metric_time_period_join_type,
                unit_of_observation_types,
            ) in observation_types_by_join_type.items():
                for (
                    unit_of_observation_type,
                    unit_of_analysis_type,
                    time_period,
                ) in itertools.product(
                    unit_of_observation_types,
                    population_config.units_of_analysis,
                    collection_config.time_periods,
                ):
                    builder = AssignmentsByTimePeriodViewBuilder(
                        population_type=population_type,
                        unit_of_analysis_type=unit_of_analysis_type,
                        unit_of_observation_type=unit_of_observation_type,
                        metric_time_period_join_type=metric_time_period_join_type,
                        time_period=time_period,
                    )
                    if builder.address in builders_by_address:
                        continue

                    builders_by_address[builder.address] = builder
    return sorted(builders_by_address.values(), key=lambda b: builder.address.to_str())


def collect_aggregated_metric_view_builders_for_collection(
    collection_config: AggregatedMetricsCollection,
) -> list[AggregatedMetricsBigQueryViewBuilder | UnionAllBigQueryViewBuilder]:
    """For the given collection config, returns all view builders that must be deployed
    to calculate these metrics. Returns both the AggregatedMetricsBigQueryViewBuilder
    views which actually calculate the metrics and the UnionAllBigQueryViewBuilder views
    which union the metrics from different time period configs into one view.

    Assumes that collect_assignments_by_time_period_builders_for_collections() has
    already been called with this config to produce the appropriate
    AssignmentsByTimePeriodViewBuilder views referenced by this collection.
    """
    builders: list[
        AggregatedMetricsBigQueryViewBuilder | UnionAllBigQueryViewBuilder
    ] = []

    for metric_class in METRIC_CLASSES:
        if not is_metric_class_supported_by_optimized_format(metric_class):
            continue

        for (
            population_type,
            population_config,
        ) in collection_config.population_configs.items():
            metrics = population_config.metrics_of_class(metric_class)
            if not metrics:
                continue

            for unit_of_analysis_type in population_config.units_of_analysis:
                time_period_specific_builders = []
                for time_period in collection_config.time_periods:
                    time_period_specific_builders.append(
                        AggregatedMetricsBigQueryViewBuilder(
                            dataset_id=collection_config.output_dataset_id,
                            population_type=population_type,
                            unit_of_analysis_type=unit_of_analysis_type,
                            metric_class=metric_class,
                            metrics=metrics,
                            time_period=time_period,
                        )
                    )
                builders.extend(time_period_specific_builders)
                builders.append(
                    _build_time_periods_unioned_view_builder(
                        dataset_id=collection_config.output_dataset_id,
                        population_type=population_type,
                        unit_of_analysis_type=unit_of_analysis_type,
                        metric_class=metric_class,
                        parents=time_period_specific_builders,
                    )
                )

    return builders
