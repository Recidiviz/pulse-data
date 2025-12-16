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
from recidiviz.aggregated_metrics.all_aggregated_metrics_view_builder import (
    generate_all_aggregated_metrics_view_builder,
)
from recidiviz.aggregated_metrics.assignments_by_time_period_view_builder import (
    AssignmentsByTimePeriodViewBuilder,
)
from recidiviz.aggregated_metrics.models.aggregated_metric import (
    AssignmentEventAggregatedMetric,
    AssignmentSpanAggregatedMetric,
    MetricTimePeriodToAssignmentJoinType,
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
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.big_query.union_all_big_query_view_builder import (
    UnionAllBigQueryViewBuilder,
)
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.observations.metric_unit_of_observation_type import (
    MetricUnitOfObservationType,
)
from recidiviz.utils.immutable_key_dict import ImmutableKeyDict

METRIC_CLASSES: list[AggregatedMetricClassType] = [
    PeriodEventAggregatedMetric,
    PeriodSpanAggregatedMetric,
    AssignmentEventAggregatedMetric,
    AssignmentSpanAggregatedMetric,
]


def _build_time_periods_unioned_view_address(
    dataset_id: str,
    population_type: MetricPopulationType,
    unit_of_analysis_type: MetricUnitOfAnalysisType,
    metric_class: AggregatedMetricClassType,
    collection_tag: str | None,
) -> BigQueryAddress:
    collection_tag_part = "" if not collection_tag else f"{collection_tag}__"
    population_name = population_type.population_name_short
    unit_of_analysis_name = unit_of_analysis_type.short_name
    metric_class_name = metric_class.metric_class_name_lower()
    view_id = f"{collection_tag_part}{population_name}_{unit_of_analysis_name}_{metric_class_name}_aggregated_metrics"

    return BigQueryAddress(dataset_id=dataset_id, table_id=view_id)


def _build_time_periods_unioned_view_builder(
    dataset_id: str,
    population_type: MetricPopulationType,
    unit_of_analysis_type: MetricUnitOfAnalysisType,
    metric_class: AggregatedMetricClassType,
    parents: list[AggregatedMetricsBigQueryViewBuilder],
    collection_tag: str | None,
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
        collection_tag=collection_tag,
    )

    found_period_names = set()
    for parent in parents:
        if (period_name := parent.time_period.period_name) in found_period_names:
            raise ValueError(
                f"Found multiple parents for view [{view_address.to_str()}] with "
                f"multiple parents with period_name [{period_name}]"
            )
        found_period_names.add(period_name)

    if not parents:
        raise ValueError(
            f"Expected at least one parent for UnionAllBigQueryViewBuilder "
            f"[{view_address.to_str()}]"
        )

    unit_of_analysis = MetricUnitOfAnalysis.for_type(unit_of_analysis_type)

    def _parent_view_to_select_statement(
        vb: AggregatedMetricsBigQueryViewBuilder,
    ) -> str:
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
        parent_view_to_select_statement=_parent_view_to_select_statement,
        clustering_fields=unit_of_analysis.primary_key_columns,
    )


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
                MetricTimePeriodToAssignmentJoinType, set[MetricUnitOfObservationType]
            ] = defaultdict(set)
            for metric_class in METRIC_CLASSES:
                join_type: MetricTimePeriodToAssignmentJoinType = (
                    metric_class.metric_time_period_to_assignment_join_type()
                )
                metrics = population_config.metrics_of_class(metric_class)
                if not metrics:
                    continue

                for m in metrics:
                    observation_types_by_join_type[join_type].add(
                        m.unit_of_observation_type
                    )

            for (
                metric_time_period_to_assignment_join_type,
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
                        metric_time_period_to_assignment_join_type=metric_time_period_to_assignment_join_type,
                        time_period=time_period,
                    )
                    if builder.address in builders_by_address:
                        continue

                    builders_by_address[builder.address] = builder
    return sorted(builders_by_address.values(), key=lambda b: builder.address.to_str())


def collect_aggregated_metric_view_builders_for_collection(
    collection_config: AggregatedMetricsCollection,
) -> list[
    AggregatedMetricsBigQueryViewBuilder
    | UnionAllBigQueryViewBuilder
    | SimpleBigQueryViewBuilder
]:
    """For the given collection config, returns all view builders that must be deployed
    to calculate these metrics. Returns the AggregatedMetricsBigQueryViewBuilder
    views which actually calculate the metrics, the UnionAllBigQueryViewBuilder views
    which union the metrics from different time period configs into one view and the
    view that joins all metric type-specific view with *all* metrics specified in the
    config.

    Assumes that collect_assignments_by_time_period_builders_for_collections() has
    already been called with this config to produce the appropriate
    AssignmentsByTimePeriodViewBuilder views referenced by this collection.
    """
    builders: ImmutableKeyDict[
        BigQueryAddress,
        (
            AggregatedMetricsBigQueryViewBuilder
            | UnionAllBigQueryViewBuilder
            | SimpleBigQueryViewBuilder
        ),
    ] = ImmutableKeyDict()

    for metric_class in METRIC_CLASSES:
        for (
            population_type,
            population_config,
        ) in collection_config.population_configs.items():
            metrics = population_config.metrics_of_class(metric_class)
            if not metrics:
                continue

            for unit_of_analysis_type in population_config.units_of_analysis:
                time_period_specific_builders: ImmutableKeyDict[
                    BigQueryAddress, AggregatedMetricsBigQueryViewBuilder
                ] = ImmutableKeyDict()
                for time_period in collection_config.time_periods:
                    builder = AggregatedMetricsBigQueryViewBuilder(
                        dataset_id=collection_config.output_dataset_id,
                        population_type=population_type,
                        unit_of_analysis_type=unit_of_analysis_type,
                        metric_class=metric_class,
                        metrics=metrics,
                        time_period=time_period,
                        collection_tag=collection_config.collection_tag,
                        disaggregate_by_observation_attributes=collection_config.disaggregate_by_observation_attributes,
                    )
                    time_period_specific_builders[builder.address] = builder
                builders.update(time_period_specific_builders)

                unioned_builder = _build_time_periods_unioned_view_builder(
                    dataset_id=collection_config.output_dataset_id,
                    population_type=population_type,
                    unit_of_analysis_type=unit_of_analysis_type,
                    metric_class=metric_class,
                    parents=list(time_period_specific_builders.values()),
                    collection_tag=collection_config.collection_tag,
                )

                builders[unioned_builder.address] = unioned_builder

    for (
        population_type,
        population_config,
    ) in collection_config.population_configs.items():
        for unit_of_analysis_type in population_config.units_of_analysis:
            all_metrics_builder = generate_all_aggregated_metrics_view_builder(
                unit_of_analysis=MetricUnitOfAnalysis.for_type(unit_of_analysis_type),
                population_type=population_type,
                metrics=population_config.metrics,
                dataset_id_override=population_config.output_dataset_id,
                collection_tag=collection_config.collection_tag,
            )

            builders[all_metrics_builder.address] = all_metrics_builder

    return list(builders.values())
