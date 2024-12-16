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
"""Creates view builders that generate SQL views calculating period-span metrics"""
from collections import defaultdict
from typing import Dict, List

from recidiviz.aggregated_metrics.legacy.aggregated_metrics_utils import (
    get_joined_metrics_by_observation_type_query,
)
from recidiviz.aggregated_metrics.metric_time_period_config import MetricTimePeriod
from recidiviz.aggregated_metrics.models.aggregated_metric import (
    PeriodSpanAggregatedMetric,
)
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysis,
)
from recidiviz.calculator.query.bq_utils import (
    join_on_columns_fragment,
    list_to_query_string,
    nonnull_current_date_clause,
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
    revert_nonnull_end_date_clause,
)
from recidiviz.observations.dataset_config import dataset_for_observation_type_cls
from recidiviz.observations.metric_unit_of_observation import MetricUnitOfObservation
from recidiviz.observations.metric_unit_of_observation_type import (
    MetricUnitOfObservationType,
)
from recidiviz.observations.span_type import SpanType


# TODO(#35914): This function should become unused once we've migrated over to optimized
#  aggregated metrics queries.
def get_period_span_time_specific_cte(
    unit_of_analysis: MetricUnitOfAnalysis,
    population_type: MetricPopulationType,
    metrics: List[PeriodSpanAggregatedMetric],
    metric_time_period: MetricTimePeriod,
) -> str:
    """Returns query template for calculating period span metrics for a single metric time period,
    unioning together across metrics from each unit of observation"""

    metric_subqueries_by_unit_of_observation: List[str] = []
    metrics_by_unit_of_observation_type: Dict[
        MetricUnitOfObservationType, List[PeriodSpanAggregatedMetric]
    ] = defaultdict(list)
    for metric in metrics:
        metrics_by_unit_of_observation_type[metric.unit_of_observation_type].append(
            metric
        )

    for unit_of_observation_type in sorted(
        metrics_by_unit_of_observation_type.keys(), key=lambda t: t.value
    ):
        metrics_for_unit_of_observation = metrics_by_unit_of_observation_type[
            unit_of_observation_type
        ]
        unit_of_observation = MetricUnitOfObservation(type=unit_of_observation_type)

        shared_columns_string = list_to_query_string(
            sorted(
                {
                    *unit_of_observation.primary_key_columns,
                    *unit_of_analysis.primary_key_columns,
                }
            ),
            table_prefix="assign",
        )
        metric_aggregation_fragment = ",\n".join(
            sorted(
                metric.generate_aggregation_query_fragment(
                    filter_observations_by_type=True,
                    read_observation_attributes_from_json=True,
                    observations_cte_name="ses",
                    span_start_date_col="ses.start_date",
                    span_end_date_col="ses.end_date",
                    period_start_date_col="pop.population_start_date",
                    period_end_date_col="pop.population_end_date",
                    original_span_start_date="ses.span_start_date",
                )
                for metric in metrics_for_unit_of_observation
            )
        )

        unit_of_analysis_join_columns_str = (
            unit_of_analysis.get_primary_key_columns_query_string()
        )

        spans_dataset_id = dataset_for_observation_type_cls(
            unit_of_observation=unit_of_observation.type, observation_type_cls=SpanType
        )

        metric_subquery = f"""
SELECT
    {unit_of_analysis_join_columns_str},
    population_start_date AS start_date,
    population_end_date AS end_date,
    period,
    {metric_aggregation_fragment}
FROM (
    SELECT
        {shared_columns_string},
        GREATEST(assign.assignment_date, spans.start_date) AS start_date,
        spans.start_date AS span_start_date,
        {revert_nonnull_end_date_clause(
            f"LEAST({nonnull_end_date_clause('spans.end_date')}, {nonnull_end_date_clause('assign.end_date')})"
        )} AS end_date,
        span,
        span_attributes,
    FROM
        `{{project_id}}.{spans_dataset_id}.all_{unit_of_observation.type.value.lower()}_spans_materialized` AS spans
    INNER JOIN
        `{{project_id}}.aggregated_metrics.{population_type.population_name_short}_{unit_of_analysis.type.short_name}_metrics_{unit_of_observation.type.short_name}_assignment_sessions_materialized` assign
    ON
        {join_on_columns_fragment(columns=unit_of_observation.primary_key_columns_ordered, table1="spans", table2="assign")}
        AND (
          assign.assignment_date
            BETWEEN spans.start_date AND {nonnull_end_date_exclusive_clause("spans.end_date")}
          OR spans.start_date
            BETWEEN assign.assignment_date AND {nonnull_end_date_exclusive_clause("assign.end_date")}
        )
        -- Disregard zero day sample spans for calculating population metrics
        AND assign.assignment_date != {nonnull_end_date_clause("assign.end_date")}
) ses
INNER JOIN
    time_periods pop
ON
    ses.start_date < pop.population_end_date
    AND pop.population_start_date < {nonnull_current_date_clause("ses.end_date")}
WHERE
    period = "{metric_time_period.value}"
GROUP BY
    {unit_of_analysis_join_columns_str},
    population_start_date, population_end_date, period"""
        metric_subqueries_by_unit_of_observation.append(metric_subquery)
    # Construct query by joining metrics across all units of observation
    return get_joined_metrics_by_observation_type_query(
        unit_of_analysis, metric_subqueries_by_unit_of_observation, metrics
    )
