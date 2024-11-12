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
"""Creates view builders that generate SQL views calculating period-event metrics"""
from collections import defaultdict
from typing import Dict, List

from recidiviz.aggregated_metrics.aggregated_metrics_utils import (
    get_joined_metrics_by_observation_type_query,
    get_unioned_time_granularity_clause,
)
from recidiviz.aggregated_metrics.dataset_config import AGGREGATED_METRICS_DATASET_ID
from recidiviz.aggregated_metrics.metric_time_periods import MetricTimePeriod
from recidiviz.aggregated_metrics.models.aggregated_metric import (
    PeriodEventAggregatedMetric,
)
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    join_on_columns_fragment,
    nonnull_end_date_clause,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysis,
)
from recidiviz.observations.dataset_config import dataset_for_observation_type_cls
from recidiviz.observations.event_type import EventType
from recidiviz.observations.metric_unit_of_observation import MetricUnitOfObservation
from recidiviz.observations.metric_unit_of_observation_type import (
    MetricUnitOfObservationType,
)


# TODO(#29291): This function should become unused once we've migrated over to optimized
#  aggregated metrics queries.
def get_period_event_time_specific_cte(
    unit_of_analysis: MetricUnitOfAnalysis,
    population_type: MetricPopulationType,
    metrics: List[PeriodEventAggregatedMetric],
    metric_time_period: MetricTimePeriod,
) -> str:
    """Returns query template for calculating assignment span metrics for a single metric time period,
    unioning together across metrics from each unit of observation"""

    metric_subqueries_by_unit_of_observation: List[str] = []

    metrics_by_unit_of_observation_type: Dict[
        MetricUnitOfObservationType, List[PeriodEventAggregatedMetric]
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
        metric_aggregation_fragment = ",\n".join(
            [
                metric.generate_aggregation_query_fragment(
                    filter_observations_by_type=True,
                    read_observation_attributes_from_json=True,
                    event_date_col="events.event_date",
                )
                for metric in metrics_for_unit_of_observation
            ]
        )

        unit_of_analysis_join_columns_str = (
            unit_of_analysis.get_primary_key_columns_query_string("assign")
        )

        events_dataset_id = dataset_for_observation_type_cls(
            unit_of_observation=unit_of_observation_type, observation_type_cls=EventType
        )

        metric_subquery = f"""
SELECT
    {unit_of_analysis_join_columns_str},
    population_start_date AS start_date,
    population_end_date AS end_date,
    period,
    {metric_aggregation_fragment}
FROM
    time_periods pop
INNER JOIN
    `{{project_id}}.aggregated_metrics.{population_type.population_name_short}_{unit_of_analysis.type.short_name}_metrics_{unit_of_observation.type.short_name}_assignment_sessions_materialized` assign
ON
    assign.assignment_date < pop.population_end_date
    AND {nonnull_end_date_clause("assign.end_date")} >= pop.population_start_date
LEFT JOIN
    `{{project_id}}.{events_dataset_id}.all_{unit_of_observation.type.value.lower()}_events_materialized` AS events
ON
    {join_on_columns_fragment(columns=sorted(unit_of_observation.primary_key_columns), table1="events", table2="assign")}
    -- Include events occurring on the last date of an end-date exclusive span,
    -- but exclude events occurring on the last date of an end-date exclusive analysis period.
    AND events.event_date BETWEEN GREATEST(assign.assignment_date, pop.population_start_date)
        AND LEAST(
            {nonnull_end_date_clause("assign.end_date")},
            DATE_SUB(pop.population_end_date, INTERVAL 1 DAY)
        )
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


# TODO(#29291): This function should become unused once we've migrated over to optimized
#  aggregated metrics queries.
def generate_period_event_aggregated_metrics_view_builder(
    unit_of_analysis: MetricUnitOfAnalysis,
    population_type: MetricPopulationType,
    metrics: List[PeriodEventAggregatedMetric],
) -> SimpleBigQueryViewBuilder:
    """
    Returns a SimpleBigQueryViewBuilder that calculates PeriodEvent metrics for the specified
    unit of analysis, population, and set of metrics.
    """
    view_id = f"{population_type.population_name_short}_{unit_of_analysis.type.short_name}_period_event_aggregated_metrics"
    view_description = f"""
    Metrics for the {population_type.population_name_short} population calculated using
    events across an entire analysis period, disaggregated by {unit_of_analysis.type.short_name}.

    All end_dates are exclusive, i.e. the metric is for the range [start_date, end_date).
    """

    query_template = f"""
WITH time_periods AS (
    SELECT * FROM `{{project_id}}.aggregated_metrics.metric_time_periods_materialized`
)
, month_metrics AS ({get_period_event_time_specific_cte(
    unit_of_analysis = unit_of_analysis,
    population_type = population_type,
    metrics = metrics,
    metric_time_period = MetricTimePeriod.MONTH,
)})
""" + get_unioned_time_granularity_clause(
        unit_of_analysis=unit_of_analysis,
        metrics=metrics,
    )

    return SimpleBigQueryViewBuilder(
        dataset_id=AGGREGATED_METRICS_DATASET_ID,
        view_id=view_id,
        view_query_template=query_template,
        description=view_description,
        should_materialize=True,
        clustering_fields=unit_of_analysis.primary_key_columns,
    )
