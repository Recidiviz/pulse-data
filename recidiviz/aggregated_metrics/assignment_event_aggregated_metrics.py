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
"""Generates view builder calculating assignment-event metrics"""
from typing import List

from recidiviz.aggregated_metrics.aggregated_metrics_utils import (
    get_unioned_time_granularity_clause,
)
from recidiviz.aggregated_metrics.dataset_config import AGGREGATED_METRICS_DATASET_ID
from recidiviz.aggregated_metrics.metric_time_periods import MetricTimePeriod
from recidiviz.aggregated_metrics.models.aggregated_metric import (
    AssignmentEventAggregatedMetric,
)
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.views.analyst_data.models.metric_population_type import (
    MetricPopulation,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysis,
)


def generate_assignment_event_aggregated_metrics_view_builder(
    aggregation_level: MetricUnitOfAnalysis,
    population: MetricPopulation,
    metrics: List[AssignmentEventAggregatedMetric],
) -> SimpleBigQueryViewBuilder:
    """
    Returns a SimpleBigQueryViewBuilder that calculates AssignmentEvent metrics for the specified
    aggregation level, population, and set of metrics.
    """
    view_id = f"{population.population_name_short}_{aggregation_level.level_name_short}_assignment_event_aggregated_metrics"
    view_description = f"""
    Metrics for the {population.population_name_short} population calculated using
    `person_events` over some window following assignment, for all assignments
    during an analysis period, disaggregated by {aggregation_level.level_name_short}.

    All end_dates are exclusive, i.e. the metric is for the range [start_date, end_date).
    """

    # helper function to get weekly and monthly aggregated CTEs
    def get_time_specific_ctes(unit_of_analysis: MetricTimePeriod) -> str:
        """
        Returns CTEs for weekly and monthly aggregated metrics.
        """
        if unit_of_analysis not in (MetricTimePeriod.WEEK, MetricTimePeriod.MONTH):
            raise ValueError(f"Unsupported unit_of_analysis: {unit_of_analysis.value}")

        return f"""
    SELECT
        {aggregation_level.get_index_columns_query_string()},
        population_start_date AS start_date,
        population_end_date AS end_date,
        period,
        {metric_aggregation_fragment_outer}
    FROM (
        SELECT
            {aggregation_level.get_index_columns_query_string("assign")},
            population_start_date,
            population_end_date,
            period,
            assign.person_id,
            assign.assignment_date,
            {metric_aggregation_fragment_inner}
        FROM
            time_periods pop
        LEFT JOIN
            assignments assign
        ON
            assign.assignment_date BETWEEN population_start_date AND DATE_SUB(population_end_date, INTERVAL 1 DAY)
        LEFT JOIN
            `{{project_id}}.analyst_data.person_events_materialized` events
        ON
            assign.person_id = events.person_id
            AND events.event_date >= assign.assignment_date
        WHERE
            period = "{unit_of_analysis.value}"
        GROUP BY
            {aggregation_level.get_index_columns_query_string("assign")},
            population_start_date,
            population_end_date,
            period,
            assign.person_id,
            assign.assignment_date
    )
    GROUP BY
        {aggregation_level.get_index_columns_query_string()},
        population_start_date, population_end_date, period"""

    metric_aggregation_fragment_inner = ",\n".join(
        [
            metric.generate_aggregation_query_fragment(
                event_date_col="events.event_date",
                assignment_date_col="assign.assignment_date",
            )
            for metric in metrics
            if isinstance(metric, AssignmentEventAggregatedMetric)
        ]
    )
    metric_aggregation_fragment_outer = ",\n".join(
        [metric.generate_aggregate_time_periods_query_fragment() for metric in metrics]
    )

    query_template = f"""
WITH time_periods AS (
    SELECT * FROM `{{project_id}}.aggregated_metrics.metric_time_periods_materialized`
)
,
assignments AS (
    SELECT *
    FROM
        `{{project_id}}.aggregated_metrics.{population.population_name_short}_{aggregation_level.level_name_short}_metrics_assignment_sessions_materialized`
)

, week_metrics AS ({get_time_specific_ctes(MetricTimePeriod.WEEK)})

, month_metrics AS ({get_time_specific_ctes(MetricTimePeriod.MONTH)})
""" + get_unioned_time_granularity_clause(
        aggregation_level=aggregation_level,
        metrics=metrics,
    )

    return SimpleBigQueryViewBuilder(
        dataset_id=AGGREGATED_METRICS_DATASET_ID,
        view_id=view_id,
        view_query_template=query_template,
        description=view_description,
        should_materialize=False,
        clustering_fields=aggregation_level.primary_key_columns,
    )
