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
from typing import List

from recidiviz.aggregated_metrics.aggregated_metrics_utils import (
    get_unioned_time_granularity_clause,
)
from recidiviz.aggregated_metrics.dataset_config import AGGREGATED_METRICS_DATASET_ID
from recidiviz.aggregated_metrics.models.aggregated_metric import (
    PeriodEventAggregatedMetric,
)
from recidiviz.aggregated_metrics.models.metric_aggregation_level_type import (
    MetricAggregationLevel,
)
from recidiviz.aggregated_metrics.models.metric_population_type import MetricPopulation
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET


def generate_period_event_aggregated_metrics_view_builder(
    aggregation_level: MetricAggregationLevel,
    population: MetricPopulation,
    metrics: List[PeriodEventAggregatedMetric],
) -> SimpleBigQueryViewBuilder:
    """
    Returns a SimpleBigQueryViewBuilder that calculates PeriodEvent metrics for the specified
    aggregation level, population, and set of metrics.
    """
    view_id = f"{population.population_name_short}_{aggregation_level.level_name_short}_period_event_aggregated_metrics"
    view_description = f"""
    Metrics for the {population.population_name_short} population calculated using 
    `person_events` across an entire analysis period, disaggregated by {aggregation_level.level_name_short}.

    All end_dates are exclusive, i.e. the metric is for the range [start_date, end_date).
    """
    query_template = (
        f"""
WITH time_periods AS (
    SELECT * FROM `{{project_id}}.{{aggregated_metrics_dataset}}.metric_time_periods_materialized`
)
,
assignments AS (
    SELECT *
    FROM
        `{{project_id}}.{{aggregated_metrics_dataset}}.{population.population_name_short}_{aggregation_level.level_name_short}_metrics_assignment_sessions_materialized`
)

, month_metrics AS (
    SELECT
        {aggregation_level.get_index_columns_query_string("assign")},
        population_start_date AS start_date,
        population_end_date AS end_date,
        period,
    """
        + ",\n".join(
            [
                metric.generate_aggregation_query_fragment(
                    event_date_col="events.event_date"
                )
                for metric in metrics
            ]
        )
        + f"""
    FROM 
        time_periods pop
    INNER JOIN
        assignments assign
    ON
        assign.assignment_date < pop.population_end_date
        AND {nonnull_end_date_clause("assign.end_date")} >= pop.population_start_date
    LEFT JOIN 
        `{{project_id}}.{{analyst_dataset}}.person_events_materialized` AS events
    ON 
        events.person_id = assign.person_id
        -- Include events occurring on the last date of an end-date exclusive span,
        -- but exclude events occurring on the last date of an end-date exclusive analysis period.
        AND events.event_date BETWEEN GREATEST(assign.assignment_date, pop.population_start_date) 
            AND LEAST(
                {nonnull_end_date_clause("assign.end_date")}, 
                DATE_SUB(pop.population_end_date, INTERVAL 1 DAY)
            )
    WHERE
        period = "MONTH"
    GROUP BY 
        {aggregation_level.get_index_columns_query_string()},
        population_start_date, population_end_date, period
)"""
        + get_unioned_time_granularity_clause(
            aggregation_level=aggregation_level,
            metrics=metrics,
        )
    )

    return SimpleBigQueryViewBuilder(
        dataset_id=AGGREGATED_METRICS_DATASET_ID,
        view_id=view_id,
        view_query_template=query_template,
        description=view_description,
        analyst_dataset=ANALYST_VIEWS_DATASET,
        aggregated_metrics_dataset=AGGREGATED_METRICS_DATASET_ID,
        should_materialize=False,
        clustering_fields=aggregation_level.index_columns,
    )
