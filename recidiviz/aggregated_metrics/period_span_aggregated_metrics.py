# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
from typing import List

from recidiviz.aggregated_metrics.dataset_config import AGGREGATED_METRICS_DATASET_ID
from recidiviz.aggregated_metrics.models.aggregated_metric import (
    PeriodSpanAggregatedMetric,
)
from recidiviz.aggregated_metrics.models.metric_aggregation_level_type import (
    MetricAggregationLevel,
)
from recidiviz.aggregated_metrics.models.metric_population_type import MetricPopulation
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    nonnull_current_date_clause,
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
    revert_nonnull_end_date_clause,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET


def generate_period_span_aggregated_metrics_view_builder(
    aggregation_level: MetricAggregationLevel,
    population: MetricPopulation,
    metrics: List[PeriodSpanAggregatedMetric],
) -> SimpleBigQueryViewBuilder:
    """
    Returns a SimpleBigQueryViewBuilder that calculates PeriodSpan metrics for the specified
    aggregation level, population, and set of metrics.
    """
    view_id = f"{population.population_name_short}_{aggregation_level.level_name_short}_period_span_aggregated_metrics"
    view_description = f"""
    Metrics for the {population.population_name_short} population calculated using 
    `person_spans` across an entire analysis period, disaggregated by {aggregation_level.level_name_short}.

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
,
eligible_spans AS (
    SELECT
        spans.person_id,
        {aggregation_level.get_index_columns_query_string("assign")},
        GREATEST(assign.assignment_date, spans.start_date) AS start_date,
        spans.start_date AS span_start_date,
        {revert_nonnull_end_date_clause(
            f"LEAST({nonnull_end_date_clause('spans.end_date')}, {nonnull_end_date_clause('assign.end_date')})"
        )} AS end_date,
        span,
        span_attributes,
    FROM
        `{{project_id}}.{{analyst_dataset}}.person_spans_materialized` AS spans
    INNER JOIN
        assignments assign
    ON
        assign.person_id = spans.person_id
        AND (
          assign.assignment_date
            BETWEEN spans.start_date AND {nonnull_end_date_exclusive_clause("spans.end_date")}
          OR spans.start_date
            BETWEEN assign.assignment_date AND {nonnull_end_date_exclusive_clause("assign.end_date")}
        )
        -- Disregard zero day sample spans for calculating population metrics
        AND assign.assignment_date != {nonnull_end_date_clause("assign.end_date")}
)
SELECT
    population_start_date AS start_date,
    population_end_date AS end_date,
    period,
    {aggregation_level.get_index_columns_query_string()},
"""
        + ",\n".join(
            [
                metric.generate_aggregation_query_fragment(
                    span_start_date_col="ses.start_date",
                    span_end_date_col="ses.end_date",
                    period_start_date_col="pop.population_start_date",
                    period_end_date_col="pop.population_end_date",
                )
                for metric in metrics
            ]
        )
        + f"""
FROM 
    eligible_spans ses
INNER JOIN 
    time_periods pop
ON 
    ses.start_date < pop.population_end_date
    AND pop.population_start_date < {nonnull_current_date_clause("ses.end_date")}
GROUP BY 
    {aggregation_level.get_index_columns_query_string()}, 
    population_start_date, population_end_date, period
    """
    )
    return SimpleBigQueryViewBuilder(
        dataset_id=AGGREGATED_METRICS_DATASET_ID,
        view_id=view_id,
        view_query_template=query_template,
        description=view_description,
        aggregated_metrics_dataset=AGGREGATED_METRICS_DATASET_ID,
        analyst_dataset=ANALYST_VIEWS_DATASET,
        should_materialize=False,
        clustering_fields=aggregation_level.index_columns,
    )
