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
"""File with utils specific to aggregated metrics methods."""

from typing import List, Sequence

from recidiviz.aggregated_metrics.metric_time_period_config import MetricTimePeriod
from recidiviz.aggregated_metrics.models.aggregated_metric import AggregatedMetric
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysis,
)


# function for aggregating monthly-levels to quarter and year
def get_unioned_time_granularity_clause(
    unit_of_analysis: MetricUnitOfAnalysis,
    metrics: Sequence[AggregatedMetric],
) -> str:
    """
    Returns select statement with month, quarterly, and year-granularity metrics all
    calculated from the monthly level.

    Params
    ------
    unit_of_analysis : MetricUnitOfAnalysis
        A MetricUnitOfAnalysis object used to get index columns of the query

    metrics : List[AggregatedMetric]
        A list of AggregatedMetric objects used to construct the select statement
    """
    index_cols = unit_of_analysis.get_primary_key_columns_query_string()
    metric_cols = ",\n\t".join(
        [metric.generate_aggregate_time_periods_query_fragment() for metric in metrics]
    )

    return f"""
SELECT
    *
FROM
    month_metrics

UNION ALL

SELECT
    {index_cols},
    population_start_date AS start_date,
    population_end_date AS end_date,
    time_periods.period,
    {metric_cols},
FROM
    time_periods
INNER JOIN
    month_metrics
ON
    month_metrics.start_date >= time_periods.population_start_date
    AND month_metrics.end_date <= time_periods.population_end_date
WHERE
    time_periods.period IN (
        "{MetricTimePeriod.QUARTER.value}", "{MetricTimePeriod.YEAR.value}"
    )
GROUP BY
    {index_cols}, population_start_date, population_end_date, time_periods.period"""


def get_joined_metrics_by_observation_type_query(
    unit_of_analysis: MetricUnitOfAnalysis,
    subqueries: List[str],
    metrics: Sequence[AggregatedMetric],
) -> str:
    """Left joins together all subqueries using the unit of analysis and time period fields"""
    joined_query = f"SELECT * FROM ({subqueries[0]})" + "\n".join(
        sorted(
            f"""
    FULL OUTER JOIN ({x}) 
        USING ({unit_of_analysis.get_primary_key_columns_query_string()}, start_date, end_date, period)"""
            for i, x in enumerate(subqueries)
            if i > 0
        )
    )
    index_cols = unit_of_analysis.get_primary_key_columns_query_string()
    # Select columns in their original order, since separation by metric unit of observation scrambles the order
    metric_cols_ordered = ",\n\t".join([metric.name for metric in metrics])
    return f"""
SELECT
    {index_cols},
    start_date,
    end_date,
    period,
    {metric_cols_ordered} 
FROM ({joined_query})"""
