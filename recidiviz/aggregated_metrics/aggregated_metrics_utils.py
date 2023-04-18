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

from typing import Optional, Sequence

from recidiviz.aggregated_metrics.models.aggregated_metric import AggregatedMetric
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysis,
)


# function for aggregating monthly-levels to quarter and year
def get_unioned_time_granularity_clause(
    aggregation_level: MetricUnitOfAnalysis,
    metrics: Sequence[AggregatedMetric],
    manual_metrics_str: Optional[str] = None,
) -> str:
    """
    Returns select statement with month, quarterly, and year-granularity metrics all
    calculated from the monthly level.

    Params
    ------
    aggregation_level : MetricUnitOfAnalysis
        A MetricUnitOfAnalysis object used to get index columns of the query

    metrics : List[AggregatedMetric]
        A list of AggregatedMetric objects used to construct the select statement

    manual_metrics_str : Optional[str]
        An optional string of columns to be appended in front of `metrics`.
        Do not include trailing commas.
        Example: manual_metrics_str = "SUM(assignments) AS assignments"
    """
    index_cols = aggregation_level.get_index_columns_query_string()
    metric_cols = ",\n\t".join(
        [metric.generate_aggregate_time_periods_query_fragment() for metric in metrics]
    )
    if manual_metrics_str:
        metric_cols = manual_metrics_str + ",\n\t" + metric_cols

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
    time_periods.period != "MONTH"
GROUP BY
    {index_cols}, population_start_date, population_end_date, time_periods.period"""
