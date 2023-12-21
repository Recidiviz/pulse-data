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
"""
Utilities for generating and analyzing aggregated metrics in python notebooks
"""

from datetime import datetime
from typing import List, Optional

import pandas as pd

from recidiviz.aggregated_metrics.aggregated_metric_view_collector import (
    UNIT_OF_ANALYSIS_TYPES_BY_POPULATION_TYPE,
)
from recidiviz.aggregated_metrics.assignment_event_aggregated_metrics import (
    get_assignment_event_time_specific_cte,
)
from recidiviz.aggregated_metrics.assignment_span_aggregated_metrics import (
    get_assignment_span_time_specific_cte,
)
from recidiviz.aggregated_metrics.metric_time_periods import MetricTimePeriod
from recidiviz.aggregated_metrics.models.aggregated_metric import (
    AggregatedMetric,
    AssignmentEventAggregatedMetric,
    AssignmentSpanAggregatedMetric,
    PeriodEventAggregatedMetric,
    PeriodSpanAggregatedMetric,
)
from recidiviz.aggregated_metrics.models.aggregated_metric_configurations import (
    AVG_DAILY_POPULATION,
)
from recidiviz.aggregated_metrics.period_event_aggregated_metrics import (
    get_period_event_time_specific_cte,
)
from recidiviz.aggregated_metrics.period_span_aggregated_metrics import (
    get_period_span_time_specific_cte,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    METRIC_UNITS_OF_ANALYSIS_BY_TYPE,
    MetricUnitOfAnalysisType,
)


def get_time_period_cte(
    interval_unit: MetricTimePeriod,
    interval_length: int,
    min_date: datetime,
    max_date: Optional[datetime],
) -> str:
    """Returns query template for generating time periods at custom intervals falling between the min and max dates.
    If no max date is provided, use current date."""

    if interval_unit in [
        MetricTimePeriod.DAY,
        MetricTimePeriod.WEEK,
        MetricTimePeriod.MONTH,
        MetricTimePeriod.QUARTER,
        MetricTimePeriod.YEAR,
    ]:
        if interval_length == 1:
            period_str = f'"{interval_unit.value}"'
        else:
            period_str = f'"{MetricTimePeriod.CUSTOM.value}"'
    else:
        raise ValueError(
            f"Interval type {interval_unit.value} is not a valid interval type."
        )
    interval_str = f"INTERVAL {interval_length} {interval_unit.value}"
    min_date_str = f'"{min_date.strftime("%Y-%m-%d")}"'
    max_date_str = (
        f'"{max_date.strftime("%Y-%m-%d")}"'
        if max_date
        else 'CURRENT_DATE("US/Eastern")'
    )
    return f"""
SELECT
    population_start_date,
    DATE_ADD(population_start_date, {interval_str}) AS population_end_date,
    {period_str} as period,
FROM
    UNNEST(GENERATE_DATE_ARRAY(
        {min_date_str},
        {max_date_str},
        {interval_str}
    )) AS population_start_date
WHERE
    DATE_ADD(population_start_date, {interval_str}) <= CURRENT_DATE("US/Eastern")
    AND DATE_ADD(population_start_date, {interval_str}) <= {max_date_str}
"""


def get_custom_aggregated_metrics_query_template(
    metrics: List[AggregatedMetric],
    unit_of_analysis_type: MetricUnitOfAnalysisType,
    population_type: MetricPopulationType,
    time_interval_unit: MetricTimePeriod,
    time_interval_length: int,
    min_date: datetime = datetime(2020, 1, 1),
    max_date: datetime = datetime(2023, 1, 1),
) -> str:
    """Returns a query template to generate all metrics for specified unit of analysis, population, and time periods"""
    if not metrics:
        raise ValueError("Must provide at least one metric - none provided.")
    if (
        unit_of_analysis_type
        not in UNIT_OF_ANALYSIS_TYPES_BY_POPULATION_TYPE[population_type]
    ):
        raise ValueError(
            f"Unsupported population and unit of analysis pair: {unit_of_analysis_type.value}, {population_type.value}"
        )
    unit_of_analysis = METRIC_UNITS_OF_ANALYSIS_BY_TYPE[unit_of_analysis_type]
    time_period_cte = get_time_period_cte(
        time_interval_unit, time_interval_length, min_date, max_date
    )

    all_ctes_query_template = f"""
WITH time_periods AS (
    {time_period_cte}
)"""
    all_joins_query_template = """
-- join all metrics on unit-of-analysis and attribute struct to return original columns
SELECT
    *,
    DATE_DIFF(end_date, start_date, DAY) AS days_in_period
FROM
    period_span_metrics
"""

    period_span_metrics = [
        m for m in metrics if isinstance(m, PeriodSpanAggregatedMetric)
    ]
    # Always include average daily population metric
    if AVG_DAILY_POPULATION not in period_span_metrics:
        period_span_metrics.append(AVG_DAILY_POPULATION)
    period_span_cte = get_period_span_time_specific_cte(
        unit_of_analysis=unit_of_analysis,
        population_type=population_type,
        metrics=period_span_metrics,
        metric_time_period=MetricTimePeriod.CUSTOM,
    )
    all_ctes_query_template += f"""
, period_span_metrics AS (
{period_span_cte}
)"""

    period_event_metrics = [
        m for m in metrics if isinstance(m, PeriodEventAggregatedMetric)
    ]
    if period_event_metrics:
        period_event_cte = get_period_event_time_specific_cte(
            unit_of_analysis=unit_of_analysis,
            population_type=population_type,
            metrics=period_event_metrics,
            metric_time_period=MetricTimePeriod.CUSTOM,
        )
        all_ctes_query_template += f"""
, period_event_metrics AS (
    {period_event_cte}
)"""
        all_joins_query_template += """
LEFT JOIN
    period_event_metrics
USING
    (state_code, start_date, end_date)"""

    assignment_event_metrics = [
        m for m in metrics if isinstance(m, AssignmentEventAggregatedMetric)
    ]
    if assignment_event_metrics:
        assignment_event_cte = get_assignment_event_time_specific_cte(
            unit_of_analysis=unit_of_analysis,
            population_type=population_type,
            metrics=assignment_event_metrics,
            metric_time_period=MetricTimePeriod.CUSTOM,
        )
        all_ctes_query_template += f"""
, assignment_event_metrics AS (
    {assignment_event_cte}
)"""
        all_joins_query_template += """
LEFT JOIN
    assignment_event_metrics
USING
    (state_code, start_date, end_date)"""

    assignment_span_metrics = [
        m for m in metrics if isinstance(m, AssignmentSpanAggregatedMetric)
    ]
    if assignment_span_metrics:
        assignment_span_cte = get_assignment_span_time_specific_cte(
            unit_of_analysis=unit_of_analysis,
            population_type=population_type,
            metrics=assignment_span_metrics,
            metric_time_period=MetricTimePeriod.CUSTOM,
        )
        all_ctes_query_template += f"""
, assignment_span_metrics AS (
    {assignment_span_cte}
)"""
        all_joins_query_template += """
LEFT JOIN
    assignment_span_metrics
USING
    (state_code, start_date, end_date)"""

    query_template = f"""

{all_ctes_query_template}
{all_joins_query_template}
"""
    return query_template


def get_custom_aggregated_metrics(
    metrics: List[AggregatedMetric],
    unit_of_analysis_type: MetricUnitOfAnalysisType,
    population_type: MetricPopulationType,
    time_interval_unit: MetricTimePeriod,
    time_interval_length: int,
    min_date: datetime = datetime(2020, 1, 1),
    max_date: datetime = datetime(2023, 1, 1),
    print_query_template: bool = False,
    project_id: str = "recidiviz-staging",
) -> pd.DataFrame:
    """Returns a dataframe consisting of all metrics for specified unit of analysis, population, and time periods"""
    query_template = get_custom_aggregated_metrics_query_template(
        metrics,
        unit_of_analysis_type,
        population_type,
        time_interval_unit,
        time_interval_length,
        min_date,
        max_date,
        # strip the project id prefix from the query template, since this can not be read by pd.read_gbq
    ).replace("{project_id}.", "")
    if print_query_template:
        print(query_template)
    return pd.read_gbq(query_template, project_id=project_id, progress_bar_type="tqdm")
