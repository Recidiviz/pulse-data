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
"""Returns a query template to generate all metrics for specified unit of analysis,
population, and time periods using legacy (expensive) query structure.
"""
from datetime import datetime
from typing import List, Optional

from recidiviz.aggregated_metrics.legacy.assignment_event_aggregated_metrics import (
    get_assignment_event_time_specific_cte,
)
from recidiviz.aggregated_metrics.legacy.assignment_span_aggregated_metrics import (
    get_assignment_span_time_specific_cte,
)
from recidiviz.aggregated_metrics.legacy.period_event_aggregated_metrics import (
    get_period_event_time_specific_cte,
)
from recidiviz.aggregated_metrics.legacy.period_span_aggregated_metrics import (
    get_period_span_time_specific_cte,
)
from recidiviz.aggregated_metrics.metric_time_period_config import (
    MetricTimePeriod,
    MetricTimePeriodConfig,
)
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
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysis,
    MetricUnitOfAnalysisType,
    get_static_attributes_query_for_unit_of_analysis,
)
from recidiviz.aggregated_metrics.standard_deployed_unit_of_analysis_types_by_population_type import (
    UNIT_OF_ANALYSIS_TYPES_BY_POPULATION_TYPE,
)
from recidiviz.utils.string_formatting import fix_indent


def _get_time_period_cte(
    interval_unit: MetricTimePeriod,
    interval_length: int,
    min_end_date: datetime,
    max_end_date: Optional[datetime],
    rolling_period_unit: Optional[MetricTimePeriod] = None,
    rolling_period_length: Optional[int] = None,
) -> str:
    """Returns query template for generating time periods at custom intervals falling
    between the min and max dates. If no max date is provided, use current date.
    """

    if not min_end_date.tzinfo:
        raise ValueError(
            f"Building a time period CTE with a min_end_date [{min_end_date}] that is "
            f"not timezone-aware. Consider using helpers like "
            f"current_datetime_us_eastern() to build this date."
        )

    if max_end_date and not min_end_date.tzinfo:
        raise ValueError(
            f"Building a time period CTE with a min_end_date [{min_end_date}] that is "
            f"not timezone-aware. Consider using helpers like "
            f"current_date_us_eastern() to build this date."
        )

    metric_time_period_config = MetricTimePeriodConfig(
        interval_unit=interval_unit,
        interval_length=interval_length,
        min_period_end_date=min_end_date.date(),
        max_period_end_date=max_end_date.date() if max_end_date else None,
        rolling_period_unit=rolling_period_unit,
        rolling_period_length=rolling_period_length,
    )

    return f"""
SELECT
    metric_period_start_date AS population_start_date,
    metric_period_end_date_exclusive AS population_end_date,
    "CUSTOM" AS period
FROM (
{fix_indent(metric_time_period_config.build_query(), indent_level=4)}
)
"""


# TODO(#35909), TODO(#35910), TODO(#35911): Delete this once all callers use an
#  optimized version of the template builder instead.
def get_legacy_custom_aggregated_metrics_query_template(
    metrics: List[AggregatedMetric],
    unit_of_analysis_type: MetricUnitOfAnalysisType,
    population_type: MetricPopulationType,
    time_interval_unit: MetricTimePeriod,
    time_interval_length: int,
    min_end_date: datetime,
    max_end_date: Optional[datetime] = None,
    rolling_period_unit: Optional[MetricTimePeriod] = None,
    rolling_period_length: Optional[int] = None,
) -> str:
    """Returns a query template to generate all metrics for specified unit of analysis,
    population, and time periods using legacy (expensive) query structure.
    """
    if not metrics:
        raise ValueError("Must provide at least one metric - none provided.")
    if (
        unit_of_analysis_type
        not in UNIT_OF_ANALYSIS_TYPES_BY_POPULATION_TYPE[population_type]
    ):
        raise ValueError(
            f"Unsupported population and unit of analysis pair: {unit_of_analysis_type.value}, {population_type.value}"
        )
    unit_of_analysis = MetricUnitOfAnalysis.for_type(unit_of_analysis_type)
    time_period_cte = _get_time_period_cte(
        time_interval_unit,
        time_interval_length,
        min_end_date,
        max_end_date,
        rolling_period_unit,
        rolling_period_length,
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
        all_joins_query_template += f"""
LEFT JOIN
    period_event_metrics
USING
    ({unit_of_analysis.get_primary_key_columns_query_string()}, start_date, end_date, period)"""

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
        all_joins_query_template += f"""
LEFT JOIN
    assignment_event_metrics
USING
    ({unit_of_analysis.get_primary_key_columns_query_string()}, start_date, end_date, period)"""

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
        all_joins_query_template += f"""
LEFT JOIN
    assignment_span_metrics
USING
    ({unit_of_analysis.get_primary_key_columns_query_string()}, start_date, end_date, period)"""

    # Aggregate static attributes
    static_attributes_query = get_static_attributes_query_for_unit_of_analysis(
        unit_of_analysis.type
    )
    if static_attributes_query:
        join_clause = f"""
LEFT JOIN
    ({get_static_attributes_query_for_unit_of_analysis(unit_of_analysis.type)})
USING
    ({unit_of_analysis.get_primary_key_columns_query_string()})
"""
    else:
        join_clause = ""

    # Use a final SELECT statement to exclude 'period' and rename 'period_alt' to 'period'
    final_select_statement = f"""
    SELECT
        * EXCEPT(period),
        CASE
            WHEN {time_interval_length} = 1 THEN '{time_interval_unit.value}'
            ELSE period
        END AS period
    FROM (
        {all_joins_query_template} 
        {join_clause}
    ) subquery
    """

    query_template = f"""
    {all_ctes_query_template}
    {final_select_statement}
    """
    return query_template
