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

import json
import os
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
from recidiviz.aggregated_metrics.metric_time_period_config import (
    MetricTimePeriod,
    MetricTimePeriodConfig,
)
from recidiviz.aggregated_metrics.models import aggregated_metric_configurations as amc
from recidiviz.aggregated_metrics.models.aggregated_metric import (
    AggregatedMetric,
    AssignmentEventAggregatedMetric,
    AssignmentSpanAggregatedMetric,
    EventMetricConditionsMixin,
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
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.state.views.analyst_data.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    METRIC_UNITS_OF_ANALYSIS_BY_TYPE,
    MetricUnitOfAnalysisType,
    get_static_attributes_query_for_unit_of_analysis,
)
from recidiviz.utils.string_formatting import fix_indent


def get_time_period_cte(
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


def get_custom_aggregated_metrics_query_template(
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


def get_custom_aggregated_metrics(
    metrics: List[AggregatedMetric],
    *,
    unit_of_analysis_type: MetricUnitOfAnalysisType,
    population_type: MetricPopulationType,
    time_interval_unit: MetricTimePeriod,
    time_interval_length: int,
    # TODO(#34770): Update these parameters to make it clear they're end date based
    min_date: datetime = datetime(2020, 1, 1),
    max_date: datetime = datetime(2023, 1, 1),
    rolling_period_unit: Optional[MetricTimePeriod] = None,
    rolling_period_length: Optional[int] = None,
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
        rolling_period_unit,
        rolling_period_length,
        # strip the project id prefix from the query template, since this can not be read by pd.read_gbq
    ).replace("{project_id}.", "")
    if print_query_template:
        print(query_template)
    return pd.read_gbq(query_template, project_id=project_id, progress_bar_type="tqdm")


def get_event_attrs(ea: str, e: str) -> str:
    "Returns the value for the relevant event attribute"

    try:
        event_attrs = json.loads(ea)
        if e in event_attrs:
            return event_attrs[e]
    except ValueError:
        pass  # If an error occurs, just continue to the default return statement

    return ""


def get_person_events(
    *,
    state_code: str,
    metrics: list[str],
    project_id: str = "recidiviz-staging",
    min_date: datetime = datetime(2020, 1, 1),
    max_date: datetime = datetime(2023, 1, 1),
    parse_attributes: bool = False,
    output_file_path: str = "",
    supervisor_id: str = "",
    officer_ids: Optional[list[str]] = None,
    print_query: bool = False,
) -> pd.DataFrame:
    """
    Returns a dataframe for all the person_events that contribute to a given metric in aggregated metrics, along with
    information on the attributes and officers associated with the event. Useful for data validation, particularly
    at the officer/supervisor level

    state_code: string to filter to relevant state
    metrics: list of event types we want to pull events for
    min_date: start of time range we want to pull events for
    max_date: end of time range we want to pull events for (inclusive)
    parse_attributes: boolean specifying whether the output df should parse `event_attributes`
    output_file_path: File path for output
    supervisor_id: If the events are being pulled for a set of officers associated with a supervisor, the ID can be
        included in the output name
    officer_ids: If the events are being pulled for a set of officers, provide their external ids

    Args:
        officer_ids (object):
    """

    if not metrics:
        raise ValueError("Must provide at least one metric - none provided.")
    # If filtering to specific officers, this formats the necessary filter for the query
    officer_ids_filter = ""
    if officer_ids:
        officer_ids_sql = list_to_query_string(officer_ids, quoted=True)
        officer_ids_filter = f"""
            AND officer_id IN ({officer_ids_sql})
        """

    min_date_str = f'"{min_date.strftime("%Y-%m-%d")}"'
    max_date_str = (
        f'"{max_date.strftime("%Y-%m-%d")}"'
        if max_date
        else 'CURRENT_DATE("US/Eastern")'
    )

    # Iterate through the provided list of metrics
    metric_dfs = []
    for metric_set_str in metrics:
        metric_set = getattr(amc, metric_set_str)
        # For a single metric, the output from  getattr should not be a list. But for a metric with some kind of
        # disaggregation (e.g. by violation type), the output will be a list
        if not isinstance(metric_set, list):
            metric_set = [metric_set]
        for metric in metric_set:
            if not isinstance(metric, EventMetricConditionsMixin):
                raise ValueError(
                    "Must be a metric related to events such as EventCountMetrics or AssignmentEvent Metric."
                )
            if not isinstance(metric, AggregatedMetric):
                raise ValueError("Must be an AggregatedMetric.")
            print(metric.name)

            # TODO(#29291): Update this query to use the observation-specific table for
            #  the metric.
            query = f"""
                    SELECT
                        e.state_code,
                        e.person_id,
                        pei.external_id,
                        e.event,
                        e.event_date,
                        e.event_attributes,
                        s.officer_id,
                        s.assignment_date,
                        s.end_date_exclusive,
                    FROM `observations__person_event.all_person_events_materialized` e
                    INNER JOIN `aggregated_metrics.supervision_officer_metrics_person_assignment_sessions_materialized` s
                        ON e.person_id = s.person_id
                        AND (e.event_date between s.assignment_date and COALESCE(s.end_date,'9999-01-01'))
                        AND e.state_code = '{state_code}' 
                    LEFT JOIN `normalized_state.state_person_external_id` pei
                        ON e.person_id = pei.person_id
                        AND e.state_code = pei.state_code
                    WHERE ({metric.get_observation_conditions_string(filter_by_observation_type=True, read_observation_attributes_from_json=True)})
                    {officer_ids_filter}
                    AND e.event_date BETWEEN {min_date_str} AND {max_date_str}

                """

            if print_query:
                print(query)

            metric_df = pd.read_gbq(
                query,
                project_id=project_id,
                progress_bar_type="tqdm_notebook",
            )
            metric_df["metric"] = metric.name

            keep_cols = [
                "person_id",
                "external_id",
                "event",
                "metric",
                "event_date",
                "officer_id",
            ]
            # Some wrangling if the output needs to separate out the event attributes
            if parse_attributes:
                try:
                    event_attributes_example = metric_df[
                        metric_df["event_attributes"] != "{}"
                    ].event_attributes.iloc[0]
                    event_attributes = json.loads(event_attributes_example).keys()
                    for e in event_attributes:
                        metric_df[e] = metric_df["event_attributes"].apply(
                            get_event_attrs, e=e
                        )
                except IndexError:
                    print("No attributes to parse")

            if not parse_attributes:
                metric_df = metric_df[keep_cols]

            metric_dfs.append(metric_df)

    # Concatenate the different dfs
    events = pd.concat(metric_dfs, ignore_index=True)

    # Tag duplicates that might occur across metrics (especially true for inferred starts)
    events["duplicated"] = events.duplicated(["external_id", "event_date"], keep=False)
    events = events.sort_values(by=["person_id", "metric"])

    if output_file_path != "":
        events.to_excel(
            os.path.join(output_file_path, f"metrics_events_{supervisor_id}.xlsx"),
            index=False,
        )

    return events
