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
"""Returns all aggregated metric view builders for specified populations and units of analysis related to impact reports"""

from typing import List, Optional

from dateutil.relativedelta import relativedelta

from recidiviz.aggregated_metrics.impact_reports_aggregated_metrics_configurations import (
    AVG_DAILY_POPULATION_TASK_ALMOST_ELIGIBLE_METRICS_INCARCERATION,
    AVG_DAILY_POPULATION_TASK_ALMOST_ELIGIBLE_METRICS_SUPERVISION,
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_NOT_VIEWED_METRICS_INCARCERATION,
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_NOT_VIEWED_METRICS_SUPERVISION,
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_VIEWED_METRICS_INCARCERATION,
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_VIEWED_METRICS_SUPERVISION,
    AVG_DAILY_POPULATION_TASK_MARKED_INELIGIBLE_METRICS_INCARCERATION,
    AVG_DAILY_POPULATION_TASK_MARKED_INELIGIBLE_METRICS_SUPERVISION,
    DISTINCT_ACTIVE_USERS_ALL_INCARCERATION_TASKS,
    DISTINCT_ACTIVE_USERS_ALL_SUPERVISION_TASKS,
    DISTINCT_ACTIVE_USERS_INCARCERATION,
    DISTINCT_ACTIVE_USERS_SUPERVISION,
    DISTINCT_REGISTERED_USERS_INCARCERATION,
    DISTINCT_REGISTERED_USERS_SUPERVISION,
)
from recidiviz.aggregated_metrics.metric_time_period_config import MetricTimePeriod
from recidiviz.aggregated_metrics.models.aggregated_metric import AggregatedMetric
from recidiviz.aggregated_metrics.models.aggregated_metric_configurations import (
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_METRICS_INCARCERATION,
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_METRICS_SUPERVISION,
)
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import IMPACT_REPORTS_DATASET_ID
from recidiviz.calculator.query.state.views.analyst_data.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.common.date import current_datetime_us_eastern, first_day_of_month
from recidiviz.tools.analyst.aggregated_metrics_utils import (
    get_custom_aggregated_metrics_query_template,
)

METRICS_BY_TIME_PERIOD: dict[MetricTimePeriod, list[AggregatedMetric]] = {
    MetricTimePeriod.DAY: [
        *AVG_DAILY_POPULATION_TASK_ALMOST_ELIGIBLE_METRICS_INCARCERATION,
        *AVG_DAILY_POPULATION_TASK_ALMOST_ELIGIBLE_METRICS_SUPERVISION,
        *AVG_DAILY_POPULATION_TASK_ELIGIBLE_METRICS_INCARCERATION,
        *AVG_DAILY_POPULATION_TASK_ELIGIBLE_METRICS_SUPERVISION,
        *AVG_DAILY_POPULATION_TASK_MARKED_INELIGIBLE_METRICS_INCARCERATION,
        *AVG_DAILY_POPULATION_TASK_MARKED_INELIGIBLE_METRICS_SUPERVISION,
        *AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_VIEWED_METRICS_INCARCERATION,
        *AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_VIEWED_METRICS_SUPERVISION,
        *AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_NOT_VIEWED_METRICS_INCARCERATION,
        *AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_NOT_VIEWED_METRICS_SUPERVISION,
    ],
    MetricTimePeriod.WEEK: [
        *DISTINCT_ACTIVE_USERS_INCARCERATION,
        DISTINCT_ACTIVE_USERS_ALL_INCARCERATION_TASKS,
        *DISTINCT_ACTIVE_USERS_SUPERVISION,
        DISTINCT_ACTIVE_USERS_ALL_SUPERVISION_TASKS,
        DISTINCT_REGISTERED_USERS_INCARCERATION,
        DISTINCT_REGISTERED_USERS_SUPERVISION,
    ],
    MetricTimePeriod.MONTH: [
        *DISTINCT_ACTIVE_USERS_INCARCERATION,
        DISTINCT_ACTIVE_USERS_ALL_INCARCERATION_TASKS,
        *DISTINCT_ACTIVE_USERS_SUPERVISION,
        DISTINCT_ACTIVE_USERS_ALL_SUPERVISION_TASKS,
        DISTINCT_REGISTERED_USERS_INCARCERATION,
        DISTINCT_REGISTERED_USERS_SUPERVISION,
    ],
    MetricTimePeriod.YEAR: [
        *DISTINCT_ACTIVE_USERS_INCARCERATION,
        DISTINCT_ACTIVE_USERS_ALL_INCARCERATION_TASKS,
        *DISTINCT_ACTIVE_USERS_SUPERVISION,
        DISTINCT_ACTIVE_USERS_ALL_SUPERVISION_TASKS,
        DISTINCT_REGISTERED_USERS_INCARCERATION,
        DISTINCT_REGISTERED_USERS_SUPERVISION,
    ],
}

METRICS_BY_UNIT_OF_ANALYSIS_TYPE: dict[
    MetricUnitOfAnalysisType, list[AggregatedMetric]
] = {
    MetricUnitOfAnalysisType.SUPERVISION_DISTRICT: [
        *AVG_DAILY_POPULATION_TASK_ALMOST_ELIGIBLE_METRICS_SUPERVISION,
        *AVG_DAILY_POPULATION_TASK_ELIGIBLE_METRICS_SUPERVISION,
        *AVG_DAILY_POPULATION_TASK_MARKED_INELIGIBLE_METRICS_SUPERVISION,
        *AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_VIEWED_METRICS_SUPERVISION,
        *AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_NOT_VIEWED_METRICS_SUPERVISION,
        *DISTINCT_ACTIVE_USERS_SUPERVISION,
        DISTINCT_REGISTERED_USERS_SUPERVISION,
    ],
    MetricUnitOfAnalysisType.FACILITY: [
        *AVG_DAILY_POPULATION_TASK_ALMOST_ELIGIBLE_METRICS_INCARCERATION,
        *AVG_DAILY_POPULATION_TASK_ELIGIBLE_METRICS_INCARCERATION,
        *AVG_DAILY_POPULATION_TASK_MARKED_INELIGIBLE_METRICS_INCARCERATION,
        *AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_VIEWED_METRICS_INCARCERATION,
        *AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_NOT_VIEWED_METRICS_INCARCERATION,
        *DISTINCT_ACTIVE_USERS_INCARCERATION,
        DISTINCT_REGISTERED_USERS_INCARCERATION,
    ],
    MetricUnitOfAnalysisType.STATE_CODE: [
        *AVG_DAILY_POPULATION_TASK_ALMOST_ELIGIBLE_METRICS_INCARCERATION,
        *AVG_DAILY_POPULATION_TASK_ALMOST_ELIGIBLE_METRICS_SUPERVISION,
        *AVG_DAILY_POPULATION_TASK_ELIGIBLE_METRICS_INCARCERATION,
        *AVG_DAILY_POPULATION_TASK_ELIGIBLE_METRICS_SUPERVISION,
        *AVG_DAILY_POPULATION_TASK_MARKED_INELIGIBLE_METRICS_INCARCERATION,
        *AVG_DAILY_POPULATION_TASK_MARKED_INELIGIBLE_METRICS_SUPERVISION,
        *AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_VIEWED_METRICS_INCARCERATION,
        *AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_VIEWED_METRICS_SUPERVISION,
        *AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_NOT_VIEWED_METRICS_INCARCERATION,
        *AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_NOT_VIEWED_METRICS_SUPERVISION,
        *DISTINCT_ACTIVE_USERS_INCARCERATION,
        DISTINCT_ACTIVE_USERS_ALL_INCARCERATION_TASKS,
        *DISTINCT_ACTIVE_USERS_SUPERVISION,
        DISTINCT_ACTIVE_USERS_ALL_SUPERVISION_TASKS,
        DISTINCT_REGISTERED_USERS_INCARCERATION,
        DISTINCT_REGISTERED_USERS_SUPERVISION,
    ],
}

# today is the current datetime
today = current_datetime_us_eastern()
# Most of our metrics will use time periods that end on the most recent first of the month.
# We generate two months worth of report data by default, so set our default end dates to cover two
# months
most_recent_first_of_month = first_day_of_month(today)
MIN_END_DATE = most_recent_first_of_month - relativedelta(months=1)
MAX_END_DATE = most_recent_first_of_month


def get_metrics_subset(
    time_period: MetricTimePeriod,
    unit_of_analysis_type: MetricUnitOfAnalysisType,
) -> List[AggregatedMetric]:
    intersection_metrics = []
    for metric in METRICS_BY_TIME_PERIOD[time_period]:
        if metric in METRICS_BY_UNIT_OF_ANALYSIS_TYPE[unit_of_analysis_type]:
            intersection_metrics.append(metric)

    return intersection_metrics


def get_impact_reports_aggregated_metrics_view_builders() -> (
    List[SimpleBigQueryViewBuilder]
):
    """
    Collects all aggregated metrics view builders for impact reports at the state and day level
    """

    # For all code below, let's use the following example:
    # Let's say it's currently October, 2024. We want to generate metrics for the months of
    # August, 2024 and September, 2024
    view_builders = []
    for unit_of_analysis_type, _ in METRICS_BY_UNIT_OF_ANALYSIS_TYPE.items():
        for time_period, _ in METRICS_BY_TIME_PERIOD.items():
            metrics = get_metrics_subset(
                time_period=time_period,
                unit_of_analysis_type=unit_of_analysis_type,
            )
            # For each time period, we want all end_dates to land on the 1st of the month.
            # In our example, yearly start_date-end_date would be as follows:
            #   August report: 2023-09-01 - 2024-09-01
            #   September report: 2023-10-01 - 2024-10-01
            # monthly would be:
            #   August report: 2024-08-01 - 2024-09-01
            #   September report: 2024-09-01 - 2024-10-01
            # weekly would be:
            #   August report: 2024-08-25 - 2024-09-01
            #   September report: 2024-09-24 - 2024-10-01
            # daily would be:
            #   August report: 2024-08-31 - 2024-09-01
            #   September report: 2024-09-30 - 2024-10-01
            query_template = get_custom_aggregated_metrics_query_template(
                metrics=metrics,
                unit_of_analysis_type=unit_of_analysis_type,
                population_type=MetricPopulationType.JUSTICE_INVOLVED,
                time_interval_unit=time_period,
                time_interval_length=1,
                min_end_date=MIN_END_DATE,
                max_end_date=MAX_END_DATE,
                # The rolling_period parameters let us get metrics for any time interval, each month. For
                # example, when time_period is YEAR, then we can get year-long metrics for each month of
                # the year instead of the default, which would be to do year-long metrics on just the
                # first of the year. This allows us to generate reports for periods that don't fall
                # neatly along year boundaries.
                rolling_period_unit=MetricTimePeriod.MONTH,
                rolling_period_length=1,
            )
            view_builder = view_builder_helper(
                query_template=query_template,
                unit_of_analysis_type=unit_of_analysis_type,
                time_period=time_period,
            )
            view_builders.append(view_builder)

            if time_period in [MetricTimePeriod.WEEK, MetricTimePeriod.MONTH]:
                weekly_view_builder = construct_weekly_rolling_agg_metric_view_builder(
                    metrics=metrics,
                    unit_of_analysis_type=unit_of_analysis_type,
                    time_period=time_period,
                )
                view_builders.append(weekly_view_builder)

    return view_builders


def construct_weekly_rolling_agg_metric_view_builder(
    metrics: List[AggregatedMetric],
    unit_of_analysis_type: MetricUnitOfAnalysisType,
    time_period: MetricTimePeriod,
) -> SimpleBigQueryViewBuilder:
    """
    Helper function that creates the view builders for aggregated metrics on a weekly rolling cadence
    """
    # Make two tables (UNIONed together) to calculate metrics for each week of the month, where the
    # end date begins on the 8th of the month (aka the beginning of the week is the 1st of the month).
    # In our example, for weekly metrics, start_date-end_date would be as follows:
    #   August report: 2024-08-01 - 2024-08-08, 2024-08-08 - 2024-08-15, 2024-08-15 - 2024-08-22, 2024-08-22 - 2024-08-29, 2024-08-29 - 2024-09-05
    #   September report: 2024-09-01 - 2024-09-08, 2024-09-08 - 2024-09-15, 2024-09-15 - 2024-09-22, 2024-09-22 - 2024-09-29, 2024-09-29 - 2021-10-06
    # and for monthly metrics, start_date-end_date would be as follows:
    #   August report: 2024-07-08 - 2024-08-08, 2024-07-15 - 2024-08-15, 2024-07-22 - 2024-08-22, 2024-07-29 - 2024-08-29, 2024-08-05 - 2024-09-05
    #   September report: 2024-08-08 - 2024-09-08, 2024-08-15 - 2024-09-15, 2024-08-22 - 2024-09-22, 2024-08-29 - 2024-09-29, 2024-09-06 - 2024-10-06
    query_templates_to_union = []
    for month_num in range(1, 3):
        # Start with an end date on the 8th of the previous month
        min_end_date = (
            most_recent_first_of_month
            - relativedelta(months=month_num)
            + relativedelta(days=7)
        )
        # The last row for the month should have an end date <= the 7th of the following month so
        # the maximum last week would begin on the last day of the month in question
        # (e.g. 2024-09-30 - 2024-10-07)
        max_end_date = min_end_date + relativedelta(months=1) - relativedelta(days=1)

        query_template = get_custom_aggregated_metrics_query_template(
            metrics=metrics,
            unit_of_analysis_type=unit_of_analysis_type,
            population_type=MetricPopulationType.JUSTICE_INVOLVED,
            time_interval_unit=time_period,
            time_interval_length=1,
            min_end_date=min_end_date,
            max_end_date=max_end_date,
            rolling_period_unit=MetricTimePeriod.WEEK,
            rolling_period_length=1,
        )
        query_templates_to_union.append(f"""SELECT * FROM ({query_template})""")

    unioned_query_template = " UNION ALL ".join(query_templates_to_union)
    return view_builder_helper(
        query_template=unioned_query_template,
        unit_of_analysis_type=unit_of_analysis_type,
        time_period=time_period,
        table_type="rolling_weekly",
    )


def view_builder_helper(
    query_template: str,
    unit_of_analysis_type: MetricUnitOfAnalysisType,
    time_period: MetricTimePeriod,
    table_type: Optional[str] = None,
) -> SimpleBigQueryViewBuilder:
    """
    Helper function that creates view builder for a given view.
    """
    if table_type is not None:
        view_id = f"{MetricPopulationType.JUSTICE_INVOLVED.population_name_short}_{unit_of_analysis_type.short_name}_{time_period.value.lower()}_{table_type}_aggregated_metrics"
    else:
        view_id = f"{MetricPopulationType.JUSTICE_INVOLVED.population_name_short}_{unit_of_analysis_type.short_name}_{time_period.value.lower()}_aggregated_metrics"

    view_description = f"""
    Metrics for the {MetricPopulationType.JUSTICE_INVOLVED.population_name_short} population calculated using 
    ad-hoc logic, disaggregated by {unit_of_analysis_type.short_name} and calculated for
    each day for the {time_period.value} period.

    All end_dates are exclusive, i.e. the metric is for the range [start_date, end_date).
    """

    view_builder = SimpleBigQueryViewBuilder(
        dataset_id=IMPACT_REPORTS_DATASET_ID,
        view_query_template=query_template,
        view_id=view_id,
        description=view_description,
        should_materialize=True,
        clustering_fields=["state_code"],
    )

    return view_builder
