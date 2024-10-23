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

from calendar import monthrange
import datetime
from typing import List, Optional
from dateutil.relativedelta import relativedelta

from recidiviz.aggregated_metrics.metric_time_periods import MetricTimePeriod
from recidiviz.aggregated_metrics.models.aggregated_metric import AggregatedMetric
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import IMPACT_REPORTS_DATASET_ID
from recidiviz.calculator.query.state.views.analyst_data.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.tools.analyst.aggregated_metrics_utils import (
    get_custom_aggregated_metrics_query_template,
)
from recidiviz.aggregated_metrics.models.aggregated_metric_configurations import (
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_METRICS_INCARCERATION,
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_METRICS_SUPERVISION,
)

from recidiviz.aggregated_metrics.impact_reports_aggregated_metrics_configurations import (
    AVG_DAILY_POPULATION_TASK_MARKED_INELIGIBLE_METRICS_INCARCERATION,
    AVG_DAILY_POPULATION_TASK_MARKED_INELIGIBLE_METRICS_SUPERVISION,
    AVG_DAILY_POPULATION_TASK_ALMOST_ELIGIBLE_METRICS_INCARCERATION,
    AVG_DAILY_POPULATION_TASK_ALMOST_ELIGIBLE_METRICS_SUPERVISION,
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_VIEWED_METRICS_INCARCERATION,
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_VIEWED_METRICS_SUPERVISION,
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_NOT_VIEWED_METRICS_INCARCERATION,
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_NOT_VIEWED_METRICS_SUPERVISION,
    DISTINCT_ACTIVE_USERS_INCARCERATION,
    DISTINCT_ACTIVE_USERS_SUPERVISION,
    DISTINCT_REGISTERED_USERS_INCARCERATION,
    DISTINCT_REGISTERED_USERS_SUPERVISION,
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
        *DISTINCT_ACTIVE_USERS_SUPERVISION,
        DISTINCT_REGISTERED_USERS_INCARCERATION,
        DISTINCT_REGISTERED_USERS_SUPERVISION,
    ],
    MetricTimePeriod.MONTH: [
        *DISTINCT_ACTIVE_USERS_INCARCERATION,
        *DISTINCT_ACTIVE_USERS_SUPERVISION,
        DISTINCT_REGISTERED_USERS_INCARCERATION,
        DISTINCT_REGISTERED_USERS_SUPERVISION,
    ],
    MetricTimePeriod.YEAR: [
        *DISTINCT_ACTIVE_USERS_INCARCERATION,
        *DISTINCT_ACTIVE_USERS_SUPERVISION,
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
        *DISTINCT_ACTIVE_USERS_SUPERVISION,
        DISTINCT_REGISTERED_USERS_INCARCERATION,
        DISTINCT_REGISTERED_USERS_SUPERVISION,
    ],
}

# today is the current datetime
today = datetime.datetime.today()
# most_recent_monday is the date of the most recent first of the month
most_recent_first_of_month = datetime.datetime(today.year, today.month, 1)
MIN_DATE = most_recent_first_of_month - relativedelta(months=2)
MAX_DATE = most_recent_first_of_month


def get_metrics_subset(
    time_period: MetricTimePeriod,
    unit_of_analysis_type: MetricUnitOfAnalysisType,
) -> List[AggregatedMetric]:
    intersection_metrics = []
    for metric in METRICS_BY_TIME_PERIOD[time_period]:
        if metric in METRICS_BY_UNIT_OF_ANALYSIS_TYPE[unit_of_analysis_type]:
            intersection_metrics.append(metric)

    return intersection_metrics


def get_impact_reports_aggregated_metrics_view_builders() -> List[
    SimpleBigQueryViewBuilder
]:
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

            # For yearly metrics, we want all start_dates and end_dates to land on the 1st of the month
            # In our example, start_date-end_date would be as follows:
            # August report: 2023-09-01 - 2024-09-01
            # September report: 2023-10-01 - 2024-10-01
            if time_period == MetricTimePeriod.YEAR:
                rolling_period_unit = MetricTimePeriod.MONTH
                rolling_period_length = 1
                min_date = MIN_DATE - relativedelta(
                    months=11
                )  # we already subtracted 2 months above
                max_date = MAX_DATE
                query_template = get_custom_aggregated_metrics_query_template(
                    metrics=metrics,
                    unit_of_analysis_type=unit_of_analysis_type,
                    population_type=MetricPopulationType.JUSTICE_INVOLVED,
                    time_interval_unit=time_period,
                    time_interval_length=1,
                    min_date=min_date,
                    max_date=max_date,
                    # The rolling_period parameters let us get metrics for any time interval, each month. For
                    # example, when time_period is YEAR, then we can get year-long metrics for each month of
                    # the year instead of the default, which would be to do year-long metrics on just the
                    # first of the year. This allows us to generate reports for periods that don't fall
                    # neatly along year boundaries.
                    rolling_period_unit=rolling_period_unit,
                    rolling_period_length=rolling_period_length,
                )
                view_builder = view_builder_helper(
                    query_template=query_template,
                    unit_of_analysis_type=unit_of_analysis_type,
                    time_period=time_period,
                )
                view_builders.append(view_builder)

            # For monthly metrics, we want all start_dates and end_dates to land on the 1st of the month
            # In our example, start_date-end_date would be as follows:
            # August report: 2024-08-01 - 2024-09-01
            # September report: 2024-09-01 - 2024-10-01
            if time_period == MetricTimePeriod.MONTH:
                min_date = MIN_DATE
                max_date = MAX_DATE
                query_template = get_custom_aggregated_metrics_query_template(
                    metrics=metrics,
                    unit_of_analysis_type=unit_of_analysis_type,
                    population_type=MetricPopulationType.JUSTICE_INVOLVED,
                    time_interval_unit=time_period,
                    time_interval_length=1,
                    min_date=min_date,
                    max_date=max_date,
                )
                view_builder = view_builder_helper(
                    query_template=query_template,
                    unit_of_analysis_type=unit_of_analysis_type,
                    time_period=time_period,
                )
                view_builders.append(view_builder)

            # For daily metrics, we want to ensure that all end_dates land on the 1st of the month
            # The MIN_DATE and MAX_DATE correspond to the start_date
            # This logic shifts the min and max dates to ensure that all daily metrics have end_dates that fall on the first of the month
            # In our example, start_date-end_date would be as follows:
            # August report: 2024-08-31 - 2024-09-01
            # September report: 2024-09-30 - 2024-10-01
            if time_period == MetricTimePeriod.DAY:
                min_date = (MIN_DATE + relativedelta(months=1)) - relativedelta(days=1)
                max_date = (MAX_DATE + relativedelta(months=1)) - relativedelta(days=1)
                rolling_period_unit = MetricTimePeriod.DAY
                # rolling_period_length should be the previous months number of days
                rolling_period_length = calculate_previous_month_num_days()
                query_template = get_custom_aggregated_metrics_query_template(
                    metrics=metrics,
                    unit_of_analysis_type=unit_of_analysis_type,
                    population_type=MetricPopulationType.JUSTICE_INVOLVED,
                    time_interval_unit=time_period,
                    time_interval_length=1,
                    min_date=min_date,
                    max_date=max_date,
                    # The rolling_period parameters let us get metrics for any time interval, every n days. For
                    # example, if time_period is DAY, then we can get day-long metrics for the last day of
                    # the month instead of the default, which would be to do day-long metrics for each day
                    # of the month. This allows us to generate reports for periods that don't fall
                    # neatly along month boundaries.
                    rolling_period_unit=rolling_period_unit,
                    rolling_period_length=rolling_period_length,
                )
                view_builder = view_builder_helper(
                    query_template=query_template,
                    unit_of_analysis_type=unit_of_analysis_type,
                    time_period=time_period,
                )
                view_builders.append(view_builder)

            # For weekly metrics, we need 2 different tables for 2 different charts in the automated leadership reports
            if time_period == MetricTimePeriod.WEEK:
                weekly_view_builders = construct_weekly_agg_metric_view_builders(
                    metrics=metrics,
                    unit_of_analysis_type=unit_of_analysis_type,
                    time_period=time_period,
                )
                view_builders.extend(weekly_view_builders)

    return view_builders


def calculate_previous_month_num_days() -> int:
    """Based on the current day, calculate the previous month's number of days.
    This number is used as the rolling_period_length for daily aggregated metrics as
    well as weekly aggregated metrics (that end on the first of the month)."""
    last_month = today - relativedelta(months=1)
    rolling_period_length = monthrange(last_month.year, last_month.month)[1]
    return rolling_period_length


def construct_weekly_agg_metric_view_builders(
    metrics: List[AggregatedMetric],
    unit_of_analysis_type: MetricUnitOfAnalysisType,
    time_period: MetricTimePeriod,
) -> List[SimpleBigQueryViewBuilder]:
    """
    Helper function that creates the view builders for weekly aggregated metrics
    """
    weekly_view_builders = []

    # First, make a table to calculate metrics for the last week of the month (ending on the first of the following month)
    # In our example, start_date-end_date would be as follows:
    # August report: 2024-08-25 - 2024-09-01
    # September report: 2024-09-24 - 2024-10-01

    # The MIN_DATE and MAX_DATE correspond to the start_date
    # For end_on_first weekly metrics, we want to ensure that all end_dates land on the 1st of the month
    # This logic shifts the min and max dates to ensure that all end_on_first weekly metrics have end_dates that fall on the first of the month
    min_date = (MIN_DATE + relativedelta(months=1)) - relativedelta(weeks=1)
    max_date = (MAX_DATE + relativedelta(months=1)) - relativedelta(weeks=1)
    rolling_period_unit = MetricTimePeriod.DAY
    # rolling_period_length should be the previous months number of days
    rolling_period_length = calculate_previous_month_num_days()
    query_template = get_custom_aggregated_metrics_query_template(
        metrics=metrics,
        unit_of_analysis_type=unit_of_analysis_type,
        population_type=MetricPopulationType.JUSTICE_INVOLVED,
        time_interval_unit=time_period,
        time_interval_length=1,
        min_date=min_date,
        max_date=max_date,
        # The rolling_period parameters let us get metrics for any time interval, every n days. For
        # example, if time_period is WEEK, then we can get week-long metrics every n days
        # instead of the default, which would be to do week-long metrics on just the
        # first of the week. This allows us to generate reports for periods that don't fall
        # neatly along week boundaries.
        rolling_period_unit=rolling_period_unit,
        rolling_period_length=rolling_period_length,
    )
    view_builder = view_builder_helper(
        query_template=query_template,
        unit_of_analysis_type=unit_of_analysis_type,
        time_period=time_period,
        table_type="end_on_first",
    )
    weekly_view_builders.append(view_builder)

    # Second, make two tables to calculate metrics for each week of the month (starting on the first of the month)
    # In our example, start_date-end_date would be as follows:
    # August report: 2024-08-01 - 2024-08-08, 2024-08-08 - 2024-08-15, 2024-08-15 - 2024-08-22, 2024-08-22 - 2024-08-29, 2024-08-29 - 2024-09-05
    # September report: 2024-09-01 - 2024-09-08, 2024-09-08 - 2024-09-15, 2024-09-15 - 2024-09-22, 2024-09-22 - 2024-09-29, 2024-09-29 - 2021-10-06
    query_templates_to_union = []
    for month_num in range(1, 3):
        min_date = most_recent_first_of_month - relativedelta(months=month_num)
        max_date = (
            most_recent_first_of_month
            - relativedelta(months=month_num - 1)
            + relativedelta(days=7)
        )

        query_template = get_custom_aggregated_metrics_query_template(
            metrics=metrics,
            unit_of_analysis_type=unit_of_analysis_type,
            population_type=MetricPopulationType.JUSTICE_INVOLVED,
            time_interval_unit=time_period,
            time_interval_length=1,
            min_date=min_date,
            max_date=max_date,
        )
        query_templates_to_union.append(f"""SELECT * FROM ({query_template})""")

    unioned_query_template = " UNION ALL ".join(query_templates_to_union)
    view_builder = view_builder_helper(
        query_template=unioned_query_template,
        unit_of_analysis_type=unit_of_analysis_type,
        time_period=time_period,
        table_type="start_on_first",
    )
    weekly_view_builders.append(view_builder)

    return weekly_view_builders


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
