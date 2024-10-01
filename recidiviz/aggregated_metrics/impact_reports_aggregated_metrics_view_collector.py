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

import datetime
from typing import List
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

    view_builders = []
    for unit_of_analysis_type, _ in METRICS_BY_UNIT_OF_ANALYSIS_TYPE.items():
        for time_period, _ in METRICS_BY_TIME_PERIOD.items():
            metrics = get_metrics_subset(
                time_period=time_period,
                unit_of_analysis_type=unit_of_analysis_type,
            )
            query_template = get_custom_aggregated_metrics_query_template(
                metrics=metrics,
                unit_of_analysis_type=unit_of_analysis_type,
                population_type=MetricPopulationType.JUSTICE_INVOLVED,
                time_interval_unit=time_period,
                time_interval_length=1,
                min_date=MIN_DATE,
                max_date=MAX_DATE,
                # The rolling_period parameters let us get metrics for any time interval, each day. For
                # example, if time_period is MONTH, then we can get month-long metrics for each day of
                # the month instead of the default, which would be to do month-long metrics on just the
                # first of the month. This allows us to generate reports for periods that don't fall
                # neatly along month boundaries.
                rolling_period_unit=MetricTimePeriod.DAY,
                rolling_period_length=7,
            )

            view_id = f"{MetricPopulationType.JUSTICE_INVOLVED.population_name_short}_{unit_of_analysis_type.short_name}_{time_period.value.lower()}_aggregated_metrics"

            view_description = f"""
            Metrics for the {MetricPopulationType.JUSTICE_INVOLVED.population_name_short} population calculated using 
            ad-hoc logic, disaggregated by {unit_of_analysis_type.short_name} and calculated for
            each day for the {time_period.value} period.

            All end_dates are exclusive, i.e. the metric is for the range [start_date, end_date).
            """

            view_builders.append(
                SimpleBigQueryViewBuilder(
                    dataset_id=IMPACT_REPORTS_DATASET_ID,
                    view_query_template=query_template,
                    view_id=view_id,
                    description=view_description,
                    should_materialize=True,
                    clustering_fields=["state_code"],
                )
            )

    return view_builders
