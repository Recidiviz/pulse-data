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
"""Dataset configuration for aggregated metrics views related to impact reports"""

from recidiviz.aggregated_metrics.impact_reports_aggregated_metrics_configurations import (
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_VIEWED_METRICS,
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_NOT_VIEWED_METRICS,
    AVG_DAILY_POPULATION_TASK_INELIGIBLE_METRICS,
    AVG_DAILY_POPULATION_TASK_ALMOST_ELIGIBLE_METRICS,
    DISTINCT_ACTIVE_USERS,
    DISTINCT_REGISTERED_USERS,
)
from recidiviz.aggregated_metrics.impact_reports_aggregated_metrics_view_collector import (
    get_impact_reports_aggregated_metrics_view_builders,
)
from recidiviz.aggregated_metrics.models.aggregated_metric_configurations import (
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_METRICS_INCARCERATION,
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_METRICS_SUPERVISION,
)
from recidiviz.aggregated_metrics.metric_time_periods import MetricTimePeriod
from recidiviz.aggregated_metrics.models.aggregated_metric import AggregatedMetric

METRICS_BY_TIME_PERIOD: dict[MetricTimePeriod, list[AggregatedMetric]] = {
    MetricTimePeriod.DAY: [
        *AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_NOT_VIEWED_METRICS,
        *AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_VIEWED_METRICS,
        *AVG_DAILY_POPULATION_TASK_ALMOST_ELIGIBLE_METRICS,
        *AVG_DAILY_POPULATION_TASK_ELIGIBLE_METRICS_INCARCERATION,
        *AVG_DAILY_POPULATION_TASK_ELIGIBLE_METRICS_SUPERVISION,
        *AVG_DAILY_POPULATION_TASK_INELIGIBLE_METRICS,
    ],
    MetricTimePeriod.WEEK: [
        *DISTINCT_ACTIVE_USERS,
        DISTINCT_REGISTERED_USERS,
    ],
    MetricTimePeriod.MONTH: [
        *DISTINCT_ACTIVE_USERS,
        DISTINCT_REGISTERED_USERS,
    ],
}

IMPACT_REPORTS_VIEWS = get_impact_reports_aggregated_metrics_view_builders(
    METRICS_BY_TIME_PERIOD
)
