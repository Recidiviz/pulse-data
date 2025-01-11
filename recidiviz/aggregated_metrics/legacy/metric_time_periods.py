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
"""End-date exclusive time ranges at month, quarter, and year intervals, starting on the
provided date.

 TODO(#35914): Deprecate this view entirely, instead using optimized aggregated metrics
  queries that are built off of MetricTimePeriodConfig.
"""

from recidiviz.aggregated_metrics.dataset_config import AGGREGATED_METRICS_DATASET_ID
from recidiviz.aggregated_metrics.metric_time_period_config import (
    MetricTimePeriodConfig,
)
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "metric_time_periods"
_METRICS_YEARS_TRACKED = 7
_METRIC_YEARS_TRACKED_WEEK_OVERRIDE = 2

_VIEW_DESCRIPTION = (
    f"End-date exclusive time ranges at month, quarter, and year intervals over "
    f"the past {_METRICS_YEARS_TRACKED} years. End-date exclusive time ranges at week "
    f"intervals over the past {_METRIC_YEARS_TRACKED_WEEK_OVERRIDE} years."
)


_YEAR_PERIODS_QUERY = MetricTimePeriodConfig.monthly_year_periods(
    lookback_months=_METRICS_YEARS_TRACKED * 12
).build_query()

_QUARTER_PERIODS_QUERY = MetricTimePeriodConfig.monthly_quarter_periods(
    lookback_months=_METRICS_YEARS_TRACKED * 12
).build_query()

_MONTH_PERIODS_QUERY = MetricTimePeriodConfig.month_periods(
    lookback_months=_METRICS_YEARS_TRACKED * 12
).build_query()

_WEEK_PERIODS_QUERY = MetricTimePeriodConfig.week_periods(
    lookback_weeks=_METRIC_YEARS_TRACKED_WEEK_OVERRIDE * 52
).build_query()


_QUERY_TEMPLATE = f"""
WITH periods AS (

{_YEAR_PERIODS_QUERY}
UNION ALL
{_QUARTER_PERIODS_QUERY}
UNION ALL
{_MONTH_PERIODS_QUERY}
UNION ALL
{_WEEK_PERIODS_QUERY}

)
SELECT 
    period,
    metric_period_start_date AS population_start_date,
    metric_period_end_date_exclusive AS population_end_date
FROM periods
"""

METRIC_TIME_PERIODS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=AGGREGATED_METRICS_DATASET_ID,
    view_id=_VIEW_NAME,
    view_query_template=_QUERY_TEMPLATE,
    description=_VIEW_DESCRIPTION,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        METRIC_TIME_PERIODS_VIEW_BUILDER.build_and_print()
