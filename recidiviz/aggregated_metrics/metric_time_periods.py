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
"""End-date exclusive time ranges at month, quarter, and year intervals, starting on the provided date."""
import enum

from recidiviz.aggregated_metrics.dataset_config import AGGREGATED_METRICS_DATASET_ID
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


class MetricTimePeriod(enum.Enum):
    CUSTOM = "CUSTOM"
    DAY = "DAY"
    WEEK = "WEEK"
    MONTH = "MONTH"
    QUARTER = "QUARTER"
    YEAR = "YEAR"


_VIEW_NAME = "metric_time_periods"
_METRICS_YEARS_TRACKED = 7
_METRIC_YEARS_TRACKED_WEEK_OVERRIDE = 2
_METRIC_WEEK_START_DAY = "MONDAY"

_VIEW_DESCRIPTION = (
    "End-date exclusive time ranges at month, quarter, and year intervals over the "
    f"past {_METRICS_YEARS_TRACKED} years."
)

_QUERY_TEMPLATE = f"""
WITH date_array AS (
    SELECT
        month,
    FROM
        UNNEST(GENERATE_DATE_ARRAY(
            DATE_SUB(
                DATE_TRUNC(DATE_SUB(CURRENT_DATE("US/Eastern"), INTERVAL 1 DAY), MONTH),
                INTERVAL {_METRICS_YEARS_TRACKED} YEAR
            ),
            DATE_TRUNC(DATE_SUB(CURRENT_DATE("US/Eastern"), INTERVAL 1 DAY), MONTH),
            INTERVAL 1 MONTH
        )) AS month
)

-- get week start dates where the week starts on {_METRIC_WEEK_START_DAY}
, week_array AS (
    SELECT
        week,
    FROM
        UNNEST(GENERATE_DATE_ARRAY(
            -- 2 years before yesterday, week starting {_METRIC_WEEK_START_DAY}
            -- we limit how far back we go for query materialization time reasons
            DATE_TRUNC(
                DATE_SUB(
                    DATE_SUB(CURRENT_DATE("US/Eastern"), INTERVAL 1 DAY),
                    INTERVAL {_METRIC_YEARS_TRACKED_WEEK_OVERRIDE} YEAR
                ), WEEK({_METRIC_WEEK_START_DAY})
            ),
            -- most recent complete week starting {_METRIC_WEEK_START_DAY}
            DATE_TRUNC(
                DATE_SUB(
                    DATE_SUB(CURRENT_DATE("US/Eastern"), INTERVAL 1 DAY),
                    INTERVAL 1 WEEK
                ), WEEK({_METRIC_WEEK_START_DAY})
            ), INTERVAL 1 WEEK
        )) AS week
)

-- Define week, month, quarter, and year end-date-exclusive periods
SELECT
    *
FROM (

SELECT
    "{MetricTimePeriod.WEEK.value}" AS period,
    week AS population_start_date,
    DATE_ADD(week, INTERVAL 1 WEEK) AS population_end_date,
FROM
    week_array

UNION ALL

SELECT
    "{MetricTimePeriod.MONTH.value}" AS period,
    month AS population_start_date,
    DATE_ADD(month, INTERVAL 1 MONTH) AS population_end_date,
FROM
    date_array

UNION ALL

-- we repeat the quarter period monthly to allow greater flexibility downstream
SELECT
    "{MetricTimePeriod.QUARTER.value}" AS period,
    month AS population_start_date,
    DATE_ADD(month, INTERVAL 1 QUARTER) AS population_end_date,
FROM
    date_array

UNION ALL

-- we repeat the year period monthly to allow greater flexibility downstream
SELECT
    "{MetricTimePeriod.YEAR.value}" AS period,
    month AS population_start_date,
    DATE_ADD(month, INTERVAL 1 YEAR) AS population_end_date,
FROM
    date_array
)
-- keep completed periods only
WHERE
    -- OK if = current date since exclusive
    population_end_date <= CURRENT_DATE("US/Eastern")
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
