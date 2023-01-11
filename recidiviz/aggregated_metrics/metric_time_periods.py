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

from recidiviz.aggregated_metrics.dataset_config import AGGREGATED_METRICS_DATASET_ID
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "metric_time_periods"
_METRICS_YEARS_TRACKED = "7"

_VIEW_DESCRIPTION = (
    "End-date exclusive time ranges at month, quarter, and year intervals over the "
    f"past {_METRICS_YEARS_TRACKED} years."
)

_QUERY_TEMPLATE = """
WITH date_array AS (
    SELECT
        month,
    FROM
        UNNEST(GENERATE_DATE_ARRAY(
            DATE_SUB(
                DATE_TRUNC(DATE_SUB(CURRENT_DATE("US/Eastern"), INTERVAL 1 DAY), MONTH),
                INTERVAL {metrics_years_tracked} YEAR
            ),
            DATE_TRUNC(DATE_SUB(CURRENT_DATE("US/Eastern"), INTERVAL 1 DAY), MONTH),
            INTERVAL 1 MONTH
        )) AS month
)

-- Define month, quarter, and year end-date-exclusive periods
SELECT
    *
FROM (
SELECT
    "MONTH" AS period,
    month AS population_start_date,
    DATE_ADD(month, INTERVAL 1 MONTH) AS population_end_date,
FROM
    date_array 

UNION ALL

-- we repeat the quarter period monthly to allow greater flexibility downstream
SELECT
    "QUARTER" AS period,
    month AS population_start_date,
    DATE_ADD(month, INTERVAL 1 QUARTER) AS population_end_date,
FROM
    date_array 

UNION ALL

-- we repeat the year period monthly to allow greater flexibility downstream
SELECT
    "YEAR" AS period,
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
    metrics_years_tracked=_METRICS_YEARS_TRACKED,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        METRIC_TIME_PERIODS_VIEW_BUILDER.build_and_print()
