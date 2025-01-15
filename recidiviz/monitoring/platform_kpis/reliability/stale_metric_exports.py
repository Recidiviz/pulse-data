# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Logic to calculate the total hours when any metric export was stale."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.monitoring.platform_kpis.dataset_config import PLATFORM_KPIS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

STALE_METRIC_EXPORTS_VIEW_ID = "stale_metric_exports"
STALE_METRIC_EXPORTS_DESCRIPTION = (
    "Calculates the total hours when any metric export was stale. If a metric was stale "
    "across a month boundary, it will be attributed to the month that it resolved in."
)

VIEW_QUERY = f"""
WITH 
-- [!!!] n.b. the next 4 CTEs are taken from create_sub_sessions_with_attributes, but modified 
--            slightly for different handling of "zero-day" sessions, as we are using time stamps and
--            we to filter out "zero-minute" windows
periods_cte AS (
    SELECT
        export_month,
        start_date,
        end_date
    FROM `{{project_id}}.{{platform_kpis_dataset}}.stale_metric_export_spans_by_state_materialized`
),
-- generates a set of unique period boundary dates based on the start and end dates of periods.
period_boundary_dates AS (
    SELECT DISTINCT
        export_month, 
        boundary_date
    FROM periods_cte,
    UNNEST([start_date, end_date]) AS boundary_date
),
-- generates sub-sessions based on each boundary date and its subsequent date
export_sub_sessions AS (
    SELECT
        export_month,
        boundary_date AS start_date,
        LEAD(boundary_date) OVER (PARTITION BY export_month ORDER BY boundary_date) AS end_date
    FROM
        period_boundary_dates
),
-- add the attributes from the original periods to the overlapping sub-sessions
sub_sessions_with_attributes AS (
    SELECT
        export_month, 
        se.start_date,
        se.end_date
    FROM
        export_sub_sessions se
    INNER JOIN
        periods_cte c
    USING
        (export_month)
    WHERE
        se.start_date >= c.start_date AND se.start_date < c.end_date
),
-- deduplicate sessions windows, as across states we might have duplicate sessions
sub_sessions_deduped AS (
    SELECT
        export_month,
        start_date,
        end_date
    FROM sub_sessions_with_attributes
    GROUP BY 1, 2, 3
),
-- join adjacent stale spans
stale_exports_by_month_coalesced AS (
  {aggregate_adjacent_spans(
      table_name='sub_sessions_deduped',
      index_columns=['export_month'],
  )}
),
-- calculate metrics from these dates to make more transparent what our output cols are
stale_export_hours_by_month AS (
    SELECT 
    export_month, 
    SUM(TIMESTAMP_DIFF(end_date, start_date, HOUR)) as hours_stale
    FROM stale_exports_by_month_coalesced
    GROUP BY export_month
),
-- determine all months where there _could_ have been data
dates_with_expected_data as (
  SELECT export_month 
  FROM UNNEST(GENERATE_DATE_ARRAY('2023-01-01', DATE_TRUNC(CURRENT_DATE('US/Eastern'), MONTH), INTERVAL 1 MONTH)) as export_month
)
-- fill in dates without stale stale as 0s!
SELECT 
    export_month,
    IFNULL(hours_stale, 0) as hours_stale
FROM stale_export_hours_by_month
FULL OUTER JOIN dates_with_expected_data
USING (export_month)
"""


STALE_METRIC_EXPORTS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    view_query_template=VIEW_QUERY,
    dataset_id=PLATFORM_KPIS_DATASET,
    view_id=STALE_METRIC_EXPORTS_VIEW_ID,
    description=STALE_METRIC_EXPORTS_DESCRIPTION,
    platform_kpis_dataset=PLATFORM_KPIS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        STALE_METRIC_EXPORTS_VIEW_BUILDER.build_and_print()
