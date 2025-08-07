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
"""Logic for building time windows when metric exports were stale, grouped by state."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.monitoring.platform_kpis.dataset_config import PLATFORM_KPIS_DATASET
from recidiviz.source_tables.yaml_managed.datasets import VIEW_UPDATE_METADATA_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

STALE_METRIC_EXPORT_SPANS_BY_STATE_VIEW_ID = "stale_metric_export_spans_by_state"
STALE_METRIC_EXPORT_SPANS_BY_STATE_DESCRIPTION = (
    "Computes time windows when metric exports were stale, grouped by state. Windows "
    "that cross month boundaries will be attributed to the month that they were resolved in. "
    "Metrics that are currently stale are not included in this view."
)
HOUR_GAP_BEFORE_STALE = 24

VIEW_QUERY = f"""
WITH 
-- filter to non-sandbox metric exports
view_update_exports AS (
  SELECT
    success_timestamp,
    export_job_name,
    CASE 
      WHEN state_code IS NULL THEN "STATE_AGNOSTIC"  -- some exports are for all states, label them as state agnostic
      ELSE UPPER(state_code) -- some of the older state_codes are lower case
    END AS state_code 
  FROM `{{project_id}}.{{view_update_metadata_dataset}}.metric_view_data_export_tracker`
  WHERE
    destination_override IS NULL
    AND sandbox_dataset_prefix IS NULL
), 
-- create windows between metric exports for each job
export_lags AS (
  SELECT
    success_timestamp,
    LAG(success_timestamp) OVER prev_row AS prev_success_timestamp,
    export_job_name,
    state_code  
    FROM view_update_exports
    QUALIFY prev_success_timestamp IS NOT NULL -- drop first date (no data)
    WINDOW prev_row AS (PARTITION BY export_job_name, state_code ORDER BY success_timestamp ASC)
), 
-- create "stale" windows during which a give export is stale
stale_exports_by_month_by_region AS (
  SELECT
    state_code,
    DATE(EXTRACT(YEAR FROM success_timestamp), EXTRACT(MONTH FROM success_timestamp), 1) as export_month,
    TIMESTAMP_ADD(prev_success_timestamp, INTERVAL {HOUR_GAP_BEFORE_STALE} HOUR) AS start_date,
    success_timestamp AS end_date
    FROM export_lags
  WHERE TIMESTAMP_DIFF(success_timestamp, prev_success_timestamp, HOUR) > {HOUR_GAP_BEFORE_STALE}
),
-- [!!!] n.b. the next 3 CTEs are taken from create_sub_sessions_with_attributes, but modified 
--       slightly for different handling of "zero-day" sessions since we are using timestamps
-- generates a set of unique period boundary dates based on the start and end dates of periods.
export_boundary_dates AS (
    SELECT DISTINCT
        export_month, 
        state_code,
        boundary_date
    FROM stale_exports_by_month_by_region,
    UNNEST([start_date, end_date]) AS boundary_date
),
-- generates sub-sessions based on each boundary date and its subsequent date
export_sub_sessions AS (
    SELECT
        export_month,
        state_code,
        boundary_date AS start_date,
        LEAD(boundary_date) OVER (PARTITION BY export_month, state_code ORDER BY boundary_date) AS end_date
    FROM
        export_boundary_dates
),
-- add the attributes from the original periods to the overlapping sub-sessions
sub_sessions_with_attributes AS (
    SELECT
        export_month, 
        state_code,
        se.start_date,
        se.end_date
    FROM
        export_sub_sessions se
    INNER JOIN
        stale_exports_by_month_by_region c
    USING
        (export_month, state_code)
    WHERE
        se.start_date >= c.start_date AND se.start_date < c.end_date
),
-- deduplicate session windows, as all exports usually go stale at once
sub_sessions_deduped AS (
    SELECT
        state_code,
        export_month,
        start_date,
        end_date
    FROM sub_sessions_with_attributes
    GROUP BY 1, 2, 3, 4
),
-- join adjacent stale spans
stale_exports_by_month_by_region_coalesced AS (
  {aggregate_adjacent_spans(
      table_name='sub_sessions_deduped',
      index_columns=['export_month', 'state_code'],
  )}
)
-- select from these dates to make more transparent what our output cols are
SELECT 
  state_code,
  export_month,
  start_date,
  end_date
  FROM stale_exports_by_month_by_region_coalesced
"""


STALE_METRIC_EXPORT_SPANS_BY_STATE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    view_query_template=VIEW_QUERY,
    dataset_id=PLATFORM_KPIS_DATASET,
    view_id=STALE_METRIC_EXPORT_SPANS_BY_STATE_VIEW_ID,
    description=STALE_METRIC_EXPORT_SPANS_BY_STATE_DESCRIPTION,
    view_update_metadata_dataset=VIEW_UPDATE_METADATA_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        STALE_METRIC_EXPORT_SPANS_BY_STATE_VIEW_BUILDER.build_and_print()
