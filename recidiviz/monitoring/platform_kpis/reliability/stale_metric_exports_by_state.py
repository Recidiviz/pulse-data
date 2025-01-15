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
"""Logic to calculates the total number of hours each state had stale metric exports"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.monitoring.platform_kpis.dataset_config import PLATFORM_KPIS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

STALE_METRIC_EXPORTS_BY_STATE_VIEW_ID = "stale_metric_exports_by_state"
STALE_METRIC_EXPORTS_BY_STATE_DESCRIPTION = (
    "Calculates the total number of hours each state had stale metric exports. If a "
    "metric was stale across a month boundary, it will be attributed to the month that "
    "it resolved in."
)

VIEW_QUERY = """
WITH 
-- determine all months where there _could_ have been data
dates_with_expected_data as (
  SELECT state_code, export_month 
  FROM (
    SELECT DISTINCT state_code
    FROM `{project_id}.{platform_kpis_dataset}.stale_metric_export_spans_by_state_materialized`
  ),
  UNNEST(GENERATE_DATE_ARRAY('2023-01-01', DATE_TRUNC(CURRENT_DATE('US/Eastern'), MONTH), INTERVAL 1 MONTH)) as export_month
),
-- determine what months we do know there was stale data
existing_stale_data AS (
SELECT
    state_code,
    export_month,
    SUM(TIMESTAMP_DIFF(end_date, start_date, HOUR)) as hours_stale
FROM `{project_id}.{platform_kpis_dataset}.stale_metric_export_spans_by_state_materialized`
GROUP BY state_code, export_month
)
-- fill in dates without stale data as 0s!
SELECT 
    state_code, 
    export_month,
    IFNULL(hours_stale, 0) as hours_stale
FROM dates_with_expected_data
FULL OUTER JOIN existing_stale_data
USING (state_code, export_month)
"""


STALE_METRIC_EXPORTS_BY_STATE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    view_query_template=VIEW_QUERY,
    dataset_id=PLATFORM_KPIS_DATASET,
    view_id=STALE_METRIC_EXPORTS_BY_STATE_VIEW_ID,
    description=STALE_METRIC_EXPORTS_BY_STATE_DESCRIPTION,
    platform_kpis_dataset=PLATFORM_KPIS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        STALE_METRIC_EXPORTS_BY_STATE_VIEW_BUILDER.build_and_print()
