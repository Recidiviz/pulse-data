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
"""Logic for calculating statistics about the time to resolution for hard failing 
validations.
"""

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.monitoring.platform_kpis.dataset_config import PLATFORM_KPIS_DATASET
from recidiviz.monitoring.platform_kpis.reliability.validation_hard_failure_spans import (
    VALIDATION_HARD_FAILURE_SPANS_VIEW_BUILDER,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.types import assert_type

VALIDATION_ONGOING_HARD_FAILURES_VIEW_ID = "validation_ongoing_hard_failures"
VALIDATION_ONGOING_HARD_FAILURES_DESCRIPTION = (
    "Groups ongoing failure start dates by month."
)

VIEW_QUERY = """
WITH 
-- group ongoing failures by initial failure date
ongoing_failures_per_month_per_state AS (
SELECT 
  state_code,
  CAST(DATE_TRUNC(failure_datetime, MONTH) AS DATE) AS validation_month,
  COUNT(*) as num_ongoing_failures_started
FROM `{project_id}.{platform_kpis_dataset}.{validation_hard_failure_spans_table_id}`
WHERE validation_resolution_status = 'ONGOING'
GROUP BY state_code, validation_month
),
-- determine all months between the first month that an initial ongoing month was seen and today
months_wth_ongoing_failures AS (
  SELECT 
    GENERATE_DATE_ARRAY(MIN(validation_month), DATE_TRUNC(CURRENT_DATE('US/Eastern'), MONTH), INTERVAL 1 MONTH) AS validation_month
  FROM ongoing_failures_per_month_per_state
),
-- cross months_wth_ongoing_failures with states
months_with_ongoing_failures_by_state AS (
  SELECT 
    state_code, 
    validation_month
  FROM (SELECT DISTINCT state_code FROM ongoing_failures_per_month_per_state),
  UNNEST((SELECT validation_month FROM months_wth_ongoing_failures)) AS validation_month
)
-- fills gaps in ongoing_failures_per_month_per_state with 0s
SELECT 
  state_code,
  validation_month AS failure_month,
  IFNULL(num_ongoing_failures_started, 0) AS num_ongoing_failures_started
FROM months_with_ongoing_failures_by_state
FULL OUTER JOIN ongoing_failures_per_month_per_state
USING(state_code, validation_month)
"""


VALIDATION_ONGOING_HARD_FAILURES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    view_query_template=VIEW_QUERY,
    dataset_id=PLATFORM_KPIS_DATASET,
    view_id=VALIDATION_ONGOING_HARD_FAILURES_VIEW_ID,
    description=VALIDATION_ONGOING_HARD_FAILURES_DESCRIPTION,
    platform_kpis_dataset=PLATFORM_KPIS_DATASET,
    validation_hard_failure_spans_table_id=assert_type(
        VALIDATION_HARD_FAILURE_SPANS_VIEW_BUILDER.materialized_address, BigQueryAddress
    ).table_id,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VALIDATION_ONGOING_HARD_FAILURES_VIEW_BUILDER.build_and_print()
