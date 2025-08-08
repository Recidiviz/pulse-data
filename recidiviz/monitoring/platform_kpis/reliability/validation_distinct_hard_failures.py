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
"""Logic for calculating the number of distinct hard failures that occurred during each
month that we have validation data.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.monitoring.platform_kpis.dataset_config import PLATFORM_KPIS_DATASET
from recidiviz.monitoring.platform_kpis.reliability.validation_hard_failure_spans import (
    VALIDATION_HARD_FAILURE_SPANS_VIEW_BUILDER,
)
from recidiviz.source_tables.externally_managed.datasets import (
    VALIDATION_RESULTS_DATASET_ID,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.validation_output_views import (
    VALIDATIONS_COMPLETION_TRACKER_BIGQUERY_ADDRESS,
)

VALIDATION_DISTINCT_HARD_FAILURES_VIEW_ID = "validation_distinct_hard_failures"
VALIDATION_DISTINCT_HARD_FAILURES_DESCRIPTION = (
    "Computes number of distinct hard failures for each month. If a hard failure spans "
    "multiple months, it will be added once to each month that is is failing."
)

VIEW_QUERY = """
WITH 
-- builds the set of all months during which validations have been running
months_to_track AS (
  SELECT failure_month
  FROM
  (
    SELECT 
      GENERATE_DATE_ARRAY(
        CAST(DATE_TRUNC(MIN(success_timestamp), MONTH) AS DATE),
        DATE_TRUNC(CURRENT_DATE('US/Eastern'), MONTH), 
        INTERVAL 1 MONTH
      ) AS months_with_validations_active
    FROM `{project_id}.{validation_results_dataset}.{validations_completion_tracker_table_id}`
    WHERE sandbox_dataset_prefix IS NULL
  ) months,
  UNNEST(months.months_with_validations_active) as failure_month
),
-- builds the set of all states that ever had validations running
all_validations_enabled_states AS (
  SELECT DISTINCT region_code AS state_code 
  FROM `{project_id}.{validation_results_dataset}.{validations_completion_tracker_table_id}`
  WHERE region_code IS NOT NULL AND sandbox_dataset_prefix IS NULL
),
-- determines the set of months that each distinct failure was failing during
distinct_failures_with_month AS (
  SELECT
    state_code,
    validation_name,
    failure_month  
  FROM (
    SELECT
      state_code,
      validation_name,
      failure_datetime,
      resolution_datetime,
      GENERATE_DATE_ARRAY(
          DATE(DATE_TRUNC(failure_datetime, MONTH)),
          DATE(DATE_TRUNC(COALESCE(resolution_datetime, 
          CURRENT_DATETIME("US/Eastern")), MONTH)), INTERVAL 1 MONTH
      ) AS failure_month_dates
    FROM
      `{project_id}.{platform_kpis_dataset}.{validation_hard_failure_spans_table_id}`
  ), UNNEST(failure_month_dates) AS failure_month
)
SELECT
  state_code,
  failure_month,
  COUNT(validation_name) AS distinct_failures
FROM months_to_track, all_validations_enabled_states
LEFT OUTER JOIN
distinct_failures_with_month
USING (state_code, failure_month)
GROUP BY state_code, failure_month
"""


VALIDATION_DISTINCT_HARD_FAILURES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    view_query_template=VIEW_QUERY,
    dataset_id=PLATFORM_KPIS_DATASET,
    view_id=VALIDATION_DISTINCT_HARD_FAILURES_VIEW_ID,
    description=VALIDATION_DISTINCT_HARD_FAILURES_DESCRIPTION,
    validation_results_dataset=VALIDATION_RESULTS_DATASET_ID,
    validations_completion_tracker_table_id=VALIDATIONS_COMPLETION_TRACKER_BIGQUERY_ADDRESS.table_id,
    platform_kpis_dataset=PLATFORM_KPIS_DATASET,
    validation_hard_failure_spans_table_id=VALIDATION_HARD_FAILURE_SPANS_VIEW_BUILDER.table_for_query.table_id,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VALIDATION_DISTINCT_HARD_FAILURES_VIEW_BUILDER.build_and_print()
