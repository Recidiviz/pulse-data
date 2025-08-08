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
"""Logic to compute hard failure spans, or time windows during which a validation was
in a state of hard failure.
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.monitoring.platform_kpis.dataset_config import PLATFORM_KPIS_DATASET
from recidiviz.source_tables.externally_managed.datasets import (
    VALIDATION_RESULTS_DATASET_ID,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.validation_models import ValidationResultStatus
from recidiviz.validation.validation_output_views import (
    VALIDATION_RESULTS_BIGQUERY_ADDRESS,
    VALIDATIONS_COMPLETION_TRACKER_BIGQUERY_ADDRESS,
)

ONGOING_OR_REMOVED_STATUS = "ONGOING_OR_REMOVED"
ONGOING_STATUS = "ONGOING"
REMOVED_STATUS = "REMOVED"
VALIDATION_HARD_FAILURE_SPANS_VIEW_ID = "validation_hard_failure_spans"
VALIDATION_HARD_FAILURE_SPANS_DESCRIPTION = (
    "Computes the start and end windows of each hard failure for any validation; the "
    "end window of a hard failure can either be SUCCESS or FAIL_SOFT. This view DOES NOT "
    "take into account whether the resolution was a threshold bump or a fixing of the "
    "underlying issue."
)

VIEW_QUERY = f"""
WITH 
-- determine the last time each validation was run by state (each state is technically
-- run individually)
most_recent_validation_run_by_state AS (
  SELECT 
    region_code as state_code,
    CAST(MAX(success_timestamp) AS DATETIME) as most_recent_run_datetime
  FROM `{{project_id}}.{{validation_results_dataset}}.{{validation_tracker_table_id}}`
  WHERE sandbox_dataset_prefix IS NULL
  GROUP BY state_code
),
-- filters validation rows down to state changes when we either started hard failing OR
-- stopped hard failing
validation_hard_failure_state_changes AS (
  SELECT 
    region_code AS state_code,
    validation_name,
    validation_result_status,
    run_datetime,
  FROM `{{project_id}}.{{validation_results_dataset}}.{{validation_results_table_id}}`
  WHERE did_run IS TRUE -- filter out runs whose queries errored
  QUALIFY (
  -- we stopped failing
    ( 
      (
        -- we either resolved to a non-failing status
        validation_result_status != '{ValidationResultStatus.FAIL_HARD.value}'
        OR 
        (
          -- we are failing and this is the LAST time we've seen this validation
          validation_result_status = '{ValidationResultStatus.FAIL_HARD.value}'
          AND 
          LEAD(validation_result_status) OVER ordered_rows_for_validation IS NULL
        )
      )
      -- and the last row is failing hard
      AND LAG(validation_result_status) OVER ordered_rows_for_validation = '{ValidationResultStatus.FAIL_HARD.value}'
    )
    OR 
    -- we started failing
    ( 
      validation_result_status = '{ValidationResultStatus.FAIL_HARD.value}'
      AND (
        -- started failing after not failing before
        LAG(validation_result_status) OVER ordered_rows_for_validation != '{ValidationResultStatus.FAIL_HARD.value}'
        OR 
        -- started failing after not having run before
        LAG(validation_result_status) OVER ordered_rows_for_validation IS NULL 
      )
    )
  )
  WINDOW ordered_rows_for_validation AS (PARTITION BY region_code, validation_name ORDER BY run_datetime ASC)
),
-- combine state changes when we went from hard failing to not hard failing, additionally
-- capturing if the last state we saw was hard failing
validation_hard_failure_spans AS (
  SELECT 
    state_code,
    validation_name,
    validation_result_status AS validation_failure_status,
    run_datetime AS failure_datetime,
    CASE
      WHEN 
        LEAD(validation_result_status) OVER ordered_rows_for_validation != '{ValidationResultStatus.FAIL_HARD.value}' 
      THEN 
        LEAD(validation_result_status) OVER ordered_rows_for_validation
      ELSE '{ONGOING_OR_REMOVED_STATUS}'
    END AS validation_resolution_status,
    CASE
      WHEN 
        LEAD(run_datetime) OVER ordered_rows_for_validation IS NOT NULL 
      THEN LEAD(run_datetime) OVER ordered_rows_for_validation
      ELSE run_datetime
    END AS resolution_datetime
  FROM validation_hard_failure_state_changes 
  QUALIFY (
      -- anchor on the hard failure
      validation_result_status = '{ValidationResultStatus.FAIL_HARD.value}' 
      AND
      (
        -- we previously were not hard failing
        LAG(validation_result_status) OVER ordered_rows_for_validation != '{ValidationResultStatus.FAIL_HARD.value}'
        -- the first validation run was a failure
        OR 
        LAG(validation_result_status) OVER ordered_rows_for_validation IS NULL
        OR 
        -- the last validation run was a failure, there are two cases we need to handle:
        --  1. S F      <-- if there was a single last failure run, we just want the last failure as the anchor
        --  2. S F F    <-- if there were multiple failures in a row, we want the second to last failure as the anchor
        (
          LAG(validation_result_status) OVER ordered_rows_for_validation != '{ValidationResultStatus.FAIL_HARD.value}'
          AND 
          LEAD(validation_result_status) OVER ordered_rows_for_validation IS NULL
        )
      )
  )
  WINDOW ordered_rows_for_validation AS (PARTITION BY state_code, validation_name ORDER BY run_datetime ASC)
)
  -- join validation hard failure spans with last run times to determine if failures that 
  -- are ONGOING_OR_REMOVED are actually ongoing or were removed
SELECT * FROM validation_hard_failure_spans WHERE validation_resolution_status != '{ONGOING_OR_REMOVED_STATUS}'
UNION ALL 
SELECT 
  s.state_code,
  s.validation_name,
  s.validation_failure_status,
  s.failure_datetime,
  IF(
    -- run_datetime of each run are all the same for results table, but the completion tracker
    -- success_timestamp is the time when the validations are finished running, so we add a 
    -- little wiggle room for validation run time
    s.resolution_datetime BETWEEN DATETIME_SUB(m.most_recent_run_datetime, INTERVAL 1 DAY) AND DATETIME_ADD(m.most_recent_run_datetime, INTERVAL 1 DAY), 
    '{ONGOING_STATUS}',
    '{REMOVED_STATUS}'
  ) AS validation_resolution_status,
  IF(
    s.resolution_datetime BETWEEN DATETIME_SUB(m.most_recent_run_datetime, INTERVAL 1 DAY) AND DATETIME_ADD(m.most_recent_run_datetime, INTERVAL 1 DAY), 
    NULL,
    s.resolution_datetime
  ) AS resolution_datetime,
FROM (
  SELECT * FROM validation_hard_failure_spans WHERE validation_resolution_status = '{ONGOING_OR_REMOVED_STATUS}'
) s
LEFT JOIN most_recent_validation_run_by_state m
USING(state_code)
"""


VALIDATION_HARD_FAILURE_SPANS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    view_query_template=VIEW_QUERY,
    dataset_id=PLATFORM_KPIS_DATASET,
    view_id=VALIDATION_HARD_FAILURE_SPANS_VIEW_ID,
    description=VALIDATION_HARD_FAILURE_SPANS_DESCRIPTION,
    validation_results_dataset=VALIDATION_RESULTS_DATASET_ID,
    validation_results_table_id=VALIDATION_RESULTS_BIGQUERY_ADDRESS.table_id,
    validation_tracker_table_id=VALIDATIONS_COMPLETION_TRACKER_BIGQUERY_ADDRESS.table_id,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VALIDATION_HARD_FAILURE_SPANS_VIEW_BUILDER.build_and_print()
