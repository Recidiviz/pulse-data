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
"""This view aggregates early discharge stats at the officer-level. It is used to
generate reports that can be used by supervisors to identify officers who are
not completing early discharges in a timely manner. These only include
information for the most recent complete months."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.utils.us_me_query_fragments import (
    cis_900_employee_to_supervisor_match,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# States currently supported
SUPPORTED_STATES = ["US_ME"]

EARLY_DISCHARGE_REPORTS_PER_OFFICER_VIEW_NAME = "early_discharge_reports_per_officer"

EARLY_DISCHARGE_REPORTS_PER_OFFICER_VIEW_DESCRIPTION = """
This view aggregates early discharge stats at the officer-level. It is used to
generate reports that can be used by supervisors to identify officers who are
not completing early discharges in a timely manner. These only include
information for the most recent complete months."""

EARLY_DISCHARGE_REPORTS_PER_OFFICER_QUERY_TEMPLATE = f"""
WITH early_discharges_last_1_month_agg_metrics AS (
  SELECT 
    state_code,
    officer_id,
    officer_name,
    end_date,
    ROUND(avg_population_task_eligible_early_discharge, 1) AS avg_population_task_eligible_early_discharge,
    task_completions_early_discharge
  FROM 
    `{{project_id}}.aggregated_metrics.supervision_officer_aggregated_metrics_materialized`
  WHERE period IN ('MONTH')
    AND avg_critical_caseload_size > 0
    -- Only keep metrics where end_date is the current_month
    AND end_date = DATE_TRUNC(CURRENT_DATE('US/Eastern'), MONTH)
    AND state_code IN ('{{supported_states}}')
),

early_discharges_last_6_months_agg_metrics AS (
  SELECT 
      state_code,
      officer_id,
      officer_name,
      MAX(end_date) AS end_date,
      SUM(task_completions_early_discharge) AS task_completions_early_discharge_6_months
  FROM (
      SELECT *
      FROM 
        `{{project_id}}.aggregated_metrics.supervision_officer_aggregated_metrics_materialized`
      WHERE period IN ('QUARTER')
        AND avg_critical_caseload_size IS NOT NULL
        AND avg_critical_caseload_size != 0
      -- Keep last two quarters to make them a semester
        AND (end_date = DATE_TRUNC(CURRENT_DATE('US/Eastern'), MONTH) 
            OR end_date = DATE_SUB(DATE_TRUNC(CURRENT_DATE('US/Eastern'), MONTH), INTERVAL 3 MONTH))
  )
  WHERE state_code IN ('{{supported_states}}')
  GROUP BY 1,2,3
),

combined_early_discharges_agg_metrics AS (
  SELECT
    IFNULL(ed1.state_code, ed6.state_code) AS state_code,
    IFNULL(ed1.officer_id, ed6.officer_id) AS officer_id,
    IFNULL(ed1.officer_name, ed6.officer_name) AS officer_name,
    ed1.end_date,
    ed1.avg_population_task_eligible_early_discharge,
    IFNULL(ed1.task_completions_early_discharge, 0) AS task_completions_early_discharge,
    ed6.task_completions_early_discharge_6_months,
  FROM early_discharges_last_1_month_agg_metrics ed1
  FULL OUTER JOIN early_discharges_last_6_months_agg_metrics ed6
    USING (state_code, officer_id)
),

employee_to_supervisor_map AS (
    -- Maine
    #TODO(#39511): use ingested entity data instead of CIS_900_EMPLOYEE_latest
    {cis_900_employee_to_supervisor_match()}
)

SELECT 
  ed.state_code,
  ed.officer_id,
  ed.officer_name,
  esm.officer_email,
  esm2.officer_name AS supervisor_name,
  esm2.officer_id AS supervisor_id,
  esm2.officer_email AS supervisor_email,
  ed.end_date AS report_date,
  ed.avg_population_task_eligible_early_discharge AS avg_eligible_month,
  ed.task_completions_early_discharge AS early_terminations_month,
  ed.task_completions_early_discharge_6_months AS early_terminations_6_months,
-- 1 month stats
FROM 
  combined_early_discharges_agg_metrics ed
-- Map officer to supervisor
LEFT JOIN 
  employee_to_supervisor_map esm
USING (state_code, officer_id)
-- Grab supervisor name
LEFT JOIN 
  employee_to_supervisor_map esm2
ON 
  esm2.officer_id = esm.supervisor_id
    AND esm2.state_code = ed.state_code"""

EARLY_DISCHARGE_REPORTS_PER_OFFICER_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=EARLY_DISCHARGE_REPORTS_PER_OFFICER_VIEW_NAME,
    description=EARLY_DISCHARGE_REPORTS_PER_OFFICER_VIEW_DESCRIPTION,
    view_query_template=EARLY_DISCHARGE_REPORTS_PER_OFFICER_QUERY_TEMPLATE,
    us_me_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_ME, instance=DirectIngestInstance.PRIMARY
    ),
    supported_states="', '".join(SUPPORTED_STATES),
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        EARLY_DISCHARGE_REPORTS_PER_OFFICER_VIEW_BUILDER.build_and_print()
