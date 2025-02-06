# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
#
# This program is free software: you can redistribute it AND/or modify
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
"""US_MI - view that returns Insights and Workflows related metrics for MI agents with active caseloads (to be connected to a google sheet for MI leadership use) """

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    OUTLIERS_VIEWS_DATASET,
    WORKFLOWS_VIEWS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION
from recidiviz.utils.metadata import local_project_id_override

US_MI_INSIGHTS_WORKFLOWS_DETAILS_FOR_LEADERSHIP_VIEW_NAME = (
    "us_mi_insights_workflows_details_for_leadership"
)

US_MI_INSIGHTS_WORKFLOWS_DETAILS_FOR_LEADERSHIP_VIEW_DESCRIPTION = """Insights and Workflows related metrics for for MI agents with active caseloads (to be connected to a google sheet for MI leadership use)"""

US_MI_INSIGHTS_WORKFLOWS_DETAILS_FOR_LEADERSHIP_QUERY_TEMPLATE = """
WITH
-- get the most recent end date from calculated outliers results and the start_date for when we should begin to count logins
-- (current_end_date - 1 year) if it's been over a year since the full launch, 2023-08-01 if it hasn't been a year since the full launch yet
current_period AS (
  SELECT
    current_end_date,
    CASE WHEN 
            current_end_date > DATE(2024,8,1)
         THEN 
            DATE_SUB(current_end_date, INTERVAL 1 YEAR)
         ELSE 
            DATE(2023,8,1) 
        END AS login_start_date
  FROM (
    SELECT 
        MAX(end_date) AS current_end_date
    FROM `{project_id}.{outliers_dataset}.supervision_officer_outlier_status_materialized`
    WHERE state_code = 'US_MI'
        AND period = 'YEAR'
  )
),
-- get the list of current MI officers (aka agents with a currently active caseload)
current_officers AS (
    SELECT
        off.state_code,
        off.external_id as officer_id,
        JSON_VALUE(off.full_name, '$.given_names') AS officer_first_name,
        JSON_VALUE(off.full_name, '$.surname') AS officer_last_name,
        off_email.email AS officer_email,
        JSON_VALUE(sup.full_name, '$.given_names') AS supervisor_first_name,
        JSON_VALUE(sup.full_name, '$.surname') AS supervisor_last_name,
        sup.email AS supervisor_email,
        off.supervision_district AS officer_district,
        current_end_date AS end_date,
        'YEAR' AS period
    FROM `{project_id}.{outliers_dataset}.supervision_officers_materialized` off
    INNER JOIN `{project_id}.{normalized_state_dataset}.state_staff` off_email
        ON off.staff_id = off_email.staff_id
    LEFT JOIN `{project_id}.{outliers_dataset}.supervision_officer_supervisors_materialized` sup
        ON off.supervisor_external_id = sup.external_id 
        AND off.state_code = sup.state_code,
    current_period
),
-- for each officer included in outliers, set the flag outlier_on_most_recent_period for whether they were an outlier for at least one metric on the current period
outlier_status_flag AS (
  SELECT
    state_code,
    officer_id,
    period,
    end_date,
    MAX(status = 'FAR') AS outlier_on_most_recent_period
  FROM `{project_id}.{outliers_dataset}.supervision_officer_outlier_status_materialized` stat
  INNER JOIN current_period 
    ON stat.end_date = current_period.current_end_date
  WHERE period = 'YEAR' 
  GROUP BY 1,2,3,4
),
-- for every officer that was determined to be an outlier for that metric, pull in their metric rate
outlier_rates AS (
  SELECT 
    state_code,
    officer_id,
    period,
    end_date,
    metric_rate,
    metric_id
  FROM `{project_id}.{outliers_dataset}.supervision_officer_outlier_status_materialized` stat
  INNER JOIN current_period 
    ON stat.end_date = current_period.current_end_date
  WHERE 
    status = 'FAR'
    AND period = 'YEAR'
),
-- pull in the number of logins per user
-- NOTE: this technically pulls in the number of logins into the Recidiviz tool overall (not necessarily just workflows)
--       but this is a good enough proxy for now
logins AS (
  SELECT
    ref.state_code, 
    user_external_id as officer_id,
    SUM(1) AS total_logins,
    COUNT(DISTINCT DATE_TRUNC(original_timestamp, MONTH))/DATE_DIFF(MAX(current_end_date), MAX(login_start_date), MONTH) AS pct_months_login_past_year
  FROM `{project_id}.auth0_prod_action_logs.success_login` log,
    current_period
  INNER JOIN `{project_id}.{workflows_dataset}.reidentified_dashboard_users` ref
    ON log.user_hash = ref.user_id
  WHERE DATE(original_timestamp) BETWEEN login_start_date AND current_end_date
  GROUP BY 1,2
),
-- get the number of clients currently surfaced as eligible for an opportunity in workflows
workflows_currently_eligible AS (
  SELECT *
  FROM (
    SELECT 
        officer_id, 
        completion_event_type AS opp, 
        person_external_id
    FROM `{project_id}.{workflows_dataset}.current_impact_funnel_status_materialized`
    WHERE 
      state_code = 'US_MI' AND 
      remaining_criteria_needed = 0 AND 
      status <> 'DENIED'
  )
  PIVOT
  (
    -- #2 aggregate
    count(DISTINCT person_external_id) AS num_eligible
    -- #3 pivot_column
    FOR opp in (
      'FULL_TERM_DISCHARGE',
      'EARLY_DISCHARGE',
      'SUPERVISION_LEVEL_DOWNGRADE_AFTER_INITIAL_CLASSIFICATION_REVIEW_DATE',
      'TRANSFER_TO_LIMITED_SUPERVISION',
      'SUPERVISION_LEVEL_DOWNGRADE_BEFORE_INITIAL_CLASSIFICATION_REVIEW_DATE'
    )
  )
),
-- get the number of clients currently surfaced as eligible BUT marked ineligible for an opportunity in workflows
workflows_currently_marked_ineligible AS (
  SELECT * FROM (
    SELECT 
        officer_id, 
        completion_event_type AS opp, 
        person_external_id
    FROM `{project_id}.{workflows_dataset}.current_impact_funnel_status_materialized`
    WHERE 
      state_code = 'US_MI' AND 
      remaining_criteria_needed = 0 AND 
      status = 'DENIED'
  )
  PIVOT
  (
    -- #2 aggregate
    count(DISTINCT person_external_id) AS num_marked_ineligible
    -- #3 pivot_column
    FOR opp in (
      'FULL_TERM_DISCHARGE',
      'EARLY_DISCHARGE',
      'SUPERVISION_LEVEL_DOWNGRADE_AFTER_INITIAL_CLASSIFICATION_REVIEW_DATE',
      'TRANSFER_TO_LIMITED_SUPERVISION',
      'SUPERVISION_LEVEL_DOWNGRADE_BEFORE_INITIAL_CLASSIFICATION_REVIEW_DATE'
    )
  )
),
achievement_recognitions AS (
    SELECT 
        Completed_By_Staff_Omnni_Employee_Id as officer_id, 
        COUNT(DISTINCT Supervision_Schedule_Activity_Id) as n_recognitions
    FROM `{project_id}.{raw_data_up_to_date_views_dataset}.COMS_Supervision_Schedule_Activities_latest`,
    current_period
    WHERE 
        Activity = 'Achievement Recognition'
        AND DATE(Completed_Date) >= DATE_SUB(current_end_date, INTERVAL 1 YEAR)
    group by 1
)

-- join everything together and make readable column names
SELECT 
  agg.end_date 
    AS `Data Updated AS Of`,
  COALESCE(o.officer_district, 'UNKNOWN') 
    AS `Region`,
  o.officer_last_name 
    AS `Agent Last Name`,
  o.officer_first_name 
    AS `Agent First Name`,
  UPPER(o.officer_email) 
    AS `Agent Email`,
  o.supervisor_last_name 
    AS `Supervisor Last Name`,
  o.supervisor_first_name 
    AS `Supervisor First Name`,
  UPPER(o.supervisor_email) 
    AS `Supervisor Email`,
  UPPER(CAST((flag.officer_id IS NOT NULL) AS STRING)) 
    AS `Meets Criteria for Insights`,
  COALESCE(UPPER(CAST(flag.outlier_on_most_recent_period AS STRING)), 'N/A') 
    AS `Agent is Outlier on at Least One Insights Metric`,
  IF(flag.officer_id IS NOT NULL, ROUND(agg.avg_daily_population, 0), NULL) 
    AS `Average Daily Caseload`,
  IF(flag.officer_id IS NOT NULL, ROUND(ab_rates.metric_rate, 2), NULL)  
    AS `Absconder Warrants Rate if Outlier on Metric`,
  IF(flag.officer_id IS NOT NULL, agg.absconsions_bench_warrants, NULL)  
    AS `Count of Absconder Warrants`,
  IF(flag.officer_id IS NOT NULL, ROUND(inc_rates.metric_rate, 2), NULL)  
    AS `Incarceration Rate if Outlier on Metric`,
  IF(flag.officer_id IS NOT NULL, agg.incarceration_starts_AND_inferred, NULL) `Count of Incarcerations`,
  COALESCE(logins.total_logins, 0) 
    AS `Total Logins in Last Year Since Tool Launch`,
  ROUND(COALESCE(logins.pct_months_login_past_year, 0),2) 
    AS `Pct of Months with at Least One Login in Last Year Since Tool Launch`,
  agg.task_completions_early_discharge 
    AS `# of Clients Granted Early Discharge in Last Year`,
  agg.task_completions_supervision_level_downgrade_before_initial_classification_review_date 
    AS `# of Clients Granted Supervision Level Mismatch in Last Year`,
  agg.task_completions_supervision_level_downgrade_after_initial_classification_review_date 
    AS `# of Clients Granted Supervision Level Downgrade in Last Year`,
  agg.task_completions_while_eligible_full_term_discharge 
    AS `# of Clients Granted Overdue for Discharge in Last Year`,
  agg.task_completions_transfer_to_limited_supervision 
    AS `# of Clients Granted Minimum Telephone Reporting in Last Year`,
  COALESCE(elig.num_eligible_EARLY_DISCHARGE, 0) 
    AS `# of Clients Currently Eligible for Early Discharge`,
  COALESCE(elig.num_eligible_SUPERVISION_LEVEL_DOWNGRADE_BEFORE_INITIAL_CLASSIFICATION_REVIEW_DATE, 0) 
    AS `# of Clients Currently with a Supervision Level Mismatch`,
  COALESCE(elig.num_eligible_SUPERVISION_LEVEL_DOWNGRADE_AFTER_INITIAL_CLASSIFICATION_REVIEW_DATE, 0) 
    AS `# of Clients Currently Eligible for a Supervision Level Downgrade`,
  COALESCE(elig.num_eligible_TRANSFER_TO_LIMITED_SUPERVISION, 0) 
    AS `# of Clients Currently Eligible for Minimum Telephone Reporting`,
  COALESCE(elig.num_eligible_FULL_TERM_DISCHARGE, 0) 
    AS `# of Clients Currently Eligible for Full Term Discharge`,
  COALESCE(inelig.num_marked_ineligible_EARLY_DISCHARGE, 0) 
    AS `# of Clients Currently Marked Ineligible for Early Discharge`,
  COALESCE(inelig.num_marked_ineligible_SUPERVISION_LEVEL_DOWNGRADE_BEFORE_INITIAL_CLASSIFICATION_REVIEW_DATE, 0) 
    AS `# of Clients Currently Marked Ineligible for a Supervision Level Mismatch`,
  COALESCE(inelig.num_marked_ineligible_SUPERVISION_LEVEL_DOWNGRADE_AFTER_INITIAL_CLASSIFICATION_REVIEW_DATE, 0) 
    AS `# of Clients Currently Marked Ineligible for a Supervision Level Downgrade`,
  COALESCE(inelig.num_marked_ineligible_TRANSFER_TO_LIMITED_SUPERVISION, 0) 
    AS `# of Clients Currently Marked Ineligible for Minimum Telephone Reporting`,
  COALESCE(inelig.num_marked_ineligible_FULL_TERM_DISCHARGE, 0) 
    AS `# of Clients Currently Marked Ineligible for Full Term Discharge`,
  COALESCE(n_recognitions, 0)
    AS `# of Achievement Recognitions Used in Past Year`
FROM current_officers o
LEFT JOIN logins
  USING(officer_id, state_code)
LEFT JOIN outlier_status_flag flag
  USING(officer_id, state_code, period, end_date)
INNER JOIN `{project_id}.aggregated_metrics.supervision_officer_or_previous_if_transitional_aggregated_metrics_materialized` agg
  USING(state_code, officer_id, period, end_date)
LEFT JOIN (SELECT * FROM outlier_rates WHERE metric_id = 'absconsions_bench_warrants') ab_rates
  USING(state_code, officer_id, period, end_date)
LEFT JOIN (SELECT * FROM outlier_rates WHERE metric_id = 'incarceration_starts_AND_inferred') inc_rates
  USING(state_code, officer_id, period, end_date)
LEFT JOIN workflows_currently_eligible elig
  USING(officer_id)
LEFT JOIN workflows_currently_marked_ineligible inelig
  USING(officer_id)
LEFT JOIN achievement_recognitions
  USING(officer_id)
WHERE
  o.state_code = 'US_MI'
ORDER BY 
    o.officer_district, 
    o.officer_last_name
"""

US_MI_INSIGHTS_WORKFLOWS_DETAILS_FOR_LEADERSHIP_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    view_id=US_MI_INSIGHTS_WORKFLOWS_DETAILS_FOR_LEADERSHIP_VIEW_NAME,
    dataset_id=ANALYST_VIEWS_DATASET,
    description=US_MI_INSIGHTS_WORKFLOWS_DETAILS_FOR_LEADERSHIP_VIEW_DESCRIPTION,
    view_query_template=US_MI_INSIGHTS_WORKFLOWS_DETAILS_FOR_LEADERSHIP_QUERY_TEMPLATE,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    workflows_dataset=WORKFLOWS_VIEWS_DATASET,
    outliers_dataset=OUTLIERS_VIEWS_DATASET,
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_MI,
        instance=DirectIngestInstance.PRIMARY,
    ),
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_PRODUCTION):
        US_MI_INSIGHTS_WORKFLOWS_DETAILS_FOR_LEADERSHIP_VIEW_BUILDER.build_and_print()
