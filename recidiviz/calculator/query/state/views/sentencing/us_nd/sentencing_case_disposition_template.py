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
"""View logic that associates cases with the PSI staff and the eventual sentencing disposition"""
from recidiviz.calculator.query.state.views.sentencing.us_nd.sentencing_sentence_cohort_template import (
    US_ND_SENTENCE_IMPOSED_GROUP_SUMMARY,
)

# TODO(#39399): Switch over to risk-specific assessment scores in this view (since
# `assessment_score`, `assessment_score_start`, `assessment_score_end`, and related
# fields aren't generally specific to assessments in the 'RISK' class).
US_ND_SENTENCING_SENTENCE_COHORT_TEMPLATE = f"""
SELECT
  all_cohorts.state_code, 
  all_cohorts.person_id, 
  all_cohorts.gender, 
  null AS assessment_score,
  IF(
    sentence_type = "INCARCERATION",
    CASE
      WHEN sentence_length_days_max < 365 THEN "INCARCERATION|0-1 years"
      WHEN sentence_length_days_max >= 365 AND sentence_length_days_max < 365*3 THEN "INCARCERATION|1-2 years"
      WHEN sentence_length_days_max >= 365*3 AND sentence_length_days_max < 365*6 THEN "INCARCERATION|3-5 years"
      WHEN sentence_length_days_max >= 365*6 THEN "INCARCERATION|6-? years"
    END,
    sentence_type -- TODO(#41264) Consolidate this sentence type and length logic with that of sentencing_sentence_cohort.py
    ) AS cohort_group,
  serving_start_date AS sentence_start_date,
  all_cohorts.most_severe_description AS most_severe_description,
  all_cohorts.any_is_violent AS any_is_violent,
  all_cohorts.any_is_drug AS any_is_drug,
  all_cohorts.any_is_sex_offense AS any_is_sex_offense
FROM ({US_ND_SENTENCE_IMPOSED_GROUP_SUMMARY}) all_cohorts
WHERE all_cohorts.state_code = 'US_ND'
QUALIFY ROW_NUMBER() OVER (
     PARTITION BY state_code, person_id, serving_start_date
     ORDER BY imposed_date ASC
     ) = 1
"""

US_ND_SENTENCING_CASE_DISPOSITION_TEMPLATE = f"""
WITH sentence_cohort AS (
    SELECT * FROM ({US_ND_SENTENCING_SENTENCE_COHORT_TEMPLATE})
),
psi_cases AS (
  SELECT case_id, LOWER(email) AS psi_email, external_id as staff_id,
  FROM `{{project_id}}.sentencing_views.sentencing_staff_record_materialized`, 
  UNNEST(JSON_VALUE_ARRAY(case_ids)) AS case_id
)
SELECT
  "US_ND" AS state_code, 
  psi_email,
  psi_cases.staff_id,
  cases.client_id,
  pei.person_id,
  pei.external_id,
  case_id,
  sentence_cohort.gender,
  lsir_score,
  assessment_score,
  cohort_group AS disposition,
  '' AS location_id,
  cases.county AS location_name,
  cases.due_date AS due_date,
  cases.completion_date AS completion_date,
  cases.sentence_date AS sentence_date,
  cases.assigned_date AS assigned_date,
  sentence_cohort.sentence_start_date,
  DATE_DIFF(assigned_date, sentence_cohort.sentence_start_date, DAY) as assigned_date_diff_days,
  sentence_cohort.most_severe_description AS most_severe_description,
  sentence_cohort.any_is_violent  AS any_is_violent,
  sentence_cohort.any_is_drug  AS any_is_drug,
  sentence_cohort.any_is_sex_offense  AS any_is_sex_offense
FROM `{{project_id}}.sentencing_views.sentencing_client_record_historical_materialized` clients,
UNNEST(JSON_VALUE_ARRAY(case_ids)) AS case_id
INNER JOIN psi_cases
  USING (case_id)
INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
  USING (state_code, external_id)
INNER JOIN `{{project_id}}.us_nd_state.state_staff` ss
  ON LOWER(ss.email) = psi_cases.psi_email
INNER JOIN `{{project_id}}.us_nd_state.state_staff_external_id` ssid
  ON ssid.staff_id = ss.staff_id AND ssid.id_type = 'US_ND_DOCSTARS_OFFICER'
INNER JOIN `{{project_id}}.sentencing_views.sentencing_case_record_historical_materialized` cases
  ON case_id = cases.external_id 
  AND clients.external_id = cases.client_id
INNER JOIN sentence_cohort
  ON sentence_cohort.state_code = pei.state_code AND sentence_cohort.person_id = pei.person_id
  AND DATE_DIFF(sentence_cohort.sentence_start_date, assigned_date, DAY) >= 0
WHERE
  clients.state_code = "US_ND"
  AND (cases.completion_date < CURRENT_DATETIME OR cases.completion_date IS NULL)
QUALIFY ROW_NUMBER() OVER (PARTITION BY pei.person_id, case_id ORDER BY assigned_date_diff_days desc) = 1
"""
