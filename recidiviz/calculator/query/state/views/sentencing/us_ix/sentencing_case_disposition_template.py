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

US_IX_SENTENCING_CASE_DISPOSITION_TEMPLATE = """
WITH cs_sentences AS (
SELECT
    sess.state_code,
    sess.person_id,
    sess.gender,
    sess.assessment_score_start AS assessment_score,
    CASE
        WHEN sess.compartment_level_2 = "PROBATION" THEN "PROBATION"
        WHEN sess.compartment_level_2 = "TREATMENT_IN_PRISON" THEN "RIDER"
        WHEN sess.compartment_level_2 = "GENERAL" THEN "TERM"
    END AS cohort_group,
    sess.start_date AS sentence_start_date,
FROM `{project_id}.sessions.compartment_sessions_materialized` sess
WHERE
    sess.compartment_level_1 IN ("INCARCERATION", "INCARCERATION_OUT_OF_STATE",
                                 "SUPERVISION", "SUPERVISION_OUT_OF_STATE")
    AND sess.compartment_level_2 IN ("GENERAL", "TREATMENT_IN_PRISON", "PROBATION")
),
sp_sentences AS (
SELECT
    sess.state_code,
    sess.person_id,
    sess.gender,
    sess.assessment_score_start AS assessment_score,
    "PROBATION" AS cohort_group,
    sp.effective_date AS sentence_start_date,
FROM `{project_id}.sessions.sentences_preprocessed_materialized` sp
INNER JOIN `{project_id}.sessions.compartment_sessions_materialized` sess
  ON sp.state_code = sess.state_code
  AND sp.person_id = sess.person_id
  AND sp.session_id_closest = sess.session_id
WHERE sentence_sub_type = "PROBATION"
),
sentence_cohort AS (
    # This union may result in more than one sentence per case, but we later use QUALIFY
    # to select only a single sentence per case (the soonest after the assigned date)
    SELECT * FROM cs_sentences
    UNION ALL
    SELECT * FROM sp_sentences
),
psi_cases AS (
  SELECT case_id, LOWER(email) AS psi_email, external_id as staff_id,
  FROM `{project_id}.sentencing_views.sentencing_staff_record_materialized`,
  UNNEST(JSON_VALUE_ARRAY(case_ids)) AS case_id
)
SELECT
  "US_IX" AS state_code, 
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
  LocationId AS location_id,
  LocationName AS location_name,
  cases.due_date AS due_date,
  cases.completion_date AS completion_date,
  cases.sentence_date AS sentence_date,
  cases.assigned_date AS assigned_date,
  sentence_cohort.sentence_start_date,
  DATE_DIFF(assigned_date, sentence_cohort.sentence_start_date, DAY) as assigned_date_diff_days,
FROM `{project_id}.sentencing_views.sentencing_client_record_historical_materialized` clients,
UNNEST(JSON_VALUE_ARRAY(case_ids)) AS case_id
INNER JOIN psi_cases
  USING (case_id)
INNER JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
  USING (state_code, external_id)
INNER JOIN `{project_id}.us_ix_state.state_staff` ss
  ON LOWER(ss.email) = psi_cases.psi_email
INNER JOIN `{project_id}.us_ix_state.state_staff_external_id` ssid
  ON ssid.staff_id = ss.staff_id AND ssid.id_type = 'US_IX_EMPLOYEE'
INNER JOIN `{project_id}.{us_ix_raw_data_up_to_date_dataset}.com_PSIReport_latest` psi
  ON psi.PSIReportId = case_id
INNER JOIN `{project_id}.{us_ix_raw_data_up_to_date_dataset}.ref_Location_latest` loc
  USING (LocationId)
INNER JOIN `{project_id}.sentencing_views.sentencing_case_record_historical_materialized` cases
  ON case_id = cases.external_id
  AND clients.external_id = cases.client_id
INNER JOIN sentence_cohort
  ON sentence_cohort.state_code = pei.state_code AND sentence_cohort.person_id = pei.person_id
  AND DATE_DIFF(sentence_cohort.sentence_start_date, assigned_date, DAY) >= 0
WHERE
  clients.state_code = "US_IX"
  AND cases.sentence_date < CURRENT_DATETIME
  AND (cases.completion_date < CURRENT_DATETIME OR cases.completion_date IS NULL)
QUALIFY ROW_NUMBER() OVER (PARTITION BY pei.person_id, sentence_date ORDER BY assigned_date_diff_days ASC) = 1
"""
