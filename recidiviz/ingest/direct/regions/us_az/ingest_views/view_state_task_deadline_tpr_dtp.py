# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Query containing release dates for people who are incarcerated and have been approved
or tentatively approved for the Transition Release Program or Drug Transition Program. 

The eligible date field contains the date on which they became eligible for transition
release. This is the earliest possible date they could be released on whichever type 
of transition is denoted in the task_subtype field, according to ACIS. It is possible 
that a person is approved for both programs; they will have a separate row for each 
transition program, with the released date stored in ACIS for that program. 

If a person is approved or tentatively approved and later denied, their eligibility
date will be cleared from this table.
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- Return one row per person, per incarceration stint, when the person has been approved
-- or tentatively approved for transition release for that stint. Each row will contain
-- the date the person is planned to be released on TPR, if one exists, as well as the date
-- on which that release date was last updated in ACIS. 
tpr_eligibility_dates AS (
SELECT 
  PERSON_ID,
  DOC_ID,
  FINAL_OFFENSE_ID,
  SC_EPISODE_ID,
  task_subtype,
  IF(transition_release_eligibility_date BETWEEN '1900-01-01' AND '2100-01-01', transition_release_eligibility_date, NULL) AS transition_release_eligibility_date,
  update_datetime_elig,
  approval_status
  FROM (
    SELECT DISTINCT 
      CAST(COALESCE(
        NULLIF(TRANSITION_PROGRAM_RLS_DTM_ML, 'NULL'),
        NULLIF(TRANSITION_PROGRAM_RLS_DTM_ARD, 'NULL'), 
        NULLIF(TRANSITION_PROGRAM_RLS_DTM, 'NULL')) AS DATETIME) AS transition_release_eligibility_date,
      CAST(COALESCE(
        NULLIF(off.UPDT_DTM_ML, 'NULL'), 
        NULLIF(off.UPDT_DTM_ARD, 'NULL'), 
        NULLIF(off.UPDT_DTM, 'NULL')) AS DATETIME) AS update_datetime_elig,
      ep.PERSON_ID,
      ep.DOC_ID,
      sc.FINAL_OFFENSE_ID,
      sc.SC_EPISODE_ID,
      lookups.DESCRIPTION AS approval_status,
      'Standard Transition Release' AS task_subtype
    FROM {AZ_DOC_SC_OFFENSE@ALL} off
    JOIN {AZ_DOC_SC_EPISODE} sc
      ON(sc.FINAL_OFFENSE_ID = off.OFFENSE_ID)
    JOIN {DOC_EPISODE} ep
      USING(DOC_ID)
    LEFT JOIN {LOOKUPS} lookups
      ON(TRANSITION_PROGRAM_STATUS_ID = LOOKUP_ID)
  )
),
-- Return one row per person, per incarceration stint, when the person has been approved
-- or tentatively approved for drug transition release for that stint. Each row will contain
-- the date the person is planned to be released on DTP, if one exists, as well as the date
-- on which that release date was last updated in ACIS. 
dtp_eligibility_dates AS (
SELECT 
PERSON_ID,
  DOC_ID,
  FINAL_OFFENSE_ID,
  SC_EPISODE_ID,
  task_subtype,
  IF(drug_transition_release_eligibility_date BETWEEN '1900-01-01' AND '2100-01-01', drug_transition_release_eligibility_date, NULL) AS drug_transition_release_eligibility_date,
  update_datetime_elig,
  approval_status
  FROM (
    SELECT DISTINCT 
      CAST(COALESCE(
        NULLIF(DRUG_TRANSITION_PGM_RLS_DTM_ML, 'NULL'),
        NULLIF(DRUG_TRANSITION_PGM_RLS_DTM_ARD, 'NULL'), 
        NULLIF(DRUG_TRANSITION_PGM_RLS_DTM, 'NULL')) AS DATETIME) AS drug_transition_release_eligibility_date,
      CAST(COALESCE(
        NULLIF(off.UPDT_DTM_ML, 'NULL'), 
        NULLIF(off.UPDT_DTM_ARD, 'NULL'), 
        NULLIF(off.UPDT_DTM, 'NULL')) AS DATETIME) AS update_datetime_elig,
      ep.PERSON_ID,
      ep.DOC_ID,
      sc.FINAL_OFFENSE_ID,
      sc.SC_EPISODE_ID,
      lookups.DESCRIPTION AS approval_status,
      'Drug Transition Release' AS task_subtype
    FROM {AZ_DOC_SC_OFFENSE@ALL} off
    JOIN {AZ_DOC_SC_EPISODE} sc
    ON(sc.FINAL_OFFENSE_ID = off.OFFENSE_ID)
    JOIN {DOC_EPISODE} ep
    USING(DOC_ID) 
    LEFT JOIN {LOOKUPS} lookups
      ON(DRUG_TRANSITION_PROGRAM_STATUS_ID = LOOKUP_ID)
  )
),
-- TPR: There are a number of instances where an eligibility date is tracked in the system as 
-- having been updated 2 or 3 times at the exact same time. We deduplicate those rows
-- deterministically and maintain the latest eligibility date entered at the time.
dedup_tpr AS (
  SELECT DISTINCT
  PERSON_ID,
  DOC_ID,
  FINAL_OFFENSE_ID,
  SC_EPISODE_ID,
  task_subtype,
  MAX(transition_release_eligibility_date) OVER (PARTITION BY PERSON_ID, FINAL_OFFENSE_ID, update_datetime_elig) AS transition_release_eligibility_date,
  update_datetime_elig,
  -- If someone has multiple approval statuses with the exact same update_datetime, 
  -- prioritize the most decarceral one.
  FIRST_VALUE(approval_status) OVER (PARTITION BY PERSON_ID, FINAL_OFFENSE_ID, update_datetime_elig
  ORDER BY CASE 
    WHEN approval_status = 'Approved' THEN 1
    WHEN approval_status = 'Tentative' THEN 2
    WHEN approval_status = 'Denied' THEN 3
    ELSE NULL
  END) AS approval_status 
  FROM tpr_eligibility_dates
),
-- DTP: There are a number of instances where an eligibility date is tracked in the system as 
-- having been updated 2 or 3 times at the exact same time. We deduplicate those rows
-- deterministically and maintain the latest eligibility date entered at the time.
dedup_dtp AS (
  SELECT DISTINCT
  PERSON_ID,
  DOC_ID,
  FINAL_OFFENSE_ID,
  SC_EPISODE_ID,
  task_subtype,
  MAX(drug_transition_release_eligibility_date) OVER (PARTITION BY PERSON_ID, FINAL_OFFENSE_ID, update_datetime_elig) AS drug_transition_release_eligibility_date,
  update_datetime_elig,
  -- If someone has multiple approval statuses with the exact same update_datetime, 
  -- prioritize the most decarceral one.
  FIRST_VALUE(approval_status) OVER (PARTITION BY PERSON_ID, FINAL_OFFENSE_ID, update_datetime_elig
  ORDER BY CASE 
    WHEN approval_status = 'Approved' THEN 1
    WHEN approval_status = 'Tentative' THEN 2
    WHEN approval_status = 'Denied' THEN 3
    ELSE NULL
  END) AS approval_status
  FROM dtp_eligibility_dates
),
-- Maintain rows where the eligibility date for TPR changed; discard the rest. 
tpr_changed_rows AS (
SELECT * EXCEPT (prev_elig_date, prev_approval_status)
FROM (
  SELECT 
    *,
    LAG(transition_release_eligibility_date) OVER person_offense_window AS prev_elig_date,
    LAG(approval_status) OVER person_offense_window AS prev_approval_status
  FROM dedup_tpr 
  WINDOW person_offense_window AS (
      PARTITION BY PERSON_ID, FINAL_OFFENSE_ID 
      ORDER BY update_datetime_elig)
) sub
-- if eligibility date OR approval status changed, add a row to the ledger
WHERE 
  transition_release_eligibility_date IS DISTINCT FROM prev_elig_date
  OR approval_status IS DISTINCT FROM prev_approval_status
),
-- Maintain rows where the eligibility date for DTP changed; discard the rest. 
dtp_changed_rows AS (
SELECT * EXCEPT (prev_elig_date, prev_approval_status)
FROM (
  SELECT 
    *,
    LAG(drug_transition_release_eligibility_date) OVER person_offense_window AS prev_elig_date,
    LAG(approval_status) OVER person_offense_window AS prev_approval_status
  FROM dedup_dtp 
  WINDOW person_offense_window AS (
      PARTITION BY PERSON_ID, FINAL_OFFENSE_ID 
      ORDER BY update_datetime_elig)
) sub
-- if eligibility date OR approval status changed, add a row to the ledger
WHERE 
  drug_transition_release_eligibility_date IS DISTINCT FROM prev_elig_date
  OR approval_status IS DISTINCT FROM prev_approval_status
)

SELECT * FROM tpr_changed_rows
UNION ALL 
SELECT * FROM dtp_changed_rows
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_az",
    ingest_view_name="state_task_deadline_tpr_dtp",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
