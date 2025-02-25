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
  'Standard Transition Release' AS task_subtype
FROM {AZ_DOC_SC_OFFENSE@ALL} off
LEFT JOIN {AZ_DOC_SC_COMMITMENT}
USING(COMMITMENT_ID)
LEFT JOIN {AZ_DOC_SC_EPISODE} sc
USING(SC_EPISODE_ID)
LEFT JOIN {DOC_EPISODE} ep
USING(DOC_ID)
WHERE TRANSITION_PROGRAM_STATUS_ID IN (
  '10650', -- Approved
  '10652' -- Tentative
)
AND sc.FINAL_OFFENSE_ID = off.OFFENSE_ID
-- This is only true in 52 rows out of ~30k
AND PERSON_ID IS NOT NULL
),
-- Return one row per person, per incarceration stint, when the person has been approved
-- or tentatively approved for drug transition release for that stint. Each row will contain
-- the date the person is planned to be released on DTP, if one exists, as well as the date
-- on which that release date was last updated in ACIS. 
dtp_eligibility_dates AS (
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
  'Drug Transition Release' AS task_subtype
FROM {AZ_DOC_SC_OFFENSE@ALL} off
LEFT JOIN {AZ_DOC_SC_COMMITMENT} comm
USING(COMMITMENT_ID)
LEFT JOIN {AZ_DOC_SC_EPISODE} sc
ON(sc.SC_EPISODE_ID = comm.SC_EPISODE_ID
AND sc.FINAL_OFFENSE_ID = off.OFFENSE_ID)
LEFT JOIN {DOC_EPISODE} ep
USING(DOC_ID)
WHERE DRUG_TRANSITION_PROGRAM_STATUS_ID IN (
  '10650', -- Approved
  '10652' -- Tentative
)
-- This is only true in 52 rows out of ~30k
AND PERSON_ID IS NOT NULL
),
-- Maintain rows where the eligibility date for TPR changed; discard the rest. 
tpr_changed_rows AS (
-- 43,946 rows total
SELECT * EXCEPT (prev_elig_date)
FROM (
  SELECT 
    *,
    LAG(transition_release_eligibility_date) OVER (
      PARTITION BY PERSON_ID, DOC_ID 
      ORDER BY update_datetime_elig, transition_release_eligibility_date NULLS LAST) AS prev_elig_date
  FROM tpr_eligibility_dates 
) sub
-- if eligibility date changed, add a row to the ledger
WHERE 
  transition_release_eligibility_date != prev_elig_date
  OR (transition_release_eligibility_date IS NULL and prev_elig_date IS NOT NULL)
  OR (transition_release_eligibility_date IS NOT NULL and prev_elig_date IS NULL)
),
-- Maintain rows where the eligibility date for DTP changed; discard the rest. 
dtp_changed_rows AS (
-- 43,946 rows total
SELECT * EXCEPT (prev_elig_date)
FROM (
  SELECT 
    *,
    LAG(drug_transition_release_eligibility_date) OVER (
      PARTITION BY PERSON_ID, DOC_ID 
      ORDER BY update_datetime_elig, drug_transition_release_eligibility_date NULLS LAST) AS prev_elig_date
  FROM dtp_eligibility_dates 
) sub
-- if eligibility date changed, add a row to the ledger
WHERE 
  drug_transition_release_eligibility_date != prev_elig_date
  OR (drug_transition_release_eligibility_date IS NULL and prev_elig_date IS NOT NULL)
  OR (drug_transition_release_eligibility_date IS NOT NULL and prev_elig_date IS NULL)
)
-- There are a number of instances where an eligbility date is tracked in the system as 
-- having been updated 2 or 3 times at the exact same time. We deduplicate those rows
-- deterministically and maintain the earliest eligibility date entered at the time.
SELECT *
  FROM (
    SELECT * FROM tpr_changed_rows
    UNION ALL 
    SELECT * FROM dtp_changed_rows
  )
QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID, task_subtype, update_datetime_elig
    ORDER BY transition_release_eligibility_date NULLS LAST) = 1
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_az",
    ingest_view_name="state_task_deadline",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
