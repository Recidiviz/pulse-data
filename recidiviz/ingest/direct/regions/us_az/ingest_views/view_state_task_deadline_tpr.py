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
or tentatively approved for the Transition Release Program, or people who have been 
released on the Transition Release Program in the past.

The eligible date field contains the date on which they became eligible for transition
release. The due date field contains the date they were actually released, if they 
have been released, or the date they are eligible for transition release if they are
currently incarcerated.
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
eligibility_dates AS (
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
  ep.DOC_ID
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
-- Return one row per person, per incarceration stint, where a person has already been
-- released on a standard transition release. Each row will contain the date on which 
-- the person was released, and the best approximation of the date the movement data was
-- last updated.
release_dates AS (
SELECT DISTINCT
  EXTRACT(DATE FROM CAST(NULLIF(MOVEMENT_DATE, 'NULL') AS DATETIME)) AS transition_release_movement_date, 
  -- There was a system migration on 2019-11-30, so all rows with movements before that date have that date as their UPDT_DTM.
  LEAST(CAST(NULLIF(UPDT_DTM, 'NULL') AS DATETIME), CAST(NULLIF(MOVEMENT_DATE,'NULL') AS DATETIME)) AS update_datetime_release, 
  DOC_ID
FROM {AZ_DOC_INMATE_TRAFFIC_HISTORY}
WHERE MOVEMENT_CODE_ID = '71' -- Standard Transition Release
AND MOVEMENT_DATE IS NOT NULL
), 
-- Return one row per person, incarceration stint, and combination of planned release date 
-- ("eligibility date"), actual release date, and the dates that each of those values were 
-- updated.
elig_and_due_dates AS (
SELECT DISTINCT
  FIRST_VALUE(transition_release_eligibility_date) OVER (
      PARTITION BY PERSON_ID, DOC_ID, update_datetime_elig
      ORDER BY update_datetime_elig,
      -- very rarely, there are two dates entered with the same update_datetime_elig. sort them deterministically
      transition_release_eligibility_date)
  AS transition_release_eligibility_date,
  MIN(transition_release_movement_date) OVER (PARTITION BY PERSON_ID, DOC_ID, update_datetime_release
      ORDER BY update_datetime_release, 
      -- very rarely, there are two dates entered with the same update_datetime_external. sort them deterministically
      transition_release_movement_date) AS actual_release_date,
  update_datetime_elig,
  update_datetime_release,
  PERSON_ID,
  DOC_ID
FROM eligibility_dates elig
LEFT JOIN release_dates rel
USING(DOC_ID)
), 
-- Return the same as above, but filtered only to include rows where the eligibility date
-- or actual release date changed from the previous row. Replace eligibility dates that fall after 
-- their respective due dates with '1000-01-01'. Use the actual release date as the due
-- date, if one exists. If the person does not have a release date, use the eligibility date 
-- as the due date.
filter_to_changed_dates AS (
SELECT DISTINCT 
  *,
  LAG(transition_release_eligibility_date) OVER (
    PARTITION BY PERSON_ID, DOC_ID 
    ORDER BY update_datetime_elig, update_datetime_release) AS prev_elig_date,
  LAG(actual_or_expected_release_date) OVER (
    PARTITION BY PERSON_ID, DOC_ID 
    ORDER BY update_datetime_release, update_datetime_elig) AS prev_due_date,
FROM (
SELECT DISTINCT
  -- it is useful for analysis to know that the eligibility date was later, but ingest 
  -- does not allow that reverse-ordering.
  IF(transition_release_eligibility_date > actual_release_date, 
    CAST('1000-01-01' AS DATE), transition_release_eligibility_date)
  AS transition_release_eligibility_date, 
  -- if no release date already passed, use eligibility date,
  -- since it is the earliest date the person can be released on TPR.
  IF (actual_release_date IS NULL, 
    transition_release_eligibility_date, actual_release_date) 
  AS actual_or_expected_release_date, 
  update_datetime_elig,
  update_datetime_release, 
  PERSON_ID,
  DOC_ID
FROM elig_and_due_dates)
), 
-- Return one row per combination of person, incarceration stint, eligibility date, 
-- actual release date, and the date on which the field that most recently changed (eligibility
-- date or release date) was updated. Rank the rows to be able to deduplicate the singular
-- occurrence of two release dates appearing in the system at the exact same time.
final_dedup AS (
SELECT DISTINCT
*, 
-- in the singular case where one person has two rows with the same update_datetime_external, 
-- deterministically choose one to keep
ROW_NUMBER() OVER (PARTITION BY PERSON_ID, update_datetime_external 
  ORDER BY transition_release_eligibility_date, actual_or_expected_release_date) AS rn
FROM (
    SELECT DISTINCT
    transition_release_eligibility_date,
    actual_or_expected_release_date,
    CASE WHEN
    -- if eligibility date AND due date changed, track the earliest update_datetime
        (transition_release_eligibility_date != prev_elig_date
        OR (transition_release_eligibility_date IS NULL and prev_elig_date IS NOT NULL)
        OR (transition_release_eligibility_date IS NOT NULL and prev_elig_date IS NULL))
        AND (actual_or_expected_release_date != prev_due_date
        OR (actual_or_expected_release_date IS NULL and prev_due_date IS NOT NULL)
        OR (actual_or_expected_release_date IS NOT NULL and prev_due_date IS NULL))
        THEN LEAST(update_datetime_elig,update_datetime_release)
    -- if eligibility date changed, track that update_datetime
        WHEN (transition_release_eligibility_date != prev_elig_date
        OR (transition_release_eligibility_date IS NULL and prev_elig_date IS NOT NULL)
        OR (transition_release_eligibility_date IS NOT NULL and prev_elig_date IS NULL))
        THEN update_datetime_elig
    -- actual or planned release date changed, track that update_datetime
        WHEN  (actual_or_expected_release_date != prev_due_date
        OR (actual_or_expected_release_date IS NULL and prev_due_date IS NOT NULL)
        OR (actual_or_expected_release_date IS NOT NULL and prev_due_date IS NULL))
        then update_datetime_release
    END AS update_datetime_external,
    PERSON_ID,
    DOC_ID,
    FROM filter_to_changed_dates
    -- filter to only include rows where some date changed
    WHERE (transition_release_eligibility_date != prev_elig_date
    OR (transition_release_eligibility_date IS NULL and prev_elig_date IS NOT NULL)
    OR (transition_release_eligibility_date IS NOT NULL and prev_elig_date IS NULL))
    OR (actual_or_expected_release_date != prev_due_date
    OR (actual_or_expected_release_date IS NULL and prev_due_date IS NOT NULL)
    OR (actual_or_expected_release_date IS NOT NULL and prev_due_date IS NULL)))
WHERE update_datetime_external IS NOT NULL)

SELECT 
  * EXCEPT(rn)
FROM final_dedup 
WHERE rn = 1
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_az",
    ingest_view_name="state_task_deadline_tpr",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
