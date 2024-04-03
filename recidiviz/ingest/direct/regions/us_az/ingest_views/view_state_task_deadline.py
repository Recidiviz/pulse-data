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
WITH eligibility_dates AS (
SELECT DISTINCT 
  CAST(COALESCE(
    NULLIF(TRANSITION_PROGRAM_RLS_DTM_ML, 'NULL'),
    NULLIF(TRANSITION_PROGRAM_RLS_DTM_ARD, 'NULL'), 
    NULLIF(TRANSITION_PROGRAM_RLS_DTM, 'NULL')) AS DATETIME) AS transition_release_eligibility_date,
  CAST(COALESCE(
    NULLIF(off.UPDT_DTM_ML, 'NULL'), 
    NULLIF(off.UPDT_DTM_ARD, 'NULL'), 
    NULLIF(off.UPDT_DTM, 'NULL')) AS DATETIME) AS update_datetime_external,
  ep.PERSON_ID,
  ep.DOC_ID
FROM {AZ_DOC_SC_OFFENSE@ALL} off
LEFT JOIN {AZ_DOC_SC_COMMITMENT}
USING(COMMITMENT_ID)
LEFT JOIN {AZ_DOC_SC_EPISODE}
USING(SC_EPISODE_ID)
LEFT JOIN {DOC_EPISODE} ep
USING(DOC_ID)
WHERE TRANSITION_PROGRAM_STATUS_ID IN (
  '10650', -- Approved
  '10652' -- Tentative
)
-- This is only true in 52 rows out of ~30k
AND PERSON_ID IS NOT NULL
),
release_dates AS (
-- if there is a standard transition released documented for doc ID (incarceration stint)
-- in inmate movement history, then use that date as the due date. If there is not, use
-- the eligibility date as the due date, since it is the earliest date the person can 
-- be released on TPR.
-- NOTE: it is possible that they were approved or tentatively approved for TPR but never 
-- released on it. if we see many people past their due date but not incarcerated, that could be why.
SELECT DISTINCT
  EXTRACT(DATE FROM CAST(NULLIF(MOVEMENT_DATE, 'NULL') AS DATETIME)) AS transition_release_movement_date, 
  -- There was a system migration on 2019-11-30, so all rows with movements before that date have that date as their UPDT_DTM.
  LEAST(CAST(NULLIF(UPDT_DTM, 'NULL') AS DATETIME), CAST(NULLIF(MOVEMENT_DATE,'NULL') AS DATETIME)) AS update_datetime_external, 
  DOC_ID
FROM {AZ_DOC_INMATE_TRAFFIC_HISTORY}
WHERE MOVEMENT_CODE_ID = '71' -- Standard Transition Release
AND MOVEMENT_DATE IS NOT NULL
), 
elig_and_due_dates AS (
SELECT DISTINCT
  -- in some cases, a person was released on TPR before the eligibility date listed for their
  -- controlling offense. when this happens, update the eligibility date to be the date
  -- of release to reflect that the person must have been eligible when they were released.
    FIRST_VALUE(transition_release_eligibility_date) OVER (
        PARTITION BY PERSON_ID, DOC_ID, EXTRACT(DATE FROM LEAST(elig.update_datetime_external, rel.update_datetime_external)) 
        ORDER BY elig.update_datetime_external, rel.update_datetime_external, 
        -- very rarely, there are two dates entered with the same update_datetime_external. sort them deterministically
        transition_release_eligibility_date
    ) AS transition_release_eligibility_date,
    -- When there is more than one movement date associated with the same update_datetime_external, deterministically choose the earlier one.
    -- As of 3/28/24 this only happens one time.
    MIN(transition_release_movement_date) OVER (PARTITION BY PERSON_ID, DOC_ID, EXTRACT(DATE FROM LEAST(elig.update_datetime_external, rel.update_datetime_external)) 
        ORDER BY elig.update_datetime_external, rel.update_datetime_external, 
        -- very rarely, there are two dates entered with the same update_datetime_external. sort them deterministically
        transition_release_eligibility_date) AS actual_or_expected_release_date,
    EXTRACT (DATE FROM LEAST(elig.update_datetime_external, rel.update_datetime_external) ) AS update_datetime_external,
  PERSON_ID,
  DOC_ID
FROM eligibility_dates elig
LEFT JOIN release_dates rel
USING(DOC_ID)
)
SELECT DISTINCT
  -- ensure that due date is not before eligibility date
  LEAST(transition_release_eligibility_date, actual_or_expected_release_date) AS transition_release_eligibility_date, 
  actual_or_expected_release_date, 
  update_datetime_external, 
  PERSON_ID,
  DOC_ID,
  ROW_NUMBER() OVER (
    PARTITION BY PERSON_ID, DOC_ID
    ORDER BY update_datetime_external, transition_release_eligibility_date, 
    actual_or_expected_release_date) AS sequence_num
FROM elig_and_due_dates
WHERE update_datetime_external IS NOT NULL;
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_az",
    ingest_view_name="state_task_deadline",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
