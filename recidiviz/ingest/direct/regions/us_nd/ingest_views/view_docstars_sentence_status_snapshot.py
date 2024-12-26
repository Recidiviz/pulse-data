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
"""Query containing sentence status snapshots for sentences stored in Docstars."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH
-- This CTE cleans the raw data needed for this view. There are a very small number of 
-- rows where the termination or revocation date listed is in the future; this clears
-- those out so that they do not cause an error in ingest, since they do not seem to 
-- represent an actual revocation or case termination that happened in the past. 
docstars_offendercasestable_cleaned AS (
  SELECT 
    oct_all.SID,
    oct_all.CASE_NUMBER,
    -- If a case was sentenced out of state, we do not know the cardinality of court 
    -- cases to charges, and we cannot guarantee that it aligns with that of ND, which this
    -- view is build to accommodate. This is a component of some entity external IDs, but 
    -- is never used as a unique identifier, so these sentences will still be uniquely
    -- identifiable. They will also still end up in the appropriate inferred sentence groups
    -- by virtue of sharing charges and offense dates.
    IF((ot.COUNTY IS NOT NULL AND CAST(ot.COUNTY AS INT64) > 100), NULL, ot.COURT_NUMBER) AS COURT_NUMBER,
    oct_all.DESCRIPTION,
    oct_all.TA_TYPE,
    IF(DATE(oct_all.TERM_DATE) > @update_timestamp, NULL, CAST(oct_all.TERM_DATE AS DATETIME)) AS TERM_DATE,
    IF(DATE(oct_all.REV_DATE) > @update_timestamp, NULL, CAST(oct_all.REV_DATE AS DATETIME)) AS REV_DATE,
    CAST(oct_all.RecDate AS DATETIME) AS RecDate,
  FROM {docstars_offendercasestable@ALL} oct_all
  -- There are four cases that appear in the @ALL version of this table but not the _latest
  -- version, so they do not have associated sentences ingested. This is a side effect
  -- of manual raw data pruning and will be resolved once automatic raw data pruning is 
  -- deployed.
  JOIN {docstars_offendercasestable} oct
  USING(CASE_NUMBER)
  JOIN {docstars_offensestable} ot
  USING(CASE_NUMBER)
  -- We do not have any charges ingested with a CONVICTED status for sentences that are 
  -- still Pre-Trial, so we do not want to include them here.
  WHERE oct.DESCRIPTION != 'Pre-Trial'
),
-- This CTE pulls only the necessary raw data fields from the docstars_offendercasestable.
-- The TERM_DATE and REV_DATE fields, in conjunction with TA_TYPE, allow us to determine
-- what the status of a sentence is. This CTE stores the previous values of those fields
-- in each row so we can track when they change.
all_entries AS (
  SELECT
    SID,
    CASE_NUMBER,
    -- Populate NULL COURT_NUMBER values with the string 'NULL' so they are usable in 
    -- sorts and partitions in this view.
    IFNULL(COURT_NUMBER, 'NULL') AS COURT_NUMBER,
    DESCRIPTION,
    TA_TYPE,
    TERM_DATE,
    LAG(TERM_DATE) OVER (PARTITION BY SID, CASE_NUMBER, IFNULL(COURT_NUMBER, 'NULL') ORDER BY REV_DATE, TERM_DATE, RecDate) AS PREV_TERM_DATE,
    REV_DATE,
    LAG(REV_DATE) OVER (PARTITION BY SID, CASE_NUMBER, IFNULL(COURT_NUMBER, 'NULL') ORDER BY REV_DATE, TERM_DATE, RecDate) AS PREV_REV_DATE,
    RecDate,
  FROM docstars_offendercasestable_cleaned
  ),
-- This CTE collects and infers statuses based on the TA_TYPE field and the REV_DATE and 
-- TERM_DATE fields in the raw data.  
dates_and_statuses AS (
SELECT
  SID,
  CASE_NUMBER, 
  COURT_NUMBER,
  CASE 
   -- Store the date that a revocation occurred when the status is changing to REVOKED.
    WHEN PREV_REV_DATE IS NULL AND REV_DATE IS NOT NULL THEN REV_DATE
    -- Store the date that a case was terminated when there is a non-revocation termination.
    WHEN PREV_TERM_DATE IS NULL AND TERM_DATE IS NOT NULL THEN TERM_DATE 
    -- Otherwise, store the date that the row appeared or was updated
    ELSE RecDate
  END AS STATUS_UPDATE_DATETIME,
  CASE 
   -- The termination reason (TA_TYPE) can sometimes indicate a revocation when there is no REV_DATE. 
   -- This seems to be the result of inconsistent data entry practices. We use an 'OR' operator
   -- here to capture all revocations, whether they are indicated by date or by termination reason.
    WHEN REV_DATE IS NOT NULL OR TA_TYPE IN ('9', '10') THEN 'REVOKED'
    -- This TA_TYPE indicates that someone has absconded, but has not yet been revoked.
    -- The TERM_DATE field is filled in with the date their absconsion was made official. 
    -- This is almost always followed by a TA_TYPE = '9' to indicate they were revoked as a result
    -- of the absconsion.
    WHEN REV_DATE IS NULL AND TERM_DATE IS NOT NULL AND TA_TYPE = '13' THEN 'SUSPENDED'
    WHEN REV_DATE IS NULL AND TERM_DATE IS NOT NULL AND (TA_TYPE != '13' OR TA_TYPE IS NULL) THEN 'COMPLETED'
    WHEN REV_DATE IS NULL AND TERM_DATE IS NULL AND TA_TYPE IS NULL AND UPPER(DESCRIPTION) = 'PRE-TRIAL' THEN 'PENDING'
    WHEN REV_DATE IS NULL AND TERM_DATE IS NULL AND TA_TYPE IS NULL AND UPPER(DESCRIPTION) IN ('SUSPENDED', 'DEFERRED') THEN 'SUSPENDED'
    WHEN REV_DATE IS NULL AND TERM_DATE IS NULL AND TA_TYPE IS NULL AND UPPER(DESCRIPTION) NOT IN ('PRE-TRIAL','DEFERRED','SUSPENDED') THEN 'SERVING'
    -- This is included to map to PRESENT_WITHOUT_INFO, so that we can track how many 
    -- updates are not covered by our logic over time and judge when an update may be 
    -- necessary. It is a protective measure and currently does not appear in the ingest 
    -- view results.
    ELSE 'NO STATUS'
  END AS STATUS
FROM all_entries),
-- There are 14 cases as of 12/11/2024 that have data entry errors that suggest a person
-- was revoked or absconded after they were released from supervision. There is also
-- one case where a person is listed as being revoked on two subsequent days. 
-- We cannot correct these without knowing the true sequence of events, so we collect the earliest noted
-- case completion or revocation date for each case here to exclude those rogue updates in the final 
-- subquery.
completion_or_revocation_dates AS (
  SELECT 
    SID, 
    CASE_NUMBER, 
    COURT_NUMBER,
    MIN(status_update_datetime) AS min_completion_or_revocation_date,
  FROM dates_and_statuses
  WHERE STATUS IN ('COMPLETED', 'REVOKED')
  GROUP BY 1,2,3
)
SELECT DISTINCT
  SID,
  CASE_NUMBER,
  -- Set NULL COURT_NUMBER values to actual NULLs instead of placeholders used for sorting.
  -- This will enture that sentence external IDs match across views. 
  IF(COURT_NUMBER = 'NULL', CAST(NULL AS STRING), COURT_NUMBER) AS COURT_NUMBER,
  STATUS_UPDATE_DATETIME,
  STATUS,
FROM dates_and_statuses
LEFT JOIN completion_or_revocation_dates
USING(SID, CASE_NUMBER, COURT_NUMBER)
WHERE STATUS_UPDATE_DATETIME <= min_completion_or_revocation_date
OR min_completion_or_revocation_date IS NULL
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_nd",
    ingest_view_name="docstars_sentence_status_snapshot",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
