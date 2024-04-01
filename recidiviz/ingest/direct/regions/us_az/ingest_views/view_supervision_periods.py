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
"""Query containing supervision period information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH critical_dates AS (
SELECT DISTINCT
  traffic.INMATE_TRAFFIC_HISTORY_ID,
  traffic.DOC_ID, 
  doc_ep.PERSON_ID, 
  -- Do not include timestamp because same-day movements are often logged out of order.
  CAST(CAST(REPLACE(MOVEMENT_DATE,'.0000000','') AS DATETIME) AS DATE) AS critical_date,
  MOVEMENT_CODE_ID, 
  COALESCE(traffic.MOVEMENT_REASON_ID,  traffic.INTERNAL_MOVE_REASON_ID) AS MOVEMENT_REASON_ID,
  LOCATOR_CODE_ID,
FROM {AZ_DOC_INMATE_TRAFFIC_HISTORY} traffic
LEFT JOIN {DOC_EPISODE} doc_ep
USING (DOC_ID)
WHERE traffic.MOVEMENT_DATE IS NOT NULL
),
base AS (
SELECT DISTINCT
  INMATE_TRAFFIC_HISTORY_ID,
  DOC_ID, 
  critical_dates.PERSON_ID, 
  critical_date,
  MOVEMENT_CODE_ID, 
  MOVEMENT_REASON_ID,
  LOCATOR_CODE_ID, 
FROM critical_dates
),
base_with_descriptions AS ( 
  SELECT * FROM (
  SELECT DISTINCT
    base.*,
    mvmt_codes.MOVEMENT_DESCRIPTION,
    custody_lookup.DESCRIPTION AS in_out_custody,
    action_lookup.DESCRIPTION AS period_action,
  FROM base 
  LEFT JOIN {AZ_DOC_MOVEMENT_CODES} mvmt_codes
  USING(MOVEMENT_CODE_ID) 
  -- This table and field establish what the right course of action is regarding the period as a 
  -- result of this movement (close, open, or reopen)
  LEFT JOIN {LOOKUPS} action_lookup
  ON(PRSN_CMM_SUPV_EPSD_LOGIC_ID = action_lookup.LOOKUP_ID)
  LEFT JOIN {LOOKUPS} custody_lookup
  ON (IN_OUT_CUSTODY = custody_lookup.LOOKUP_ID))
  -- only include periods related to supervision
  WHERE UPPER(period_action) LIKE '%SUPERVISION EPISODE%'
),
periods AS (
  SELECT 
    INMATE_TRAFFIC_HISTORY_ID,
    DOC_ID,
    PERSON_ID,
    in_out_custody AS adm_in_out_custody,
    critical_date AS admission_date,
    period_action AS adm_action,
    COALESCE(MOVEMENT_DESCRIPTION, 'UNKNOWN') AS admission_reason,
    LEAD(critical_date) OVER (person_period_window) AS release_date,
    LEAD(COALESCE(MOVEMENT_DESCRIPTION, 'UNKNOWN')) OVER (person_period_window) AS release_reason,
    LEAD(period_action) OVER (person_period_window) AS rel_action,
  FROM base_with_descriptions
  WINDOW person_period_window AS (PARTITION BY DOC_ID, PERSON_ID ORDER BY critical_date,
  -- deterministiscally sort redundant same-day movements
  INMATE_TRAFFIC_HISTORY_ID,
  COALESCE(MOVEMENT_DESCRIPTION, 'UNKNOWN')
  )
), carry_forward_IDs AS (
  -- when someone absconds in AZ, their supervision period is closed and the DPP_ID 
  -- associated with it is cleared from the system. Since we associate periods of 
  -- absconsion with the original supervision period the person is absconding from, 
  -- we want to keep that period open until the person is no longer on absconsion status.

  -- This CTE also filters out a number of periods we don't want:
  -- 1. periods with no IDs
  -- 2. periods where a client was in custody, except for those that we are assuming
  --    are investigative periods
  -- 3. periods that begin with a release from supervision, except those that mark periods
  --    of absconsion.
  -- There are many exclusions to account for various edge cases to the above three categories.
  SELECT
    DOC_ID,
    PERSON_ID,
    adm_in_out_custody,
    admission_date,
    adm_action,
    admission_reason,
    release_date,
    release_reason,
    rel_action,
  FROM periods
  WHERE (DOC_ID IS NOT NULL AND PERSON_ID IS NOT NULL)
  -- exclude periods that begin with a return to custody, unless it is of the form
  -- "temporary placement --> revocation"
  AND (UPPER(adm_in_out_custody) != "IN" OR (
    UPPER(ADMISSION_REASON) = 'TEMPORARY PLACEMENT'
    AND UPPER(REL_ACTION) = 'CLOSE SUPERVISION EPISODE & REOPEN LAST PRISON EPISODE'))
  -- these are periods that begin with a return to custody, but have data errors in the 
  -- associated in_out_custody field. Excludes a total of ~28,000 rows.
  AND UPPER(admission_reason) NOT LIKE "%IN CUSTODY%"
  AND UPPER(admission_reason) NOT LIKE "%REVOKED%"
  -- exclude periods that begin with a release from supervision
  AND UPPER(admission_reason) NOT LIKE "%END DATE%"
  AND UPPER(admission_reason) NOT LIKE "%END OF SUPERVISION%"
  -- exclude periods that begin with an end to a supervision period, unless it is of the form
  -- "earned release --> expiration"
  AND (NOT (UPPER(ADM_ACTION) LIKE "%CLOSE PRISON%" AND UPPER(ADM_ACTION) LIKE "%CLOSE SUPERVISION%")
    OR (UPPER(ADM_ACTION) LIKE "%CLOSE PRISON%" AND UPPER(ADM_ACTION) LIKE "%CLOSE SUPERVISION%"
       AND UPPER(RELEASE_REASON) IN ('OLD/NEW CODE - EXPIRATION', 'COMMUNITY SUPERVISION END DATE')))
  -- exclude periods that begin with a discharge from supervision, excluding periods of 
  -- absconsion that AZ considers closed but we want to track.
  AND NOT (UPPER(ADM_ACTION) = 'CLOSE SUPERVISION EPISODE' AND UPPER(ADMISSION_REASON) != 'RELEASEE ABSCOND')
  -- exclude periods that were activated in error. These are not formally closed, so lead to overlapping periods. 
  AND UPPER(admission_reason) NOT LIKE "%ACTIVATED IN ERROR%"
  -- exclude periods that begin with a person's death
  AND UPPER(admission_reason) != "DEATH"
  WINDOW person_period_window AS (PARTITION BY DOC_ID, PERSON_ID ORDER BY admission_date)
)
SELECT DISTINCT
  DOC_ID,
  PERSON_ID,
  admission_date,
  admission_reason,
  release_date,
  release_reason,
  ROW_NUMBER() OVER (PARTITION BY DOC_ID, PERSON_ID ORDER BY admission_date) as period_seq
FROM carry_forward_IDs
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_az",
    ingest_view_name="supervision_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
