# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Query containing staff role/location period information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- First take the role and location periods for each staff; in the next step, we'll filter
-- these periods to only keep periods for staff with known staff IDs or staff-type roles.
role_loc_pds AS (
SELECT DISTINCT
  PARTYID,
  PARTYRELSTART,
  NULLIF(PARTYRELEND, '9999-12-31 00:00:00') AS PARTYRELEND,
  WORKASSIGNMENT,
  PRIMARYSITEIND,
  RELATEDPARTYID,
  PARTYRELTYPE,
  RANK() OVER (
    PARTITION BY 
      PARTYID,
      PARTYRELSTART,
      PARTYRELEND,
      WORKASSIGNMENT
    ORDER BY 
      CASE PRIMARYSITEIND WHEN 'Y' THEN 0 WHEN 'N' THEN 1 ELSE 2 END,
      RELATEDPARTYID) AS loc_seq,
  RANK() OVER (
    PARTITION BY PARTYID
    ORDER BY 
      PARTYRELSTART,
      PARTYRELEND,
      WORKASSIGNMENT) AS role_seq
FROM (
  SELECT 
    rp.PARTYID,
    rp.PARTYRELSTART,
    rp.PARTYRELEND,
    rp.WORKASSIGNMENT,
    rp.PRIMARYSITEIND,
    pp.PARTYTYPE,
    pp2.PARTYTYPE AS RELATEDPARTYTYPE,
    rp.RELATEDPARTYID,
    rp.PARTYRELTYPE
  FROM {RELATEDPARTY} rp 
  LEFT JOIN {PARTYPROFILE} pp USING(PARTYID)
  LEFT JOIN {PARTYPROFILE} pp2 ON rp.RELATEDPARTYID = pp2.PARTYID
) both_party_details
WHERE PARTYRELSTART != '1000-01-01 00:00:00' 
-- This is a magic date used in non-nullable date columns in AR which indicates missing
-- data. We filter on this date in order to avoid ingesting periods without start dates.
  AND PARTYTYPE = '1' 
  AND RELATEDPARTYTYPE = '2' 
  AND PARTYID IN (
    SELECT DISTINCT PARTYID
    FROM {PERSONPROFILE}
  )
)
SELECT * EXCEPT(PARTYRELTYPE)
FROM role_loc_pds
QUALIFY LOGICAL_OR(
  PARTYRELTYPE IN (
      '1AA', -- ADC Employee
      '1AB', -- ADC Employee/Extra Help
      '1AC', -- ACSD CTE
      '1AE', -- DOC Employee
      '1AI', -- IFI
      '1AP', -- PIE
      '1BB', -- Board of Corrections
      '1CA', -- Private Prison Staff	
      '2AA', -- ACC Employee
      '2BB', -- Parole Board
      '3BO', -- City Jail Employee
      '3BP', -- Police Department Employee
      '3BS', -- Sheriff Department Employee
      '3BT' -- County Jail Employee
    ) OR
    PARTYID IN (
      SELECT DISTINCT PPOFFICERID
      FROM {SUPERVISIONEVENT}
      UNION DISTINCT
      SELECT WORKASSIGNMENTAUTHBY
      FROM {JOBPROGRAMASGMT}
      UNION DISTINCT
      SELECT STAFFIDPERFASSESS
      FROM {RISKNEEDCLASS}
    )
  ) OVER (PARTITION BY PARTYID)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ar",
    ingest_view_name="staff_role_location_period",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
