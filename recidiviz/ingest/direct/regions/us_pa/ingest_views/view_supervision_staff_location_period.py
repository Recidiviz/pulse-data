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

"""Query containing supervision staff location period information.

Starts with the handmade roster and uses locations from there to capture most accurate 
info possible for current employees, then fills as many remaining officers' locations as 
possible using dbo_PRS_FACT_PAROLEE_CNTC_SMRY.PRL_AGNT_ORG_NAME
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
contacts_data AS (
    SELECT DISTINCT
        PRL_AGNT_EMPL_NO, 
        PRL_AGNT_ORG_NAME,
        CAST(CREATED_DATE AS DATETIME) AS CREATED_DATE,
        MAX(CAST(CREATED_DATE AS DATETIME)) OVER (PARTITION BY TRUE) AS last_file_update_datetime,
        MAX(CAST(CREATED_DATE AS DATETIME)) OVER (PARTITION BY PRL_AGNT_EMPL_NO) AS last_appearance_datetime,
        'CONTACTS' AS source,
        PERSON_ID
    FROM {dbo_PRS_FACT_PAROLEE_CNTC_SUMRY} contacts
    WHERE PRL_AGNT_ORG_NAME IS NOT NULL
), roster_data AS (
    SELECT 
        EmployeeID,
        Org_Name,
        DO_Orgcode,
        CAST(update_datetime AS DATETIME) AS update_datetime,
        MAX(CAST(update_datetime AS DATETIME)) OVER (PARTITION BY TRUE) AS last_file_update_datetime,
        MAX(CAST(update_datetime AS DATETIME)) OVER (PARTITION BY EmployeeID) AS last_appearance_datetime,
        'ROSTER' AS source,
        -- roster data will not need to be broken out by PERSON_ID, and if it does, we should assume that the roster data is more recent
        '9999999' AS PERSON_ID
    FROM {RECIDIVIZ_REFERENCE_staff_roster@ALL}
),
location_external_ids AS (
  SELECT DISTINCT UPPER(Org_Name) AS ORG_NAME, Org_cd 
  FROM {dbo_LU_PBPP_Organization}
  UNION ALL
  SELECT DISTINCT UPPER(ORG_NAME) AS ORG_NAME, Org_cd 
  FROM {RECIDIVIZ_REFERENCE_locations_from_supervision_contacts}
),
cleaned_data AS (
  SELECT DISTINCT
    *
  FROM contacts_data
  LEFT JOIN location_external_ids 
  ON (UPPER(location_external_ids.ORG_NAME) = UPPER(contacts_data.PRL_AGNT_ORG_NAME))

  UNION ALL 

  SELECT DISTINCT
    roster_data.* EXCEPT(DO_Orgcode),
    location_external_ids.Org_Name,
    COALESCE(Org_cd, DO_Orgcode) as Org_cd
  FROM roster_data
  LEFT JOIN location_external_ids 
  ON (UPPER(location_external_ids.ORG_NAME) = UPPER(roster_data.Org_name))
),
critical_dates AS (
  SELECT DISTINCT * FROM (
        SELECT DISTINCT
            PRL_AGNT_EMPL_NO, 
            Org_cd,
            CREATED_DATE,
            -- Add person_id to deterministically sort contacts that happened on the same day
            LAG(Org_cd) OVER (
                PARTITION BY PRL_AGNT_EMPL_NO
                ORDER BY CREATED_DATE, PERSON_ID, Org_cd) 
                AS prev_location,
            last_file_update_datetime,
            last_appearance_datetime,
            source
        FROM cleaned_data) cd
    WHERE
    -- officer just started working 
    (prev_location IS NULL AND Org_cd IS NOT NULL) 
    -- officer changed locations
    OR prev_location != Org_cd
    -- include most recent updates, even when neither of the previous conditions are true
    OR CREATED_DATE = last_file_update_datetime
),
all_periods AS (
SELECT DISTINCT
    PRL_AGNT_EMPL_NO,
    Org_cd,
    CREATED_DATE AS start_date,
    CASE 
        -- If a staff member stops appearing in the roster, close their employment period
        -- on the last date we receive a roster that included them
        WHEN LEAD(CREATED_DATE) OVER person_window IS NULL 
            AND CREATED_DATE < last_file_update_datetime
            AND source = 'ROSTER'
            THEN last_appearance_datetime 
        -- There is a more recent update to this person's location
        WHEN LEAD(CREATED_DATE) OVER person_window IS NOT NULL 
            THEN LEAD(CREATED_DATE) OVER person_window 
        -- All currently-employed staff will appear in the latest roster
        ELSE CAST(NULL AS DATETIME)
    END AS end_date
FROM critical_dates
WHERE Org_cd IS NOT NULL
WINDOW person_window AS (PARTITION BY PRL_AGNT_EMPL_NO ORDER BY CREATED_DATE,org_cd)
)
SELECT DISTINCT
    PRL_AGNT_EMPL_NO AS employee_id,
    Org_cd AS location_id,
    start_date,
    end_date,
    ROW_NUMBER() OVER (PARTITION BY PRL_AGNT_EMPL_NO ORDER BY start_date,org_cd) AS period_seq_num
FROM all_periods
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_pa",
    ingest_view_name="supervision_staff_location_period",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
