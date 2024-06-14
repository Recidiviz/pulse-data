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
"""Query containing incarceration period information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- This CTE results in one row per movement in the base movements table, with certain 
-- relevant fields attached, as well as the person's overall PERSON_ID.
base AS (
SELECT DISTINCT
  traffic.INMATE_TRAFFIC_HISTORY_ID,
  traffic.DOC_ID, 
  ep.PERSON_ID, 
  -- Do not include timestamp because same-day movements are often logged out of order.
  CAST(CAST(MOVEMENT_DATE AS DATETIME) AS DATE) AS MOVEMENT_DATE,
  traffic.MOVEMENT_CODE_ID, 
  traffic.UNIT_ID, 
  traffic.PRISON_ID,
  traffic.LOCATOR_CODE_ID, 
  traffic.DESTINATION_COURT_ID, 
  traffic.DESTINATION_HOSPITAL_ID, 
FROM {AZ_DOC_INMATE_TRAFFIC_HISTORY} traffic
LEFT JOIN {DOC_EPISODE} ep
USING (DOC_ID)
WHERE traffic.MOVEMENT_DATE IS NOT NULL
AND MOVEMENT_CODE_ID IS NOT NULL
),
-- This CTE results in one row per movement in the base movements table, with certain 
-- fields decoded using relevant lookup tables. The movements are filtered so that 
-- only those related to prison episodes are included.
base_with_descriptions AS ( 
  SELECT DISTINCT
    base.*,
    mvmt_codes.MOVEMENT_DESCRIPTION,
    action_lookup.DESCRIPTION AS period_action,
    prison_lookup.DESCRIPTION AS facility,
    unit_lookup.DESCRIPTION AS housing_unit_name,
    CASE 
      WHEN mvmt_codes.IN_OUT_CUSTODY = '5357' THEN 'IN'
      WHEN mvmt_codes.IN_OUT_CUSTODY = '5358' THEN 'OUT'
    END AS IN_OUT_CUSTODY
  FROM base 
  LEFT JOIN {AZ_DOC_MOVEMENT_CODES} mvmt_codes
  USING(MOVEMENT_CODE_ID) 
  LEFT JOIN {LOOKUPS} prison_lookup
  ON(PRISON_ID = prison_lookup.LOOKUP_ID)
  LEFT JOIN {LOOKUPS} unit_lookup
  ON(UNIT_ID = unit_lookup.LOOKUP_ID)
  LEFT JOIN LOOKUPS_generated_view action_lookup
  -- This field establishes what the right course of action is regarding the period as a 
  -- result of this movement (close, open, or reopen)
  ON(PRSN_CMM_SUPV_EPSD_LOGIC_ID = action_lookup.LOOKUP_ID)
  -- Only include rows related to prison episodes.
  WHERE UPPER(action_lookup.DESCRIPTION) LIKE "%PRISON%" 
  OR action_lookup.DESCRIPTION IS NULL 
  OR UPPER(action_lookup.DESCRIPTION) = 'NOT APPLICABLE'
),
-- This CTE results in one row per locator code from the AZ_DOC_LOCATOR_CODE reference file,
-- preprocessed so joining using a date field will yield the location information that was
-- accurate on that date.
locations_preprocessed AS (
  SELECT DISTINCT
    LOCATOR_CODE_ID,
    LOCATOR_NAME,
    CUSTODY_LEVEL_ID,
    CURRENT_USE_ID,
    ACTIVE,
    CASE WHEN 
      -- if this is the first entry for this code, null out the create datetime
      -- so that we use this entry for any instance of the code that appears before
      -- this decoder was created
      LAG(CREATE_DTM) OVER (PARTITION BY LOCATOR_CODE_ID ORDER BY CAST(CREATE_DTM AS DATETIME)) IS NULL
      THEN CAST(NULL AS DATETIME)
      ELSE CAST(CREATE_DTM AS DATETIME)
    END AS CREATE_DTM,
    LAG(CAST(CREATE_DTM AS DATETIME)) OVER (PARTITION BY LOCATOR_CODE_ID ORDER BY CAST(CREATE_DTM AS DATETIME)) AS PREV_CREATE_DTM,
    LEAD(CAST(CREATE_DTM AS DATETIME)) OVER (PARTITION BY LOCATOR_CODE_ID ORDER BY CAST(CREATE_DTM AS DATETIME)) AS NEXT_CREATE_DTM, 
    CAST(UPDT_DTM AS DATETIME) AS UPDT_DTM,
    FROM {AZ_DOC_LOCATOR_CODE}
),
-- This CTE joins the base CTE to the location CTE, resulting in one row per movement, 
-- with relevant locations attached.
base_with_locations AS (
  SELECT
    base_with_descriptions.*,
    -- If there is no location ID for the LOCATOR_CODE_ID, or there is no LOCATOR_CODE_ID, use the 
    -- UNIT_ID associated with this movement entry. 
    COALESCE(UPPER(loc_codes_in.LOCATOR_NAME), housing_unit_name) AS ORIGIN_LOC_DESC,
    loc_codes_in.CUSTODY_LEVEL_ID,
    loc_codes_in.CURRENT_USE_ID,
  FROM base_with_descriptions
  LEFT JOIN locations_preprocessed loc_codes_in
  ON(loc_codes_in.LOCATOR_CODE_ID = base_with_descriptions.LOCATOR_CODE_ID 
  -- The right row for this code is the one that was active at the time of the movement. 
  AND (MOVEMENT_DATE >= loc_codes_in.CREATE_DTM OR loc_codes_in.CREATE_DTM IS NULL)
  AND (MOVEMENT_DATE < loc_codes_in.UPDT_DTM OR loc_codes_in.ACTIVE='Y' 
  -- Case when there's only one entry
  OR (PREV_CREATE_DTM IS NULL AND NEXT_CREATE_DTM IS NULL)))
  WHERE MOVEMENT_DATE IS NOT NULL
  AND MOVEMENT_CODE_ID != 'NULL'
),
-- This CTE joins the custody level of a given location to the results of the base with 
-- locations CTE, resulting in one row per movement with location and custody level attached.
custody_level AS (
  SELECT 
    base_with_locations.*,
    CASE WHEN
      CUSTODY_LEVEL_ID IS NOT NULL THEN 
      CONCAT(
        custody_lookup.DESCRIPTION, '-',
        current_use_lookup.DESCRIPTION)
      ELSE CAST(NULL AS STRING)
    END AS CUSTODY_LEVEL,
  FROM base_with_locations 
  LEFT JOIN {LOOKUPS} custody_lookup 
  ON(CUSTODY_LEVEL_ID = custody_lookup.LOOKUP_ID
  AND custody_lookup.LOOKUP_CATEGORY='CU_LEVEL')
  LEFT JOIN {LOOKUPS} current_use_lookup 
  ON(CURRENT_USE_ID = current_use_lookup.LOOKUP_ID
  AND current_use_lookup.LOOKUP_CATEGORY = 'CURRENT_USE')
),
-- This CTE is similar to base_with_descriptions in that it produces the same results as
-- the previous CTE, but with key location information decoded using relevant lookup tables.

-- Looking up the unit name based on the UNIT_ID provided in the TRAFFIC table, where 
-- the LOCATOR_CODE_ID is also provided, sometimes yields a description that
-- conflicts with the description of the unit based on the locator code ID.
-- Default to using the LOCATOR_CODE_ID for location rather than UNIT_ID, since 
-- custody level is based on the LOCATOR_CODE_ID. Use UNIT_ID when there is LOCATOR_CODE_ID.
locations_decoded AS (
  SELECT 
    INMATE_TRAFFIC_HISTORY_ID,
    DOC_ID,
    PERSON_ID,
    MOVEMENT_DATE,
    COALESCE(IN_OUT_CUSTODY,'INTERNAL') AS MOVEMENT_DIRECTION,
    MOVEMENT_DESCRIPTION,
    period_action,
    ORIGIN_LOC_DESC,
    facility,
    custody_level.CUSTODY_LEVEL,
    TRIM(jail_lookup.DESCRIPTION, 'Court in ') AS county_jail_location, 
    TRIM(hospital_lookup.DESCRIPTION, 'Court in ') AS hospital_location,
  FROM custody_level
  LEFT JOIN {LOOKUPS} jail_lookup
  ON(DESTINATION_COURT_ID = jail_lookup.LOOKUP_ID)
  LEFT JOIN {LOOKUPS} hospital_lookup
  ON(DESTINATION_HOSPITAL_ID = hospital_lookup.LOOKUP_ID)
),
-- This CTE results in one row per person, incarceration stint, and set of key characteristics
-- including location and custody level. Any time one of those characteristics changes, 
-- the previous period is closed and a new one is opened, with the reasons for the change 
-- documented in "admission_reason" and "release_reason" fields.
periods AS (
SELECT 
  PERSON_ID,
  DOC_ID,
  MOVEMENT_DATE AS admission_date,
  MOVEMENT_DESCRIPTION AS admission_reason,
  LEAD(MOVEMENT_DATE) OVER person_window AS release_date,
  LEAD(MOVEMENT_DESCRIPTION) OVER person_window AS release_reason,
  PERIOD_ACTION AS adm_action,
  LEAD(PERIOD_ACTION) OVER person_window AS rel_action,
  MOVEMENT_DIRECTION,
  LEAD(MOVEMENT_DIRECTION) OVER person_window AS next_mvmt_direction,
  LAG(MOVEMENT_DIRECTION) OVER person_window AS prev_mvmt_direction,
  facility,
  ORIGIN_LOC_DESC AS housing_unit,
  custody_level,
  county_jail_location,
  hospital_location,
  action_ranking
FROM (
SELECT
  *,
  CASE WHEN 
    UPPER(PERIOD_ACTION) LIKE "%OPEN%" OR UPPER(MOVEMENT_DESCRIPTION) LIKE "%CREATE%" THEN 1
    WHEN UPPER(PERIOD_ACTION) LIKE "%NOT APPLICABLE%" OR MOVEMENT_DESCRIPTION IS NULL THEN 2
    WHEN UPPER(PERIOD_ACTION) LIKE "%CLOSE PRISON%" THEN 3 
  END AS action_ranking
FROM locations_decoded 
)
WINDOW person_window AS (
  PARTITION BY PERSON_ID, DOC_ID 
  ORDER BY MOVEMENT_DATE, action_ranking, MOVEMENT_DIRECTION DESC, MOVEMENT_DESCRIPTION DESC, 
  -- deterministically sort movements on days when people are logged as being in many
  -- places at once.
  ORIGIN_LOC_DESC, facility, hospital_location, county_jail_location)
)

SELECT 
  PERSON_ID,
  DOC_ID,
  admission_date,
  admission_reason,
  release_date,
  release_reason,
  facility,
  housing_unit,
  custody_level,
  county_jail_location,
  hospital_location,
   ROW_NUMBER() OVER (
    PARTITION BY PERSON_ID, DOC_ID 
    ORDER BY admission_date, action_ranking, MOVEMENT_DIRECTION DESC) AS period_seq
FROM periods
WHERE DOC_ID IS NOT NULL AND PERSON_ID IS NOT NULL
-- only keep zero-day periods if they are overall period admissions or releases, to preserve
-- admission and release reasons
AND (admission_date != release_date 
  OR (admission_date=release_date AND (UPPER(adm_action) LIKE "%CREATE%" OR UPPER(adm_action) LIKE "%OPEN%"))
  OR (admission_date=release_date AND (UPPER(rel_action) LIKE "%CLOSE PRISON%"))
  OR release_date IS NULL)
AND (MOVEMENT_DIRECTION != 'OUT' 
-- These are internal movements (transfers, out to hospital, etc)
OR (MOVEMENT_DIRECTION = 'OUT' AND adm_action = 'Not Applicable'))
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_az",
    ingest_view_name="incarceration_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
