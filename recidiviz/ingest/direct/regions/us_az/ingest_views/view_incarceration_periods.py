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
WITH base AS (
SELECT DISTINCT
  traffic.INMATE_TRAFFIC_HISTORY_ID,
  traffic.DOC_ID, 
  ep.PERSON_ID, 
  -- Do not include timestamp because same-day movements are often logged out of order.
  CAST(CAST(MOVEMENT_DATE AS DATETIME) AS DATE) AS MOVEMENT_DATE,
  traffic.MOVEMENT_CODE_ID, 
  COALESCE(traffic.MOVEMENT_REASON_ID,  traffic.INTERNAL_MOVE_REASON_ID) AS MOVEMENT_REASON_ID,
  traffic.UNIT_ID, 
  traffic.PRISON_ID,
  traffic.LOCATOR_CODE_ID, 
  traffic.DESTINATION_COURT_ID, 
  traffic.DESTINATION_HOSPITAL_ID, 
FROM {AZ_DOC_INMATE_TRAFFIC_HISTORY} traffic
LEFT JOIN {DOC_EPISODE} ep
USING (DOC_ID)
WHERE traffic.MOVEMENT_DATE IS NOT NULL
),
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
),
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
),
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
locations_decoded AS (
  -- Looking up the unit name based on the UNIT_ID provided in the TRAFFIC table, where 
  -- the LOCATOR_CODE_ID is also provided, sometimes yields a description that
  -- conflicts with the description of the unit based on the locator code ID.
  -- Default to using the LOCATOR_CODE_ID for location rather than UNIT_ID, since 
  -- custody level is based on the LOCATOR_CODE_ID. Use UNIT_ID when there is LOCATOR_CODE_ID.
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
periods AS (
  SELECT 
    DOC_ID,
    PERSON_ID,
    MOVEMENT_DATE AS admission_date,
    MOVEMENT_DIRECTION AS admission_direction,
    period_action AS adm_action,
    COALESCE(MOVEMENT_DESCRIPTION, 'STATUS_CHANGE') AS admission_reason,
    LEAD(MOVEMENT_DATE) OVER (person_period_window) AS release_date,
    LEAD(MOVEMENT_DIRECTION) OVER(person_period_window) AS release_direction,
    LEAD(COALESCE(MOVEMENT_DESCRIPTION, 'STATUS_CHANGE')) OVER (person_period_window) AS release_reason,
    LEAD(period_action) OVER (person_period_window) AS rel_action,
    custody_level,
    facility,
    ORIGIN_LOC_DESC AS housing_unit,
    county_jail_location, 
    hospital_location, 
  FROM locations_decoded
  WINDOW person_period_window AS (PARTITION BY DOC_ID, PERSON_ID ORDER BY MOVEMENT_DATE, MOVEMENT_DIRECTION DESC, 
  -- deterministiscally sort redundant same-day movements
  COALESCE(MOVEMENT_DESCRIPTION, 'STATUS_CHANGE'),
  -- deterministically sort the 4 cases where someone appears to be in two locations at the exact same time
  ORIGIN_LOC_DESC, 
  -- a failsafe to sort deterministically when all specific test cases have been evaded 
  INMATE_TRAFFIC_HISTORY_ID
  )
), 
filter_periods AS (
  SELECT * FROM (
    SELECT 
      *,
      LAG(rel_action) OVER (person_period_window) AS prev_rel_action,
    FROM periods
    WHERE admission_direction != 'OUT'
    -- this only applies to ~100 rows from before 2000, and is assumed to be a data entry error.
    AND (DOC_ID IS NOT NULL AND PERSON_ID IS NOT NULL)
    -- exclude zero-day periods
    AND (CAST(admission_date AS DATE) != CAST(release_date AS DATE) 
    -- include open periods
    OR release_date IS NULL)
    WINDOW person_period_window AS (PARTITION BY DOC_ID, PERSON_ID ORDER BY admission_date)) sub
  -- exclude transitions that happen after one period is closed and before another period is open
  WHERE NOT (UPPER(prev_rel_action) LIKE '%CLOSE%' AND UPPER(adm_action) NOT LIKE '%OPEN%') 
  OR prev_rel_action IS NULL
)

SELECT 
  DOC_ID,
  PERSON_ID,
  admission_date,
  admission_reason,
  release_date,
  release_reason,
  custody_level,
  housing_unit,
  county_jail_location,
  hospital_location,
  facility,
  ROW_NUMBER() OVER(PARTITION BY DOC_ID, PERSON_ID ORDER BY admission_date) as period_seq
FROM filter_periods
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_az",
    ingest_view_name="incarceration_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="person_id,admission_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
