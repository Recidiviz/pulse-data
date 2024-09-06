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
-- Create a list of dates that movements occurred and the reasons for those movements, as well as 
-- the locations associated with those movements.
get_dates_from_movements AS (
SELECT DISTINCT
  traffic.DOC_ID, 
  ep.PERSON_ID, 
  -- Do not include timestamp because same-day movements are often logged out of order.
  CAST(CAST(MOVEMENT_DATE AS DATETIME) AS DATE) AS MOVEMENT_DATE,
  CAST(NULL AS DATE) AS CLASSIFICATION_DATE,
  -- include the incarceration period logic as a prefix for the movement for filtering later on, since there are only 4 options
  CONCAT(IFNULL(UPPER(REPLACE(action_lookup.OTHER, '-','')),'NULL'), '-', mvmt_codes.MOVEMENT_DESCRIPTION) AS MOVEMENT_DESCRIPTION,
  CAST(NULL AS STRING) AS CLASSIFICATION_CUSTODY_LEVEL,
  CASE WHEN
      traffic.LOCATOR_CODE_ID IS NOT NULL THEN 
      CONCAT(
        IFNULL(custody_level_lookup.DESCRIPTION, 'None'), '-',
        IFNULL(current_use_lookup.DESCRIPTION, 'None'))
      ELSE CAST(NULL AS STRING)
  END AS HOUSING_UNIT_DETAIL,
  -- If there is no location ID for the LOCATOR_CODE_ID, or there is no LOCATOR_CODE_ID, use the 
  -- UNIT_ID associated with this movement entry. 
  -- If a row has a populated hospital or county jail field, assume that is where the person is even if they are also assigned a location in a more permanent facility.
  UPPER(REPLACE(jail_lookup.DESCRIPTION, 'Court in ', '')) AS jail_location, 
-- For some reason hospital locations sometimes say "Court in XX County"
  UPPER(REPLACE(REPLACE(hospital_lookup.DESCRIPTION, 'Court in ', ''), 'Hospital in ', '')) AS hospital_location,
  traffic.LOCATOR_CODE_ID, 
  traffic.UNIT_ID, 
  traffic.PRISON_ID,
FROM {AZ_DOC_INMATE_TRAFFIC_HISTORY} traffic
LEFT JOIN {DOC_EPISODE} ep
  USING (DOC_ID)
LEFT JOIN {AZ_DOC_MOVEMENT_CODES} mvmt_codes
  USING(MOVEMENT_CODE_ID) 
LEFT JOIN {LOOKUPS} prison_lookup
  ON(PRISON_ID = prison_lookup.LOOKUP_ID)
LEFT JOIN {LOOKUPS} unit_lookup
  ON(UNIT_ID = unit_lookup.LOOKUP_ID)
LEFT JOIN {LOOKUPS} action_lookup
-- This field establishes what the right course of action is regarding the period as a 
-- result of this movement (close, open, or reopen)
  ON(PRSN_CMM_SUPV_EPSD_LOGIC_ID = action_lookup.LOOKUP_ID)
LEFT JOIN {LOOKUPS} jail_lookup
  ON(DESTINATION_COURT_ID = jail_lookup.LOOKUP_ID)
LEFT JOIN {LOOKUPS} hospital_lookup
  ON(DESTINATION_HOSPITAL_ID = hospital_lookup.LOOKUP_ID)
-- Only include rows related to prison episodes.
LEFT JOIN {AZ_DOC_LOCATOR_CODE} locator
  ON(traffic.LOCATOR_CODE_ID = locator.LOCATOR_CODE_ID AND locator.ACTIVE = 'Y')
LEFT JOIN {LOOKUPS} custody_level_lookup
  ON(locator.CUSTODY_LEVEL_ID = custody_level_lookup.LOOKUP_ID)
LEFT JOIN {LOOKUPS} current_use_lookup
  ON(locator.CURRENT_USE_ID = current_use_lookup.LOOKUP_ID)
WHERE (traffic.MOVEMENT_DATE IS NOT NULL
  AND NULLIF(MOVEMENT_CODE_ID, 'NULL') IS NOT NULL)
  AND (UPPER(action_lookup.DESCRIPTION) LIKE "%PRISON%" 
  OR action_lookup.DESCRIPTION IS NULL 
  OR UPPER(action_lookup.DESCRIPTION) = 'NOT APPLICABLE'
  -- These releases only have supervision-related actions, but are releases from incarceration,
  -- so we have to include them specifically.
  OR UPPER(MOVEMENT_DESCRIPTION) = 'RELEASE ISC')
),
-- A helper CTE for excluding erroneous classifications later on.
-- Collects the most admission & recent release date associated with a given person & incarceration stint,
-- and creates a flag to indicate whether they were reincarcerated after their most
-- recent release.
key_dates_and_status AS (
  SELECT 
    PERSON_ID,
    DOC_ID,
    LATEST_INTAKE_DATE,
    MAX(MOVEMENT_DATE) OVER (PARTITION BY PERSON_ID, DOC_ID) AS LATEST_RELEASE_DATE,
    (LATEST_INTAKE_DATE > MAX(MOVEMENT_DATE) OVER (PARTITION BY PERSON_ID, DOC_ID)) AS returned_after_release
  FROM (
  SELECT DISTINCT
    PERSON_ID,
    DOC_ID,
    MAX(MOVEMENT_DATE) OVER (PARTITION BY PERSON_ID, DOC_ID) AS LATEST_INTAKE_DATE,
  FROM get_dates_from_movements
  WHERE MOVEMENT_DESCRIPTION LIKE "CREATE-%" 
  OR MOVEMENT_DESCRIPTION LIKE "REOPEN-%"
  )
  -- left join because I want all of the admissions to be included, even if there are no releases
  LEFT JOIN (
    SELECT * 
    FROM get_dates_from_movements
  WHERE MOVEMENT_DESCRIPTION LIKE "CLOSE-%" 
  OR MOVEMENT_DESCRIPTION = 'NULL-Release ISC'
  )
  USING(PERSON_ID, DOC_ID)
),
-- Get dates that classification assessments were performed and the final custody
-- levels assigned as a result of each. 
get_classification_dates AS (
SELECT DISTINCT 
  class.DOC_ID,
  ep.PERSON_ID,
  CAST(NULL AS DATE) AS MOVEMENT_DATE,
  CAST(START_DTM AS DATETIME) AS CLASSIFICATION_DATE,
  'CLASSIFICATION' AS MOVEMENT_DESCRIPTION,
  custody_lookup.DESCRIPTION AS CLASSIFICATION_CUSTODY_LEVEL, 
  CAST(NULL AS STRING) AS HOUSING_UNIT_DETAIL,
  CAST(NULL AS STRING) AS jail_location,
  CAST(NULL AS STRING) AS hospital_location,
  CAST(NULL AS STRING) AS LOCATOR_CODE_ID,
  CAST(NULL AS STRING) AS UNIT_ID,
  CAST(NULL AS STRING) AS PRISON_ID,
FROM {DOC_CLASSIFICATION} class
LEFT JOIN {LOOKUPS} custody_lookup
ON(CUSTODY_DISCIPLINE_LEVEL_ID = custody_lookup.LOOKUP_ID)
LEFT JOIN {DOC_EPISODE} ep
USING (DOC_ID)
WHERE STATUS_ID = '1486' -- Finalized
),
-- Combine the two data sources to make a master list of events with critical dates
all_dates AS (
  SELECT * FROM get_dates_from_movements
  UNION ALL
  SELECT * FROM get_classification_dates
),
-- Since each row in this CTE is associated with a change to either a custody level
-- or location, we assume that whatever attributes are not changed in 
-- a given row are the same as they were the last time they were assigned. This CTE 
-- carries forward the last assigned values of unaltered attributes to rows tracking a 
-- change to another attribute.
carry_forward_attributes AS (
  SELECT DISTINCT
    PERSON_ID,
    DOC_ID,    
    -- prioritize classification custody levels as we assume they are more reliable for the time being
    LAST_VALUE(CLASSIFICATION_CUSTODY_LEVEL IGNORE NULLS) OVER person_window AS custody_level,
    LAST_VALUE(LOCATOR_CODE_ID IGNORE NULLS) OVER person_window AS locator_code_id,
    LAST_VALUE(UNIT_ID IGNORE NULLS) OVER person_window AS unit_id,
    LAST_VALUE(PRISON_ID IGNORE NULLS) OVER person_window AS prison_id,
    LAST_VALUE(HOUSING_UNIT_DETAIL IGNORE NULLS) OVER person_window AS housing_unit_detail,
    jail_location,
    hospital_location,
    COALESCE(MOVEMENT_DATE, CLASSIFICATION_DATE) AS period_start_date,
    MOVEMENT_DESCRIPTION,
    action_ranking
  FROM (
    SELECT *, 
    CASE  
      WHEN MOVEMENT_DESCRIPTION LIKE 'CREATE-%' OR MOVEMENT_DESCRIPTION LIKE 'REOPEN-%' THEN 1
      WHEN MOVEMENT_DESCRIPTION = 'CLASSIFICATION' THEN 2
      WHEN MOVEMENT_DESCRIPTION LIKE 'NULL-%' AND MOVEMENT_DESCRIPTION NOT LIKE '%-Release ISC' THEN 3
      WHEN MOVEMENT_DESCRIPTION = 'NULL-Release ISC' THEN 4
      WHEN MOVEMENT_DESCRIPTION LIKE 'CLOSE-%' THEN 5
    END AS action_ranking
    FROM all_dates
  )
  WINDOW person_window AS (PARTITION BY PERSON_ID, DOC_ID ORDER BY COALESCE(MOVEMENT_DATE,CLASSIFICATION_DATE),
  action_ranking,
  --deterministically sort when people are in multiple locations at the same time
  locator_code_id, prison_id, unit_id, hospital_location, jail_location, CLASSIFICATION_CUSTODY_LEVEL, housing_unit_detail
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
-- Create periods based on the critical dates with all attributes carried forward as appropriate.
periods AS (
  SELECT * FROM (
  SELECT DISTINCT
    PERSON_ID,
    DOC_ID,
    locator_code_id,
    unit_id,
    prison_id,
    housing_unit_detail,
    jail_location,
    hospital_location,
    custody_level,
    period_start_date AS start_date,
    LEAD(period_start_date) OVER person_window AS end_date,
    MOVEMENT_DESCRIPTION AS start_reason,
    LEAD(MOVEMENT_DESCRIPTION) OVER person_window AS end_reason,
    LAG(MOVEMENT_DESCRIPTION) OVER person_window AS prev_end_reason,
    action_ranking,
    LATEST_RELEASE_DATE,
    returned_after_release,
    LATEST_INTAKE_DATE
    FROM carry_forward_attributes
    LEFT JOIN key_dates_and_status
    USING(PERSON_ID, DOC_ID)
 WINDOW person_window AS (PARTITION BY PERSON_ID, DOC_ID ORDER BY period_start_date, 
 action_ranking,
  -- when both movements begin with NULL, order deterministically (these are all internal movements & are doubled, one OUT and one IN)
  MOVEMENT_DESCRIPTION DESC,
  -- There is exactly one case of a person being in two locations with all other things being equal. This sorts the two rows deterministically.
  locator_code_id, prison_id, unit_id, hospital_location, jail_location, custody_level, housing_unit_detail)
)
-- Each of these conditions helps avoid including classifications that happen after a
-- period has closed, which incorrectly keeps periods open forever.
-- 1. Exclude events that come directly after period closures, but are not period openings.
WHERE NOT (PREV_END_REASON LIKE "CLOSE-%" AND start_reason NOT LIKE 'CREATE-%' AND start_reason NOT LIKE 'REOPEN-%')
-- 2. Exclude classification events that come after a person's latest release movement,
-- when we know the person was not returned to custody after that release.
AND NOT (
  start_reason = 'CLASSIFICATION' 
  -- This is the most recent event
  AND end_reason IS NULL 
  -- This event takes place after the most recent release
  AND start_date > IFNULL(LATEST_RELEASE_DATE,'9999-12-31') 
  -- The person has not been returned to custody since that release, meaning
  -- this event takes place while they are at liberty
  AND NOT returned_after_release)
  -- 3. Exclude classifications that are logged without any entrance into prison at all.
  AND NOT (start_reason = 'CLASSIFICATION' 
  -- this is the most recent movement
  AND end_reason IS NULL 
  -- for this to be NULL, there must be no OPEN or REOPEN movements associated with this 
  -- person & incarceration stint
  AND LATEST_INTAKE_DATE IS NULL)
), 
-- Transform location codes to human-readable location names.
decode_locations AS (
  SELECT DISTINCT
    PERSON_ID,
    DOC_ID,
    COALESCE(locator.LOCATOR_NAME, unit.UNIT_NAME) AS location_name,
    prison.PRISON_NAME AS prison_name,
    housing_unit_detail,
    jail_location,
    hospital_location,
    custody_level,
    start_date,
    end_date,
    start_reason,
    end_reason,
    action_ranking
  FROM periods
  LEFT JOIN {AZ_DOC_LOCATOR_CODE} locator
    ON(periods.LOCATOR_CODE_ID=locator.LOCATOR_CODE_ID AND locator.ACTIVE='Y')
  LEFT JOIN {AZ_DOC_UNIT} unit
    ON(periods.UNIT_ID = unit.UNIT_ID AND unit.ACTIVE = 'Y')
  LEFT JOIN {AZ_DOC_PRISON} prison
    ON(periods.PRISON_ID = prison.PRISON_ID AND prison.ACTIVE = 'Y')
)
SELECT DISTINCT
  PERSON_ID,
  DOC_ID,
  location_name,
  prison_name,
  housing_unit_detail,
  jail_location,
  hospital_location,
  custody_level,
  start_date,
  end_date,
  -- Remove period action prefixes from movement reasons
  REGEXP_REPLACE(start_reason, r'^[A-Z]*-', '') AS start_reason,
  REGEXP_REPLACE(end_reason, r'^[A-Z]*-', '') AS end_reason,
  ROW_NUMBER() OVER (PARTITION BY PERSON_ID, DOC_ID ORDER BY START_DATE, END_DATE NULLS LAST,
  action_ranking,
  -- deterministically sort movements that happened on the same day with the same period logic
  start_reason, end_reason, location_name, housing_unit_detail, custody_level) AS period_seq
FROM decode_locations
WHERE PERSON_ID IS NOT NULL
AND start_reason NOT LIKE "CLOSE-%" 
AND start_reason NOT LIKE "%-Release ISC"
AND (start_date != end_date OR (start_date = end_date AND (
  start_reason LIKE 'CREATE-%' OR start_reason LIKE 'REOPEN-%'
  OR end_reason LIKE 'CLOSE-%' or end_reason LIKE '%-Release ISC') 
  ) OR end_date IS NULL);
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_az",
    ingest_view_name="incarceration_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
