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
"""Query containing state person information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- Constructs a list of addresses attached to PERSON_ID values. This is done separately
-- since each component of the address is stored in a distinct field and encoded.
complete_addresses AS (
SELECT * FROM (
    SELECT DISTINCT
    -- These values tend to change over time, presumably as more accurate information
    -- is added to the system to replace older, inaccurate occupancy information.
    -- The highest CHANGE_ID value corresponds to the row that was most recently updated,
    -- so this subquery pulls residency values from that row for later use.
    -- In cases where CHANGE_ID is always NULL due to data integrity issues, we use values
    -- from the most recently updated row by UPDT_DTM.
        PERSON_ID, 
        LOCATION_ID, 
        CONCAT(
            COALESCE(location.RESIDENCE_STREET_NUMBER,''), ' ',
            COALESCE(location.RESIDENCE_STREET_NAME,''), ' ',
            COALESCE(street_lookup.DESCRIPTION,''), ' ', -- st, blvd, ave, etc
            COALESCE(city_lookup.DESCRIPTION,''), ' ', -- city
            COALESCE(state_lookup.DESCRIPTION,''), ' ', -- state
            COALESCE(location.RESIDENCE_ZIP_CODE,'')
        ) AS full_address,
        ROW_NUMBER() OVER (
            PARTITION BY PERSON_ID 
            ORDER BY occ.CHANGE_ID DESC, 
            occ.UPDT_DTM DESC
        ) AS recency_rank
    FROM {OCCUPANCY} occ
    LEFT JOIN {LOCATION} location
    USING(LOCATION_ID)
    LEFT JOIN {LOOKUPS} state_lookup
    ON (location.RESIDENCE_STATE_CODE = state_lookup.LOOKUP_ID)
    LEFT JOIN {LOOKUPS} street_lookup
    ON (location.RESIDENCE_SUFFIX_ID = street_lookup.LOOKUP_ID)
    LEFT JOIN {LOOKUPS} city_lookup
    ON (location.RESIDENCE_CITY = city_lookup.LOOKUP_ID)
    WHERE CURRENT_OCCUPANCY = 'Y') sub
WHERE recency_rank = 1
),
-- Constructs a list of relevant demographic attributes attached to PERSON_ID values. 
-- This is done separately since each component is stored in a distinct field and encoded.
most_recent_demographics AS (
    -- These values tend to change over time, presumably as more accurate information
    -- is added to the system to replace older, inaccurate demographic information.
    -- The highest CHANGE_ID value corresponds to the row that was most recently updated,
    -- so this subquery pulls race and ethnicity values from that row for later use.
    -- In cases where CHANGE_ID is always NULL due to data integrity issues, we use values
    -- from the most recently updated row by UPDT_DTM.
SELECT
    PERSON_ID,
    RACE,
    ETHNICITY 
FROM (
    SELECT DISTINCT
        p.person_ID,
        d.RACE AS RACE_CODE, 
        race_lookup.DESCRIPTION AS RACE,
        d.ETHNICITY AS ETHNICITY_CODE,
        eth_lookup.DESCRIPTION AS ETHNICITY,
        ROW_NUMBER() OVER (
            PARTITION BY PERSON_ID 
            ORDER BY d.CHANGE_ID DESC, 
            d.UPDT_DTM DESC
        ) AS recency_rank
    FROM {PERSON} p
    LEFT JOIN {DEMOGRAPHICS} d
    USING (PERSON_ID)
    LEFT JOIN {LOOKUPS} race_lookup
    ON (RACE = race_lookup.LOOKUP_ID)
    LEFT JOIN {LOOKUPS} eth_lookup
    ON (ETHNICITY = eth_lookup.LOOKUP_ID)) sub
    WHERE recency_rank = 1
),
-- The result of this CTE is a simplified collection of the most recent information pertaining
-- to each person with a PERSON_ID in the PERSON table who is listed with PERSON_TYPE "INMATE".
base_person_info AS(
SELECT DISTINCT
    p.PERSON_ID, 
    p.ADC_NUMBER,
    p.FIRST_NAME,
    p.MIDDLE_NAME,
    p.SURNAME,
    p.DATE_OF_BIRTH,
    p.GENDER AS GENDER_CODE,
    gender_lookup.DESCRIPTION AS GENDER, 
    d.RACE AS RACE, 
    d.ETHNICITY AS ETHNICITY,
    complete_addresses.LOCATION_ID,
    TRIM(complete_addresses.full_address) AS full_address
FROM {PERSON} p
LEFT JOIN most_recent_demographics d
USING (PERSON_ID)
LEFT JOIN complete_addresses 
USING (PERSON_ID)
LEFT JOIN {LOOKUPS} gender_lookup
ON (GENDER = gender_lookup.LOOKUP_ID)
WHERE p.PERSON_TYPE_ID = '8476'  -- "Inmate"
)

SELECT 
    PERSON_ID,
    ADC_NUMBER,
    FIRST_NAME,
    MIDDLE_NAME,
    SURNAME,
    GENDER,
    RACE,
    DATE_OF_BIRTH,
    ETHNICITY,
    full_address
FROM base_person_info
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_az",
    ingest_view_name="state_person",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
