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
"""Query containing information about incarceration incidents and outcomes."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- This CTE constructs rudimentary location periods for each person so that the facility
-- in which an incident took place can be accurately reflected, since that information
-- does not exist in any incident review tables.
traffic_preprocessed AS (
  SELECT 
    DOC_ID, 
    PRISON_ID, 
    CAST(MOVEMENT_DATE AS DATETIME) AS MOVEMENT_DATE, 
    LEAD(CAST(MOVEMENT_DATE AS DATETIME)) 
        OVER (
            PARTITION BY DOC_ID 
            ORDER BY CAST(MOVEMENT_DATE AS DATETIME)) 
        AS NEXT_MOVEMENT_DATE
  FROM {AZ_DOC_INMATE_TRAFFIC_HISTORY} traffic
),
-- This CTE aggregates basic information about each incident from four discipline-related
-- tables that each contain some specific piece of associated information. Using this approach,
-- it is possible  for one actual incident to have many outcomes; each outcome will be tied to 
-- a distinct STAFF_REVIEW_ID, but have otherwise identical information.
base AS (
SELECT 
    doc_ep.PERSON_ID,
    review.DOC_ID, 
    review.STAFF_REVIEW_ID,
    review.STATE_VIOLATION_ID, --lookup
    review.VIOLATION_DTM AS incident_date,
    traffic_preprocessed.PRISON_ID, --lookup
    review.GENERAL_LOCATION_ID,  --lookup
    review.VIOLATION_STATEMENT,
    review.STAFF_AWARE_DTM AS report_date,
    penalty.PENALTY_ID,
    penalty.PENALTY_TYPE_ID, --lookup
    penalty.PENALTY_IMPOSED_DTM,
    penalty.PENALTY_END_DTM,
    CASE PENALTY_TYPE_ID
        WHEN '2106' THEN extra_duty_days_to_complete
        ELSE number_of_penalty_days
    END AS number_of_penalty_days,
    -- There are occasionally multiple hearings for the same incident; we store the 
    -- earliest one to be able to gauge compliance with any statutory requirements for 
    -- promptness.
    MIN(hearing.HEARING_DTM) OVER (PARTITION BY STAFF_REVIEW_ID) AS FIRST_HEARING_DTM,
    sanction.COMMENTS AS penalty_free_text_description
FROM {AZ_DOC_DSC_STAFF_REVIEW} review
LEFT JOIN {AZ_DOC_DSC_PENALTY} penalty
USING(STAFF_REVIEW_ID)
LEFT JOIN {AZ_DOC_DSC_SCHED_HRNG_HIST} hearing
USING(STAFF_REVIEW_ID)
LEFT JOIN {AZ_DOC_DSC_SANCTION} sanction
USING(STAFF_REVIEW_ID)
LEFT JOIN {DOC_EPISODE} doc_ep
USING(DOC_ID)
LEFT JOIN traffic_preprocessed
-- use the traffic table and the incident date to determine the facility in which an incident took place.
ON(traffic_preprocessed.DOC_ID = review.DOC_ID 
AND ((CAST(VIOLATION_DTM AS DATETIME)>= MOVEMENT_DATE AND CAST(VIOLATION_DTM AS DATETIME) < NEXT_MOVEMENT_DATE)
OR (CAST(VIOLATION_DTM AS DATETIME) >= MOVEMENT_DATE AND NEXT_MOVEMENT_DATE IS NULL)))
),
-- This CTE repeatedly joins to the LOOKUPS table to decode all necessary fields.
code_lookup AS (
SELECT 
    PERSON_ID,
    STAFF_REVIEW_ID,
    UPPER(rule_violation.DESCRIPTION) AS incident_type,
    incident_date,
    prison_lookup.DESCRIPTION AS facility,
    loc_lookup.DESCRIPTION AS location_within_facility,
    VIOLATION_STATEMENT,
    report_date,
    PENALTY_ID,
    UPPER(penalty_type_lookup.DESCRIPTION) AS penalty_type,
    PENALTY_IMPOSED_DTM,
    PENALTY_END_DTM,
    number_of_penalty_days,
    FIRST_HEARING_DTM,
    penalty_free_text_description
FROM base
LEFT JOIN {LOOKUPS} loc_lookup
ON(GENERAL_LOCATION_ID = loc_lookup.LOOKUP_ID)
LEFT JOIN {LOOKUPS} penalty_type_lookup
ON(PENALTY_TYPE_ID = penalty_type_lookup.LOOKUP_ID)
LEFT JOIN {AZ_DOC_DSC_RULE_VIOLATION} rule_violation
ON(STATE_VIOLATION_ID = RULE_VIOLATION_ID)
LEFT JOIN {LOOKUPS} prison_lookup
ON(PRISON_ID = prison_lookup.LOOKUP_ID)
)
SELECT * 
FROM code_lookup
-- Only the case in 13 rows as of 4/4/24 due to mismatched DOC_IDs between DOC_EPISODE
-- and AZ_DOC_DSC_STAFF_REVIEW.
WHERE PERSON_ID IS NOT NULL
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_az",
    ingest_view_name="incarceration_incidents",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
