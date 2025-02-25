# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""
Query containing movement information, both external (into/out of/between facilities)
and internal (unit/bed assignments within facilities).

Incarceration periods are created by merging rows from `elite_externalmovements`
that correspond to sequential movements into and out of a facility with rows from
`elite_bedassignmenthistory` and `elite_livingunits` that correspond to sequential
movements within facilities. Eventually, the goal is to map reasons for those internal
movements using `elite_programservices`, `elite_courseactivities`, and `elite_offenderprogramprofiles`.
Currently, all internal movements have a placeholder reason `INTERNAL_MOVEMENT` when
one cannot be inferred from flanking external movements (i.e. transfers between facilities,
admissions from liberty, or releases).

Rows in `elite_externalmovements` can be used to model
"edges" of continuous periods of incarceration, and multiple such rows can be used to
model a continuous period of incarceration. An ingest view walks over rows for a single
person in this table and stitches together "edges" to create continuous periods. We join
two "edges" to make a continuous period if the movements are associated with the same
`OFFENDER_BOOK_ID`, the `MOVEMENT_SEQ` values are sequential, and the `TO_AGY_LOC_ID`
value of the `IN` edge matches the `FROM_AGY_LOC_ID` value of the `OUT` edge.

We further break down those continuous periods into periods of specific bed assignments.
The final ingest view result is indexed on each person and each period of assignment to a
particular living unit and custody level. Since the facility does not change across a person's period
of incarceration in that facility, regardless of their living unit assignment or custody level, their
movements into, out of, and between facilities are preserved when we add these additional
levels of detail.

It's possible for this approach to produce multiple periods for a person that are
missing release information. It's also possible for this logic to produce a period
that only contains release information. Both of these situations are handled by IP
pre-processing.

"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- This CTE collects all of the critical dates on which any attribute we care about changed
-- throughout the course of a person's incarceration. The result is one row per person, 
-- attribute, and date that attribute was assigned. These attributes are pulled from multiple
-- sources, so not all rows have all attributes filled in; each row will only contain
-- information about the attribute(s) stored in one of the three source tables
-- included here. These attributes will be combined later to form a complete picture.
critical_dates AS (
  SELECT DISTINCT
    REPLACE(REPLACE(OFFENDER_BOOK_ID,',',''), '.00', '') AS OFFENDER_BOOK_ID,
    CAST(MOVEMENT_DATE AS DATETIME) AS MOVEMENT_DATE,
    REPLACE(REPLACE(MOVEMENT_SEQ,',',''), '.00', '') AS MOVEMENT_SEQ,
    MOVEMENT_REASON_CODE,
    DIRECTION_CODE,
    CASE 
        WHEN TO_AGY_LOC_ID IS NOT NULL THEN TO_AGY_LOC_ID
        WHEN TO_AGY_LOC_ID IS NULL AND FROM_AGY_LOC_ID IS NOT NULL THEN FROM_AGY_LOC_ID
    END AS facility,
    CAST(NULL AS STRING) AS custody_level,
    CAST(NULL AS STRING) AS override_reason,
    CAST(NULL AS STRING) AS bed_assignment
  FROM {elite_externalmovements}

  UNION ALL 

  SELECT DISTINCT
    REPLACE(REPLACE(OFFENDER_BOOK_ID,',',''), '.00', '') AS OFFENDER_BOOK_ID,
    CAST(ASSESSMENT_DATE AS DATETIME) AS ASSESSMENT_DATE,
    REPLACE(REPLACE(ASSESSMENT_SEQ,',',''), '.00', '') AS ASSESSMENT_SEQ,
    'STATUS_CHANGE' AS MOVEMENT_REASON_CODE,
    'INTERNAL_MOVEMENT' AS DIRECTION_CODE,
    CAST(NULL AS STRING) AS facility,
    IF(REVIEW_SUP_LEVEL_TYPE='',CALC_SUP_LEVEL_TYPE, REVIEW_SUP_LEVEL_TYPE) AS custody_level,
    -- Trim to 200 characters to avoid exceeding the 255 character limit on the column
    LEFT(COALESCE(ASSESS_COMMENT_TEXT, OVERRIDE_REASON), 200) AS override_reason,
    CAST(NULL AS STRING) AS bed_assignment
  FROM {recidiviz_elite_OffenderAssessments}
  WHERE ASSESSMENT_TYPE_ID IN (
    '1,008.00', -- INITIAL ASSESSMENT	
    '1,009.00,', -- RECLASSIFICATION
    '1,495.00', -- RECLASSIFICATION	
    '1,308.00', -- SCORE SHEET ONLY	
    '1,664.00', -- SCORE SHEET ONLY	
    '1,328.00', -- NEW ARRIVAL	
    '1,371.00', -- INITIAL CLASSIFICATION	
    '2,528.00', -- Initial Classification	
    '2,828.00', -- Reclassification	
    '3,070.00', -- Score Sheet Only	
    '4,102.00', -- Reclassification - 36 mos or less to serve	
    '4,306.00', -- Initial Classification - 36 mos or less	
    '108,309.00','113,564.00', -- INITAL CLASSIFICATION - MALE	
    '108,515.00','114,361.00', -- INITAL CLASSIFICATION - FEMALE	
    '108,737.00','114,005.00', -- RECLASSIFICATION - MALE	
    '108,956.00','114,804.00', -- RECLASSIFICATION - FEMALE	
    '109,142.00', -- INITAL CLASSIFICATION - FEMALE - 42 MOS OR LESS	
    '109,346.00','113,792.00', -- INITAL CLASSIFICATION - MALE - 42 MOS OR LESS	
    '109,547.00', -- RECLASSIFICATION - FEMALE - 42 MOS OR LESS	
    '109,587.00', -- RECLASSIFICATION - MALE - 42 MOS OR LESS	
    '110,100.00', -- ESCAPEE RETURNED	
    '114,138.00', -- RECLASSIFICATION - MALE - 42 MOS OF LESS	
    '114,566.00', -- INITAL CLASSIFICATION - FEMALE - 60 MOS OR LESS	
    '114,938.00' -- RECLASSIFICATION - FEMALE - 60 MOS OR LESS	
  )

  UNION ALL 

  SELECT DISTINCT
    REPLACE(REPLACE(OFFENDER_BOOK_ID,',',''), '.00', '') AS OFFENDER_BOOK_ID,
    CAST(ASSIGNMENT_DATE AS DATETIME) AS ASSIGNMENT_DATE,
    bed_assignments.BED_ASSIGN_SEQ,
    'BED_ASSIGNMENT_CHANGE' AS MOVEMENT_REASON_CODE,
    'INTERNAL_MOVEMENT' AS DIRECTION_CODE,
    CAST(NULL AS STRING) AS facility,
    CAST(NULL AS STRING) AS custody_level,
    CAST(NULL AS STRING) AS override_reason,
    ref.DESCRIPTION AS bed_assignment,
  FROM {elite_bedassignmenthistory} bed_assignments
  LEFT JOIN {elite_livingunits} ref
  USING(LIVING_UNIT_ID)
), 
-- This CTE takes the rows from the previous result and carries forward attributes that 
-- did not change since the previous update. This results in one row for every time any
-- attribute of a person's incarceration changed. Each row contains every attribute that was 
-- true on that date.
dates_with_all_attributes AS (
  SELECT 
    OFFENDER_BOOK_ID,
    MOVEMENT_DATE,
    MOVEMENT_SEQ,
    MOVEMENT_REASON_CODE,
    DIRECTION_CODE,
    LAST_VALUE(facility ignore nulls) OVER (person_window range between UNBOUNDED preceding and current row) AS facility,
    LAST_VALUE(custody_level ignore nulls) OVER (person_window range between UNBOUNDED preceding and current row) AS custody_level,
    LAST_VALUE(override_reason ignore nulls) OVER (person_window range between UNBOUNDED preceding and current row) AS override_reason,
    LAST_VALUE(bed_assignment ignore nulls) OVER (person_window range between UNBOUNDED preceding and current row) AS bed_assignment,
  FROM critical_dates
  -- MOVEMENT_REASON_CODE is a part of the ordering here to deterministically sort movements and assessments that
  -- happened on the same day. It sorts admissions and transfers before status changes; status changes that follow OUT
  -- movements are excluded later anyway.
  WINDOW person_window AS (
    PARTITION BY OFFENDER_BOOK_ID 
    ORDER BY MOVEMENT_DATE, CAST(MOVEMENT_SEQ AS INT64), MOVEMENT_REASON_CODE, DIRECTION_CODE)
), 
-- This CTE uses the collection of attributes and the dates they changed to construct
-- periods during which specific things were true about a person's incarceration. 
full_periods AS (
  SELECT 
    ROW_NUMBER() OVER person_window AS period_sequence, 
    * 
FROM (
    SELECT
        OFFENDER_BOOK_ID,
        MOVEMENT_REASON_CODE AS admission_reason_code,
        MOVEMENT_DATE AS start_date,
        MOVEMENT_SEQ,
        facility,
        DIRECTION_CODE,
        LEAD(DIRECTION_CODE) OVER person_window AS next_direction_code,
        LEAD(MOVEMENT_DATE) OVER person_window AS end_date,
        LEAD(MOVEMENT_REASON_CODE) OVER person_window AS release_reason_code,
        custody_level,
        override_reason,
        bed_assignment
    FROM dates_with_all_attributes
    WINDOW person_window AS (
      PARTITION BY OFFENDER_BOOK_ID 
      ORDER BY MOVEMENT_DATE, CAST(MOVEMENT_SEQ AS INT64), MOVEMENT_REASON_CODE, DIRECTION_CODE)
    ) sub
    WHERE facility != 'OUT'
     -- Exclude rows that begin with a release or escape
    AND NOT (DIRECTION_CODE = 'OUT' AND NEXT_DIRECTION_CODE = 'INTERNAL_MOVEMENT')
    AND admission_reason_code != 'ESCP'
    -- Exclude rows with a probation violation as the release reason. This is a result 
    -- of the way movements are tracked in this scenario: a person is admitted to either
    -- DEFP or NTAD, then transferred to a different facility on the same day they start a 
    -- period of incarceration at the new facility with admission reason NPRB (so all of
    -- these periods are redundant)
    AND (release_reason_code NOT IN ('NPRB','NTAD') OR release_reason_code IS NULL)
    -- Exclude rows that start with a person being released in error and end with them 
    -- being readmitted
    AND NOT (admission_reason_code = 'ERR' AND release_reason_code = 'READMN')
    WINDOW person_window AS (
      PARTITION BY OFFENDER_BOOK_ID 
      ORDER BY start_date, CAST(MOVEMENT_SEQ AS INT64), admission_reason_code)
), 
-- The results of this CTE are structured the same as the previous results, but 
-- have custody levels and bed assignments filled in when they can be inferred. This is the 
-- case in two scenarios: 
  -- 1. When either attribute is NULL and the person just arrived in a facility, we infer
  --    that they are going through the intake process and have not been formally assigned yet.
  -- 2. When eitehr attribute is NULL and the person did *not* just arrive in a facility, 
  --    we fill in the attribute with a true unknown signifier. 
infer_missing_attributes AS (
  SELECT 
    period_sequence,
    OFFENDER_BOOK_ID as offender_book_id,
    admission_reason_code,
    start_date,
    facility,
    end_date,
    release_reason_code,
    CASE
      WHEN custody_level IS NULL AND period_sequence = 1 THEN 'INFERRED-INTAKE'
      WHEN custody_level IS NULL AND period_sequence !=1 THEN 'INFERRED-UNCLASS'
      ELSE custody_level
    END AS custody_level,
    override_reason,
    CASE
      WHEN bed_assignment IS NULL AND period_sequence = 1 THEN 'UNKNOWN-INTAKE'
      WHEN bed_assignment IS NULL AND period_sequence !=1 THEN 'UNKNOWN-UNKNOWN'
      ELSE bed_assignment
    END AS bed_assignment
  FROM full_periods
)
SELECT 
  *
FROM infer_missing_attributes
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_nd",
    ingest_view_name="elite_movements_incarceration_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
