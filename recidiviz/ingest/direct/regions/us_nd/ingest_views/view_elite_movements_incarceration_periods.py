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
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
 WITH critical_dates AS (
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
)
, dates_with_all_attributes AS (
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
), full_periods AS (
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
    order_by_cols="offender_book_id, period_sequence",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
