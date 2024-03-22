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
"""Query containing incarceration period information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 

-- Incarceration periods are constructed using the following steps:

-- I. Identifying transition dates, which are the dates on which an incarcerated individual 
-- experiences a change in status (including the beginnings/ends of an incarceration term).
-- II. Unioning transitions of each type (external movement, bed assignment, and 
-- custody classification), deduplicating transitions that share a date, and sorting the
-- final set of transition dates.
-- III. Building periods by matching the sorted transition dates, which are processed and 
-- filtered to yield the view's output.

facility_locs AS (
  SELECT org.*
  FROM {ORGANIZATIONPROF} org
  LEFT JOIN {ORGANIZATIONPROF} dept
  ON org.ORGDEPTCODE = dept.PARTYID

  -- Some types of movements are recorded for people on supervision, usually right after 
  -- a release from incarceration or before a violation return. Periods constructed from
  -- those movements will be filtered out to ensure that people on supervision aren't 
  -- mis-categorized as incarcerated. Supervision periods constructed from EXTERNALMOVEMENT 
  -- data can be identified using the fact that the associated organization will not have
  -- a parent department. The exception is for movements preceding a prison return from 
  -- supervision, which often have a parent department of C1 (ACC Director's Office) but 
  -- don't yet indicate incarceration. Therefore, extra conditions are added for periods 
  -- with a C1 department to only include community correction center locations, thus 
  -- excluding field supervision. 

  -- This list of incarceration-specific locations is also used to determine whether or
  -- not a final outgoing movement should be interpreted as the end of an incarceration term. 
  -- If the final movement is not to one of these locations, then it's treated as terminal
  -- by the sealed_periods CTE; otherwise, it's probably a transfer or similar movement
  -- and the final period will be left open.

  WHERE dept.ORGANIZATIONTYPE IN ('A1','I5','U5') OR (
    dept.ORGANIZATIONTYPE = 'C1' AND (
      org.ORGANIZATIONTYPE = 'D5' OR org.ORGANIZATIONTYPE LIKE 'C%'
    )
  )
),

-- STEP I. IDENTIFYING TRANSITIONS

external_movements AS (
  -- Movements into, from, or between facilities are recorded in the EXTERNALMOVEMENT table.
  -- Movement codes lower than 40 indicate moves into a facility, whereas movement codes 40
  -- and higher (along with alphanumeric codes) indicate moves out of a facility. Movements
  -- from one facility to another will often show up in the data as 2 moves: the outgoing
  -- movement from one facility and then the incoming movement into another, usually on the 
  -- same day. In these codes, the second movement code will usually be the converse of the
  -- first, such as a 90 ('Transferred to Another Facility') followed by a 30 ('Received 
  -- from Another Facility').
  SELECT DISTINCT
    OFFENDERID,
    CAST(
      CONCAT(
        SPLIT(EXTERNALMOVEMENTDATE, ' ')[OFFSET(0)],' ',EXTERNALMOVEMENTTIME
      ) AS DATETIME
    ) AS EMDATETIME,
    EXTERNALMOVEMENTCODE,
    -- Movements with a non-null code but missing reason are rare (2 instances as of Dec. 2023), but they
    -- get set to 'Not Specified' here so that neither the code nor the reason can be null.
    COALESCE(REASONFORMOVEMENT,'99') AS REASONFORMOVEMENT,
    LOCATIONREPORTMOVEMENT,
    OTHERLOCATIONCODE,
    CASE 
      WHEN EXTERNALMOVEMENTCODE < '40' AND NOT REGEXP_CONTAINS(EXTERNALMOVEMENTCODE, '[[:alpha:]]') THEN 'IN' 
      WHEN EXTERNALMOVEMENTCODE IS NULL THEN NULL 
      ELSE 'OUT' 
    END AS direction
  FROM {EXTERNALMOVEMENT}
  WHERE REGEXP_CONTAINS(OFFENDERID, r'^[[:digit:]]+$') AND
      EXTERNALMOVEMENTCODE IS NOT NULL AND (
        LOCATIONREPORTMOVEMENT IN (
          SELECT PARTYID 
          FROM facility_locs
        ) OR
        OTHERLOCATIONCODE IN (
          SELECT PARTYID 
          FROM facility_locs
        )
      )
),

external_movements_processed AS (
  -- Location is standardized as the location someone moves into for each movement, which
  -- entails using different location columns for 'IN' and 'OUT' movements.
  SELECT DISTINCT 
    OFFENDERID,
    EMDATETIME,
    EXTERNALMOVEMENTCODE,
    REASONFORMOVEMENT,
    CASE 
      WHEN direction = 'IN' THEN LOCATIONREPORTMOVEMENT 
      WHEN direction = 'OUT' THEN OTHERLOCATIONCODE 
      ELSE NULL 
    END AS loc_to,
    direction
  FROM external_movements
),

-- The next 3 CTEs capture the critical dates on which someone's status meaningfully
-- changes in the following ways:
-- 1. Changes in facility
-- 2. Changes in locations within a facility
-- 3. Changes in custody level

external_movement_transition_dates AS (
  SELECT *
  FROM (
    SELECT DISTINCT 
      OFFENDERID,
      EMDATETIME,
      EXTERNALMOVEMENTCODE,
      REASONFORMOVEMENT,
      loc_to,
      LAG(loc_to) OVER ordered_movements AS last_loc_to,
      LEAD(loc_to) OVER ordered_movements AS next_loc_to,
      direction
    FROM external_movements_processed
    WINDOW ordered_movements AS (PARTITION BY OFFENDERID ORDER BY EMDATETIME)
  ) adjacent_movements
  -- The other 2 transition date CTEs don't include the LEAD value in their WHERE
  -- condition, but it's done here to retain information about a prison term's closing.
  WHERE loc_to != last_loc_to OR last_loc_to IS NULL OR next_loc_to IS NULL
),

bed_assignment_transition_dates AS (
  SELECT 
    ba_transitions.*,
    ha.HOUSINGAREANAME,
    CONCAT(COALESCE(ha.TYPEOFBED,'NA'),'-',COALESCE(ha.PRIMARYUSEOFBED,'NA')) AS bed_type,
    'BED_ASSIGNMENT' as movement_type 
  FROM (
    SELECT 
      OFFENDERID,
      BADATETIME,
      FACILITYWHEREBEDLOCATED,
      INMATEHOUSINGAREAID,
      BEDUSE,
      BEDASSIGNMENTREASON 
    FROM (
      SELECT 
        *,
        LAG(FACILITYWHEREBEDLOCATED) OVER ordered_assignments AS last_facility,
        LAG(INMATEHOUSINGAREAID) OVER ordered_assignments AS last_area,
        CAST(
          CONCAT(
            SPLIT(BEDASSIGNMENTDATE, ' ')[OFFSET(0)],' ',BEDASSIGNMENTTIME
          ) AS DATETIME
        ) AS BADATETIME
      FROM {BEDASSIGNMENT}
      -- Pending bed assignments shouldn't be interpreted as movements; other statuses
      -- (such as 'Unassigned') can still be treated as movements, since the bed assignment
      -- did take place but may have changed since.
      WHERE BEDASSIGNMENTSTATUS != 'P'
      WINDOW ordered_assignments AS (PARTITION BY OFFENDERID ORDER BY BEDASSIGNMENTDATE,BEDASSIGNMENTTIME)
    ) ba_processed
    WHERE FACILITYWHEREBEDLOCATED != last_facility OR 
      INMATEHOUSINGAREAID != last_area OR
      last_facility IS NULL OR
      last_area IS NULL
  ) ba_transitions
  LEFT JOIN {HOUSINGAREA} ha
  ON 
    ba_transitions.FACILITYWHEREBEDLOCATED = ha.PARTYID AND ba_transitions.INMATEHOUSINGAREAID = ha.FACILITYBUILDINGID 
),

custody_classification_transition_dates AS (
  SELECT *
  FROM (
      SELECT
      *,
      LAG(MODIFIEDCUSTODYGRADE) OVER ordered_classifications as last_custody_grade
      FROM (
        SELECT *
        FROM (
          SELECT DISTINCT 
            OFFENDERID,
            -- The CUSTODYCLASS table distinguishes between SUGGESTEDCUSTODYGRADE and
            -- MODIFIEDCUSTODYGRADE, and uses MODIFIEDCUSTODYGRADE for departures from the
            -- automatically prescribed custody grade. Therefore, in most cases these values
            -- will be the same, but only MODIFIEDCUSTODYGRADE will be accurate for departures.
            MODIFIEDCUSTODYGRADE,
            -- Unlike other transition data, custody classification dates lack a time component.
            -- This means that in the common scenario where one's custody level and facility
            -- location change simultaneously, the data will usually show a small delay between 
            -- the custody classification and the relocation (though the events do coincide
            -- in some rare cases, necessitating the deduplication in step II).
            CAST(CUSTODYCLASSIFDATE AS DATETIME) AS CCDATETIME,
            'CUSTODY_LVL_CHANGE' AS movement_type,
            ROW_NUMBER() OVER (PARTITION BY OFFENDERID, CUSTODYCLASSIFDATE ORDER BY CUSTODYCLASSIFSEQNBR DESC) AS cc_seq
          FROM {CUSTODYCLASS} 
        ) cc
      -- The system will sometimes multiple custody assignments on the same date, uniquely
      -- identified by CUSTODYCLASSIFSEQNBR. In these cases, the custody classification
      -- with the highest CUSTODYCLASSIFSEQNBR is the final custody level decision for 
      -- that datetime, so we only use that one.
      WHERE cc_seq = 1
      ) cc_deduped
    WINDOW ordered_classifications AS (PARTITION BY OFFENDERID ORDER BY CCDATETIME)
    ) cc_processed
    WHERE MODIFIEDCUSTODYGRADE != last_custody_grade OR
    last_custody_grade IS NULL
),

-- STEP II. COMPILING/PROCESSING TRANSITIONS

joined_transitions AS (
  -- Union the 3 transition date CTEs via an outer join. This means that on dates where
  -- only one type of transition occurs, other types of transition information will be 
  -- null, whereas dates with multiple types of transition will have information for each.
  -- Both of these cases are handled by coalescing the data to either retreive the non-null
  -- value, or prioritize the values based on the transition type (for instance, if an external 
  -- movement and a bed assignment occur on the same date, the external movement will be
  -- used to set the new location).
  SELECT 
    OFFENDERID,
    EMDATETIME,
    EXTERNALMOVEMENTCODE,
    REASONFORMOVEMENT,
    -- Once the transition data has been coalesced as described above, another round of
    -- coalesces is used together with window functions to infer information that has
    -- been specified by a previous transition but has not changed in a following transition.
    -- For instance, if somebody gets assigned to custody level C3, and then later changes 
    -- facilities, the latter transition will lack custody level data, but we assume that
    -- the custody level is still C3.
    COALESCE(loc_to, LAST_VALUE(loc_to IGNORE NULLS) OVER prior_moves) AS loc_to,
    direction,
    COALESCE(MODIFIEDCUSTODYGRADE, LAST_VALUE(MODIFIEDCUSTODYGRADE IGNORE NULLS) OVER prior_moves) AS MODIFIEDCUSTODYGRADE,
    COALESCE(HOUSINGAREANAME, LAST_VALUE(HOUSINGAREANAME IGNORE NULLS) OVER prior_moves) AS HOUSINGAREANAME,
    COALESCE(bed_type, LAST_VALUE(bed_type IGNORE NULLS) OVER prior_moves) AS bed_type,
    RANK() OVER (PARTITION BY OFFENDERID ORDER BY EMDATETIME) AS MOVE_SEQ 
  FROM (
    SELECT 
      COALESCE(em.OFFENDERID,cc.OFFENDERID,ba.OFFENDERID) AS OFFENDERID,
      COALESCE(EMDATETIME,CCDATETIME,BADATETIME) AS EMDATETIME,
      COALESCE(EXTERNALMOVEMENTCODE,cc.movement_type,ba.movement_type) AS EXTERNALMOVEMENTCODE,
      COALESCE(REASONFORMOVEMENT,cc.movement_type,ba.BEDASSIGNMENTREASON) AS REASONFORMOVEMENT,
      COALESCE(loc_to,FACILITYWHEREBEDLOCATED) AS loc_to,
      COALESCE(direction,cc.movement_type,ba.movement_type) AS direction,
      MODIFIEDCUSTODYGRADE,
      HOUSINGAREANAME,
      bed_type
    FROM external_movement_transition_dates em
    FULL OUTER JOIN custody_classification_transition_dates cc
    ON em.OFFENDERID = cc.OFFENDERID AND EMDATETIME = CCDATETIME
    FULL OUTER JOIN bed_assignment_transition_dates ba
    ON em.OFFENDERID = ba.OFFENDERID AND EMDATETIME = BADATETIME
  ) transitions_coalesced
  WINDOW prior_moves AS (PARTITION BY OFFENDERID ORDER BY EMDATETIME ROWS UNBOUNDED PRECEDING)
),

deduped_transitions as (
  -- To avoid constructing duplicate/overlapping periods, transition data must be
  -- deduplicated such that transition dates are unique at the person level. When there
  -- are multiple types of transition on a given date, this CTE collapses them into a 
  -- single transition. Different types of period information are handled differently:
  -- person, date, and sequence number don't require processing as they will be the same
  -- (since MOVE_SEQ is set using a RANK not ROW_NUMBER). Movement code, reason, and direction
  -- are concatenated and separated by hyphens. Other data is taken from the transition
  -- that specifies it: for example, if a bed assignment and custody assignment occur at the
  -- same time, the location data is pulled from the bed assignment transition and the custody
  -- level data is pulled from the custody assignment transition.
  SELECT DISTINCT
    OFFENDERID,
    EMDATETIME, 
    STRING_AGG(EXTERNALMOVEMENTCODE,'-') OVER ba_then_cc AS  EXTERNALMOVEMENTCODE,
    STRING_AGG(REASONFORMOVEMENT,'-') OVER ba_then_cc AS  REASONFORMOVEMENT,
    FIRST_VALUE(loc_to) OVER ba_then_cc AS loc_to,
    STRING_AGG(direction,'-') OVER ba_then_cc AS  direction,
    LAST_VALUE(MODIFIEDCUSTODYGRADE) OVER ba_then_cc AS MODIFIEDCUSTODYGRADE,
    FIRST_VALUE(HOUSINGAREANAME) OVER ba_then_cc AS HOUSINGAREANAME,
    FIRST_VALUE(bed_type) OVER ba_then_cc AS bed_type,
    MOVE_SEQ
  FROM (
    SELECT 
      OFFENDERID,
      MOVE_SEQ 
    FROM joined_transitions 
    GROUP BY OFFENDERID, MOVE_SEQ 
    HAVING(COUNT(*)>1)
  ) duplicate_dates 
  LEFT JOIN joined_transitions all_dates 
  USING(OFFENDERID,MOVE_SEQ)
  -- Partition the transition dates and order the data such that bed assignments precede
  -- custody assignments, allowing us to pick the correct data using FIRST_VALUE and LAST_VALUE.
  WINDOW ba_then_cc AS (
    PARTITION BY OFFENDERID,EMDATETIME,MOVE_SEQ 
    -- External movement transitions shouldn't (and currently don't) coincide with other
    -- transition types, but are ranked last in this sorting algorithm to avoid nondeterminism.
    ORDER BY
      CASE WHEN direction = 'BED_ASSIGNMENT' THEN 2
      WHEN direction = 'CUSTODY_LVL_CHANGE' THEN 1
      ELSE 0 END
    DESC
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  )
),

all_transitions AS (
  -- Union the unique transition dates with the data deduplicated in the previous CTE.
  SELECT * 
  FROM (
    SELECT jt.* 
    FROM joined_transitions jt
    LEFT JOIN deduped_transitions dt
    USING(OFFENDERID,MOVE_SEQ)
    WHERE dt.OFFENDERID IS NULL
  ) unique_only
  UNION ALL (
    SELECT * 
    FROM deduped_transitions
  )
),

-- STEP III. CONSTRUCTING/FILTERING PERIODS

basic_periods AS (
  -- Construct periods using the full set of transition dates.
  SELECT 
    td_starts.OFFENDERID,
    td_starts.EXTERNALMOVEMENTCODE AS entry_code,
    td_starts.REASONFORMOVEMENT AS entry_reason,
    td_ends.EXTERNALMOVEMENTCODE AS exit_code,
    td_ends.REASONFORMOVEMENT AS exit_reason,
    td_starts.loc_to AS loc,
    td_starts.MODIFIEDCUSTODYGRADE AS custody_grade,
    td_starts.HOUSINGAREANAME AS HOUSINGAREANAME,
    td_starts.bed_type AS bed_type,
    td_starts.EMDATETIME AS start_date,
    td_ends.EMDATETIME AS end_date,
    td_starts.direction,
    -- Because of the left join, a person's final transition will be treated as the start
    -- of an open period. Here, we identify terminal movements, which should result in 
    -- period closure.
    td_ends.EMDATETIME IS NULL AND 
      td_starts.direction = 'OUT' AND 
      td_starts.loc_to NOT IN (
        SELECT PARTYID 
        FROM facility_locs
      ) AS is_terminal
  FROM all_transitions td_starts
  LEFT JOIN all_transitions td_ends
  ON 
    td_starts.OFFENDERID = td_ends.OFFENDERID AND 
    td_starts.MOVE_SEQ = td_ends.MOVE_SEQ - 1
),

sealed_periods AS (
  -- Drop periods starting with a terminal movement, and shift those periods' exit 
  -- codes/reasons to the previous period to set the correct period closure data.
  SELECT DISTINCT 
    OFFENDERID,
    start_date,
    end_date,
    loc,
    custody_grade,
    HOUSINGAREANAME,
    bed_type,
    entry_code,
    entry_reason,
    COALESCE(terminating_code, exit_code) AS exit_code,
    COALESCE(terminating_reason, exit_reason) AS exit_reason
  FROM (
    SELECT 
      *,
      LEAD(
        CASE WHEN is_terminal THEN entry_code ELSE NULL END
      ) OVER ordered_periods AS terminating_code,
      LEAD(
        CASE WHEN is_terminal THEN entry_reason ELSE NULL END
      ) OVER ordered_periods AS terminating_reason 
    FROM basic_periods
    WINDOW ordered_periods AS (PARTITION BY OFFENDERID ORDER BY start_date)
  ) shifted_exit_details
  WHERE NOT is_terminal
)

SELECT 
  OFFENDERID,
  start_date,
  end_date,
  custody_grade,
  HOUSINGAREANAME,
  bed_type,
  entry_code,
  entry_reason,
  exit_code,
  exit_reason,
  op.ORGANIZATIONNAME,
  op.ORGANIZATIONTYPE,
  op.ORGCOUNTYCODE,
  ROW_NUMBER() OVER (PARTITION BY sp.OFFENDERID ORDER BY sp.start_date) AS SEQ
FROM sealed_periods sp
LEFT JOIN {ORGANIZATIONPROF} op
ON sp.loc = op.PARTYID
WHERE sp.loc IN (
  SELECT PARTYID 
  FROM facility_locs
)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ar",
    ingest_view_name="incarceration_period",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
