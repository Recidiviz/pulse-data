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

/*
Incarceration periods are constructed using the following steps:
I. Identifying transition dates, which are the dates on which an incarcerated individual 
experiences a change in status (including the beginnings/ends of an incarceration term).
II. Unioning transitions of each type (external movement, bed assignment, and 
custody classification), deduplicating transitions that share a date, and sorting the
final set of transition dates.
III. Building periods by matching the sorted transition dates, which are processed and 
filtered to yield the view's output.
*/

/*
Some types of movements are recorded for people on supervision, usually right after 
a release from incarceration or before a violation return. Periods constructed from
those movements will be filtered out to ensure that people on supervision aren't 
mis-categorized as incarcerated. If neither location associated with a movement is
considered a facility (based on the following list of facility-type ORGANIZATIONTYPE
codes), then it's considered a supervision-only movement and this excluded.
*/
facility_locs AS (
  SELECT *
  FROM {ORGANIZATIONPROF}
  WHERE ORGANIZATIONTYPE IN (
    'B1', -- State Prison Unit
    'B8', -- County Jail Backup
    'BF', -- Reception Center
    'D5', -- Community Corrections Center
    'B2', -- Work Release Center
    'BG', -- Inmate Hospital
    'I4', -- Other State Law Enforc. Agency
    'B6', -- County Jail Contract Condition
    'B7', -- County 309-In Jail
    'BC', -- City Jail Backup
    'E3', -- Police Department
    'I5', -- Other State DOC
    'U1', -- US Marshal
    'BA', -- City Jail Contract Conditional
    'U5', -- Federal Prison
    'BB', -- City 309-In Jail
    'BE', -- Ark Non-DOC Facility
    'C5', -- ACC Institu. Parole Services
    'U3', -- Immigration & Nat. Service
    'C3', -- ACC Residential Services
    'B9', -- Arkansas Concurrent Sentences
    /*
    E1 locations are a special case: any of these locations may or may not represent an 
    actual facility location in the data, depending on the movement code associated with it.
    That is, a given E1 location ID may be considered an actual facility location in one
    case, and may be simply used to denote a shift in jurisdiction in another. This 
    inconsistency is handled in the following CTE.
    */
    'E1'  -- County Jail/Sheriff
  ) AND 
  -- The location with PARTYID 0141218, named 'Unknown Location', has a type of I4 ('Other State Law Enforc. Agency'),
  -- which is included in the list of allowed facility types specified above. However, this
  -- particular location is only ever used when someone is discharged to another state, and
  -- therefore does not indicate the person actually being incarcerated in this 'location'.
  PARTYID != '0141218' 
),

-- STEP I. IDENTIFYING TRANSITIONS

/*
Movements into, from, or between facilities are recorded in the EXTERNALMOVEMENT table.
Movement codes lower than 40, along with 2A and 2B, indicate moves into a facility,
whereas movement codes 40 and higher (excepting 38), along with 8A, 8B, and 8J, indicate 
moves out of a facility.  Movements from one facility to another will often show up in 
the data as 2 moves: the outgoing movement from one facility and then the incoming movement 
into another, usually on the same day. In these codes, the second movement code will usually 
be the converse of the first, such as a 90 ('Transferred to Another Facility') followed 
by a 30 ('Received from Another Facility').
*/
external_movements AS (
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
    /*
    Organizations with an ORGANIZATIONTYPE of E1 ('County Jail/Sheriff') only represent
    physical locations where someone can be detained in the EXTERNALMOVEMENT data if they
    are the reporting location for a 42 ('Released Permanently (Jail Detainee)') movement 
    or the other location for a 38 ('Supervision Violator (Arrested)') movement. In every
    other case, these locations are included in the data to represent a change in jurisdiction
    rather than a change in physical location (such as when someone goes on furlough).

    By flagging when an E1 location is not used to denote a physical detention location 
    using the location ID, we can ensure that these instances of the location don't have
    any matches in the facility_locs CTE.
    */
    CASE 
      WHEN op1.ORGANIZATIONTYPE = 'E1' AND EXTERNALMOVEMENTCODE != '42'
      THEN CONCAT(LOCATIONREPORTMOVEMENT,'JURISDICTIONAL_ONLY')
      ELSE LOCATIONREPORTMOVEMENT
    END AS LOCATIONREPORTMOVEMENT,
    CASE 
      WHEN op2.ORGANIZATIONTYPE = 'E1' AND EXTERNALMOVEMENTCODE != '38'
      THEN CONCAT(OTHERLOCATIONCODE,'JURISDICTIONAL_ONLY')
      ELSE OTHERLOCATIONCODE
    END AS OTHERLOCATIONCODE,
    CASE 
      WHEN 
        (EXTERNALMOVEMENTCODE < '40' OR EXTERNALMOVEMENTCODE IN ('2A','2B')) AND
          EXTERNALMOVEMENTCODE NOT IN ('8A','8B','8J') AND
          EXTERNALMOVEMENTCODE != '38'
      THEN 'IN' 
      WHEN EXTERNALMOVEMENTCODE IS NULL THEN NULL 
      ELSE 'OUT' 
    END AS direction
  FROM {EXTERNALMOVEMENT} em
  LEFT JOIN {ORGANIZATIONPROF} op1
  ON LOCATIONREPORTMOVEMENT = op1.PARTYID
  LEFT JOIN {ORGANIZATIONPROF} op2
  ON OTHERLOCATIONCODE = op2.PARTYID
  WHERE 
    REGEXP_CONTAINS(OFFENDERID, r'^[[:digit:]]+$') AND
    EXTERNALMOVEMENTCODE IS NOT NULL 
),
/*
Admissions to backup jail facilities from parole may be special 90-day revocations, but
this isn't directly specified anywhere in the raw data. To identify these 90-day revocations,
we do the following:
- Get the admission movements that could potentially be 90-day revocations, based on their
  movement code and reason (return from ADC release/parole, with reason 'County/City Jail Backup')
- Join these movements with the person's board hearings, taking only the hearings that occur
  within 120 days of the admission movement. Note that this can be 120 days on either side,
  because there's often a delay in hearing data being recorded, resulting in hearing actions
  'occurring' after someone's already been committed.
- If any of these hearings include a '5E' action ('90 Day PV'), and none of them include a 
  '5F' action ('90 Day PV Removal'), then the movement is likely the result of a 90-day revocation. 
This CTE identifies the probable 90-day revocations, and we later join on this to set flag_90
to TRUE. This flag will stay TRUE until the person's next movement, and persists through
other non-movement transitions (so if someone on a 90-day revocation has their custody class 
changed without moving facilities, they will still be counted as a 90-day revocation because 
of the original movement.)
*/
movements_with_90_day_flag AS (
  SELECT 
    OFFENDERID, 
    EMDATETIME
  FROM (
    SELECT 
      em.*,
      CASE 
        WHEN 
          PAROLEBOARDFINALACTION1 = '5E' OR
          PAROLEBOARDFINALACTION2 = '5E' OR 
          PAROLEBOARDFINALACTION3 = '5E' OR 
          PAROLEBOARDFINALACTION4 = '5E' OR 
          PAROLEBOARDFINALACTION5 = '5E' OR 
          PAROLEBOARDFINALACTION6 = '5E' 
        THEN '5E'
        WHEN 
          PAROLEBOARDFINALACTION1 = '5F' OR
          PAROLEBOARDFINALACTION2 = '5F' OR 
          PAROLEBOARDFINALACTION3 = '5F' OR 
          PAROLEBOARDFINALACTION4 = '5F' OR 
          PAROLEBOARDFINALACTION5 = '5F' OR 
          PAROLEBOARDFINALACTION6 = '5F' 
        THEN '5F'
        ELSE NULL 
        END AS relevant_pb_action
    FROM external_movements em 
    LEFT JOIN {BOARDHEARING} bh
    ON 
      em.OFFENDERID = bh.OFFENDERID AND 
      ABS(
        DATE_DIFF(
          CAST(
            CONCAT(
              SPLIT(ADMINEVENTDATE, ' ')[OFFSET(0)],' ',ADMINEVENTTIME
            ) AS DATETIME
          ),
          EMDATETIME,
          DAY
        )
      ) <= 120
      WHERE EXTERNALMOVEMENTCODE IN ('10','18') AND REASONFORMOVEMENT = '04' 
    ) 
  GROUP BY OFFENDERID, EMDATETIME
  HAVING(
    LOGICAL_OR(relevant_pb_action='5E') AND NOT 
    LOGICAL_OR(relevant_pb_action='5F')
  )
),
-- Location is standardized as the location someone moves into for each movement, which
-- entails using different location columns for 'IN' and 'OUT' movements.
external_movements_processed AS (
  SELECT 
    em_processed.*,
    -- The following two columns are created to help identify and use certain discharge data.
    em_processed.loc_to IN (
      SELECT PARTYID 
      FROM facility_locs
    ) AS to_facility_loc,
    em_processed.EXTERNALMOVEMENTCODE='40' AND NOT em_processed.has_facility_loc AS masked_discharge,
    em_flagged.OFFENDERID IS NOT NULL AS flag_90
  FROM (
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
      direction,
      LOCATIONREPORTMOVEMENT IN (
        SELECT PARTYID 
        FROM facility_locs
      ) OR
      OTHERLOCATIONCODE IN (
        SELECT PARTYID 
        FROM facility_locs
      ) as has_facility_loc,
      LOCATIONREPORTMOVEMENT,
      OTHERLOCATIONCODE
    FROM external_movements
    -- Absconsion-related events (absconsions and returns) can sometimes be reported by a 
    -- facility-type location, but don't involve any actual detention, so they're filtered out.
    WHERE EXTERNALMOVEMENTCODE NOT IN ('28','88')
  ) em_processed
  LEFT JOIN movements_with_90_day_flag em_flagged
  USING(OFFENDERID, EMDATETIME)
),
/*
Some data entry errors result in release-to-supervision movements being omitted from
EXTERNALMOVEMENT. When this happens, someone's data will often look like a move into a
facility, followed by a discharge from supervision. Because the intermediate movement is 
missing, and the discharge from supervision isn't used in constructing incarceration periods,
the person's incarceration periods would show them staying in the facility indefinitely.

This CTE identifies the discharge movements that would be 'masked' because the reporting 
location isn't a facility. Most of these aren't important for constructing incarceration
periods, but the ones that are immediately preceded by a movement into a facility location 
are needed to address the case described above.
*/
masked_discharges_and_facility_moves AS (
  SELECT * 
  FROM (
    SELECT
      *,
      LAG(to_facility_loc) OVER sorted_moves AS previously_in_facility,
      LAG(EMDATETIME) OVER sorted_moves AS last_move_date
    FROM external_movements_processed
    WHERE has_facility_loc OR masked_discharge
    WINDOW sorted_moves AS (PARTITION BY OFFENDERID ORDER BY EMDATETIME)
  ) 
  WHERE masked_discharge AND previously_in_facility
),
/*
If someone has a movement into a facility followed by a discharge from supervision, the
intermediate movement from facility -> supervision should be in the SUPERVISIONEVENT table.
For the relevant 'masked' discharges, we pull in all the supervision start events that
fall between the facility movement and the supervision discharge. The earliest of these 
events, if there are any, will be used to infer the end date of the incarceration period.
*/
inferred_discharges AS (
  SELECT * 
  FROM (
    SELECT 
      OFFENDERID,
      EMDATETIME,
      EXTERNALMOVEMENTCODE,
      SEDATE,
      last_move_date
    FROM (
      SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY OFFENDERID ORDER BY SEDATE) as se_rank
      FROM (
        SELECT 
          em.*,
          se.SUPVEVENT,
          se.SEDATE
        FROM masked_discharges_and_facility_moves em
        LEFT JOIN (
          SELECT 
            *,
            CAST(
              CAST(SUPVEVNTDATE AS DATETIME) AS DATE
            ) AS SEDATE
          FROM {SUPERVISIONEVENT}
          WHERE SUPVEVENT IN (
            'G01', -- Intake New Case
            'S48', -- Sanction Released from SSP
            'S31', -- Released from Incarceration
            'S02', -- Change Type Of Supervision
            'G07', -- Transfer Within Area
            'G02', -- Transfer Within Arkansas
            'L14', -- Administrative Closure
            'S04', -- To Non-Reporting
            'L03', -- Transfer To Other State
            'S16' -- Probation Reinstated
          )
        ) se
      ON 
        em.OFFENDERID = se.OFFENDERID AND 
        -- The dates in SUPERVISIONEVENT usually don't have a time component, so we cast
        -- all the datetimes to dates in this join so that we can still identify supervision
        -- starts that occur the same day as the pre-supervision facility movement.
        SEDATE BETWEEN CAST(last_move_date AS DATE) AND CAST(EMDATETIME AS DATE)
      )
    )
    WHERE se_rank = 1
  ) 
  WHERE SEDATE is not null 
),
/*
This CTE filters out movements that don't involve a facility-type location on either side.
This would 'mask' the discharges identified above, so before filtering, we join with 
inferred_discharges, replacing certain data for the discharge movements so that a) they
aren't masked anymore (since has_facility_loc will be set to TRUE) and b) use the date
for the supervision start event, rather than the date of discharge from supervision.

In some cases, the supervision start event occurs on the same day as the final pre-supervision
facility movement.  Since SUPERVISIONEVENT usually records dates and not datetimes, we 
simply add a second to the facility movement's datetime, ensuring that the inferred 
release to supervision doesn't show up before the movement into a facility. This creates
1-second long periods in the pre-supervision facility before the person is released; we
do this instead of replacing the facility movement entirely, in case it becomes useful to know 
what facility officially released someone to supervision.
*/
external_movements_filtered AS (
  SELECT *
  FROM (
  SELECT 
    em.OFFENDERID,
    CASE 
      WHEN dis.SEDATE IS NOT NULL AND dis.SEDATE = CAST(dis.last_move_date AS DATE) 
        THEN DATETIME_ADD(dis.last_move_date,INTERVAL 1 SECOND)
      WHEN dis.SEDATE IS NOT NULL 
        THEN CAST(dis.SEDATE AS DATETIME)
      ELSE em.EMDATETIME 
    END AS EMDATETIME,
    CASE 
      WHEN dis.SEDATE IS NOT NULL THEN 'INFERRED_RELEASE_TO_SUPERVISION'
      ELSE em.EXTERNALMOVEMENTCODE 
    END AS EXTERNALMOVEMENTCODE,
    CASE 
      WHEN dis.SEDATE IS NOT NULL THEN 'UNKNOWN'
      ELSE em.REASONFORMOVEMENT 
    END AS REASONFORMOVEMENT,
    CASE 
      -- Inferred supervision releases have their location set to the 'Unknown' location code.
      WHEN dis.SEDATE IS NOT NULL THEN '0099999'  
      ELSE em.loc_to 
    END AS loc_to,
    CASE 
      WHEN dis.SEDATE IS NOT NULL THEN 'OUT' 
      ELSE em.direction 
    END AS direction,
    CASE 
      WHEN dis.SEDATE IS NOT NULL THEN TRUE
      ELSE em.has_facility_loc 
    END AS has_facility_loc,
    em.flag_90
  FROM external_movements_processed em
  LEFT JOIN inferred_discharges dis
  USING(OFFENDERID,EMDATETIME)
  )
  -- Supervision-only movements can be identified and excluded using the locations 
  -- specified in the facility_locs CTE.
  WHERE has_facility_loc 
),

-- The next 3 CTEs capture the critical dates on which someone's status meaningfully
-- changes in the following ways:
-- 1. Changes in facility
-- 2. Changes in locations within a facility
-- 3. Changes in custody level

-- Get critical dates for changes in facility
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
      direction,
      flag_90
    FROM external_movements_filtered
    WINDOW ordered_movements AS (PARTITION BY OFFENDERID ORDER BY EMDATETIME)
  ) adjacent_movements
  -- The other 2 transition date CTEs don't include the LEAD value in their WHERE
  -- condition, but it's done here to retain information about a prison term's closing.
  WHERE loc_to != last_loc_to OR last_loc_to IS NULL OR next_loc_to IS NULL
),
-- Get critical dates for changes in locations within a facility
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
-- Get critical dates for changes in custody level
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

/*
Union the 3 transition date CTEs via an outer join. This means that on dates where
only one type of transition occurs, other types of transition information will be 
null, whereas dates with multiple types of transition will have information for each.
Both of these cases are handled by coalescing the data to either retreive the non-null
value, or prioritize the values based on the transition type (for instance, if an external 
movement and a bed assignment occur on the same date, the external movement will be
used to set the new location).
*/
joined_transitions AS (
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
    COALESCE(flag_90, LAST_VALUE(flag_90 IGNORE NULLS) OVER prior_moves) AS flag_90,
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
      bed_type,
      flag_90
    FROM external_movement_transition_dates em
    FULL OUTER JOIN custody_classification_transition_dates cc
    ON em.OFFENDERID = cc.OFFENDERID AND EMDATETIME = CCDATETIME
    FULL OUTER JOIN bed_assignment_transition_dates ba
    ON em.OFFENDERID = ba.OFFENDERID AND EMDATETIME = BADATETIME
  ) transitions_coalesced
  WINDOW prior_moves AS (PARTITION BY OFFENDERID ORDER BY EMDATETIME ROWS UNBOUNDED PRECEDING)
),
/*
To avoid constructing duplicate/overlapping periods, transition data must be
deduplicated such that transition dates are unique at the person level. When there
are multiple types of transition on a given date, this CTE collapses them into a 
single transition. Different types of period information are handled differently:
person, date, and sequence number don't require processing as they will be the same
(since MOVE_SEQ is set using a RANK not ROW_NUMBER). Movement code, reason, and direction
are concatenated and separated by hyphens. Other data is taken from the transition
that specifies it: for example, if a bed assignment and custody assignment occur at the
same time, the location data is pulled from the bed assignment transition and the custody
level data is pulled from the custody assignment transition.
*/
deduped_transitions as (
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
    flag_90,
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
-- Union the unique transition dates with the data deduplicated in the previous CTE.
all_transitions AS (
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

-- Construct periods using the full set of transition dates.
basic_periods AS (
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
    td_starts.flag_90 AS flag_90,
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
-- Drop periods starting with a terminal movement, and shift those periods' exit 
-- codes/reasons to the previous period to set the correct period closure data.
sealed_periods AS (
  SELECT DISTINCT 
    OFFENDERID,
    start_date,
    end_date,
    loc,
    custody_grade,
    HOUSINGAREANAME,
    bed_type,
    flag_90,
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
  CASE 
    WHEN COALESCE(flag_90,FALSE) THEN '90_DAY'
    ELSE NULL
  END AS special_admission_type,
  entry_code,
  entry_reason,
  exit_code,
  exit_reason,
  loc AS location_id,
  op.ORGANIZATIONTYPE,
  op.ORGCOUNTYCODE,
  ROW_NUMBER() OVER (PARTITION BY sp.OFFENDERID ORDER BY sp.start_date) AS SEQ
FROM sealed_periods sp
LEFT JOIN {ORGANIZATIONPROF} op
ON loc = op.PARTYID
WHERE loc IN (
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
