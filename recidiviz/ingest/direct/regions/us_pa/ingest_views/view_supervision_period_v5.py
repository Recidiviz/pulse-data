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
"""Query containing supervision period information.

Supervision periods are aggregated together from across the "Release" tables, namely `dbo_Release`, `dbo_ReleaseInfo`,
`dbo_RelStatus`, `dbo_RelAgentHistory`, and `dbo_Hist_Release`. These tables collectively include information on
"releases" from incarceration to parole, including changes in status and changes in supervising officer, supervision level, and supervision status for both
currently and previously supervised people.

Since probation is supervised by the county in PA, the vast majority of supervision periods in this view are parole periods.

Remaining known issues:
- There are some overlapping supervision periods due to some people having multiple ParoleNumbers that cannot be associated until ingest time (since PA IDs are complicated and require graph searching)
"""

from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""WITH

-- This CTE grabs all parole start info (parole start date and admission reason) from the history table 
-- (which holds all release info that's archived once a person is discharged) and from the current table.
-- In cases where release info appears for a person and parole count in both the current and archived table
-- (which happens in cases where the info has been archived sometime after we first started receiving data
-- from PA), we'll prioritize the final info from the history table.
-- We filter out ParoleCountIds that are "-1" (which are invalid/errors) and "0" (which usually denote incarceration related info).
-- Parole counts -1 do no appear in teh dbo_Release table.

all_parole_start_info AS (
    SELECT DISTINCT
      ParoleNumber,
      ParoleCountId,
      SAFE.PARSE_DATE('%Y%m%d', HReReldate) AS parole_start_date,
      HreEntryCode as admission_reason
    FROM {{dbo_Hist_Release}}
    WHERE ParoleCountId NOT IN ('0', '-1')
    
    UNION DISTINCT 
    
    SELECT DISTINCT 
      ParoleNumber,
      ParoleCountId,
      DATE(CAST(RelReleaseDateYear AS INT64), CAST(RelReleaseDateMonth AS INT64), CAST(RelReleaseDateDay AS INT64)) AS parole_start_date,
      RelEntryCodeofCase AS admission_reason
    FROM {{dbo_Release}}
    WHERE ParoleCountId <> '0'
),

-- This CTE grabs the start and end dates for all parole terms we want to keep in our final 
-- set of supervision periods.  We exclude parole terms that have deletion codes 50 and 51 since
-- those are parole terms that were recorded in error.  We also exclude parole terms that start
-- and end on the same day as those are likely also data entry errors.  We only observe information
-- about deletion codes and end dates from the dbo_Hist_Release table which is why we have to join
-- that table on in this CTE.

parole_counts_to_keep AS (
  SELECT DISTINCT
    ParoleNumber,
    ParoleCountId,
    parole_start_date,
    SAFE.PARSE_DATE('%Y%m%d', HReDelDate) AS parole_end_date
  FROM all_parole_start_info
  LEFT JOIN {{dbo_Hist_Release}} USING(ParoleNumber, ParoleCountId)
  WHERE ParoleCountId NOT IN ('0', '-1')
       AND (HReDelCode IS NULL OR HReDelCode NOT IN ('50', '51'))
       AND (HReDelDate IS NULL OR HReReldate < HReDelDate)
),

-- Since people can be on dual supervision in PA, they could have multiple releases
-- from incarceration recorded from them on the same day with different admission reasons
-- (ex: both parole and special probation).  This CTE dedups parole start information to
-- one row per date by string aggregating the distinct admission reasons that occur on the same
-- date.  We also note the highest parole count id associated with this parole start date for
-- reference later.

deduped_parole_starts AS (
  SELECT
    ParoleNumber,
    parole_start_date,
    parole_start_date AS key_date,
    STRING_AGG(DISTINCT admission_reason ORDER BY admission_reason) AS admission_reason,
    MAX(CAST(ParoleCountId as INT64)) AS highest_parole_count_start
  FROM all_parole_start_info
  INNER JOIN parole_counts_to_keep USING(ParoleNumber, ParoleCountId, parole_start_date)
  GROUP BY 1,2,3
),


-- This CTE compiles parole end/discharge information (termination reason and parole date) as well
-- as supervision attributes at the time of discharge (status code, supervision county, supervision
-- level, and district office).  Since there are sometimes multiple discharges recorded on the same
-- day (in cases of dual supervision), this CTE dedups it to a single set of parole end information
-- per date by prioritizing the parole end record with the highest HReleaseId (which are assigned
-- sequentially).  We also track the highest parole count id associated with this parole end date 
-- for reference later.

parole_ends AS (
  SELECT DISTINCT
    ParoleNumber,
    HReStatcode AS status_code,
    HReDelCode as termination_reason,
    SAFE.PARSE_DATE('%Y%m%d', HReDelDate) AS parole_end_date,
    SAFE.PARSE_DATE('%Y%m%d', HReDelDate) AS key_date,
    HReCntyRes AS county,
    HReGradeSup AS supervision_level,
    HReDo AS district_office,
    MAX(CAST(ParoleCountId AS INT64)) 
      OVER(PARTITION BY ParoleNumber, SAFE.PARSE_DATE('%Y%m%d', HReDelDate)) AS highest_parole_count_end
  FROM {{dbo_Hist_Release}}
  INNER JOIN parole_counts_to_keep USING(ParoleNumber, ParoleCountId)
  QUALIFY ROW_NUMBER() 
        OVER(PARTITION BY ParoleNumber, key_date
              ORDER BY CAST(HReleaseId as INT64) DESC) = 1
),

-- This CTE tracks updates in release info such as county and district office over time.
-- In cases where there are multiple updates to release info on the same date, this CTE
-- prioritizes the updates associated with the highest parole count id.
-- Note: because there are no dates associated with the updates for these fields in the 
--       raw table, we use the update_datetime of the file as an approximation.

dbo_ReleaseInfo_other AS (
  SELECT DISTINCT
      ParoleNumber,
      RelCountyResidence AS county,
      RelDO as district_office,
      update_datetime AS key_date
  FROM {{dbo_ReleaseInfo@ALL}}
  INNER JOIN parole_counts_to_keep USING(ParoleNumber, ParoleCountId)
  WHERE (RelCountyResidence IS NOT NULL OR RelDO IS NOT NULL)
  QUALIFY ROW_NUMBER() 
          OVER(PARTITION BY ParoleNumber, key_date
                ORDER BY CAST(ParoleCountId AS INT64) DESC) = 1
),

-- This CTE tracks updates in supervision level from the release info table over time.
-- In cases where there are multiple updates to supervision level on the same date, this CTE
-- prioritizes the updates associated with the highest parole count id.

dbo_ReleaseInfo_levels AS (
  SELECT DISTINCT
      ParoleNumber, 
      GREATEST(
        DATE(CAST(relCurrentRiskGradeDateYear AS INT64), 
            CAST(relCurrentRiskGradeDateMonth AS INT64), 
            CAST(relCurrentRiskGradeDateDay AS INT64)),
        parole_start_date
      ) AS key_date,
      RelFinalRiskGrade as supervision_level,
  FROM {{dbo_ReleaseInfo@ALL}}
  INNER JOIN parole_counts_to_keep USING(ParoleNumber, ParoleCountId)
  WHERE relCurrentRiskGrade IS NOT NULL
  QUALIFY ROW_NUMBER() 
    OVER(PARTITION BY ParoleNumber, key_date
        ORDER BY CAST(ParoleCountId AS INT64) DESC) = 1 
),

-- This CTE tracks updates in supervision status from the status table over time.
-- If the status was updated before the parole start date for this parole count id, we
-- associate the status update with the parole start date.
-- In cases where there are multiple updates to supervision status on the same date, this CTE
-- prioritizes the updates associated with the highest parole count id and the most recent update_datetime.

dbo_RelStatus AS (
  SELECT DISTINCT
    ParoleNumber,
    ParoleCountId,
    RelStatusCode AS status_code,
    GREATEST(
      DATE(CAST(RelStatusDateYear AS INT64), CAST(RelStatusDateMonth AS INT64), CAST(RelStatusDateDay AS INT64)),
      parole_start_date
    ) AS key_date,
    update_datetime
  FROM {{dbo_RelStatus@ALL}}
  INNER JOIN parole_counts_to_keep USING(ParoleNumber, ParoleCountId)
  WHERE RelStatusCode IS NOT NULL
  QUALIFY ROW_NUMBER() 
      OVER(PARTITION BY ParoleNumber, key_date
           ORDER BY CAST(ParoleCountId AS INT64) DESC, update_datetime DESC) = 1
),

-- This CTE tracks updates in supervision officer assignment from the officer assignment table over time.
-- If the supervision officer was updated before the parole start date for this parole count id, we
-- associate the supervision officer update with the parole start date.
-- In cases where there are multiple updates to supervision officer on the same date, this CTE
-- prioritizes the updates associated with the highest parole count id and the last assignment based on timestamp.
-- In cases where a single officer has multiple employee numbers, we associate the officer with the most recently 
-- seen employee number.
-- Finally, we also use the supervision location org code found in the supervisor field of the officer assignment
-- table to glean some additional supervision location data using a location reference table we've compiled.

dbo_RelAgentHistory AS (

    WITH 
      -- For each agent (identified by agent name), get the most recent EmpNum id for each person.
      -- There are only 16 times where a staff member has multiple EmpNum over time
      agent_employee_numbers AS (
        SELECT
          AgentName,
          Agent_EmpNum,
        FROM {{dbo_RelAgentHistory}}
        QUALIFY ROW_NUMBER() 
                  OVER(PARTITION BY AgentName 
                      ORDER BY CAST((Agent_EmpNum IS NOT NULL) AS INT64) DESC, CAST(LastModifiedDateTime AS DATETIME) DESC, ParoleNumber DESC, CAST(ParoleCountID AS INT64) DESC) = 1
      )

      SELECT 
        * EXCEPT(level_2_supervision_location_external_id, level_1_supervision_location_external_id, supervision_location_org_code),
        level_2_supervision_location_external_id AS district_office,
        level_1_supervision_location_external_id AS district_sub_office_id,
        CAST(supervision_location_org_code AS STRING) AS supervision_location_org_code,
      FROM (
          SELECT DISTINCT 
            ParoleNumber,
            CASE 
              WHEN AgentName LIKE '%Vacant, Position%' OR AgentName LIKE '%Position, Vacant%' 
                THEN 'VACANT'
              ELSE curr.Agent_EmpNum
              END AS supervising_officer_id,
            GREATEST(DATE(CAST(LastModifiedDateTime AS DATETIME)), parole_start_date) AS key_date,
            CAST(SPLIT(SupervisorName, ' ')[SAFE_OFFSET(ARRAY_LENGTH(SPLIT(SupervisorName, ' '))-2)] AS INT64) AS supervision_location_org_code,
          FROM {{dbo_RelAgentHistory}}
          LEFT JOIN agent_employee_numbers curr USING(AgentName)
          INNER JOIN parole_counts_to_keep USING(ParoleNumber, ParoleCountId)
          WHERE AgentName IS NOT NULL
          QUALIFY ROW_NUMBER() OVER(PARTITION BY ParoleNumber, key_date
                                ORDER BY CAST(ParoleCountId AS INT64) DESC, CAST(LastModifiedDateTime AS DATETIME) DESC) = 1
      )
    LEFT JOIN {{RECIDIVIZ_REFERENCE_supervision_location_ids}}
      ON CAST(Org_cd as INT64) = supervision_location_org_code
),

-- This CTE tracks updates in supervision level from the parolee table over time.
-- In cases where there are multiple updates to supervision level on the same date, this CTE
-- prioritizes the updates associated with the highest parole count id and most recent update datetime.
-- We also note the earliest supervision level date that we observe in this table for each person for
-- reference later (when we choose which source of data to get supervision level from).

dbo_Parolee_levels AS (
  SELECT *,
    MIN(key_date) OVER(PARTITION BY ParoleNumber) AS first_appearance_date
  FROM (
    SELECT distinct 
        ParoleNumber, 
        Grade AS supervision_level, 
        GREATEST(COALESCE(SAFE.PARSE_DATE('%m/%d/%Y', GradeDate), SAFE.PARSE_DATE('%d/%m/%Y', GradeDate)), parole_start_date) AS key_date,
    FROM {{dbo_Parolee@ALL}}
    INNER JOIN parole_counts_to_keep USING(ParoleNumber, ParoleCountId)
    WHERE 
      Grade IS NOT NULL
      AND COALESCE(SAFE.PARSE_DATE('%m/%d/%Y', GradeDate), SAFE.PARSE_DATE('%d/%m/%Y', GradeDate))  IS NOT NULL
    QUALIFY ROW_NUMBER() 
            OVER(PARTITION BY ParoleNumber, key_date
                  ORDER BY CAST(ParoleCountId as INT64) DESC, update_datetime DESC) = 1
  )
),

-- This CTE tracks updates to the special conditions imposed by the parole board over time.
-- We pull special conditions from the condition code table and join on the board action table
-- to get the date the parole board imposed the conditions.  We then filter down to just special 
-- conditions (which are the condition codes bookended by the START and END condition codes), and 
-- then string aggregate all conditions associated with a single ParoleNumber, ParoleCountId, and key_date.
-- 
-- Sometimes special conditions for supervision are imposed for ParoleCountId = 0, which sometimes
-- happens before a person's release onto supervision, or in other cases during an active parole term.
-- In these cases, we reassign the ParoleCountId from 0 to be (in the following priority order):
--   * whatever non-zero parole count id period the parole board action date overlaps with
--   * whatever the next non-zero parole count id period is after the parole board action date
-- If neither of those exist, then we leave ParoleCountId as 0.
-- 
-- Based on this newly revised ParoleCountId, if the board action date is before
-- the parole start date, we associate the special conditions with the parole start date.
--
-- Finally, if there are multiple sets of conditions assigned on the same day, we prioritize
-- the set associated with the highest parole count id.

dbo_ConditionCode AS (
  WITH 
  special_conditions AS (
    SELECT DISTINCT *
    FROM (
      SELECT
        ParoleNumber,
        ParoleCountId,
        BdActionID,
        DATE(CAST(BdActDateYear AS INT64), CAST(BdActDateMonth AS INT64), CAST(BdActDateDay AS INT64)) AS board_action_date,
        CndConditionCode,
        SUM(CAST((CndConditionCode = "START") AS INT64)) OVER (wind) AS conditions_started,
        SUM(CAST((CndConditionCode = "END") AS INT64)) OVER (wind) AS conditions_ended,
      FROM {{dbo_ConditionCode}} cc
      -- there are only 6 cases where board action date gets updated for a unique ParoleNumber, ParoleCountId, BdActionID over time, 
      -- and they all happened in the 1990's so going to ignore those changes 
      LEFT JOIN {{dbo_BoardAction}} ba 
        USING(ParoleNumber, ParoleCountId, BdActionID)
      WINDOW wind AS (
        PARTITION BY ParoleNumber, ParoleCountId, BdActionID ORDER BY CAST(ConditionCodeID AS INT64) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      )
    )
    WHERE conditions_started = 1 AND conditions_ended = 0 AND CndConditionCode <> 'START' 
  ),
  special_conditions_with_pcs AS (
    SELECT DISTINCT * EXCEPT(ParoleCountId),
      CASE WHEN ParoleCountId = '0'
            THEN COALESCE(current_pc, next_pc, CAST(ParoleCountId as INT64))
           ELSE CAST(ParoleCountId as INT64)
           END AS ParoleCountId,
    COALESCE(GREATEST(board_action_date, parole_start_date), board_action_date) AS key_date
    FROM (
      SELECT DISTINCT
        sp.*,
        COALESCE(
          LEAST(pc_current.parole_start_date, pc_next.parole_start_date),
          COALESCE(pc_current.parole_start_date, pc_next.parole_start_date)
        ) AS parole_start_date,
        MIN(CAST(pc_next.ParoleCountId AS INT64)) OVER(PARTITION BY sp.ParoleNumber, sp.ParoleCountId, BdActionID) AS next_pc,
        MIN(CAST(pc_current.ParoleCountId AS INT64)) OVER(PARTITION BY sp.ParoleNumber, sp.ParoleCountId, BdActionID) AS current_pc,
      FROM special_conditions sp
      LEFT JOIN parole_counts_to_keep pc_next
        ON board_action_date < pc_next.parole_start_date AND sp.ParoleNumber = pc_next.ParoleNumber
      LEFT JOIN parole_counts_to_keep pc_current
        ON board_action_date BETWEEN pc_current.parole_start_date AND COALESCE(pc_current.parole_end_date, DATE(9999,1,1)) AND sp.ParoleNumber = pc_current.ParoleNumber
    )
  )

  SELECT *
  FROM (
    SELECT
        special_conditions_with_pcs.ParoleNumber,
        special_conditions_with_pcs.ParoleCountId,
        GREATEST(key_date, pc_keep.parole_start_date) AS key_date,
        STRING_AGG(DISTINCT CndConditionCode, ',' ORDER BY CndConditionCode) AS condition_codes,
    FROM special_conditions_with_pcs
    INNER JOIN parole_counts_to_keep pc_keep
      ON special_conditions_with_pcs.ParoleNumber = pc_keep.ParoleNumber
      AND special_conditions_with_pcs.ParoleCountId = CAST(pc_keep.ParoleCountId AS INT64)
    GROUP BY 1,2,3
  )
  QUALIFY ROW_NUMBER() OVER(PARTITION BY ParoleNumber, key_date ORDER BY CAST(ParoleCountId AS INT64) DESC) = 1
),

-- This CTE joins all key dates and attributes from each source into a single table using ParoleNumber and key_date.
-- In this CTE, we also make some decisions about which data source to use for attributes that have multiple sources:
--
--   * If the key date is before the first appearance of supervision level in the dbo_Parolee table, we source level
--     from the parole end history table or from the release info table.  If the key date is after the first appearnce fo supervision
--     level in the dbo_Parolee table, we source level from the dbo_Parolee table.
--
--   * We prioritize district office information from the officer assignment, then from the parole end history table, and then finally
--     from the release info table
--
--   *  We prioritize status information from the parole ends history table and then the status table.
--
-- We also keep track of the highest parole count started vs ended at the time of each key date for reference later.

joined AS (
  SELECT
      ParoleNumber,
      key_date,
      admission_reason,
      deduped_parole_starts.parole_start_date,
      COALESCE(parole_ends.status_code, dbo_RelStatus.status_code) AS status_code,
      parole_end_date,
      termination_reason,
      COALESCE(parole_ends.county, dbo_ReleaseInfo_other.county) AS county_of_residence,
      CASE WHEN key_date < appearance.first_appearance_date THEN COALESCE(parole_ends.supervision_level, dbo_ReleaseInfo_levels.supervision_level)
          ELSE dbo_Parolee_levels.supervision_level
          END AS supervision_level,
      COALESCE(dbo_RelAgentHistory.district_office, parole_ends.district_office, dbo_ReleaseInfo_other.district_office) AS district_office,
      NULLIF(supervising_officer_id, 'VACANT') as supervising_officer_id,
      district_sub_office_id,
      supervision_location_org_code,
      condition_codes,
      MAX(COALESCE(highest_parole_count_start, 0)) OVER(PARTITION BY ParoleNumber ORDER BY key_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS highest_parole_count_started,
      MAX(COALESCE(highest_parole_count_end, 0)) OVER(PARTITION BY ParoleNumber ORDER BY key_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS highest_parole_count_ended,
  FROM deduped_parole_starts
  FULL OUTER JOIN parole_ends USING(ParoleNumber, key_date)
  FULL OUTER JOIN dbo_ReleaseInfo_other USING(ParoleNumber, key_date)
  FULL OUTER JOIN dbo_ReleaseInfo_levels USING(ParoleNumber, key_date)
  FULL OUTER JOIN dbo_RelStatus USING(ParoleNumber, key_date)
  FULL OUTER JOIN dbo_RelAgentHistory USING(ParoleNumber, key_date)
  FULL OUTER JOIN dbo_Parolee_levels USING(ParoleNumber, key_date)
  FULL OUTER JOIN dbo_ConditionCode USING(ParoleNumber, key_date)
  LEFT JOIN (SELECT distinct ParoleNumber, first_appearance_date from dbo_Parolee_levels) appearance USING(ParoleNumber)
  WHERE 
    key_date is not null
),

-- Because clients can have overlapping parole count ids in the case of dual supervision, we use this CTE
-- to reconstruct term counts to account for overlapping parole count ids.  Using the highest parole count ids
-- started and ended by each key date, we determine whether each key date corresponds with a START supervision 
-- edge (a person was previously not on supervision but now starts supervision as of this date), 
-- a TRANSITION edge (an attribute of supervision has changed but supervision has already started and continues),
-- or an END edge (all active parole count ids that have been open as of this date ends as of this date).
-- A key date with a NULL edge type is a key date associated with a time where all parole counts that were opened
-- as of that date have already been closed, and therefore shoudl be filtered out.
-- We assign a new term_count field to mark spans from a START to an END edge.

dates_by_term AS (
  SELECT 
    *,
    SUM(CAST(edge_type = 'START' as INT64)) OVER(PARTITION BY ParoleNumber ORDER BY key_date) AS term_count,
  FROM (
    SELECT *,
      CASE
          WHEN COALESCE(prev_highest_parole_count_started, 0) = COALESCE(prev_highest_parole_count_ended, 0)
                AND highest_parole_count_started > highest_parole_count_ended
              THEN "START"
          WHEN highest_parole_count_started > highest_parole_count_ended
              THEN "TRANSITION"
          WHEN highest_parole_count_started = highest_parole_count_ended
                AND parole_end_date IS NOT NULL
              THEN "END"
          ELSE NULL
        END AS edge_type        
    FROM (
      SELECT *,
        LAG(highest_parole_count_started) OVER(PARTITION BY ParoleNumber ORDER BY key_date) AS prev_highest_parole_count_started,
        LAG(highest_parole_count_ended) OVER(PARTITION BY ParoleNumber ORDER BY key_date) AS prev_highest_parole_count_ended
      FROM joined
    )
  )
  WHERE edge_type IS NOT NULL
),

-- Since we receive updates for different supervision attributes on different dates, 
-- we use LAST_VALUE to pull in the most recent non-null value for each attribute within each term count.
-- 
-- For supervision conditions, we string aggregate all condition codes we've seen imposed up until this point
-- in the term count.  (Since STRING_AGG doesn't allow us to use distinct and order by in the same function all,
-- there might be some duplicates in condition codes values in this step if two sets of conditions
-- were imposed within a single term count, and some of the conditions overlapped)

filled_in_last_value AS (
  SELECT
    ParoleNumber,
    term_count,
    key_date,
    admission_reason,
    termination_reason,
    LAST_VALUE(admission_reason IGNORE NULLS) OVER(term_window_backwards) AS supervision_type,
    LAST_VALUE(status_code IGNORE NULLS) OVER(term_window_backwards) AS status_code,
    LAST_VALUE(county_of_residence IGNORE NULLS) OVER(term_window_backwards) AS county_of_residence,
    LAST_VALUE(supervision_level IGNORE NULLS) OVER(term_window_backwards) AS supervision_level,
    LAST_VALUE(district_office IGNORE NULLS) OVER(term_window_backwards) AS district_office,
    LAST_VALUE(supervising_officer_id IGNORE NULLS) OVER(term_window_backwards) AS supervising_officer_id,
    LAST_VALUE(district_sub_office_id IGNORE NULLS) OVER(term_window_backwards) AS district_sub_office_id,
    LAST_VALUE(supervision_location_org_code IGNORE NULLS) OVER(term_window_backwards) AS supervision_location_org_code,
    STRING_AGG(condition_codes, ',') OVER(term_window_backwards) AS condition_codes,
    edge_type
  FROM dates_by_term
  WINDOW term_window_backwards AS (
    PARTITION BY ParoleNumber, term_count ORDER BY key_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
),

-- For supervision attributes that come from tables that don't have an update date
-- specifically associated with that update and instead we use the update_datetime
-- of when we received the file (which means that there will be cases where we 
-- see a delay between the actual update and when we actually see it appear in the data,
-- especially if they're updates made before we started receiving the data),
-- we also fill in any remaining nulls by looking forward within a term count and 
-- finding the first non-null value for that attribute.

filled_in_first_value AS (
  SELECT
    ParoleNumber,
    term_count,
    key_date,
    admission_reason,
    termination_reason,
    supervision_type,
    -- we will miss some statuses from earlier years because we only use last value and not first value
    -- but i think that's fine since we don't usually look that far back and we don't map anything using
    -- the status code we would assume by default (23 = active).
    status_code,
    FIRST_VALUE(county_of_residence IGNORE NULLS) OVER(term_window_forward) AS county_of_residence,
    FIRST_VALUE(supervision_level IGNORE NULLS) OVER(term_window_forward) AS supervision_level,
    FIRST_VALUE(district_office IGNORE NULLS) OVER(term_window_forward) AS district_office,
    supervising_officer_id,
    district_sub_office_id,
    supervision_location_org_code,
    condition_codes,
    edge_type
  FROM filled_in_last_value
  WINDOW term_window_forward AS (
    PARTITION BY ParoleNumber, term_count ORDER BY key_date
    ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    )
),

-- This CTE constructs initial periods by using key_date as the start date of each
-- period and then using the next key date as the end date of each period. 
-- We also keep track of the next status code associated with the next key date,
-- and reassign termination reason to be the termination reason associated with the next
-- key date.
-- Finally, we filter out any of these constructed periods that have status 19 or 24
-- (deceased and deported), as well as any periods that start with an END edge.

initial_periods AS (
  SELECT *
  FROM (
    SELECT * EXCEPT(key_date, termination_reason),
      key_date AS start_date,
      LEAD(key_date) OVER(PARTITION BY ParoleNumber ORDER BY key_date) AS end_date,
      LEAD(termination_reason) OVER(term_window) AS termination_reason,
      LEAD(status_code) OVER(term_window) AS next_status_code
    FROM filled_in_first_value
    WINDOW term_window AS (
      PARTITION BY ParoleNumber, term_count ORDER BY key_date
    )
  )
  -- filter out periods where the status is deceased and deported
  WHERE (status_code IS NULL or status_code NOT IN ('19', '54'))
  -- filter to periods that start with a start edge or a transition edge
    AND edge_type IN ('START', 'TRANSITION')
),

-- Next, we stick everything through the aggregate adjacent spans function to collapse
-- adjacent spans that share the same attributes
final_periods AS (
    {aggregate_adjacent_spans(
        table_name="initial_periods",
        attribute=["admission_reason", "termination_reason", "status_code", "supervision_type", "county_of_residence", "supervision_level", "district_office", "supervising_officer_id", "district_sub_office_id", "supervision_location_org_code", "condition_codes", "next_status_code"],
        index_columns=["ParoleNumber", "term_count"])}
)

-- Finally, we take the final periods and then create a period_seq_num to use in the external id
-- and create a prev_status_code variable to use in the mapping
SELECT 
  * EXCEPT(session_id, date_gap_id, term_count),
  LAG(status_code) OVER(term_window) AS prev_status_code,
  ROW_NUMBER() OVER(PARTITION BY ParoleNumber ORDER BY start_date, end_date NULLS LAST) AS period_seq_num
FROM final_periods
WINDOW term_window AS (
  PARTITION BY ParoleNumber, term_count ORDER BY start_date, end_date NULLS LAST
)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_pa",
    ingest_view_name="supervision_period_v5",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
