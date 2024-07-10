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

TODO(#31202): In the raw data, each individual is identified by a ParoleNumber, and each supervision stint has a ParoleCountId.  This view assumes for now that each 
ParoleCountId is a distinct and non-overlapping period of supervision and thus groups/infers information by ParoleCountId.  However, this may not be true
in practice because there are some overlapping dates in the raw data (whether intentional or due to data entry errors), and so currently this draft of the
view still results in some overlapping supervision periods.  We will revise this view when we get some more clarity from PA about whether these overlapping
parole counts are meaningful or data errors.  (Since PA overwrites their data in place, it's also possible that we are observing deleted data?? Will also
get some clarity from PA about that).
"""

from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""WITH

-- From the dbo_Release table that stores currently active release information, get all information
-- we've observed over time for admission reason (RelEntryCodeofCase) and parole start date (RelReleaseDate).
-- Also pull in update_datetime as a possible period start/end date to use later.
dbo_Release_dates AS (
  SELECT 
    ParoleNumber,
    ParoleCountId,
    RelEntryCodeOfCase,
    SAFE.PARSE_DATE('%Y%m%d', CONCAT(RelReleaseDateYear, RelReleaseDateMonth, RelReleaseDateDay)) as RelReleaseDate,
    update_datetime
  from {{dbo_Release@ALL}}
),

-- From the dbo_ReleaseInfo table that stores currently active release information, get all information
-- we've observed over time for supervision level (RelFinalRiskGrade), supervision county (RelCountyResidence),
-- and supervision district (RelDo).
-- Also pull in update_datetime as a possible period start/end date to use later.
dbo_ReleaseInfo_dates AS (
  SELECT 
    ParoleNumber, 
    ParoleCountId, 
    RelFinalRiskGrade, 
    RelCountyResidence,
    RelDO,
    update_datetime
  FROM {{dbo_ReleaseInfo@ALL}}
),

-- From the dbo_RelStatus table that stores currently active release information, get all information
-- we've observed over time for supervision status (RelStatusCode).
-- Also pull in update_datetime as a possible period start/end date to use later.
dbo_RelStatus_dates AS (
  SELECT 
    ParoleNumber, 
    ParoleCountId, 
    RelStatusCode,
    update_datetime
  FROM {{dbo_RelStatus@ALL}}
),

-- Identify all parole counts that only appear in the historical release table (dbo_Hist_Release) and 
-- don't ever appear in the current release table (dbo_Release).  This encompasses all parole counts
-- whose information was already archived before we started receiving data from Pennsylvania.
historical_parole_counts AS (
  select
    DISTINCT
    ParoleNumber,
    ParoleCountId
  from {{dbo_Hist_Release@ALL}}

  except distinct 

  select 
    DISTINCT
    ParoleNumber,
    ParoleCountId
  from {{dbo_Release@ALL}}
),

-- From the dbo_ConditionCode table, get all information we've observed over time for supervision 
-- conditions (dbo_ConditionCode), filtering out ParoleCountId = -1 (which we're told are invalid data)
-- Also pull in update_datetime as a possible period start/end date to use later.
-- TODO(#31214): What do we do about ParoleCountId = 0 since ParoleCountIds = '0' only appear in this table 
-- and rarely in other release tables? Question out to PA research.
dbo_ConditionCode_dates AS (
  SELECT
    ParoleNumber,
    ParoleCountID,
    update_datetime,
    STRING_AGG(DISTINCT CndConditionCode, ',' ORDER BY CndConditionCode) as condition_codes,
  FROM {{dbo_ConditionCode@ALL}} cc
  WHERE ParoleCountId <> '-1'
  GROUP BY ParoleNumber, ParoleCountID, update_datetime
),

-- For all ParoleCountIds that were active at some point after we started receiving data from PA,
-- grab all conditions data we've observed over time
current_conditions AS (
  SELECT  dbo_ConditionCode_dates.*
  FROM dbo_ConditionCode_dates
  LEFT JOIN historical_parole_counts USING(ParoleNumber, ParoleCountID)
  WHERE historical_parole_counts.ParoleNumber IS NULL
),

-- For all ParoleCountIds that were already archived by the time we started receiving data from PA,
-- grab the last/most recent conditions 
-- (not sure why we see conditions updated in this table after the ParoleCountId had already been 
--  archived, but in those cases, we want to go with the final set of conditions)
historical_conditions AS (
  SELECT * EXCEPT(update_datetime, recency)
  FROM (
    SELECT 
      dbo_ConditionCode_dates.*,
      ROW_NUMBER() OVER(PARTITION BY ParoleNumber, ParoleCountID ORDER BY update_datetime desc) AS recency
    FROM dbo_ConditionCode_dates
    INNER JOIN historical_parole_counts USING(ParoleNumber, ParoleCountID)
  )
  WHERE recency = 1
),

-- Join all information we get from the current release tables together by ParoleNumber, ParoleCountId, and update_datetime
-- Create a row number variable that tracks ordering of rows within a parole count id
current_release_info AS (
  SELECT
    ParoleNumber, 
    ParoleCountId,
    -1 as HReleaseId,
    RelStatusCode AS status_code,
    RelEntryCodeOfCase as supervision_type,
    RelReleaseDate as parole_start_date,
    CAST(NULL AS STRING) as termination_reason,
    CAST(NULL AS DATE) as parole_end_date,
    RelCountyResidence as county_of_residence,
    RelFinalRiskGrade as supervision_level,
    RelDO as district_office,
    condition_codes,
    update_datetime,
    ROW_NUMBER() OVER(PARTITION BY ParoleNumber, ParoleCountId ORDER BY update_datetime) as rn
  FROM dbo_Release_dates
  FULL OUTER JOIN dbo_ReleaseInfo_dates USING(ParoleNumber, ParoleCountId, update_datetime)
  FULL OUTER JOIN dbo_RelStatus_dates USING(ParoleNumber, ParoleCountId, update_datetime)
  FULL OUTER JOIN current_conditions USING(ParoleNumber, ParoleCountId, update_datetime)
),

-- Gather all information from the dbo_Hist_Release table, which contains all archived release information,
-- which means this will include all releases that were archived before we started receiving data, as well
-- as all releases that have been archived since we started receiving data.
-- For each ParoleCountId, we filter down to the row with the most recent HReleaseId and updatedatetime
-- and we filter out any ParoleCountIds that are '-1' since we're told those are invalid data.
-- 
-- TODO(#31216): should we not filter down to the most recent HReleaseId?
dbo_Hist_Release_dates AS (
  SELECT * EXCEPT(recency)
  FROM (
    SELECT 
      ParoleNumber, 
      ParoleCountId,
      CAST(HReleaseId AS INT64) as HReleaseId,
      HReStatcode as status_code,
      HReEntryCode as supervision_type,
      SAFE.PARSE_DATE('%Y%m%d', HReReldate) as parole_start_date,
      HReDelCode as termination_reason,
      -- Sometimes HReDelDate is earlier than HReReldate, so reset those to be one day after (TODO(#31217) OR EXCLUDE??)
      -- As of now, there are ~700 cases of this
      GREATEST(DATE_ADD(SAFE.PARSE_DATE('%Y%m%d', HReReldate), INTERVAL 1 DAY), SAFE.PARSE_DATE('%Y%m%d', HReDelDate)) as parole_end_date,
      HReCntyRes as county_of_residence,
      HReGradeSup as supervision_level,
      HReDo as district_office,
      condition_codes,
      ROW_NUMBER() OVER(PARTITION BY ParoleNumber, ParoleCountId ORDER BY CAST(HReleaseId AS INT64) DESC, update_datetime DESC) as recency
    FROM {{dbo_Hist_Release@ALL}}
    LEFT JOIN historical_conditions USING(ParoleNumber, ParoleCountId)
    WHERE ParoleCountId <> '-1'
  )
  WHERE recency = 1
),

-- For each agent (identified by agent name), get the most recent EmpNum id for each person.
-- There are only 16 times where a staff member has multiple EmpNum over time
agent_employee_numbers AS (
  select
    AgentName,
    Agent_EmpNum
  from (
  select
    AgentName,
    Agent_EmpNum,
    ROW_NUMBER() OVER(PARTITION BY AgentName ORDER BY CAST(LastModifiedDateTime AS DATETIME) DESC, ParoleNumber, CAST(ParoleCountID AS INT64)) as recency_rank
  from {{dbo_RelAgentHistory}}
  )
  WHERE recency_rank = 1
),

-- Gather supervision agent assignments over time from the dbo_RelAgentHistory table.
-- This table is ledger-style, so instead of pulling update_datetime and using the @ALL table,
-- we can just pull from the _latest view of the original table directly and use LastModifiedDateTime
-- as the update_datetime.
-- We also merge on RECIDIVIZ_REFERENCE_supervision_location_ids to get additional supervision information.
-- This table is mapped on using the supervision location org code that's found in the SupervisorName field.
-- When there are multiple assignments within the same d ay, we keep only the final assignment on that day

dbo_RelAgentHistory_dates AS (
  select * EXCEPT(level_2_supervision_location_external_id, level_1_supervision_location_external_id, supervision_location_org_code),
    level_2_supervision_location_external_id AS district_office,
    level_1_supervision_location_external_id AS district_sub_office_id,
    CAST(supervision_location_org_code AS STRING) AS supervision_location_org_code,
  from (
    select DISTINCT
      ParoleNumber,
      CAST(ParoleCountId as INT64) as ParoleCountId,
      CAST(LastModifiedDateTime AS DATETIME) as update_datetime,
      CASE 
        WHEN AgentName LIKE '%Vacant, Position%' OR AgentName LIKE '%Position, Vacant%' 
          THEN 'VACANT'
        ELSE curr.Agent_EmpNum
        END AS supervising_officer_id,
      CAST(SPLIT(SupervisorName, ' ')[SAFE_OFFSET(ARRAY_LENGTH(SPLIT(SupervisorName, ' '))-2)] AS INT64) AS supervision_location_org_code,
      ROW_NUMBER() OVER(PARTITION BY ParoleNumber, ParoleCountId, DATE(CAST(LastModifiedDateTime AS DATETIME)) ORDER BY CAST(LastModifiedDateTime AS DATETIME) DESC) as recency
    from {{dbo_RelAgentHistory}} hist
    left join agent_employee_numbers curr USING(AgentName)
  )
  LEFT JOIN {{RECIDIVIZ_REFERENCE_supervision_location_ids}}
    ON CAST(Org_cd AS INT64) = supervision_location_org_code
  -- if there are multiple agents assigned on the same day, only keep the final one
  WHERE recency = 1
),

-- Union all the release information together and cast ParoleCountIds and HReleaseIds properly as integers for sorting purposes later
-- 
unioned_release_info_as_edges AS (

  -- for every ParoleCount that appears in the current data that's not the earliest record, add an edge for the date we observe a change in the information
  (select 
    ParoleNumber,
    status_code,
    supervision_type,
    parole_start_date,
    termination_reason,
    parole_end_date,
    county_of_residence,
    supervision_level,
    district_office,
    condition_codes,
    DATE(update_datetime) as update_datetime,
    CAST(ParoleCountId AS INT64) as ParoleCountId,
    CAST(HReleaseId AS INT64) as HReleaseId,
    CAST(NULL as STRING) as admission_reason
  from current_release_info
  where rn <> 1)

  UNION DISTINCT

  -- for every ParoleCount that appears in the current data, add a starting edge for the earliest record that starts on the parole_start_date
  (select 
    ParoleNumber,
    status_code,
    supervision_type,
    parole_start_date,
    termination_reason,
    parole_end_date,
    county_of_residence,
    supervision_level,
    district_office,
    condition_codes,
    parole_start_date as update_datetime,
    CAST(ParoleCountId AS INT64) as ParoleCountId,
    CAST(HReleaseId AS INT64) as HReleaseId,
    supervision_type as admission_reason
  from current_release_info
  where rn = 1)

  UNION DISTINCT

  -- for ParoleCounts that only appear in the historical data, add a starting edge
  -- that starts on the parole_start_date
  (select 
    * EXCEPT(HReleaseId, ParoleCountId), 
    parole_start_date as update_datetime,
    CAST(ParoleCountId AS INT64) as ParoleCountId,
    CAST(HReleaseId AS INT64) as HReleaseId,
    supervision_type as admission_reason
  from dbo_Hist_Release_dates
  inner join historical_parole_counts USING(ParoleNumber, ParoleCountId))

  UNION DISTINCT

  -- for all ParoleCounts that appear in the historical data, add an ending edge
  -- that occurs on the parole_end_date
  (select 
    * EXCEPT(HReleaseId, ParoleCountId), 
    parole_end_date as update_datetime,
    CAST(ParoleCountId AS INT64) as ParoleCountId,
    CAST(HReleaseId AS INT64) as HReleaseId,
    CAST(NULL as STRING) as admission_reason
  from dbo_Hist_Release_dates)
),

-- Filter down to only keep rows where parole start date is not null and parole start date is distinct 
-- from parole end date if parole end date is not null since both are scenarios are data entry errors
final_combined_info_from_release_tables AS (
  SELECT *
  FROM unioned_release_info_as_edges
    WHERE 
      parole_start_date IS NOT NULL
      AND (parole_start_date <> parole_end_date OR parole_end_date IS NULL)
),

-- Join on data from release tables with supervision agent assignment data, and then use LAST_VALUE
-- to fill in information across rows.
-- For termination_reason, and parole_end_date, we'll fill in with the last seen value across all rows within the parole count.
-- For all other variables, we'll fill in with the most recently seen non-null value for each row within the parole count.
supervision_statuses_filled_in AS (
  SELECT
    ParoleNumber,
    ParoleCountId,
    update_datetime,
    HReleaseId,
    admission_reason,
    LAST_VALUE(status_code IGNORE NULLS) OVER (parole_count_window) as status_code,
    LAST_VALUE(supervision_type IGNORE NULLS) OVER (parole_count_window) as supervision_type,
    LAST_VALUE(parole_start_date IGNORE NULLS) OVER (parole_count_window) as parole_start_date,
    LAST_VALUE(county_of_residence IGNORE NULLS) OVER (parole_count_window) as county_of_residence,
    LAST_VALUE(supervision_level IGNORE NULLS) OVER (parole_count_window) as supervision_level,
    LAST_VALUE(condition_codes IGNORE NULLS) OVER (parole_count_window) as condition_codes,
    LAST_VALUE(supervising_officer_id IGNORE NULLS) OVER (parole_count_window) as supervising_officer_id,
    LAST_VALUE(agent.district_office IGNORE NULLS) OVER (parole_count_window) as agent_district_office,
    LAST_VALUE(release.district_office IGNORE NULLS) OVER (parole_count_window) as release_district_office,
    LAST_VALUE(district_sub_office_id IGNORE NULLS) OVER (parole_count_window) as district_sub_office_id,
    LAST_VALUE(supervision_location_org_code IGNORE NULLS) OVER (parole_count_window) as supervision_location_org_code,
    LAST_VALUE(termination_reason IGNORE NULLS) OVER
      (
      PARTITION BY ParoleNumber, ParoleCountID
      ORDER BY update_datetime, HReleaseId
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOllOWING
      ) as termination_reason,
    LAST_VALUE(parole_end_date IGNORE NULLS) OVER
      (
      PARTITION BY ParoleNumber, ParoleCountID
      ORDER BY update_datetime, HReleaseId
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOllOWING
      ) as parole_end_date,
  FROM final_combined_info_from_release_tables release
  -- should we join on by ParoleCountId or no?
  FULL OUTER JOIN dbo_RelAgentHistory_dates agent USING(ParoleNumber, ParoleCountID, update_datetime)
  WINDOW parole_count_window AS (
    PARTITION BY ParoleNumber, ParoleCountID
    ORDER BY update_datetime, HReleaseId
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),

-- Filter down to rows where the filled in parole start date is not null and rows where update_datetime is on or after 
-- the filled in parole start date (because otherwise those would be agent assignments that occur before the supervision start date).
-- Also filter out cases that were opened in error (which end with termination reason 50).
-- Finally, create a next_update_datetime that is set to the next seen update_datetime within the ParoleCountId
supervision_statuses_cleaned AS (
  SELECT 
    *,
    COALESCE(agent_district_office, release_district_office) AS district_office,
    LEAD(update_datetime) OVER (PARTITION BY ParoleNumber, ParoleCountId ORDER BY ParoleCountId, update_datetime, HReleaseId) as next_update_datetime
  from supervision_statuses_filled_in
  WHERE 
    parole_start_date is not null
    AND update_datetime >= parole_start_date
    -- Case was not opened in error
    AND (termination_reason <> '50' or termination_reason is null)
),

-- This CTE:
--   filters down to rows where the next_update_datetime is before or on the parole_end_date, 
--   creates next_status_code and prev_status_code, 
--   sets termination_reason only for the last row for a parole count id,
--   create status_code_update,
--   filters down to where tatus is not Awaiting Death Certificate or Deported
--   start_date <= end_date
initial_periods AS (
  SELECT
    ParoleNumber,
    ParoleCountId,
    update_datetime as start_date,
    COALESCE(next_update_datetime, parole_end_date) AS end_date,
    admission_reason,
    -- only keep termination reason if it's the last row for a parole count id
    IF(next_update_datetime = parole_end_date, termination_reason, CAST(NULL AS STRING)) AS termination_reason,
    status_code,
    -- set status_code_update as status_code only if it's the first appearance of this status_code
    IF(status_code = prev_status_code, NULL, status_code) as status_code_update,
    next_status_code,
    supervision_type,
    county_of_residence,
    supervision_level,
    condition_codes,
    NULLIF(supervising_officer_id, 'VACANT') as supervising_officer_id,
    district_office,
    district_sub_office_id,
    supervision_location_org_code,
  FROM (
    SELECT
      *,
      LEAD(status_code) OVER (PARTITION BY ParoleNumber, ParoleCountId ORDER BY update_datetime, HReleaseId) as next_status_code,
      LAG(status_code) OVER (PARTITION BY ParoleNumber, ParoleCountId ORDER BY update_datetime, HReleaseId) as prev_status_code
    FROM supervision_statuses_cleaned
    WHERE (COALESCE(next_update_datetime, DATE(9999,9,9)) <= COALESCE(parole_end_date, DATE(9999,9,9)))
  )
  -- where supervision status is not:
  --   19 (Deceased - Awaiting Death Certificate)
  --   54 (Deported)
  WHERE status_code not in ('19', '54')
    -- start_date is before or equal to end date (sometimes there are overlapping ParoleCounts)
    AND update_datetime <= COALESCE(next_update_datetime, parole_end_date, DATE(9999,9,9))
),

-- Aggregate adjacent spans into a single period if the attributes of the adjacent spans are equal
final_periods AS (
    {aggregate_adjacent_spans(
        table_name="initial_periods",
        attribute=["admission_reason", "termination_reason", "status_code", "status_code_update", "next_status_code", "supervision_type", "county_of_residence", "supervision_level", "condition_codes", "supervising_officer_id", "district_office", "district_sub_office_id", "supervision_location_org_code",],
        index_columns=["ParoleNumber"])}
)

-- finally, select all columns except those created by the aggregate_adjacent_spans function,
-- create a period sequnece number, and exclude periods that have the same start and end date
select 
  * EXCEPT(session_id, date_gap_id),
  ROW_NUMBER() OVER(PARTITION BY ParoleNumber ORDER BY start_date) as period_seq_num
from final_periods
where (start_date <> end_date or end_date is null)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_pa",
    ingest_view_name="supervision_period_v5",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
