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
"""Query containing supervision period information.

This query deconstructs all the critical dates related to someone's supervision term into edges, then reconstructs a
set of periods from those edges. The edges come from two sources:
1) Parole count info: the parole count is largely a non-overlapping span of time when someone is on supervision and PA
 annotates each parole count with a start / termination reason. In some cases, these stints do overlap and in this case,
 the query creates a supervision period if ONE OR MORE parole counts overlap with that period of time.
2) PO update dates: We learn about PO update dates from a different table and these often do not line up with the parole
 count dates. If someone is assigned a new PO outside of a parole count info stint, we do not create a new period.
 Inversely, if someone starts a parole count stint and there is no PO update, we will create a supervision period with
 no associated agent.
"""

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """WITH
parole_count_id_level_info_base AS (
  -- This subquery selects one row per continuous stint a person spends on supervision (i.e. per ParoleCountID), along
  -- with some information on the supervision level, admission/release reasons and start/end dates of that full stint.
  -- NOTE: These stints may be overlapping, in some cases due to data errors, in others due to overlapping stints on
  -- parole and probation. 

  SELECT * 
  FROM (
    SELECT 
      *, 
      -- If there is a row in the history table about this parole_count_id, that means this parole stint has been
      -- terminated and this is the most up to date information about this parole_count_id.
      ROW_NUMBER() OVER (PARTITION BY parole_number, parole_count_id ORDER BY is_history_row DESC) AS entry_priority
    FROM (
      -- These are rows with information on active supervision stints at the time of raw data upload, collected from multiple Release* tables.
      SELECT
        ParoleNumber as parole_number,
        ParoleCountID as parole_count_id,
        rs.RelStatusCode as status_code,
        r.RelEntryCodeOfCase as supervision_type,
        r.RelEntryCodeOfCase as parole_count_id_admission_reason,
        CONCAT(r.RelReleaseDateYear, r.RelReleaseDateMonth, r.RelReleaseDateDay) as parole_count_id_start_date,
        NULL as parole_count_id_termination_reason,
        NULL as parole_count_id_termination_date,
        ri.RelCountyResidence as county_of_residence,
        ri.RelFinalRiskGrade as supervision_level,
        0 AS is_history_row
      FROM {dbo_Release} r
      JOIN {dbo_ReleaseInfo} ri USING (ParoleNumber, ParoleCountID)
      JOIN {dbo_RelStatus} rs USING (ParoleNumber, ParoleCountID)

      UNION ALL

      -- These are rows with information on historical supervision stints. The Hist_Release table is where info associated 
      -- with the ParoleCountID goes on the completion of the supervision stint, all in one table.
      SELECT
        hr.ParoleNumber as parole_number,
        hr.ParoleCountID as parole_count_id,
        hr.HReStatcode as status_code,
        hr.HReEntryCode as supervision_type,
        hr.HReEntryCode as parole_count_id_admission_reason,
        hr.HReReldate as parole_count_id_start_date,
        hr.HReDelCode as parole_count_id_termination_reason,
        hr.HReDelDate as parole_count_id_termination_date,
        hr.HReCntyRes as county_of_residence,
        hr.HReGradeSup as supervision_level,
        1 AS is_history_row
      FROM {dbo_Hist_Release} hr
    )
  )
  WHERE entry_priority = 1
),
conditions_by_parole_count_id AS (
  SELECT
    ParoleNumber as parole_number,
    ParoleCountID as parole_count_id,
    STRING_AGG(DISTINCT CndConditionCode ORDER BY CndConditionCode) as condition_codes,
  FROM {dbo_ConditionCode} cc
  GROUP BY parole_number, parole_count_id
),
parole_count_id_level_info AS (
  -- This query returns one row per parole count id, with some cleanup of invalid rows
  SELECT 
    * REPLACE(
      # Sets the termination date to the day after the start date if it is set to a date before the start date
      IF(parole_count_id_termination_date IS NOT NULL,
         GREATEST(DATE_ADD(parole_count_id_start_date, INTERVAL 1 DAY), parole_count_id_termination_date),
         NULL) AS parole_count_id_termination_date
    ),
  FROM (
    SELECT 
    * REPLACE(
      SAFE.PARSE_DATE('%Y%m%d', parole_count_id_start_date) AS parole_count_id_start_date,
      SAFE.PARSE_DATE('%Y%m%d', parole_count_id_termination_date) AS parole_count_id_termination_date
    ),
    parole_count_id_start_date AS parole_count_id_start_date_raw,
    parole_count_id_termination_date AS parole_count_id_termination_date_raw
    FROM parole_count_id_level_info_base
    LEFT JOIN conditions_by_parole_count_id cp
    USING (parole_number, parole_count_id)
    WHERE parole_count_id != '-1'
  )
  # Filters out all supervision stints with no start date or for which the termination date does not parse (only ~10, 
  # usually because they dropped a digit).
  WHERE parole_count_id_start_date IS NOT NULL AND (
      parole_count_id_termination_date_raw IS NULL OR parole_count_id_termination_date IS NOT NULL
  )
),
start_count_edges AS (
    -- Returns a table where each row is a date someone started a parole count stint
    SELECT 
        '1-START' AS edge_type,
        parole_number,
        parole_count_id,
        CONCAT(parole_count_id, ':', supervision_type) AS started_supervision_type,
        CAST(NULL AS STRING) AS ended_supervision_type,
        condition_codes,
        parole_count_id_admission_reason AS edge_reason,
        parole_count_id_start_date AS edge_date,
        county_of_residence,
        supervision_level,
        CAST(NULL AS STRING) AS supervising_officer_name,
        CAST(NULL AS STRING) AS district_office,
        CAST(NULL AS STRING) AS district_sub_office_id,
        CAST(NULL AS STRING) AS supervision_location_org_code,
        1 AS open_delta,
    FROM parole_count_id_level_info 
),
end_count_edges AS (
    -- Returns a table where each row is a date someone ended a parole count stint. This does not necessarily mean
    -- this person has completed supervision as other ongoing parole count stints might still be open.
    SELECT 
        '3-END' AS edge_type,
        parole_number,
        parole_count_id,
        CAST(NULL AS STRING) AS started_supervision_type,
        CONCAT(parole_count_id, ':', supervision_type) AS ended_supervision_type,
        condition_codes,
        parole_count_id_termination_reason AS edge_reason,
        parole_count_id_termination_date AS edge_date,
        CAST(NULL AS STRING) AS county_of_residence,
        CAST(NULL AS STRING) AS supervision_level,
        CAST(NULL AS STRING) AS supervising_officer_name,
        CAST(NULL AS STRING) AS district_office,
        CAST(NULL AS STRING) AS district_sub_office_id,
        CAST(NULL AS STRING) AS supervision_location_org_code,
        -1 AS open_delta,
    FROM parole_count_id_level_info 
),
agent_update_dates AS (
  SELECT *
  FROM (
      SELECT
        ParoleNumber AS parole_number,
        ROW_NUMBER() OVER (
            PARTITION BY ParoleNumber, EXTRACT(DATE FROM po_modified_time) ORDER BY po_modified_time DESC
        ) AS agent_update_recency_rank, 
        ParoleCountID AS parole_count_id, 
        supervising_officer_name, 
        EXTRACT(DATE FROM po_modified_time) AS po_modified_date, 
        SupervisorName,
        SAFE_CAST(supervisor_info[SAFE_OFFSET(ARRAY_LENGTH(supervisor_info)-2)] AS INT64) AS supervision_location_org_code,
        ROW_NUMBER() OVER (PARTITION BY ParoleNumber, ParoleCountId ORDER BY po_modified_time) AS update_rank
      FROM (
        SELECT 
          ParoleNumber, ParoleCountID, AgentName AS supervising_officer_name,
          CAST(LastModifiedDateTime AS DATETIME) AS po_modified_time,
          SupervisorName, SPLIT(SupervisorName, ' ') AS supervisor_info
        FROM {dbo_RelAgentHistory}
      )
  )
  -- When there are multiple PO updates in a day, just pick the last one
  WHERE agent_update_recency_rank = 1
),
agent_update_edges_with_district AS (
    -- Returns a table where each row is a date someone was assigned a new parole officer
    SELECT
        '2-PO_CHANGE' AS edge_type,
        parole_number,
        parole_count_id,
        CAST(NULL AS STRING) AS started_supervision_type,
        CAST(NULL AS STRING) AS ended_supervision_type,
        CAST(NULL AS STRING) AS condition_codes,
        'TRANSFER_WITHIN_STATE' AS edge_reason,
        po_modified_date AS edge_date,
        CAST(NULL AS STRING) AS county_of_residence,
        CAST(NULL AS STRING) AS supervision_level,
        supervising_officer_name,
        level_2_supervision_location_external_id AS district_office,
        level_1_supervision_location_external_id AS district_sub_office_id,
        CAST(supervision_location_org_code AS STRING) AS supervision_location_org_code,
        0 AS open_delta,
    FROM agent_update_dates
    LEFT OUTER JOIN
    {RECIDIVIZ_REFERENCE_supervision_location_ids}
    ON SAFE_CAST(Org_cd AS INT64) = supervision_location_org_code
),
all_update_dates AS (
  -- Collects one row per critical date for building supervision periods for this person. This includes the start and
  -- end dates for a ParoleCountID as well as every time a supervising officer update is recorded. Officer updates may
  -- occur before the supervision stint is officially started as well as after it has ended.

  SELECT * FROM agent_update_edges_with_district
  UNION ALL
  SELECT * FROM start_count_edges
  UNION ALL
  SELECT * FROM end_count_edges
),
edges_with_sequence_numbers AS (
  -- Introduces several new fields to the edges list:
  --   sequence_number: The global sequence of all edges for a given person (parole_number)
  --   open_count: The count of open parole count info stints up to and including this edge. If this is 0, then a person
  --      is no longer on supervision
  --   block_sequence_number: Identifies all rows in a contiguous block of rows where the open_count is > 0, or
  --      contiguous block where the open_count = 0.
  --   open_block_did_change: If 1, then this edge is the first edge of a block, indicating a transition on or off
  --      supervision.
  SELECT 
    *,
    SUM(open_block_did_change) OVER (
      PARTITION BY parole_number
      ORDER BY sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS block_sequence_number
  FROM (
    SELECT 
      *,
      IF(open_block = prev_open_block, 0, 1) AS open_block_did_change
    FROM (
      SELECT 
        *,
        IF(open_count = 0, 0, 1) AS open_block,
        LAG(IF(open_count = 0, 0, 1)) OVER (PARTITION BY parole_number ORDER BY sequence_number) AS prev_open_block
      FROM (
        SELECT
          *,
          SUM(open_delta) OVER (
            PARTITION BY parole_number
            ORDER BY sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          ) AS open_count
        FROM (
          SELECT
            *,
            ROW_NUMBER() OVER (
              PARTITION BY parole_number 
              ORDER BY
                -- Sort unterminated end edges first
                IF(edge_date IS NULL, 1, 0),
                edge_date,
                edge_type DESC,
                CAST(parole_count_id AS INT64)
            ) AS sequence_number
          FROM all_update_dates
        )
      )
    )
  )
),
hydrated_edges AS (
  -- Returns a table with the same critical date edges, but with a number of NULL fields hydrated properly based on
  -- their relative position to other edges.
  SELECT
    sequence_number,
    open_count,
    open_block_did_change,
    -- The list of supervision types that the person has started up to this point, including supervision types that have
    -- been terminated.
    STRING_AGG(started_supervision_type, ',') OVER preceding_for_parole_number AS started_supervision_types,
    -- The list of supervision types that have been terminated up until this point.
    STRING_AGG(ended_supervision_type, ',') OVER preceding_for_parole_number AS ended_supervision_types,
    edge_type,
    parole_number,
    parole_count_id,
    CASE
      -- This is a nested start to a new parole count - treat it as a transfer
      WHEN edge_type = '1-START' AND open_count > 1 THEN 'TRANSFER_WITHIN_STATE'
      -- This is a nested end to a parole count when other counts are still ongoing - treat it as a transfer.
      WHEN edge_type = '3-END' AND open_count > 0 THEN 'TRANSFER_WITHIN_STATE'
      ELSE edge_reason
    END AS edge_reason,
    edge_date,
    LAST_VALUE(county_of_residence IGNORE NULLS) OVER preceding_for_parole_number AS county_of_residence,
    LAST_VALUE(supervision_level IGNORE NULLS) OVER preceding_for_parole_number AS supervision_level,
    LAST_VALUE(supervising_officer_name IGNORE NULLS) OVER preceding_for_parole_number AS supervising_officer_name,
    LAST_VALUE(district_office IGNORE NULLS) OVER preceding_for_parole_number AS district_office,
    LAST_VALUE(district_sub_office_id IGNORE NULLS) OVER preceding_for_parole_number AS district_sub_office_id,
    LAST_VALUE(supervision_location_org_code IGNORE NULLS) OVER preceding_for_parole_number AS supervision_location_org_code,
    LAST_VALUE(condition_codes IGNORE NULLS) OVER preceding_for_parole_number AS condition_codes,
  FROM edges_with_sequence_numbers 
  WINDOW preceding_for_parole_number AS (
    PARTITION BY parole_number, block_sequence_number
    ORDER BY sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  )
),
filtered_edges AS (
  -- Further processes the edges to remove PO updates that happen outside the context of a parole count info stint
  -- and to subtract the ended_supervision_types list from the started_supervision_types list to get the list of 
  -- currently ongoing supervision types.
  SELECT 
    parole_number,
    sequence_number,
    supervision_types,
    edge_type,
    edge_reason,
    edge_date,
    county_of_residence,
    district_office,
    district_sub_office_id,
    supervision_location_org_code,
    supervision_level,
    supervising_officer_name,
    condition_codes,
    open_count
  FROM hydrated_edges, UNNEST([(
        -- Subtracts ended types from started types to return the list of current supervision types someone is on
        SELECT
          -- Strips the parole count id from the level and aggregates distinct ongoing levels
          STRING_AGG(supervision_type, ',' ORDER BY supervision_type)
        FROM (
            SELECT
              -- Strips the parole count id from the level
              SPLIT(started_level, ':')[OFFSET(1)] AS supervision_type
            FROM 
              UNNEST(SPLIT(hydrated_edges.started_supervision_types, ',')) AS started_level 
            LEFT OUTER JOIN 
              UNNEST(SPLIT(hydrated_edges.ended_supervision_types, ','))  AS ended_level
            ON started_level = ended_level
            WHERE ended_level IS NULL
        )
    )]) AS supervision_types
  WHERE open_count > 0 OR open_block_did_change = 1
),
supervision_periods AS (
  -- Turns the edges into a set of date span periods.
  SELECT
    parole_number,
    sequence_number,
    supervision_types,
    supervising_officer_name,
    district_office,
    district_sub_office_id,
    supervision_location_org_code,
    edge_date AS admission_date, 
    edge_reason AS admission_reason,
    county_of_residence,
    supervision_level,
    condition_codes,
    LEAD(edge_date) OVER parole_number_window AS termination_date,
    LEAD(edge_reason) OVER parole_number_window AS termination_reason,
    open_count
  FROM 
    filtered_edges
  WINDOW parole_number_window AS (PARTITION BY parole_number ORDER BY sequence_number) 
)

SELECT 
    parole_number,
    -- Recompute sequence number post-filtering to maintain consecutive sequence numbers
    ROW_NUMBER() OVER (PARTITION BY parole_number ORDER BY sequence_number) AS period_sequence_number,
    supervision_types,
    admission_reason,
    admission_date AS start_date,
    termination_reason,
    termination_date,
    county_of_residence,
    district_office,
    district_sub_office_id,
    supervision_location_org_code,
    supervision_level,
    supervising_officer_name,
    condition_codes
FROM supervision_periods
-- Filter out periods created that start with termination edges
WHERE open_count != 0
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_pa",
    ingest_view_name="supervision_period_v2",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="parole_number ASC, period_sequence_number ASC",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
