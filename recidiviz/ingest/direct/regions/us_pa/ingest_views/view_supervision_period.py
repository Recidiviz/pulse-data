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
"""Query containing supervision period information."""

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import \
    DirectIngestPreProcessedIngestViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """WITH
parole_count_id_level_info_base AS (
  -- This subquery selects one row per continuous stint a person spends on supervision (i.e. per ParoleCountID), along
  -- with some information on the supervision level, admission/release reasons and start/end dates of that full stint.

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
  SELECT * EXCEPT (parole_count_id_start_date, parole_count_id_termination_date), 
    SAFE.PARSE_DATE('%Y%m%d', parole_count_id_start_date) AS parole_count_id_start_date,
    SAFE.PARSE_DATE('%Y%m%d', parole_count_id_termination_date) AS parole_count_id_termination_date
  FROM parole_count_id_level_info_base
  LEFT JOIN conditions_by_parole_count_id cp
  USING (parole_number, parole_count_id)
),
agent_update_dates AS (
  SELECT 
    ParoleNumber AS parole_number, 
    ParoleCountID AS parole_count_id, 
    supervising_officer_name, 
    EXTRACT(DATE FROM po_modified_time) AS po_modified_date, 
    SupervisorName,
    SAFE_CAST(supervisor_info[SAFE_OFFSET(ARRAY_LENGTH(supervisor_info)-2)] AS INT64) AS district_sub_office_id,
    ROW_NUMBER() OVER (PARTITION BY ParoleNumber, ParoleCountId ORDER BY po_modified_time) AS update_rank
  FROM (
    SELECT 
      ParoleNumber, ParoleCountID, AgentName AS supervising_officer_name,
      CAST(LastModifiedDateTime AS DATETIME) AS po_modified_time,
      SupervisorName, SPLIT(SupervisorName, ' ') AS supervisor_info
    FROM {dbo_RelAgentHistory}
  )
),
agent_update_dates_with_district AS (
    SELECT agent_update_dates.*, DistrictOfficeCode AS district_office
    FROM agent_update_dates
    LEFT OUTER JOIN
    {dbo_LU_PBPP_Organization}
    ON SAFE_CAST(Org_cd AS INT64) = district_sub_office_id
),
all_update_dates AS (
  -- Collects one row per critical date for building supervision periods for this person. This includes the start and
  -- end dates for a ParoleCountID as well as every time a supervising officer update is recorded. Officer updates may
  -- occur before the supervision stint is officially started as well as after it has ended.

  -- All PO update dates
  SELECT 
    parole_number,
    parole_count_id, 
    po_modified_date,
    update_rank,
    supervising_officer_name,
    district_office,
    district_sub_office_id,
    0 AS is_termination_edge
  FROM agent_update_dates_with_district
 
  UNION ALL

  -- Start dates for the ParoleCountID - will be used if the date comes before the first PO update date for this 
  -- ParoleCountID.
  SELECT 
    parole_number,
    parole_count_id, 
    parole_count_id_start_date AS po_modified_date,
    0 AS update_rank,
    NULL AS supervising_officer_name,
    -- We don't use the district office info from dbo_Release/dbo_Hist_Release because those tables have the *most 
    -- recent* district office and this should be the district office at the very beginning of the supervision stint.
    -- We leave the office blank rather than taking a guess in this case.
    NULL AS district_office,
    NULL AS district_sub_office_id,
    0 AS is_termination_edge
  FROM parole_count_id_level_info

  UNION ALL
 
  -- End dates for the ParoleCountID. Will only ever be used as a termination edge for a supervision period.
  SELECT 
    parole_number,
    parole_count_id, 
    COALESCE(parole_count_id_termination_date, DATE(9999, 09, 09)) AS po_modified_date,
    99999 AS update_rank,
    -- These PO/district values are never used on a resulting period since they will be pulled from the start edge.
    NULL AS supervising_officer_name,
    NULL AS district_office,
    NULL AS district_sub_office_id,
    1 AS is_termination_edge
  FROM parole_count_id_level_info
),
filtered_update_dates AS (
  -- Orders all edges within a parole_count_id by date, drops the ParoleCountID edges that occurred after someone was
  -- already assigned to a PO so we don't make a supervision period with a null PO.
  SELECT 
    * ,
    ROW_NUMBER() OVER (PARTITION BY parole_number, parole_count_id
                       ORDER BY all_edges_rank) AS edge_sequence_number, 
    ROW_NUMBER() OVER (PARTITION BY parole_number, parole_count_id
                       ORDER BY all_edges_rank DESC) AS edge_sequence_number_reverse,
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY parole_number, parole_count_id
                                 ORDER BY po_modified_date, update_rank) AS all_edges_rank
    FROM all_update_dates
  )
  WHERE (update_rank != 0 OR all_edges_rank = 1)
),
supervision_periods_base AS (
  -- Build supervision periods from date edges, dropping periods that come after the parole stint has ended, rejoin with
  -- ParoleCountID-level info.
  SELECT
    start_edge.parole_number, 
    start_edge.parole_count_id, 
    start_edge.edge_sequence_number AS start_edge_sequence_number,
    start_edge.supervising_officer_name,
    start_edge.district_office,
    start_edge.district_sub_office_id,
    start_edge.po_modified_date AS start_date, 
    end_edge.po_modified_date AS termination_date
  FROM 
  filtered_update_dates start_edge
  LEFT OUTER JOIN
  filtered_update_dates end_edge
  ON 
    start_edge.parole_number = end_edge.parole_number AND 
    start_edge.parole_count_id = end_edge.parole_count_id AND 
    start_edge.edge_sequence_number = end_edge.edge_sequence_number - 1
  WHERE start_edge.is_termination_edge != 1 AND start_edge.po_modified_date < end_edge.po_modified_date
),
supervision_periods_date_filtered AS (
  SELECT * EXCEPT(termination_date),
    IF(termination_date = DATE(9999, 09, 09), NULL, termination_date) AS termination_date,
    ROW_NUMBER() OVER (PARTITION BY parole_number, parole_count_id 
                       ORDER BY start_edge_sequence_number) AS period_sequence_number,
    ROW_NUMBER() OVER (PARTITION BY parole_number, parole_count_id
                       ORDER BY start_edge_sequence_number DESC) AS period_sequence_number_reverse
  FROM 
    supervision_periods_base 
  LEFT OUTER JOIN
    parole_count_id_level_info
  USING (parole_number, parole_count_id)
  WHERE parole_count_id != '-1' AND (
    parole_count_id_level_info.parole_count_id_termination_date IS NULL OR 
    supervision_periods_base.start_date <= parole_count_id_level_info.parole_count_id_termination_date)

),
supervision_periods AS (
  SELECT
    parole_number,
    parole_count_id,
    period_sequence_number,
    status_code,
    supervision_type,
    IF(period_sequence_number = 1,
       parole_count_id_admission_reason, 'TRANSFER_WITHIN_STATE') AS admission_reason,
    start_date,
    IF(period_sequence_number_reverse = 1 AND termination_date IS NOT NULL,
       parole_count_id_termination_reason, 
       IF(termination_date IS NOT NULL, 'TRANSFER_WITHIN_STATE', NULL)) AS termination_reason,
    termination_date,
    county_of_residence,
    district_office,
    district_sub_office_id,
    supervision_level,
    supervising_officer_name,
    condition_codes,
  FROM 
    supervision_periods_date_filtered
)
SELECT *
FROM supervision_periods
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region='us_pa',
    ingest_view_name='supervision_period',
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols='parole_number ASC, parole_count_id ASC'
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
