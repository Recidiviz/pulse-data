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
"""Query for supervision periods for US_TX.

    NOTE: 

    Texas sends supervision period data in a pretty counterintuitive way. It seems 
    that they join 5 separate tables using the period_ID_Number and this can lead to 
    records being joined together multiple times regardless of what is true at any given
    moment. In order to correct this, we treat the SupervisionPeriod table as if it was 5
    distinct tables each with it's corresponding fields and update time field. Here is 
    the breakdown:

    -[XREF_UPDATE_DATE]: SID_NUMBER, PERIOD_ID_NUMBER, DELETED_FLAG
    -[OSTS_UPDATE_DATE]: STATUS
    -[RMF_TIMESTAMP]: START_DATE, MAX_TERMINATION_DATE
    -[CTH_CREATION_DATE]: CASE_TYPE
    -[WTSK_UPDATE_DATE]: SUPERVISION_OFFICER


"""
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""WITH
-- Grabs all unique combinations of Period_ID_Number,start_date, and RMF_TIMESTAMP (the update datetime stamp)
start_date_cte AS (
  SELECT 
    SID_Number,
    Period_ID_Number,
    start_date,
    DATE(RMF_TIMESTAMP) as update_date,
    LAG(start_date)OVER(PARTITION BY Period_ID_Number ORDER BY RMF_TIMESTAMP asc) AS prev_start_date
  FROM `{{SupervisionPeriod@ALL}}`
  WHERE RMF_TIMESTAMP IS NOT NULL AND Deleted_Flag = "ACTIVE" AND start_date IS NOT NULL
),
-- Ranks period start_dates by update_date
rank_start_date_cte AS (
    SELECT
        SID_Number,
        Period_ID_Number,
        start_date,
        rank() OVER(PARTITION BY Period_ID_Number ORDER BY update_date DESC) AS rnk
    FROM start_date_cte
    WHERE prev_start_date IS DISTINCT FROM start_date
),
-- Grabs all unique combinations of Period_ID_Number,Max_Termination_Date, and update_date (the update datetime stamp)
Max_Termination_Date_cte AS (
  SELECT 
    SID_Number,
    Period_ID_Number,
    COALESCE(DATE(Max_termination_Date), DATE(9999,9,9)) AS Max_termination_Date,
    DATE(RMF_TIMESTAMP) as update_date,
    LAG(COALESCE(DATE(Max_termination_Date), DATE(9999,9,9))) OVER(PARTITION BY Period_ID_Number ORDER BY RMF_TIMESTAMP asc) AS prev_Max_Termination_Date
  FROM `{{SupervisionPeriod@ALL}}`
  WHERE RMF_TIMESTAMP IS NOT NULL AND Deleted_Flag = "ACTIVE"
),
-- Ranks period Max_Termination_Dates by update_date
rank_Max_Termination_Date_cte AS (
    SELECT
        SID_Number,
        Period_ID_Number,
        Max_Termination_Date,
        rank()OVER(PARTITION BY Period_ID_Number ORDER BY update_date DESC) AS rnk
    FROM Max_Termination_Date_cte
    WHERE prev_Max_Termination_Date IS DISTINCT FROM Max_Termination_Date
),
-- Creates periods using the most recent start and end dates for the periods
periods AS (
    SELECT
        s.SID_Number,
        s.Period_ID_Number,
        s.Start_Date,
        t.Max_termination_Date,
    FROM rank_start_date_cte s 
    JOIN rank_Max_Termination_Date_cte t
        USING (Period_ID_Number)
    WHERE s.rnk = 1 AND t.rnk = 1
),
-- Before we look at the supervision officer changes, select the latest change on a given date.
-- This is to avoid zero day periods where multiple changes were done on a single day.
-- Also removes records without an appropriate update date or filters to active records.
ranked_case_type_changes AS 
(
    SELECT
        SID_number,
        Period_ID_number,
        Case_Type,
        DATE(CTH_Creation_DATE) as CTH_Creation_DATE,
        RANK() OVER (PARTITION BY Period_ID_Number, DATE(CTH_Creation_DATE) ORDER BY CTH_Creation_DATE DESC) AS rnk
    FROM `{{SupervisionPeriod@ALL}}`
    WHERE CTH_Creation_DATE IS NOT NULL AND Deleted_Flag = "ACTIVE"
),
-- Grabs all case type records and their update time, ignores changes that happened after
-- Max termination end date
case_type_cte AS (
  SELECT 
    p.SID_Number,
    p.Period_ID_Number,
    Case_Type,
    p.start_date,
    date(CTH_Creation_DATE) as CTH_Creation_DATE,
    LAG(Case_type)OVER(PARTITION BY Period_ID_Number ORDER BY CTH_Creation_DATE DESC) AS prev_Case_type
  FROM ranked_case_type_changes sp
  LEFT JOIN periods p 
    USING (Period_ID_Number)
  WHERE DATE(CTH_Creation_DATE) < p.Max_Termination_Date AND rnk = 1
), 
-- Selects only the records that have a different case_type than before
case_type_changes_cte AS (
    SELECT
        SID_Number,
        Period_ID_Number,
        Case_Type,
        CASE 
        WHEN DATE(CTH_Creation_DATE) < DATE(start_date)
            THEN DATE(start_date)
        ELSE 
            DATE(CTH_Creation_DATE)
        END AS update_date
    FROM case_type_cte
    WHERE prev_Case_type IS DISTINCT FROM Case_Type
),
-- Before we look at the supervision officer changes, select the latest change on a given date.
-- This is to avoid zero day periods where multiple changes were done on a single day.
-- Also removes records without an appropriate update date or filters to active records.
ranked_status_changes AS 
(
    SELECT
        SID_number,
        Period_ID_number,
        Status,
        DATE(OSTS_UPDATE_DATE) as OSTS_UPDATE_DATE,
        RANK() OVER (PARTITION BY Period_ID_Number, DATE(OSTS_UPDATE_DATE) ORDER BY OSTS_UPDATE_DATE DESC) AS rnk
    FROM `{{SupervisionPeriod@ALL}}`
    WHERE OSTS_UPDATE_DATE IS NOT NULL AND Deleted_Flag = "ACTIVE"
),
-- Grabs all status records and their update time, ignores changes that happened after
-- Max termination end date
status_cte AS (
  SELECT 
    p.SID_Number,
    p.Period_ID_Number,
    Status,
    p.start_date,
    DATE(OSTS_UPDATE_DATE) as OSTS_UPDATE_DATE,
    LAG(Status)OVER(PARTITION BY Period_ID_Number ORDER BY OSTS_UPDATE_DATE asc) AS prev_Status
  FROM ranked_status_changes sp
  LEFT JOIN periods p 
    USING (Period_ID_Number)
  WHERE DATE(OSTS_UPDATE_DATE) < p.Max_Termination_Date and rnk = 1
),
-- Selects only the records that have a different status than before
status_changes_cte AS (
    SELECT
        SID_Number,
        Period_ID_Number,
        Status,
        CASE 
        WHEN DATE(OSTS_UPDATE_DATE) < DATE(start_date)
            THEN DATE(start_date)
        ELSE 
            DATE(OSTS_UPDATE_DATE)
        END AS update_date
    FROM Status_cte
    WHERE prev_Status IS DISTINCT FROM Status
),
-- Before we look at the supervision officer changes, select the latest change on a given date.
-- This is to avoid zero day periods where multiple changes were done on a single day.
-- Also removes records without an appropriate update date or filters to active records.
ranked_supervision_officer_changes AS 
(
    SELECT
        SID_number,
        Period_ID_number,
        Supervision_Officer,
        DATE(WTSK_UPDATE_DATE) as WTSK_UPDATE_DATE,
        RANK() OVER (PARTITION BY Period_ID_Number, DATE(WTSK_UPDATE_DATE) ORDER BY WTSK_UPDATE_DATE DESC) AS rnk
    FROM `{{SupervisionPeriod@ALL}}`
    WHERE WTSK_UPDATE_DATE IS NOT NULL AND Deleted_Flag = "ACTIVE"
),
-- Grabs all supervision officer records and their update time, ignores changes that happened after
-- Max termination end date
supervision_officer_cte AS (
  SELECT 
    p.SID_Number,
    p.Period_ID_Number,
    Supervision_Officer,
    p.start_date,
    WTSK_UPDATE_DATE,
    LAG(Supervision_Officer)OVER(PARTITION BY Period_ID_Number ORDER BY WTSK_UPDATE_DATE asc) AS prev_Supervision_Officer
  FROM ranked_supervision_officer_changes sp
  LEFT JOIN `{{Staff}}` s
    ON sp.Supervision_Officer = s.Staff_ID_Number
  LEFT JOIN periods p 
    USING (Period_ID_Number)
  WHERE DATE(WTSK_UPDATE_DATE) < p.Max_Termination_Date
  AND (Staff_ID_Number IS NOT NULL or Supervision_Officer IS NULL) AND rnk = 1
),
-- Selects only the records that have a different supervision officer than before
supervision_officer_changes_cte AS (
    SELECT
        SID_Number,
        Period_ID_Number,
        Supervision_Officer,
        CASE 
        WHEN DATE(WTSK_UPDATE_DATE) < DATE(start_date)
            THEN DATE(start_date)
        ELSE 
            DATE(WTSK_UPDATE_DATE)
        END AS update_date
    FROM Supervision_Officer_cte
    WHERE prev_Supervision_Officer IS DISTINCT FROM Supervision_Officer
),
-- Gathers assessment data
assessment_cte AS (
    SELECT
        p.SID_Number,
        p.Period_ID_Number,
        a.assessment_level,
        DATE(ASSESSMENT_DATE) AS update_date
    FROM periods p
    LEFT JOIN `{{Assessment}}` a
        ON p.SID_Number = a.SID_Number
        AND DATE(ASSESSMENT_DATE) BETWEEN DATE(p.start_date) and p.Max_termination_Date
    UNION ALL

    -- Grab latest assessment before the current SP start
    SELECT
        p.SID_Number,
        p.Period_ID_Number,
        a.assessment_level,
        DATE(p.start_date) AS update_date
    FROM periods p
    LEFT JOIN `{{Assessment}}` a
        ON p.SID_Number = a.SID_Number
    WHERE DATE(ASSESSMENT_DATE) < DATE(p.start_date)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY SID_Number ORDER BY ASSESSMENT_DATE DESC) = 1
),
-- Combines all of the critical dates where there was a change to one of the period
-- characteristics.
union_all_critical_dates AS 
(
    SELECT
        SID_Number,
        Period_ID_Number,
        DATE(update_date) AS critical_date
    FROM supervision_officer_changes_cte

    UNION DISTINCT

    SELECT
        SID_Number,
        Period_ID_Number,
        DATE(update_date) AS critical_date
    FROM status_changes_cte

    UNION DISTINCT

    SELECT
        SID_Number,
        Period_ID_Number,
        DATE(update_date) AS critical_date
    FROM case_type_changes_cte

    UNION DISTINCT

    SELECT
        SID_Number,
        Period_ID_Number,
        DATE(start_date) as critical_date,
    FROM Periods

    UNION DISTINCT

    SELECT
        SID_Number,
        Period_ID_Number,
        Max_termination_Date as critical_date,
    FROM Periods

    UNION DISTINCT

    SELECT
        SID_Number,
        Period_ID_Number,
        DATE(update_date) as critical_date,
    FROM assessment_cte
),
-- Creates empty periods using the critical dates from union_all_critical_dates
empty_periods AS
(
    SELECT
        SID_Number,
        Period_ID_Number,
        critical_date AS start_date,
        LEAD(critical_date) OVER (PARTITION BY Period_ID_Number ORDER BY critical_date asc) AS end_date
    FROM union_all_critical_dates
),
-- Combines all of the empty periods ina  given period ID and attaches associated characteristics
combined as (
    SELECT
        ep.SID_Number,
        ep.Period_ID_Number,
        ep.start_date,
        ep.end_date,
        s.status,
        so.supervision_officer,
        ct.case_type,
        a.assessment_level,
    FROM empty_periods ep
    LEFT JOIN status_changes_cte s
        ON s.Period_ID_Number = ep.Period_ID_Number
        AND s.update_date = ep.start_date
    LEFT JOIN supervision_officer_changes_cte so
        ON so.Period_ID_Number = ep.Period_ID_Number
        AND so.update_date = ep.start_date
    LEFT JOIN case_type_changes_cte ct
        ON ct.Period_ID_Number = ep.Period_ID_Number
        AND ct.update_date = ep.start_date
    LEFT JOIN assessment_cte a
        ON a.Period_ID_Number = ep.Period_ID_Number
        AND a.update_date = ep.start_date
    WHERE ep.start_date is not null
),
-- Uses the LAST_VALUE function to fill in the gaps between period characterisitc 
-- changes.
lookback_cte AS (
    SELECT
        SID_Number,
        Period_ID_Number,
        start_date,
        end_date,
        LAST_VALUE(status IGNORE NULLS) OVER w AS status,
        LAST_VALUE(supervision_officer IGNORE NULLS) OVER w AS supervision_officer,
        LAST_VALUE(case_type IGNORE NULLS) OVER w AS case_type,
        LAST_VALUE(assessment_level IGNORE NULLS) OVER w AS assessment_level,
    FROM combined
    WHERE end_date IS NOT NULL
    WINDOW w AS (PARTITION BY Period_ID_Number ORDER BY start_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
-- Ensures that the last period of a supervision stint is left opened as long as 
-- today's date is before the max termination date
fix_end_date AS
(
    SELECT DISTINCT
        SID_Number,
        Period_ID_Number,
        start_date,
        CASE 
            WHEN DATE(end_date) > @update_timestamp
                THEN NULL
            ELSE
                end_date
        END AS end_date,
        status,
        (
            UPPER(status) LIKE "%IN CUSTODY%" 
            OR UPPER(status) LIKE "%REVOKED%" 
            OR UPPER(status) LIKE "%PRE-REVOCATION%"
            OR UPPER(status) LIKE "%PENDING WARRANT CLOSURE%"
            ) 
            AND UPPER(status) NOT LIKE "%NOT REVOKED%"
            AND UPPER(status) NOT LIKE "%NOT IN CUSTODY%"
        AS in_custody_flag,
        supervision_officer,
        case_type,
        assessment_level,
    FROM lookback_cte
    WHERE start_date != "0001-01-01"
),
-- Aggregate above periods by period attributes
period_info_agg AS (
    {aggregate_adjacent_spans(
        table_name='fix_end_date',
        attribute=['assessment_level','case_type','Supervision_Officer','status', 'in_custody_flag'],
        session_id_output_name='period_info_agg',
        end_date_field_name='end_date',
        index_columns=['Period_ID_Number','SID_Number']
    )}
)
    SELECT
        SID_Number,
        Period_ID_Number,
        start_date,
        end_date,
        status,
        supervision_officer,
        case_type,
        assessment_level,
        ROW_NUMBER()OVER(PARTITION BY Period_ID_Number ORDER BY start_date ASC) AS rn,
        in_custody_flag,
    FROM period_info_agg
    WHERE 
        status IS NULL
        OR status NOT IN(
            "Death reported",
            "Deported OOC",
            "Death",
            "Discharge",
            "Erroneous Release Returned to ID",
            "Full pardon",
            "Sentence Reversed"
        )
"""
VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_tx",
    ingest_view_name="supervision_period",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
