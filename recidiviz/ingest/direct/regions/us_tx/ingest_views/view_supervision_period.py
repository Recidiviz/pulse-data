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
"""Query for supervision periods for US_TX."""
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""
WITH 
-- In less than 1 percent of period records, there are multiple records on the same update_datetime date
-- This CTE deterministically chooses a single record per update_datetime date. We also get rid of 
-- any records that were deleted. 
clean_cte AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY Period_ID_Number,update_datetime ORDER BY Supervision_Officer, Supervision_Level, Special_Conditions, Custodial_Authority, Case_Type, Start_Date, Max_Termination_Date asc) AS rn
    FROM `{{SupervisionPeriod@ALL}}`
    -- Confusing column naming but this means that the record is not deleted.
    WHERE UPPER(Deleted_Flag) = "ACTIVE"
),
-- The lag_cte grabs the traits of the previous period record to filter to only records 
-- that contain a change.
lag_cte AS (
    SELECT 
        SID_Number,
        Period_ID_Number,
        Supervision_Officer,
        Special_Conditions,
        Custodial_Authority,
        Case_Type,
        Start_Date,
        Max_Termination_Date,
        update_datetime,
        LAG(update_datetime) OVER w AS prev_update_datetime,
        LAG(Supervision_Officer) OVER w AS prev_Supervision_Officer,
        LAG(Special_Conditions) OVER w AS prev_Special_Conditions,
        LAG(Custodial_Authority) OVER w AS prev_Custodial_Authority,
        LAG(Case_Type) OVER w AS prev_Case_Type,
        LAG(Max_Termination_Date) OVER w AS prev_Max_Termination_Date,
    FROM clean_cte
    WHERE rn = 1 AND Start_Date != "0001-01-01 00:00:00"
    WINDOW w AS (PARTITION BY Period_ID_Number ORDER BY update_datetime asc)
),
-- Filters down to the records that contain a change to the supervision period. Also uses
-- update_datetime dates as start/end dates for period.
filter_cte AS
(
    SELECT
        SID_Number,
        Period_ID_Number,
        Supervision_Officer,
        Special_Conditions,
        Custodial_Authority,
        Case_Type,
        Start_Date,
        Max_Termination_Date,
        update_datetime,
        ROW_NUMBER()OVER (PARTITION BY Period_ID_Number ORDER BY update_datetime DESC) AS rn,
        LAG(update_datetime) OVER (PARTITION BY Period_ID_Number ORDER BY update_datetime asc) AS prev_update_datetime,
        LEAD(update_datetime) OVER (PARTITION BY Period_ID_Number ORDER BY update_datetime asc) AS lead_update_datetime
    FROM lag_cte 
    WHERE prev_update_datetime IS NULL 
        OR Supervision_Officer IS DISTINCT FROM prev_Supervision_Officer
        OR Special_Conditions IS DISTINCT FROM prev_Special_Conditions
        OR Custodial_Authority IS DISTINCT FROM prev_Custodial_Authority
        OR Case_Type IS DISTINCT FROM prev_Case_Type
        OR Max_Termination_Date IS DISTINCT FROM prev_Max_Termination_Date
), 
-- Creates supervision periods without supervision level. Use changes in supervision
-- period to create smaller sub-periods. Also, since Max_Termination_Date is the
-- expected date that supervision ends and not the actual date supervision ends
-- we NULL Max_Termination_Date if Max_Termination_Date < CURRENT_DATE.
sp_no_level AS
(
    SELECT
        SID_Number,
        Period_ID_Number,
        Supervision_Officer,
        Special_Conditions,
        Custodial_Authority,
        Case_Type,
        lead_update_datetime,
        CASE
            WHEN prev_update_datetime IS NULL
                THEN DATE(Start_Date)
            ELSE DATE(update_datetime)
        END AS Start_Date,
        CASE 
            WHEN lead_update_datetime IS NOT NULL
                THEN DATE(lead_update_datetime)
            WHEN DATE(Max_Termination_Date) < CURRENT_DATE
                THEN DATE(Max_Termination_Date)
            ELSE NULL
        END AS Max_Termination_Date,
    FROM filter_cte
),
-- Gathers assessment data
assessment_cte AS (
    SELECT
        SID_Number,
        assessment_level,
        DATE(ASSESSMENT_DATE) AS ASSESSMENT_DATE
    FROM `{{Assessment}}`
),
-- Left joins the assessments with their associated supervision period
connect_cte AS (
    SELECT 
        p.SID_Number,
        p.Period_ID_Number,
        p.Supervision_Officer,
        p.Special_Conditions,
        p.Custodial_Authority,
        p.Case_Type,
        lead_update_datetime,
        DATE(p.Start_Date) AS Start_Date,
        DATE(p.Max_Termination_Date) AS Max_Termination_Date,
        a.ASSESSMENT_DATE
    FROM 
        sp_no_level p
    LEFT JOIN 
        assessment_cte a
    ON 
        a.ASSESSMENT_DATE BETWEEN DATE(p.Start_Date) AND 
            COALESCE(DATE(p.Max_Termination_Date),DATE("9999-12-31"))
        AND p.SID_Number = a.SID_Number
),
-- Creates critical dates comprised of start, end, and assessment dates
critical_dates AS (
    SELECT
        SID_Number,
        Period_ID_Number,
        Supervision_Officer,
        Special_Conditions,
        Custodial_Authority,
        Case_Type,
        Start_Date AS critical_date,
        "start" as type,
    FROM connect_cte
    UNION ALL
    SELECT
        SID_Number,
        Period_ID_Number,
        Supervision_Officer,
        Special_Conditions,
        Custodial_Authority,
        Case_Type,
        Max_Termination_Date AS critical_date,
        "end" as type,
    FROM connect_cte
    -- Only get end date as critical date if it's the last period
    WHERE lead_update_datetime IS NULL
    UNION ALL
    SELECT
        SID_Number,
        Period_ID_Number,
        Supervision_Officer,
        Special_Conditions,
        Custodial_Authority,
        Case_Type,
        ASSESSMENT_DATE AS critical_date,
        "assessment" as type,
    FROM connect_cte
    WHERE ASSESSMENT_DATE IS NOT NULL
),
-- Creates periods divided by assessments
new_periods AS (
    SELECT
        SID_Number,
        Period_ID_Number,
        Supervision_Officer,
        Special_Conditions,
        Custodial_Authority,
        Case_Type,
        critical_date AS period_start,
        LAST_VALUE(critical_date IGNORE NULLS) OVER (
            PARTITION BY SID_Number, Period_ID_Number
            ORDER BY critical_date DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS period_end,
        type,
        FROM critical_dates
        WHERE critical_date IS NOT NULL
),
-- Finally joins on the correct supervision level given the assessment falls within that
-- period.
final_periods_unagg AS (
    SELECT DISTINCT
        np.SID_Number,
        np.Period_ID_Number,
        np.Supervision_Officer,
        np.Custodial_Authority,
        np.Special_Conditions,
        np.Case_Type,
        period_start AS start_date,
        period_end AS end_date,
        a.assessment_level,
    FROM new_periods np 
    LEFT JOIN assessment_cte a
        ON np.SID_Number = a.SID_Number
        AND a.ASSESSMENT_DATE >= np.period_start AND 
           a.ASSESSMENT_DATE < COALESCE(DATE(period_end),DATE("9999-12-31"))
    WHERE type != "end"
),
-- Aggregate above periods by period attributes
period_info_agg AS (
    {aggregate_adjacent_spans(
        table_name='final_periods_unagg',
        attribute=['assessment_level','case_type','Supervision_Officer',
                   'Custodial_Authority','Special_Conditions'],
        session_id_output_name='period_info_agg',
        end_date_field_name='end_date',
        index_columns=['Period_ID_Number','SID_Number']
    )}
),
-- Gather all of the officer IDs so that we only hydrate the field for officers
-- received in the officer data. 
officer_cte AS (
    SELECT
        Staff_ID_Number
    FROM `{{Staff}}`
),
-- Assign row numbers for period external id
final_periods AS (SELECT
    SID_Number,
    Period_ID_Number,
    CASE 
        WHEN Staff_ID_Number IS NULL
            THEN NULL
        ELSE 
            Supervision_Officer
    END AS Supervision_Officer,
    Custodial_Authority,
    Special_Conditions,
    Case_Type,
    start_date,
    end_date,
    assessment_level,
    ROW_NUMBER() OVER (PARTITION BY SID_Number, Period_ID_Number ORDER BY start_date) AS rn
FROM period_info_agg
LEFT JOIN officer_cte
    ON Supervision_Officer = Staff_ID_Number)
SELECT 
    *
FROM final_periods
"""
VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_tx",
    ingest_view_name="supervision_period",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
