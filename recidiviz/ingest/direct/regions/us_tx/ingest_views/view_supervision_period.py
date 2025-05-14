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
from recidiviz.ingest.direct.regions.us_tx.ingest_views.us_tx_view_query_fragments import (
    PERIOD_EXCLUSIONS_FRAGMENT,
    PHASES_FRAGMENT,
)
from recidiviz.ingest.direct.regions.us_tx.ingest_views.view_staff_location_period import (
    VIEW_QUERY_TEMPLATE as STAFF_LOCATION_PERIOD_VIEW_QUERY_TEMPLATE,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# NOTE: This is defined here instead of the fragments file because we
# pull in a full ingest view query (which could eventually cause a circular import)
#
# We use the office location of the supervision officer as the supervision site,
# and do not use custodial authority.
SUPERVISON_OFFICER_AND_SITE_SUBQUERY = f"""
WITH 
-- Gets contiguous spans of time for each staff member
staff_location_periods AS (
    {STAFF_LOCATION_PERIOD_VIEW_QUERY_TEMPLATE}
),
-- This CTE creates a view of distinct supervision officer update datetimes
-- for each individual and supervision period. To avoid zero day periods 
-- where multiple changes were done on a single day, we select for the most
-- recent staff update on any given date for each individual and supervision period.
-- Records must be active with staff update datetimes before the termination of a period.
distinct_supervision_officer_update_dates AS (
    SELECT DISTINCT
        SID_number,
        Period_ID_number,
        Supervision_Officer AS Staff_ID_Number,
        DATE(WTSK_UPDATE_DATE) as WTSK_UPDATE_DATE,
    FROM 
        `{{SupervisionPeriod@ALL}}`
    WHERE 
        WTSK_UPDATE_DATE IS NOT NULL 
    AND 
        Deleted_Flag = "ACTIVE"
    AND
        -- Using CAST AS DATETIME instead of just DATETIME for the emulator
        CAST(WTSK_UPDATE_DATE AS DATETIME) < IFNULL(DATE_SUB(DATE(Max_Termination_Date), INTERVAL 1 DAY), "9999-12-31")
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SID_number, Period_ID_number, DATE(WTSK_UPDATE_DATE)
        ORDER BY CAST(WTSK_UPDATE_DATE AS DATETIME) DESC
    ) = 1
),
-- This CTE combines distinct critical dates of officer supervision and all known location
-- starts for those officers.
-- We end up with two sets of date/location pairs in each row. 
-- WTSK_UPDATE_DATE and location_for_wtsk give us the date and location of the officer for the
-- update provided from supervision periods.
-- floored_location_period_start and staff_location_period_location give us the date and location of the officer
-- for all updates from staff location periods. Because we are flooring the date with the SP start date,
-- None of these updates will create periods before the original supervision period.
-- It also then captures subsequent changes over time (across rows), which will be deduplicated if neccessary
-- in the next CTE.
all_supervision_officer_movements_per_location_period AS (
SELECT 
  distinct_supervision_officer_update_dates.SID_number, 
  distinct_supervision_officer_update_dates.Period_id_number, 
  distinct_supervision_officer_update_dates.WTSK_UPDATE_DATE, 
  staff_location_periods.Staff_ID_number, 
  -- We hold on to this value to determine the most relevant LP if there are multiple
  -- location changes before the supervision officer starts supervising the client
  staff_location_periods.start_date AS original_location_period_date,
  staff_location_periods.Agency_of_employment AS staff_location_period_location,
  -- If the officer starts at a location while already supervising this client, then we want to have a new period with that location. 
  -- Otherwise the officer was already at this location when they began supervision this client. 
  GREATEST(start_date, distinct_supervision_officer_update_dates.WTSK_UPDATE_DATE) AS floored_location_period_start,
  CASE WHEN 
        start_date > distinct_supervision_officer_update_dates.WTSK_UPDATE_DATE 
        THEN NULL ELSE Agency_of_employment 
  END AS location_for_wtsk
FROM 
    distinct_supervision_officer_update_dates
LEFT JOIN 
    staff_location_periods 
USING
    (Staff_ID_Number)
),
-- This takes the two separate categories of critical dates and locations from the previous CTE and 
-- stacks their DISTINCT union, forming unique date/location pairs for each officer and supervision period
-- for all supervision officer movements.
all_supervision_officer_and_site_critical_dates AS (
  SELECT 
    *, 
    ROW_NUMBER() OVER(PARTITION BY SID_Number, Period_id_number ORDER BY critical_date) AS site_rn 
  FROM (

    -- Update and location for original SP updates.
    SELECT DISTINCT
        SID_Number,
        Period_id_number,
        Staff_ID_number,
        WTSK_UPDATE_DATE AS critical_date,
        location_for_wtsk AS supervision_site
    FROM 
        all_supervision_officer_movements_per_location_period
    QUALIFY ROW_NUMBER() OVER (PARTITION BY SID_number, Period_id_number, WTSK_UPDATE_DATE ORDER BY original_location_period_date DESC) = 1

    UNION DISTINCT 

    -- Update and location for staff location period rows (floored to the start date of the SP)
    SELECT
        SID_Number,
        Period_id_number,
        staff_id_number,
        floored_location_period_start AS critical_date,
        staff_location_period_location AS supervision_site
    FROM 
        all_supervision_officer_movements_per_location_period
    QUALIFY row_number() over (partition by SID_number, Period_id_number, WTSK_UPDATE_DATE order by original_location_period_date desc) = 1
  )
)
-- This CTE takes all supervision officer and site critical dates and finally turns them into periods to be used
-- with all other subperiods in this view.
SELECT 
  officer_and_site_start.SID_Number,
  officer_and_site_start.Period_id_number,
  officer_and_site_start.staff_id_number,
  officer_and_site_start.supervision_site,
  officer_and_site_start.critical_date AS start_date,
  officer_and_site_end.critical_date AS end_date
FROM 
    all_supervision_officer_and_site_critical_dates AS officer_and_site_start
LEFT JOIN
    all_supervision_officer_and_site_critical_dates AS officer_and_site_end 
ON
    officer_and_site_start.SID_Number = officer_and_site_end.SID_Number
AND 
    officer_and_site_start.SID_Number = officer_and_site_end.SID_Number
AND 
    officer_and_site_start.site_rn = officer_and_site_end.site_rn - 1
"""


VIEW_QUERY_TEMPLATE = f"""
WITH
-- Gets every supervising officer and their location changes within supervision periods.
-- Returns SID_Number, Period_ID_Number, Staff_ID_number, supervision_site, start_date, and end_date
supervision_officer_and_site_periods AS (
    {SUPERVISON_OFFICER_AND_SITE_SUBQUERY}
),
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
    LAG(Case_type)OVER(PARTITION BY Period_ID_Number ORDER BY CTH_Creation_DATE ASC) AS prev_Case_type
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
    -- In cases where there are multiple case type entries entered before the start
    -- date, choose the latest one. 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY SID_Number, Period_ID_Number, update_date ORDER BY CTH_Creation_DATE DESC) = 1
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
    -- In cases where there are multiple status entries entered before the start
    -- date, choose the latest one. 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY SID_Number, Period_ID_Number, update_date ORDER BY OSTS_UPDATE_DATE DESC) = 1
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
    LEFT JOIN `{{TROAStatusDescription}}`
        ON ASSESSMENT_STATUS = TROA_STATUS
    WHERE TROA_STATUS_DESC = "COMPLETE"
    AND Assessment_Type IN ("CST", "CSST", "SRT", "RT")
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
    LEFT JOIN `{{TROAStatusDescription}}`
        ON ASSESSMENT_STATUS = TROA_STATUS
    WHERE TROA_STATUS_DESC = "COMPLETE"
    AND Assessment_Type IN ("CST", "CSST", "SRT", "RT")
    AND DATE(ASSESSMENT_DATE) < DATE(p.start_date)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY SID_Number ORDER BY ASSESSMENT_DATE DESC) = 1
),
-- In the rare case where two seperate assessments were done in the same day, 
-- only around 2k of assessments out of 455k as of 4/18, then choose the lower supervision level.
cleaned_assessment_cte AS (
    SELECT
        SID_Number,
        Period_ID_Number,
        assessment_level,
        update_date,
    FROM assessment_cte
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SID_Number, update_date
        ORDER BY CASE assessment_level
            WHEN 'L' THEN 1
            WHEN 'LM' THEN 2
            WHEN 'M' THEN 3
            WHEN 'MH' THEN 5
            WHEN 'H' THEN 5
            ELSE 6
        END
    ) = 1
),
-- Combines all of the critical dates where there was a change to one of the period
-- characteristics.
union_all_critical_dates AS 
(
    SELECT
        SID_Number,
        Period_ID_Number,
        DATE(start_date) AS critical_date
    FROM supervision_officer_and_site_periods 

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
    FROM cleaned_assessment_cte
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
        officer_and_site.Staff_ID_Number AS supervision_officer,
        officer_and_site.supervision_site,
        ct.case_type,
        a.assessment_level,
    FROM empty_periods ep
    LEFT JOIN status_changes_cte s
        ON s.Period_ID_Number = ep.Period_ID_Number
        AND s.update_date = ep.start_date
    LEFT JOIN supervision_officer_and_site_periods AS officer_and_site
        ON officer_and_site.Period_ID_Number = ep.Period_ID_Number
        AND officer_and_site.start_date = ep.start_date
    LEFT JOIN case_type_changes_cte ct
        ON ct.Period_ID_Number = ep.Period_ID_Number
        AND ct.update_date = ep.start_date
    LEFT JOIN cleaned_assessment_cte a
        ON a.Period_ID_Number = ep.Period_ID_Number
        AND a.update_date = ep.start_date
    WHERE ep.start_date IS NOT NULL
),
-- Uses the LAST_VALUE function to fill in the gaps between period characterisitc changes.
lookback_cte AS (
    SELECT
        SID_Number,
        Period_ID_Number,
        start_date,
        end_date,
        LAST_VALUE(status IGNORE NULLS) OVER w AS status,
        LAST_VALUE(supervision_officer IGNORE NULLS) OVER w AS supervision_officer,
        LAST_VALUE(supervision_site IGNORE NULLS) OVER w AS supervision_site,
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
        supervision_officer,
        supervision_site,
        case_type,
        assessment_level,
    FROM lookback_cte
),
{PHASES_FRAGMENT},
-- Assigns the most recent phase to Subtance abuse open periods. There is a ticket
-- #TODO(#41754) to eventually refactor the Phases logic to have it be historically 
-- accurate as well.
periods_with_phases AS (
    SELECT
        SID_Number,
        Period_ID_Number,
        start_date,
        end_date,
        status,
        supervision_officer,
        supervision_site,
        assessment_level,
        CASE 
            WHEN case_type = "Substance abuse" AND end_date IS NULL 
                THEN CONCAT(case_type, " - ", phase)
            ELSE case_type
        END AS case_type
    FROM fix_end_date
    LEFT JOIN client_phases
        USING (SID_Number)
),
-- Aggregate above periods by period attributes
period_info_agg AS (
    {aggregate_adjacent_spans(
        table_name='periods_with_phases',
        attribute=['assessment_level','case_type','supervision_officer', 'supervision_site', 'status'],
        session_id_output_name='period_info_agg',
        end_date_field_name='end_date',
        index_columns=['Period_ID_Number','SID_Number']
    )}
),
-- Get's the latest special conditions as we have not been given their update datetime by TX yet
latest_special_conditions AS (
    SELECT
        SID_Number,
        Period_ID_Number,
        Special_Conditions
    FROM `{{SupervisionPeriod}}`
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SID_Number, Period_ID_Number 
        ORDER BY 
            XREF_UPDATE_DATE DESC,
            OSTS_UPDATE_DATE DESC,
            RMF_TIMESTAMP DESC,
            CTH_CREATION_DATE DESC,
            WTSK_UPDATE_DATE DESC
    ) = 1
)
SELECT
    SID_Number,
    Period_ID_Number,
    start_date,
    end_date,
    status,
    supervision_officer,
    supervision_site,
    case_type,
    assessment_level,
    ROW_NUMBER() OVER (PARTITION BY Period_ID_Number ORDER BY start_date ASC) AS rn,
    Special_Conditions
FROM 
    period_info_agg
LEFT JOIN
    latest_special_conditions
USING
    (SID_Number, Period_ID_Number)
WHERE 
    status IS NULL OR status NOT IN {PERIOD_EXCLUSIONS_FRAGMENT}
"""
VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_tx",
    ingest_view_name="supervision_period",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
