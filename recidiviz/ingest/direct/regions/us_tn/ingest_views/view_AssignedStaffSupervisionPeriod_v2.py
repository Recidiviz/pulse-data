# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Query containing supervision period information extracted from the output of the `AssignedStaff` table.
The table contains one row per supervision period.
"""

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH
officer_names AS (
    SELECT
        StaffID,
        SPLIT(LastName, ',') as SupervisionOfficerLastNameAndSuffix,
        FirstName as SupervisionOfficerFirstName
    FROM {Staff}
),
split_officer_names AS (
    SELECT
        StaffID,
        SupervisionOfficerFirstName,
        SupervisionOfficerLastNameAndSuffix[SAFE_ORDINAL(1)] as SupervisionOfficerLastName,
        REGEXP_REPLACE(SupervisionOfficerLastNameAndSuffix[SAFE_ORDINAL(2)], r'[^A-Z0-9.]', '') as SupervisionOfficerSuffix,
    FROM officer_names
),
cleaned_assignment_periods AS (
    SELECT
        OffenderID,
        (DATE(StartDate)) AS StartDate,
        (DATE(NULLIF(EndDate, 'NULL'))) AS EndDate,
        REGEXP_REPLACE(CaseType, r'[^A-Z0-9]', '') as SupervisionType,
        -- Clean up to only include expected characters (i.e. only capitalized letters, or a combination of capitalized 
        -- letters and numbers).
        REGEXP_REPLACE(AssignmentBeginReason, r'[^A-Z]', '') as AdmissionReason,
        REGEXP_REPLACE(AssignmentEndReason, r'[^A-Z]', '') as TerminationReason,
        REGEXP_REPLACE(StaffID, r'[^A-Z0-9]', '') as SupervisionOfficerID,
        REGEXP_REPLACE(AssignmentType, r'[^A-Z0-9]', '') as AssignmentType,
        REGEXP_REPLACE(SiteID, r'[^A-Z0-9]', '') as Site,
        SupervisionOfficerFirstName,
        SupervisionOfficerLastName,
        SupervisionOfficerSuffix,
    FROM {AssignedStaff} staff_periods
    LEFT JOIN split_officer_names officers
    USING (StaffID)
    -- Filter to only the supervision types that are associated with supervision periods.
    WHERE AssignmentType in (
            'PRO', # Probation
            'PAO', # Parole
            'CCC'  # Community Corrections
        )
),
raw_supervision_level_periods AS (
    -- First, group supervision level periods by person and by supervisison type (e.g. parole, probation, etc.)
    -- and then flag any periods where the supervision level is different from the previous one or the periods
    -- are noncontiguous.
    SELECT
        OffenderID,
        (DATE(PlanStartDate)) AS PlanStartDate,
        (DATE(NULLIF(PlanEndDate, 'NULL'))) AS PlanEndDate,
        -- Update PlanType to be the same as the AssignmentType used in AssignedStaff table
        CASE PlanType
            WHEN 'PARO' THEN 'PAO'
            WHEN 'PROB' THEN 'PRO'
            WHEN 'COMM' THEN 'CCC'
        END AS AssignmentType,
        SupervisionLevel,
        -- Mark when the supervision level changes or the periods are not contiguous
        IFNULL(SupervisionLevel != LAG(SupervisionLevel) OVER person_window, FALSE) AS supervision_level_changed,
        IFNULL(PlanStartDate != LAG(PlanEndDate) OVER person_window, FALSE) AS non_contiguous_period,
    FROM {SupervisionPlan}
    WHERE PlanType IN ('PARO', 'PROB', 'COMM')
    -- Partition by OffenderID and PlanType since there can be concurrent periods for the same offender
    -- under different supervision types.
    WINDOW person_window AS (PARTITION BY OffenderID, PlanType ORDER BY PlanStartDate ASC, PlanEndDate ASC)
),
ungrouped_supervision_level_sessions AS (
   SELECT
        OffenderID,
        AssignmentType,
        SupervisionLevel,
        PlanStartDate,
        -- The end_date of one period and the start_date of the next in the supervision table do not line up exactly but
        -- are set up such that if a period ends on one day, the next period starts on the next day. However, the
        -- assigned staff table has periods where the end date of one period is the same as the start date of the next
        -- period. This clause checks if this period abuts the next period, and, if so, it sets the end date of the
        -- current period to be the same as the start date of the next period.
        IF(DATE_ADD(PlanEndDate, INTERVAL 1 DAY) = LEAD(PlanStartDate) OVER person_window,
            DATE_ADD(PlanEndDate, INTERVAL 1 DAY),
            PlanEndDate) as PlanEndDate,
        -- Group supervision levels into periods bases on when an important factor changed
        COUNTIF(supervision_level_changed OR non_contiguous_period) OVER person_window AS supervision_session_id,
   FROM raw_supervision_level_periods
   WINDOW person_window AS (PARTITION BY OffenderID, AssignmentType ORDER BY PlanStartDate ASC, PlanEndDate ASC)
),
supervision_level_sessions AS (
    SELECT
        OffenderID,
        AssignmentType,
        SupervisionLevel,
        -- Group supervision levels into periods with max and min dates
        MIN(PlanStartDate) AS SessionStartDate,
        IF(LOGICAL_OR(PlanEndDate IS NULL), NULL, MAX(PlanEndDate)) AS SessionEndDate,
        supervision_session_id,
    FROM ungrouped_supervision_level_sessions
    GROUP BY 1, 2, 3, supervision_session_id
),
supervision_level_sessions_padded AS (
    -- This entire subquery is to pad the supervision level sessions to ensure that each person has a contiguous set of
    -- sessions. We do this by filling in any gaps in a person's sessions with periods where the supervision level is
    -- EXTERNAL_UNKONWN. We also pad the beginning and end of the sessions with periods where the supervision level is
    -- EXTERNAL_UNKONWN extending far into the past and future. This is so that a gap in someone's recorded supervision
    -- levels doesn't cause a gap in their supervision periods but instead is filled in with periods where the
    -- supervision level is EXTERNAL_UNKONWN.

    -- Fill in gaps between suervision level sessions with dummy sessions with "EXTERNAL_UNKNOWN" supervision levels
    SELECT
        start_session.OffenderID,
        start_session.AssignmentType,
        'EXTERNAL_UNKNOWN' AS SupervisionLevel,
        start_session.SessionEndDate AS SessionStartDate,
        end_session.SessionStartDate AS SessionEndDate,
    FROM supervision_level_sessions start_session
    INNER JOIN supervision_level_sessions end_session
    ON start_session.OffenderID = end_session.OffenderID
        AND start_session.AssignmentType = end_session.AssignmentType
        AND start_session.supervision_session_id + 1 = end_session.supervision_session_id
        AND start_session.SessionEndDate != end_session.SessionStartDate
    
    UNION ALL
    
    -- Create an "EXTERNAL_UNKNOWN" period extending from the end of the last period off to the far future
    SELECT
        OffenderID,
        AssignmentType,
        'EXTERNAL_UNKNOWN' AS SupervisionLevel,
        SessionStartDate,
        DATE(9999, 12, 31) AS SessionEndDate,
    FROM  (
        SELECT 
            OffenderID,
            AssignmentType,
            MAX(SessionEndDate) AS SessionStartDate
        FROM supervision_level_sessions
        GROUP BY 1, 2
        -- Don't create a period if the person already has an open period for this assignment type
        HAVING LOGICAL_AND(SessionEndDate IS NOT NULL) 
    ) AS max_session_end_date
    
    UNION ALL
    
    -- Create an "EXTERNAL_UNKNOWN" period starting in the far past and leading up to the start of the first period
    SELECT
        OffenderID,
        AssignmentType,
        'EXTERNAL_UNKNOWN' AS SupervisionLevel,
        DATE(1000, 1, 1) AS SessionStartDate,
        MIN(SessionStartDate) AS SessionEndDate,
    FROM supervision_level_sessions
    GROUP BY 1, 2
    
    UNION ALL
    
    -- Make sure to keep the original periods as well
    SELECT
        OffenderID,
        AssignmentType,
        SupervisionLevel,
        SessionStartDate,
        SessionEndDate,
    FROM supervision_level_sessions
),
combined_officer_and_level AS (
    SELECT
        staff_periods.OffenderID,
        SupervisionType,
        SupervisionOfficerID,
        staff_periods.AssignmentType,
        Site,
        SupervisionOfficerFirstName,
        SupervisionOfficerLastName,
        SupervisionOfficerSuffix,
        SupervisionLevel,
        -- For each overlapping pair of periods, find the time they overlapped
        COALESCE(GREATEST(staff_periods.StartDate, SessionStartDate), staff_periods.StartDate, SessionStartDate) AS StartDate,
        COALESCE(LEAST(staff_periods.EndDate, SessionEndDate), staff_periods.EndDate, SessionEndDate) AS EndDate,
    FROM cleaned_assignment_periods staff_periods
    LEFT JOIN supervision_level_sessions_padded level_periods
    -- We only want to join on periods where:
        -- 1. The OffenderID matches
        -- 2. The AssignmentType matches
        -- 3. The supervision level period overlaps with the staff period, meaning
            -- A. EITHER The supervision level period started during the staff period
            -- B. OR The staff period started during the supervision level period
    ON level_periods.OffenderID = staff_periods.OffenderID
        AND level_periods.AssignmentType = staff_periods.AssignmentType
        AND ((level_periods.SessionStartDate >= staff_periods.StartDate AND level_periods.SessionStartDate < IFNULL(staff_periods.EndDate, CURRENT_DATE))
            OR (staff_periods.StartDate >= level_periods.SessionStartDate AND staff_periods.StartDate < IFNULL(level_periods.SessionEndDate, CURRENT_DATE)))
),
all_supervision_periods AS (
    SELECT 
        combined_officer_and_level.*,
        -- If the admission or termination reason is null, we mark it as a transfer
        COALESCE(officer_period_start.AdmissionReason,'TRANS') AS AdmissionReason,
        -- If the end date is null, it is an open period and should not have a termination reason assigned to it yet, so we keep the termination reason null
        CASE WHEN combined_officer_and_level.EndDate IS NOT NULL THEN COALESCE(officer_period_end.TerminationReason,'TRANS') ELSE NULL END AS TerminationReason
    FROM combined_officer_and_level
    LEFT JOIN cleaned_assignment_periods officer_period_start
        ON combined_officer_and_level.OffenderID = officer_period_start.OffenderID 
        AND combined_officer_and_level.StartDate = officer_period_start.StartDate 
        AND COALESCE(combined_officer_and_level.SupervisionType,'') = COALESCE(officer_period_start.SupervisionType,'') 
        AND COALESCE(combined_officer_and_level.SupervisionOfficerID,'') = COALESCE(officer_period_start.SupervisionOfficerID,'') 
        AND COALESCE(combined_officer_and_level.AssignmentType,'') = COALESCE(officer_period_start.AssignmentType,'') 
        AND COALESCE(combined_officer_and_level.Site,'') = COALESCE(officer_period_start.Site,'')
    LEFT JOIN cleaned_assignment_periods officer_period_end
        ON combined_officer_and_level.OffenderID = officer_period_end.OffenderID 
        AND combined_officer_and_level.EndDate = officer_period_end.EndDate 
        AND COALESCE(combined_officer_and_level.SupervisionType,'') = COALESCE(officer_period_end.SupervisionType,'') 
        AND COALESCE(combined_officer_and_level.SupervisionOfficerID,'') = COALESCE(officer_period_end.SupervisionOfficerID,'') 
        AND COALESCE(combined_officer_and_level.AssignmentType,'') = COALESCE(officer_period_end.AssignmentType,'') 
        AND COALESCE(combined_officer_and_level.Site,'') = COALESCE(officer_period_end.Site,'')
    WHERE IFNULL(SupervisionLevel, '') != 'ZDS' -- Exclude periods where the person was marked as discharged (ZDS)
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
)
SELECT
    *,
    ROW_NUMBER() OVER person_window AS SupervisionPeriodSequenceNumber
    FROM all_supervision_periods 
    WINDOW person_window AS (PARTITION BY OffenderID ORDER BY StartDate ASC, EndDate ASC)
"""


VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_tn",
    ingest_view_name="AssignedStaffSupervisionPeriod_v2",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="OffenderID ASC, SupervisionPeriodSequenceNumber ASC",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
