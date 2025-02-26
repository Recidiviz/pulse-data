# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY, without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Query that generates the supervision period entity using the following tables:
XXXXX"""
from recidiviz.ingest.direct.regions.us_ix.ingest_views.query_fragments import (
    INVESTIGATION_DETAILS_CTE,
    LEGAL_STATUS_PERIODS_CTE,
    LEGAL_STATUS_PERIODS_SUPERVISION_FILTER,
    LOCATION_DETAILS_CTE,
    PHYSICAL_LOCATION_PERIODS_CTE,
    SUPERVISING_OFFICER_ASSIGNMENTS_CTE,
    SUPERVISION_LEVEL_CHANGES_CTE,
    TRANSFER_DETAILS_CTE,
    TRANSFER_PERIODS_CTE,
    TRANSFER_PERIODS_SUPERVISION_CTE,
)
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""
WITH
{LOCATION_DETAILS_CTE},
{LEGAL_STATUS_PERIODS_CTE},
{TRANSFER_DETAILS_CTE},
{TRANSFER_PERIODS_CTE},
{TRANSFER_PERIODS_SUPERVISION_CTE},
{SUPERVISING_OFFICER_ASSIGNMENTS_CTE},
{SUPERVISION_LEVEL_CHANGES_CTE},
{PHYSICAL_LOCATION_PERIODS_CTE},
{INVESTIGATION_DETAILS_CTE},

-- Prepend investigation periods onto legal status periods so we can get an end date
-- for the investigation status.
legal_status_periods_with_investigation_cte AS (
    SELECT *
    FROM (
        SELECT
            OffenderId,
            LegalStatusDesc,
            Priority,
            LegalStatus_StartDate,
            CASE
                WHEN LegalStatusDesc = 'Investigation' THEN
                    COALESCE(
                        LEAD(LegalStatus_StartDate) OVER (
                            PARTITION BY OffenderId
                            ORDER BY LegalStatus_StartDate
                        ),
                        -- If the above LEAD returns null it means there are no
                        -- following legal status periods and this is an open investigation
                        LegalStatus_EndDate
                    )
                ELSE LegalStatus_EndDate
            END AS LegalStatus_EndDate,
            Investigation_LocationName,
            Investigation_LocationTypeName,
            Investigation_LocationSubTypeName,
        FROM (
            SELECT
                OffenderId,
                LegalStatusDesc,
                Priority,
                LegalStatus_StartDate,
                LegalStatus_EndDate,
                NULL AS Investigation_LocationName,
                NULL AS Investigation_LocationTypeName,
                NULL AS Investigation_LocationSubTypeName,
            FROM legal_status_periods_cte

            UNION ALL

            SELECT
                OffenderId,
                'Investigation',
                '80', -- Lower priority than all legal statuses
                AssignedDate,
                -- LegalStatus table uses magic end dates rather than NULL
                IFNULL(CompletionDate, '9999-12-31'),
                LocationName,
                LocationTypeName,
                LocationSubTypeName,
            FROM investigation_details_cte
        ) a
    ) b
    WHERE {LEGAL_STATUS_PERIODS_SUPERVISION_FILTER}
),

-- Drop 0-day investigation periods which indicate an investigation that immediately
-- transitions to a valid legal status
final_legal_status_periods_cte AS (
    SELECT *
    FROM legal_status_periods_with_investigation_cte
    -- this clause has the effect of dropping only 0-day investigation periods
    WHERE LegalStatusDesc != 'Investigation'
        OR LegalStatus_StartDate != LegalStatus_EndDate
),

-- UNION together all relevant CTEs that indicate some type of status change
transitions_union AS (
    -- Transfers
    SELECT DISTINCT
        OffenderId,
        Start_TransferDate AS transition_date,
    FROM transfer_periods_supervision_cte

    UNION DISTINCT

    SELECT DISTINCT
        OffenderId,
        End_TransferDate,
    FROM transfer_periods_supervision_cte

    UNION DISTINCT

    -- Legal Status
    SELECT DISTINCT
        OffenderId,
        LegalStatus_StartDate,
    FROM final_legal_status_periods_cte

    UNION DISTINCT

    SELECT DISTINCT
        OffenderId,
        LegalStatus_EndDate,
    FROM final_legal_status_periods_cte

    UNION DISTINCT

    -- Supervising Officer
    SELECT DISTINCT
        OffenderId,
        StartDate,
    FROM supervising_officer_assignments_cte

    UNION DISTINCT

    SELECT DISTINCT
        OffenderId,
        EndDate,
    FROM supervising_officer_assignments_cte

    UNION DISTINCT

    -- Supervision Level
    SELECT DISTINCT
        OffenderId,
        DecisionDate,
    FROM supervision_level_changes_cte

    UNION DISTINCT

    SELECT DISTINCT
        OffenderId,
        NextDecisionDate,
    FROM supervision_level_changes_cte

    UNION DISTINCT

    -- Physical Location
    SELECT DISTINCT
        OffenderId,
        LocationChangeStartDate,
    FROM physical_location_periods_cte

    UNION DISTINCT

    SELECT DISTINCT
        OffenderId,
        LocationChangeEndDate,
    FROM physical_location_periods_cte
),

-- Create proto-periods by using LEAD
periods_cte AS (
    SELECT
        OffenderId,
        transition_date AS start_date,
        LEAD(transition_date) OVER (PARTITION BY OffenderId ORDER BY transition_date) AS end_date,
    FROM transitions_union
),

-- Join back to all original CTEs to get attributes for each session
periods_with_attributes AS (
    SELECT DISTINCT
        p.OffenderId,
        p.start_date,
        p.end_date,

        -- Only populate transfer details if this period matches the transfer date
        -- This prevents us from improperly setting the admission reason for subsequent
        -- periods following the actual transfer when the actual admission reason is due
        -- to some other attribute changing, like supervision level or supervising officer
        IF(p.start_date = t.Start_TransferDate, t.Start_TransferReasonDesc, NULL) AS Start_TransferReasonDesc,
        IF(p.end_date = t.End_TransferDate, t.End_TransferReasonDesc, NULL) AS End_TransferReasonDesc,
        IF(p.start_date = t.Start_TransferDate, t.Start_TransferTypeDesc, NULL) AS Start_TransferTypeDesc,
        IF(p.end_date = t.End_TransferDate, t.End_TransferTypeDesc, NULL) AS End_TransferTypeDesc,
        -- Keep location information even if this period doesn't refer to the transfer
        -- since location information is still relevant

        IFNULL(t.DOCLocationToName, ls.Investigation_LocationName) AS DOCLocationToName,
        IFNULL(t.DOCLocationToTypeName, ls.Investigation_LocationTypeName) AS DOCLocationToTypeName,
        IFNULL(t.DOCLocationToSubTypeName, ls.Investigation_LocationSubTypeName) AS DOCLocationToSubTypeName,

        ls.LegalStatusDesc,
        ls.Priority,

        sa.SupervisionAssignmentTypeDesc,
        sa.StaffId,
        sa.EmployeeTypeName,
        sa.FirstName,
        sa.MiddleName,
        sa.LastName,

        sl.RequestedSupervisionAssignmentLevel,

        pl.PhysicalLocationTypeDesc,
        pl.LocationName,

    FROM periods_cte p

    LEFT JOIN transfer_periods_supervision_cte t
        ON t.OffenderId = p.OffenderId
        AND p.start_date >= t.Start_TransferDate
        AND p.end_date <= t.End_TransferDate

    LEFT JOIN final_legal_status_periods_cte ls
        ON ls.OffenderId = p.OffenderId
        AND p.start_date >= ls.LegalStatus_StartDate
        AND p.end_date <= ls.LegalStatus_EndDate

    LEFT JOIN supervising_officer_assignments_cte sa
        ON sa.OffenderId = p.OffenderId
        AND p.start_date >= sa.StartDate
        AND p.end_date <= sa.EndDate

    LEFT JOIN supervision_level_changes_cte sl
        ON sl.OffenderId = p.OffenderId
        AND p.start_date >= sl.DecisionDate
        AND p.end_date <= sl.NextDecisionDate

    LEFT JOIN physical_location_periods_cte pl
        ON pl.OffenderId = p.OffenderId
        AND p.start_date >= pl.LocationChangeStartDate
        AND p.end_date <= pl.LocationChangeEndDate
),

supervision_periods AS (
    SELECT
        OffenderId,
        start_date,
        IF(end_date = '9999-12-31', NULL, end_date) AS end_date,
        Start_TransferReasonDesc,
        End_TransferReasonDesc,
        DOCLocationToName,
        DOCLocationToTypeName,
        LegalStatusDesc,
        StaffId,
        EmployeeTypeName,
        FirstName,
        MiddleName,
        LastName,
        RequestedSupervisionAssignmentLevel,
        PhysicalLocationTypeDesc,
        LocationName,
        ROW_NUMBER() OVER (
            PARTITION BY OffenderId
            -- Include LegalStatusDesc and StaffId since there can be multiple
            -- values for these fields during a given period
            ORDER BY
                start_date,
                end_date,
                LegalStatusDesc,
                StaffId
        ) AS period_id
    FROM periods_with_attributes
    -- Use transfer data as the source of truth for when someone is actually on supervision
    -- to prevent mistakenly open supervision periods. The one exception is for Investigation
    -- periods, which we will include since they frequently occur before a transfer is recorded.
    WHERE DOCLocationToName IS NOT NULL
        OR LegalStatusDesc = 'Investigation'
)

SELECT *
FROM supervision_periods
"""


VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_ix",
    ingest_view_name="supervision_period",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="OffenderId, period_id",
    materialize_raw_data_table_views=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
