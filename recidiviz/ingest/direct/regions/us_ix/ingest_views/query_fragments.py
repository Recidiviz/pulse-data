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
"""Shared helper fragments for the US_IX ingest view queries."""

# Query fragments that rely only on raw data tables

BED_ASSIGNMENT_PERIODS_CTE = r"""
-- This cte returns one row per bed assignment period for an incarcerated individual
-- It includes information on the bed type and bed security level, as well as the ID
-- of the FacilityLevel (housing unit) in which the bed is located.
bed_assignment_periods_cte AS (
    SELECT
        OffenderId,
        BedAssignmentId,
        CAST(FromDate AS DATETIME) AS FromDate,
        CAST(ToDate AS DATETIME) AS ToDate,
        ChangeReasonName,
        BedId,
        BedTypeDesc,
        SecurityLevelDesc AS Bed_SecurityLevelDesc,
        SecurityLevelRank AS Bed_SecurityLevelRank,
        FacilityLevelId,
    FROM {hsn_BedAssignment}
    LEFT JOIN {hsn_ChangeReason}
        USING (ChangeReasonId)
    LEFT JOIN {hsn_Bed}
        USING (BedId)
    LEFT JOIN {hsn_BedType}
        USING (BedTypeId)
    LEFT JOIN {hsn_Bed_SecurityLevel}
        USING (BedId)
    LEFT JOIN {hsn_SecurityLevel}
        USING (SecurityLevelId)
)"""

LEGAL_STATUS_PERIODS_CTE = r"""
-- Returns one row per legal status period. End dates are non-null, but current legal
-- status periods are represented with an EndDate of '9999-12-31'.
legal_status_periods_cte AS (
    SELECT DISTINCT
        c.OffenderId,
        ls.LegalStatusDesc,
        ls.Priority, -- in cases of overlapping legal statuses, use the lowest number priority 
        CAST(ols.StartDate AS DATETIME) AS LegalStatus_StartDate,
        CAST(ols.EndDate AS DATETIME) AS LegalStatus_EndDate,
    FROM {ind_OffenderLegalStatus} ols
    LEFT JOIN {ind_LegalStatusObjectCharge} lsoc
        USING (LegalStatusObjectId)
    LEFT JOIN {scl_Charge} c
        USING (ChargeId)
    LEFT JOIN {ind_LegalStatus} ls
        USING (LegalStatusId)
)"""

LEGAL_STATUS_PERIODS_INCARCERATION_CTE = r"""
-- Filters legal status periods to only incarceration statuses
legal_status_periods_incarceration_cte AS (
    SELECT *
    FROM legal_status_periods_cte
    WHERE LegalStatusDesc IN (
        'Non Idaho Commitment', -- unsure, maybe drop
        'Parole Violator', -- can be used during parole board hold OR absconscions
        'Rider',
        'Termer'
    )
)"""

LEGAL_STATUS_PERIODS_SUPERVISION_CTE = r"""
-- Filters legal status periods to only supervision statuses
legal_status_periods_supervision_cte AS (
    SELECT *
    FROM legal_status_periods_cte
    WHERE LegalStatusDesc IN (
        'Non Idaho Commitment', -- unsure, maybe drop
        'Parole',
        'Parole Violator', -- can be used during parole board hold OR absconscions
        'Probation'
    )
)"""

LOCATION_DETAILS_CTE = r"""
-- helper CTE that joins the primary location table with relevant code tables
location_details_cte AS (
    SELECT
        LocationId,
        LocationName,
        LocationTypeName,
        LocationSubTypeName,
    FROM {ref_Location} l
    LEFT JOIN {ref_LocationType} lt
        USING (LocationTypeId)
    LEFT JOIN {ref_LocationSubType} lst
        USING (LocationSubTypeId)
)"""

SUPERVISING_OFFICER_ASSIGNMENTS_CTE = r"""
-- Returns one row per supervising officer assignment
supervising_officer_assignments_cte AS (
    SELECT
        OffenderId,
        SupervisionAssignmentId,
        SupervisionAssignmentTypeId,
        SupervisionAssignmentTypeDesc,
        CAST(sa.StartDate AS DATETIME) AS StartDate,
        CAST(sa.EndDate AS DATETIME) AS EndDate,
        EmployeeId,
        StaffId,
        EmployeeTypeName,
    FROM {sup_SupervisionAssignment} sa
    LEFT JOIN {scl_MasterTerm}
        USING (MasterTermId)
    LEFT JOIN {ref_Employee} e
        USING (EmployeeId)
    LEFT JOIN {sup_SupervisionAssignmentType}
        USING (SupervisionAssignmentTypeId)
    LEFT JOIN {ref_EmployeeType}
        USING (EmployeeTypeId)
)"""

SUPERVISION_LEVEL_CHANGES_CTE = r"""
-- Returns one row per supervision level assignment event
supervision_level_changes_cte AS (
    SELECT
        OffenderId,
        SupervisionLevelChangeRequestId,
        CAST(DecisionDate AS DATETIME) AS DecisionDate,
        CAST(LEAD(DecisionDate) OVER (
            PARTITION BY OffenderId
            ORDER BY CAST(DecisionDate AS DATETIME)
        ) AS DATETIME) AS NextDecisionDate,
        p.SupervisionAssignmentLevelDesc AS PreviousSupervisionAssignmentLevelDesc,
        r.SupervisionAssignmentLevelDesc AS RequestedSupervisionAssignmentLevel,
    FROM {sup_SupervisionLevelChangeRequest}
    LEFT JOIN {scl_MasterTerm}
        USING (MasterTermId)
    LEFT JOIN {sup_SupervisionAssignmentLevel} p
        ON PreviousSupervisionAssignmentLevelId = p.SupervisionAssignmentLevelId
    LEFT JOIN {sup_SupervisionAssignmentLevel} r
        ON RequestedSupervisionAssignmentLevelId = r.SupervisionAssignmentLevelId
    -- Only keep Approved requests
    WHERE SupervisionLevelChangeRequestStatusId = '3'
)"""

# Compound query fragments (rely on previously constructed query fragments)

# requires BED_ASSIGNMENT_PERIODS_CTE
# requires FACILITY_LEVEL_DETAILS_CTE
BED_ASSIGNMENT_PERIODS_WITH_FACILITY_LEVEL_DETAILS_CTE = r"""
-- Returns one row per bed assignment period, but with all details on bed, facility
-- level (housing unit), and facility included.
bed_assignment_periods_with_facility_level_details_cte AS (
    SELECT
        OffenderId,
        BedAssignmentId,
        FromDate,
        ToDate,
        ChangeReasonName,
        BedId,
        BedTypeDesc,
        Bed_SecurityLevelDesc,
        Bed_SecurityLevelRank,
        FacilityLevelId,
        FacilityLevel_Highest_SecurityLevelDesc,
        FacilityLevel_Highest_SecurityLevelRank,
        Top_FacilityLevelId,
        Top_FacilityLevelName,
        Top_FacilityLevel_Highest_SecurityLevelDesc,
        Top_FacilityLevel_Highest_SecurityLevelRank,
        FacilityId,
        Facility_Highest_SecurityLevelDesc,
        Facility_Highest_SecurityLevelRank,
        LocationName,
        LocationTypeName,
        LocationSubTypeName,
    FROM bed_assignment_periods_cte
    LEFT JOIN facility_level_details_cte
        USING (FacilityLevelId)
)"""

# requires LOCATION_DETAILS_CTE
FACILITY_LEVEL_DETAILS_CTE = r"""
-- Ideally this would use a recursive query, but BigQuery only allows a recursive cte at the beginning
-- of a statement. Also I'm sure there would be some errors in the Postgres translation layer.

-- The hsn_FacilityLevel table is hierarchical in nature with three levels of parent/child references.
-- We define tiers as follows:
--   Tier 1: facility levels with no parents, in other words top-level facility levels.
--   Tier 2: facility levels whose parents are tier 1 levels
--   Tier 3: facility levels whose parents are tier 2 levels

-- Why even define tiers? Beds are associated with a facility level ID which may be either a cell or a
-- larger unit within a facility. The facility level code seems most appropriate for hydrating the
-- housing unit value of an incarceration period, but in cases where the facility level is actually a cell
-- this association no longer makes sense. Therefore, we create a mapping from any arbitrary facility level
-- to the top tier facility level in which it is placed in order to have a standard value for labeling
-- the housing unit.

tier_1_facility_levels AS (
    SELECT
        FacilityLevelId AS tier_1_FacilityLevelId,
        FacilityLevelName AS tier_1_FacilityLevelName,
    FROM {hsn_FacilityLevel}
    WHERE ParentFacilityLevelId IS NULL
),
tier_2_facility_levels AS (
    SELECT
        FacilityLevelId AS tier_2_FacilityLevelId,
        tier_1_FacilityLevelId,
        tier_1_FacilityLevelName,
    FROM {hsn_FacilityLevel}
    INNER JOIN tier_1_facility_levels
        ON ParentFacilityLevelId = tier_1_FacilityLevelId
),
tier_3_facility_levels AS (
    SELECT
        FacilityLevelId AS tier_3_FacilityLevelId,
        tier_2_FacilityLevelId,
    FROM {hsn_FacilityLevel}
    INNER JOIN tier_2_facility_levels
        ON ParentFacilityLevelId = tier_2_FacilityLevelId
),
all_tiers_facility_levels AS (
    SELECT
        tier_3_FacilityLevelId AS FacilityLevelId,
        tier_1_FacilityLevelId AS Top_FacilityLevelId,
        tier_1_FacilityLevelName AS Top_FacilityLevelName,
    FROM tier_3_facility_levels
    LEFT JOIN tier_2_facility_levels
        USING (tier_2_FacilityLevelId)

    UNION DISTINCT

    SELECT
        tier_2_FacilityLevelId,
        tier_1_FacilityLevelId,
        tier_1_FacilityLevelName AS Top_FacilityLevelName,
    FROM tier_2_facility_levels

    UNION DISTINCT

    SELECT
        tier_1_FacilityLevelId,
        tier_1_FacilityLevelId,
        tier_1_FacilityLevelName AS Top_FacilityLevelName,
    FROM tier_1_facility_levels
),
tier_1_security_levels AS (
    SELECT * EXCEPT (Top_FacilityLevel_SecurityLevelRank_rn)
    FROM (
        SELECT
            tier_1_FacilityLevelId AS Top_FacilityLevelId,
            SecurityLevelDesc AS Top_FacilityLevel_Highest_SecurityLevelDesc,
            SecurityLevelRank AS Top_FacilityLevel_Highest_SecurityLevelRank,
            ROW_NUMBER() OVER (
                PARTITION BY tier_1_FacilityLevelId
                ORDER BY SecurityLevelRank DESC
            ) AS Top_FacilityLevel_SecurityLevelRank_rn,
        FROM tier_1_facility_levels
        LEFT JOIN {hsn_FacilityLevel_SecurityLevel}
            ON tier_1_FacilityLevelId = FacilityLevelId
        LEFT JOIN {hsn_SecurityLevel}
            USING (SecurityLevelId)
    ) a
    WHERE a.Top_FacilityLevel_SecurityLevelRank_rn = 1
),
facility_security_levels AS (
    SELECT * EXCEPT (Facility_SecurityLevelRank_rn)
    FROM (
        SELECT
            FacilityId,
            SecurityLevelDesc AS Facility_Highest_SecurityLevelDesc,
            SecurityLevelRank AS Facility_Highest_SecurityLevelRank,
            ROW_NUMBER() OVER (
                PARTITION BY FacilityId
                ORDER BY SecurityLevelRank DESC
            ) AS Facility_SecurityLevelRank_rn,
        FROM {hsn_Facility_SecurityLevel}
        LEFT JOIN {hsn_SecurityLevel}
            USING (SecurityLevelId)
    ) a
    WHERE a.Facility_SecurityLevelRank_rn = 1
),
facility_level_details_cte AS (
    SELECT * EXCEPT (FacilityLevel_SecurityLevelRank_rn)
    FROM (
        SELECT
            FacilityLevelId,
            SecurityLevelDesc AS FacilityLevel_Highest_SecurityLevelDesc,
            SecurityLevelRank AS FacilityLevel_Highest_SecurityLevelRank,
            ROW_NUMBER() OVER (
                PARTITION BY FacilityLevelId
                ORDER BY SecurityLevelRank DESC
            ) AS FacilityLevel_SecurityLevelRank_rn,
            Top_FacilityLevelId,
            Top_FacilityLevelName,
            Top_FacilityLevel_Highest_SecurityLevelDesc,
            Top_FacilityLevel_Highest_SecurityLevelRank,
            FacilityId,
            Facility_Highest_SecurityLevelDesc,
            Facility_Highest_SecurityLevelRank,
            LocationName,
            LocationTypeName,
            LocationSubTypeName,
        FROM all_tiers_facility_levels
        LEFT JOIN {hsn_FacilityLevel}
            USING (FacilityLevelId)
        LEFT JOIN {hsn_FacilityLevel_SecurityLevel}
            USING (FacilityLevelId)
        LEFT JOIN {hsn_SecurityLevel}
            USING (SecurityLevelId)
        LEFT JOIN tier_1_security_levels
            USING (Top_FacilityLevelId)
        LEFT JOIN {hsn_Facility}
            USING (FacilityId)
        LEFT JOIN facility_security_levels
            USING (FacilityId)
        LEFT JOIN location_details_cte
            USING (LocationId)
    ) a
    WHERE a.FacilityLevel_SecurityLevelRank_rn = 1
)"""

# requires LOCATION_DETAILS_CTE
INVESTIGATION_DETAILS_CTE = r"""
-- Returns one row per investigation event. Currently limited to pre-sentencing
-- investigations, but may eventually be expanded to include additional investigation
-- types, if they are found to be utilized in Atlas.
investigation_details_cte AS (
    SELECT
        OffenderId,
        InvestigationId,
        -- Confirm this is the best date
        RequestDate,
        InvestigationStatusDesc,
        -- Investigate uses of other types of investigations, like LSU transfer: '254'
        InvestigationTypeDesc,
        LocationName,
        LocationTypeName,
        LocationSubTypeName,
    FROM {com_Investigation}
    LEFT JOIN {com_InvestigationType}
        USING (InvestigationTypeId)
    LEFT JOIN {com_InvestigationStatus}
        USING (InvestigationStatusId)
    LEFT JOIN location_details_cte
        ON ReceivingDOCLocationId = LocationId
    WHERE InvestigationTypeId IN (
        '31', -- Conversion investigation records
        '10'  -- New PSI investigations
    )
)"""

# requires LOCATION_DETAILS_CTE
OFFENDER_LOCATION_HISTORY_CTE = r"""
-- Returns one row per period of an individuals assigned location. Lines up with transfers,
-- so no need to add this to a query already utilizing the transfers table.
offender_location_history_cte AS (
    SELECT
        OffenderId,
        CAST(CurrentFromDate AS DATETIME) AS CurrentFromDate,
        CAST(CurrentToDate AS DATETIME) AS CurrentToDate,
        LocationName,
        LocationTypeName,
        LocationSubTypeName,
    FROM {ind_OffenderLocationHistory}
    LEFT JOIN location_details_cte
        USING (LocationId)
)"""

# requires LOCATION_DETAILS_CTE
PHYSICAL_LOCATION_PERIODS_CTE = r"""
-- Returns one row per "physical location" period. This table is used to track
-- when a person has absconded or is on interstate compact.
physical_location_periods_cte AS (
    SELECT
        OffenderId,
        CAST(LocationChangeStartDate AS DATETIME) AS LocationChangeStartDate,
        CAST(LocationChangeEndDate AS DATETIME) AS LocationChangeEndDate,
        PhysicalLocationTypeDesc,
        LocationName,
        LocationTypeName,
        LocationSubTypeName,
    FROM {com_PhysicalLocation}
    LEFT JOIN {com_PhysicalLocationType}
        USING (PhysicalLocationTypeId)
    LEFT JOIN location_details_cte
        USING (LocationId)
    WHERE PhysicalLocationTypeId IN (
        '4', -- Absconded
        '7'  -- Interstate Compact
    )
)"""

# requires LOCATION_DETAILS_CTE
TRANSFER_DETAILS_CTE = r"""
-- Returns one row per transfer. This is the primary source of information used in the
-- construction of supervision and incarceration periods.
transfer_details_cte AS (
    SELECT
        OffenderId,
        TransferId,
        TransferReasonDesc,
        TransferStatusDesc,
        TransferTypeDesc,
        CAST(TransferDate AS DATETIME) AS TransferDate,
        loc_from.LocationName AS DOCLocationFromName,
        loc_from.LocationTypeName AS DOCLocationFromTypeName,
        loc_from.LocationSubTypeName AS DOCLocationFromSubTypeName,
        loc_to.LocationName AS DOCLocationToName,
        loc_to.LocationTypeName AS DOCLocationToTypeName,
        loc_to.LocationSubTypeName AS DOCLocationToSubTypeName,
        Notes,
        TermId,
    FROM {com_Transfer} t
    LEFT JOIN {com_TransferReason} tr
        USING (TransferReasonId)
    LEFT JOIN {com_TransferStatus} ts
        USING (TransferStatusId)
    LEFT JOIN {com_TransferType} tt
        USING (TransferTypeId)
    LEFT JOIN location_details_cte loc_from
        ON DOCLocationFromId = loc_from.LocationId
    LEFT JOIN location_details_cte loc_to
        ON DOCLocationToId = loc_to.LocationId
)"""

# requires TRANSFER_DETAILS_CTE
TRANSFER_PERIODS_CTE = r"""
-- Converts the ledger-style of the transfer_details_cte into a period-style result set
-- with start and end dates.
transfer_periods_cte AS (
    SELECT
        OffenderId,
        TransferReasonDesc AS Start_TransferReasonDesc,
        LEAD(TransferReasonDesc) OVER transfer_window AS End_TransferReasonDesc,
        TransferTypeDesc AS Start_TransferTypeDesc,
        LEAD(TransferTypeDesc) OVER transfer_window AS End_TransferTypeDesc,
        TransferDate AS Start_TransferDate,
        LEAD(TransferDate) OVER transfer_window AS End_TransferDate,
        DOCLocationToName,
        DOCLocationToTypeName,
        DOCLocationToSubTypeName,
    FROM transfer_details_cte
    WINDOW transfer_window AS (
        -- Include TermId since a "term" in Idaho describes a period of time during
        -- which a person is under the jurisdiction of the DOC, from initial court
        -- action to final discharge.
        PARTITION BY OffenderId, TermId
        ORDER BY TransferDate
    )
)"""

# requires TRANSFER_PERIODS_CTE
TRANSFER_PERIODS_INCARCERATION_CTE = r"""
transfer_periods_incarceration_cte AS (
    SELECT *
    FROM transfer_periods_cte
    WHERE Start_TransferTypeDesc = 'Out from DOC' -- don't exclude these yet
        OR DOCLocationToTypeName IN (
        'Adult Facility/Institution',
        'Federal Facility',
        'Jail',
        'Juvenile Facility'
    )
        -- This OR clause captures interstate incarceration
        OR (
            DOCLocationToTypeName = 'State' AND (
                -- The wildcards here capture transfers leading into a facility
                -- or out of a facility
                Start_TransferTypeDesc LIKE '%Facility'
                OR
                End_TransferTypeDesc LIKE 'DOC Facility%'
            )
                -- Also include "District Office if SubTypeName is Fugitive Unit
        OR (
            DOCLocationToTypeName = 'District Office'
            AND
            DOCLocationToSubTypeName = 'Fugitive Unit'
            )
        )
)"""

# requires TRANSFER_PERIODS_CTE
TRANSFER_PERIODS_SUPERVISION_CTE = r"""
transfer_periods_supervision_cte AS (
    SELECT *
    FROM transfer_periods_cte
    WHERE Start_TransferTypeDesc = 'Out from DOC' -- don't exclude these yet
        OR DOCLocationToTypeName IN (
        'District Office'
    )
)"""

# Filter clauses
TRANSFER_PERIODS_INCARCERATION_FILTER = r"""
(
    DOCLocationToTypeName IN (
        'Adult Facility/Institution',
        'Federal Facility',
        'Jail',
        'Juvenile Facility'
    )
    -- This OR clause captures interstate incarceration
    OR (
        DOCLocationToTypeName = 'State' AND (
            -- The wildcards here capture transfers leading into a facility
            -- or out of a facility
            Start_TransferTypeDesc LIKE '%Facility'
            OR
            End_TransferTypeDesc LIKE 'DOC Facility%'
        )
    )
)"""

TRANSFER_PERIODS_SUPERVISION_FILTER = r"""
(
    DOCLocationToTypeName IN (
        'District Office'
    )
)"""

LEGAL_STATUS_PERIODS_INCARCERATION_FILTER = r"""
(
    LegalStatusDesc IN (
        'Non Idaho Commitment', -- unsure, maybe drop
        'Parole Violator', -- can be used during parole board hold OR absconscions
        'Rider',
        'Termer'
    )
)"""

LEGAL_STATUS_PERIODS_SUPERVISION_FILTER = r"""
(
    LegalStatusDesc IN (
        'Non Idaho Commitment', -- unsure, maybe drop
        'Parole',
        'Parole Violator', -- can be used during parole board hold OR absconscions
        'Probation'
    )
)"""
