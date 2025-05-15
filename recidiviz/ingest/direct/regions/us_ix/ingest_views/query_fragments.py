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

from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans

# Query fragments that rely only on raw data tables

BED_ASSIGNMENT_PERIODS_CTE = """
-- This cte returns one row per bed assignment period for an incarcerated individual
-- It includes information on the bed type and bed security level, as well as the ID
-- of the FacilityLevel (housing unit) in which the bed is located.
-- Open periods indicated with ToDate = '9999-12-31'
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
        LevelPath,
    FROM {hsn_BedAssignment}
    LEFT JOIN {hsn_ChangeReason}
        USING (ChangeReasonId)
    LEFT JOIN {hsn_Bed}
        USING (BedId)
    LEFT JOIN {hsn_BedType}
        USING (BedTypeId)
    LEFT JOIN (
            -- Each BedId can have multiple entries in
            -- hsn_Bed_SecurityLevelSecurityLevels. We pick the one that was updated
            -- most recently, as so far all the entries have the same SecurityLevelId.
            -- TODO(#17975): Might this change in the future, and if so can we get the
            -- date that the SecurityLevel was associated with the bed?
            SELECT * EXCEPT (rn)
            FROM (
                SELECT
                    BedSecurityLevelId,
                    BedId,
                    SecurityLevelId,
                    ROW_NUMBER() OVER (PARTITION BY BedId ORDER BY UpdateDate DESC, BedSecurityLevelId DESC) AS rn
                FROM {hsn_Bed_SecurityLevel}
            ) ranked_bed_security_level
            WHERE rn = 1
        ) bed_security_level USING (BedId)
    LEFT JOIN {hsn_SecurityLevel}
        USING (SecurityLevelId)
    LEFT JOIN {hsn_FacilityLevel}
        USING (FacilityLevelId)
)"""

SECURITY_LEVEL_PERIODS_CTE = """
security_level_periods_cte as (
    SELECT 
        OffenderId,
        SecurityLevelName,
        CAST(startDate AS DATETIME) AS startDate,
        CAST(LEAD(startDate) OVER (PARTITION BY OffenderId ORDER BY startDate) AS DATETIME) as endDate
    FROM (
        SELECT 
            OffenderId,
            ApprovedDate AS startDate,
            SecurityLevelId, 
            LAG(SecurityLevelId) OVER (PARTITION BY OffenderId ORDER BY ApprovedDate) as prev_level,
            LAG(ApprovedDate) OVER (PARTITION BY OffenderId ORDER BY ApprovedDate) as prev_ApprovedDate 
        FROM {ind_OffenderSecurityLevel}
    ) o
    LEFT JOIN {hsn_SecurityLevel} s
            ON o.SecurityLevelId = s.SecurityLevelId
    WHERE prev_level IS NULL OR o.SecurityLevelId != prev_level
)
"""

LEGAL_STATUS_PERIODS_CTE = """
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
    WHERE c.OffenderId IS NOT NULL 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY c.OffenderId, ls.LegalStatusDesc, DATE(ols.StartDate) ORDER BY ols.EndDate ASC) = 1
)"""

LEGAL_STATUS_PERIODS_INCARCERATION_CTE = """
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

LEGAL_STATUS_PERIODS_SUPERVISION_CTE = """
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

LOCATION_DETAILS_CTE = """
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

SUPERVISING_OFFICER_ASSIGNMENTS_CTE = """
-- Returns one row per supervising officer assignment
-- COALESCE NULL EndDate to '9999-12-31'
supervising_officer_assignments_cte AS (
    SELECT
        OffenderId,
        SupervisionAssignmentId,
        SupervisionAssignmentTypeId,
        SupervisionAssignmentTypeDesc,
        CAST(sa.StartDate AS DATETIME) AS StartDate,
        CAST(IFNULL(sa.EndDate, '9999-12-31') AS DATETIME) AS EndDate,
        -- Avoids using the employee Unknown/None
        CASE 
            WHEN TO_HEX(SHA256(EmployeeId)) =
                "8b75ec30ea0d0fedcfce5224ef7733db7dbc574a1806aa664014eb6350f5a00c"
                THEN NULL
            ELSE EmployeeId
        END AS EmployeeId,
        StaffId,
        EmployeeTypeName,
        FirstName,
        MiddleName,
        LastName
    FROM {sup_SupervisionAssignment} sa
    LEFT JOIN {scl_MasterTerm}
        USING (MasterTermId)
    LEFT JOIN {ref_Employee} e
        USING (EmployeeId)
    LEFT JOIN {sup_SupervisionAssignmentType}
        USING (SupervisionAssignmentTypeId)
    LEFT JOIN {ref_EmployeeType}
        USING (EmployeeTypeId)
    WHERE SupervisionAssignmentTypeDesc = 'Primary'
)"""

SUPERVISION_LEVEL_CHANGES_CTE = """
-- Returns one row per supervision level assignment event
-- COALESCE NULL EndDate to '9999-12-31'
supervision_level_changes_cte AS (
    SELECT
        OffenderId,
        SupervisionLevelChangeRequestId,
        CAST(DecisionDate AS DATETIME) AS DecisionDate,
        CAST(IFNULL(LEAD(DecisionDate) OVER (
            PARTITION BY OffenderId
            ORDER BY CAST(DecisionDate AS DATETIME), CAST(s.UpdateDate AS DATETIME), SupervisionLevelChangeRequestId
        ), '9999-12-31') AS DATETIME) AS NextDecisionDate,
        p.SupervisionAssignmentLevelDesc AS PreviousSupervisionAssignmentLevelDesc,
        r.SupervisionAssignmentLevelDesc AS RequestedSupervisionAssignmentLevel,
    FROM {sup_SupervisionLevelChangeRequest} s
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
BED_ASSIGNMENT_PERIODS_WITH_FACILITY_LEVEL_DETAILS_CTE = """
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
FACILITY_LEVEL_DETAILS_CTE = """
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
INVESTIGATION_DETAILS_CTE = """
-- Returns one row per investigation event. Currently limited to pre-sentencing
-- investigations, but may eventually be expanded to include additional investigation
-- types, if they are found to be utilized in Atlas.
investigation_details_cte AS (
    -- Legacy/Converted Records
    SELECT
        OffenderId,
        InvestigationId,
        CAST(TRIM(SPLIT(REGEXP_EXTRACT(RequestNotes, r'start_dt: [0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'), ':')[OFFSET(1)]) AS DATETIME) AS AssignedDate,
        CAST(
            IFNULL(TRIM(SPLIT(REGEXP_EXTRACT(RequestNotes, r'release_dt: [0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'), ':')[OFFSET(1)]), '9999-12-31') AS DATETIME
        ) AS CompletionDate,
        LocationName,
        LocationTypeName,
        LocationSubTypeName,
    FROM {com_Investigation}
    LEFT JOIN location_details_cte
        ON ReceivingDOCLocationId = LocationId
    WHERE InvestigationTypeId = '31' -- Conversion investigation records
        AND InvestigationStatusId = '3' -- Accepted

    UNION ALL

    -- Atlas Records
    SELECT
        OffenderId,
        InvestigationId,
        CAST(AssignedDate AS DATETIME) AS AssignedDate,
        CAST(CompletionDate AS DATETIME) AS CompletionDate,
        LocationName,
        LocationTypeName,
        LocationSubTypeName,
    FROM {com_Investigation}
    LEFT JOIN location_details_cte
        ON ReceivingDOCLocationId = LocationId
    WHERE InvestigationTypeId = '10'  -- New PSI investigations
        AND InvestigationStatusId = '3' -- Accepted
)"""

# requires LOCATION_DETAILS_CTE
OFFENDER_LOCATION_HISTORY_CTE = """
-- Returns one row per period of an individuals assigned location. Lines up with transfers,
-- so no need to add this to a query already utilizing the transfers table.
-- Open periods indicated with CurrentToDate = '9999-12-31'
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
PHYSICAL_LOCATION_PERIODS_CTE = """
-- Returns one row per "physical location" period. This table is used to track
-- when a person has absconded or is on interstate compact.
-- COALESCE NULL LocationChangeEndDate to '9999-12-31'
physical_location_periods_cte AS (
    SELECT
        OffenderId,
        CAST(LocationChangeStartDate AS DATETIME) AS LocationChangeStartDate,
        CAST(IFNULL(LocationChangeEndDate, '9999-12-31') AS DATETIME) AS LocationChangeEndDate,
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
TRANSFER_DETAILS_CTE = """
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
        t.UpdateDate,
        t.Locking
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
    WHERE TransferStatusDesc = "Confirmed"
)"""

# requires TRANSFER_DETAILS_CTE
TRANSFER_PERIODS_CTE = """
-- Converts the ledger-style of the transfer_details_cte into a period-style result set
-- with start and end dates.
-- COALESCE NULL End_TransferDate to '9999-12-31'
transfer_periods_cte AS (
    SELECT
        OffenderId,
        TransferReasonDesc AS Start_TransferReasonDesc,
        LEAD(TransferReasonDesc) OVER transfer_window AS End_TransferReasonDesc,
        TransferTypeDesc AS Start_TransferTypeDesc,
        LEAD(TransferTypeDesc) OVER transfer_window AS End_TransferTypeDesc,
        TransferDate AS Start_TransferDate,
        IFNULL(LEAD(TransferDate) OVER transfer_window, CAST('9999-12-31' AS DATETIME)) AS End_TransferDate,
        DOCLocationToName,
        DOCLocationToTypeName,
        DOCLocationToSubTypeName,
    FROM transfer_details_cte
    WINDOW transfer_window AS (
        PARTITION BY OffenderId
        ORDER BY TransferDate, UpdateDate, Locking
    )
)"""

# requires TRANSFER_PERIODS_CTE
TRANSFER_PERIODS_INCARCERATION_CTE = """
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
TRANSFER_PERIODS_SUPERVISION_CTE = """
transfer_periods_supervision_cte AS (
    SELECT *
    FROM transfer_periods_cte
    WHERE DOCLocationToTypeName IN (
        'District Office'
    )
        -- Exclude periods of escape from facilities
        AND DOCLocationToSubTypeName NOT IN (
            'Fugitive Unit'
        )
)"""

# Filter clauses
TRANSFER_PERIODS_INCARCERATION_FILTER = """
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

TRANSFER_PERIODS_SUPERVISION_FILTER = """
(
    DOCLocationToTypeName IN (
        'District Office'
    )
)"""

LEGAL_STATUS_PERIODS_INCARCERATION_FILTER = """
(
    LegalStatusDesc IN (
        'Investigation',
        'Non Idaho Commitment', -- unsure, maybe drop
        'Parole Violator', -- can be used during parole board hold OR absconscions
        'Rider',
        'Termer'
    )
)"""

LEGAL_STATUS_PERIODS_SUPERVISION_FILTER = """
(
    LegalStatusDesc IN (
        'Investigation',
        'Non Idaho Commitment', -- unsure, maybe drop
        'Parole',
        'Parole Violator', -- can be used during parole board hold OR absconscions
        'Probation'
    )
)"""

# Charge/Sentencing

# requires LOCATION_DETAILS_CTE
CHARGE_DETAILS_CTE = """
charge_details_cte AS (
    SELECT
        OffenderId,
        ChargeId,
        Docket,
        LocationName, -- State
        ChargeOutcomeTypeDesc,
    FROM {scl_Charge}
    LEFT JOIN {scl_ChargeOutcomeType}
        USING (ChargeOutcomeTypeId)
    LEFT JOIN location_details_cte
        ON StateId = LocationId
)"""

TERM_DETAILS_CTE = """
term_details_cte AS (
    SELECT
        OffenderId,
        MasterTermId,
        MasterTermStatusDesc,
        TermId,
        TermStatusDesc,
        CAST(TermStartDate AS DATETIME) AS TermStartDate,
        CAST(TermSentenceDate AS DATETIME) AS TermSentenceDate,
        CAST(ReleaseDate AS DATETIME) AS ReleaseDate,
        CAST(TentativeParoleDate AS DATETIME) AS TentativeParoleDate,
        CAST(NextHearingDate AS DATETIME) AS NextHearingDate,
        CAST(SegmentStartDate AS DATETIME) AS SegmentStartDate,
        CAST(IndeterminateStartDate AS DATETIME) AS IndeterminateStartDate,
        CAST(IndeterminateEndDate AS DATETIME) AS IndeterminateEndDate,
        CAST(ProbationStartDate AS DATETIME) AS ProbationStartDate,
        CAST(ProbationExpirationDate AS DATETIME) AS ProbationExpirationDate,
        CAST(RiderStartDate AS DATETIME) AS RiderStartDate,
        CAST(RiderExpiryDate AS DATETIME) AS RiderExpiryDate,
        CAST(InitialParoleHearingDate AS DATETIME) AS InitialParoleHearingDate,
        CAST(FtrdApprovedDate AS DATETIME) AS FtrdApprovedDate,
    FROM {scl_MasterTerm}
    LEFT JOIN {scl_MasterTermStatus}
        USING (MasterTermStatusId)
    LEFT JOIN {scl_Term}
        USING (MasterTermId, OffenderId)
    LEFT JOIN {scl_TermStatus}
        USING (TermStatusId)
)"""

OFFENSE_DETAILS_CTE = """
offense_details_cte AS (
    SELECT
        OffenderId,
        OffenseId,
        SentenceOrderId,
        SentenceId,
        Count,
        CAST(OffenseDate AS DATETIME) AS OffenseDate,
        OffenseCategoryDesc,
        OffenseLawDesc,
        OffenseTypeDesc,
        OFFENSE_STATUTE,
        VIOLENT_OFFENSE_IND,
    FROM {scl_Offense}
    LEFT JOIN {scl_OffenseType}
        USING (OffenseTypeId)
    LEFT JOIN {scl_OffenseCategory}
        USING (OffenseCategoryId)
    LEFT JOIN {scl_OffenseLaw}
        USING (OffenseLawId)

    -- Only join to get OffenderId
    LEFT JOIN {scl_SentenceOrder}
        USING (SentenceOrderId)

    -- Get associated SentenceId
    LEFT JOIN {scl_SentenceLinkOffense}
        USING (OffenseId)
    LEFT JOIN {scl_SentenceLink}
        USING (SentenceLinkId)
)"""

PAROLE_DETAILS_CTE = """
parole_details_cte AS (
    SELECT
        OffenderId,
        ParoleId,
        TermId,
        CAST(ReleaseDate AS DATETIME) AS ReleaseDate,
        ParoleTypeDesc,
    FROM {scl_Parole}
    LEFT JOIN {scl_ParoleType}
        USING (ParoleTypeId)
)"""

PROBATION_DETAILS_CTE = """
probation_details_cte AS (
    SELECT
        OffenderId,
        SentenceOrderId,
        ProbationSupervisionId,
        ProbationStartDesc,
        DurationYear,
        DurationMonth,
        DurationDay,
        CAST(StartDate AS DATETIME) AS StartDate,
        CAST(EndDate AS DATETIME) AS EndDate,
    FROM {scl_ProbationSupervision}
    LEFT JOIN {scl_ProbationStart}
        USING (ProbationStartId)

    -- Only join to get OffenderId
    LEFT JOIN {scl_SentenceOrder}
        USING (SentenceOrderId)
)"""

# requires LOCATION_DETAILS_CTE
SUPERVISION_DETAILS_CTE = """
supervision_details_cte AS (
    SELECT
        OffenderId,
        SupervisionId,
        MasterTermId,
        CourtAuthority,
        LocationName,
        LocationTypeName,
        LocationSubTypeName,
        JudgeId,
        FirstName,
        MiddleName,
        LastName,
        CAST(SupervisionMEDDate AS DATETIME) AS SupervisionMEDDate,
        InterstateTypeParole,
        InterstateTypeProbation,
        InterstateTypeOther,
        ParoleId,
        CAST(ImposedDate AS DATETIME) AS ImposedDate,
        CAST(StartDate AS DATETIME) AS StartDate,
        SupervisionTypeDesc,
        SupervisionStatusDesc,
    FROM {scl_Supervision}
    LEFT JOIN {scl_SupervisionStatus}
        USING (SupervisionStatusId)
    LEFT JOIN {scl_SupervisionType}
        USING (SupervisionTypeId)
    LEFT JOIN {scl_Legist}
        ON JudgeId = LegistId
        AND LegistTypeId = '1' -- Judge
    LEFT JOIN location_details_cte
        ON CourtAuthority = LocationId
)"""

# requires LOCATION_DETAILS_CTE
SENTENCE_ORDER_DETAILS_CTE = """
sentence_order_details_cte AS (
    SELECT
        OffenderId,
        TermId,
        ChargeId,
        Sequence,
        SentenceOrderId,
        ParentSentenceOrderId,
        SentenceId,
        HistoricalDocket,
        CAST(EffectiveDate AS DATETIME) AS EffectiveDate,
        CAST(SentenceDate AS DATETIME) AS SentenceDate,
        SentenceOrderCategoryDesc,
        SentenceOrderTypeDesc,
        LegalStatusDesc,
        SentenceOrderEventTypeName,
        SentenceOrderStatusName,
        IsApproved,
        StateId,
        state.LocationName AS StateName,
        CountyId,
        county.LocationName AS CountyName,
        ProsecutingAuthorityLocationId,
        prosecuting_authority.LocationName AS ProsecutingAuthorityName,
        JudgeLegistId,
        FirstName,
        MiddleName,
        LastName,
        CAST(CorrectionsCompactStartDate AS DATETIME) AS CorrectionsCompactStartDate,
        CAST(CorrectionsCompactEndDate AS DATETIME) AS CorrectionsCompactEndDate,
    FROM {scl_SentenceOrder}
    LEFT JOIN {scl_SentenceOrderType}
        USING (SentenceOrderTypeId)
    LEFT JOIN (
        SELECT DISTINCT
            SentenceOrderTypeId,
            LegalStatusDesc,
        FROM {scl_SentenceOrderType_LegalStatus}
        LEFT JOIN {scl_SentenceOrderType}
            USING (SentenceOrderTypeId)
        LEFT JOIN {ind_LegalStatus}
            USING (LegalStatusId)
    ) SentenceOrderType_LegalStatus
        USING (SentenceOrderTypeId)
    LEFT JOIN {scl_SentenceOrderCategory}
        USING (SentenceOrderCategoryId)
    LEFT JOIN {scl_SentenceOrderEventType}
        USING (SentenceOrderEventTypeId)
    LEFT JOIN {scl_SentenceOrderStatus}
        USING (SentenceOrderStatusId)
    LEFT JOIN {scl_Legist}
        ON JudgeLegistId = LegistId
    LEFT JOIN location_details_cte state
        ON StateId = state.LocationId
    LEFT JOIN location_details_cte county
        ON CountyId = county.LocationId
    LEFT JOIN location_details_cte prosecuting_authority
        ON ProsecutingAuthorityLocationId = prosecuting_authority.LocationId

    -- Get associated SentenceId
    LEFT JOIN {scl_SentenceLinkSentenceOrder}
        USING (SentenceOrderId)
    LEFT JOIN {scl_SentenceLink}
        USING (SentenceLinkId)
)"""

SENTENCE_DETAILS_CTE = """
sentence_details_cte AS (
    SELECT
        OffenderId,
        MasterTermId,
        TermId,
        SentenceId,
        Stayed,
        SentenceTypeDesc,
        SentenceStatusDesc,
        CAST(DischargeDate AS DATETIME) AS DischargeDate,
        SentenceDetailId,
        SegmentYears,
        SegmentMonths,
        SegmentDays,
        SegmentMaxYears,
        SegmentMaxMonths,
        SegmentMaxDays,
        CAST(SegmentPED AS DATETIME) AS SegmentPED,
        CAST(SegmentIndeterminateStartDate AS DATETIME) AS SegmentIndeterminateStartDate,
        CAST(SegmentIndeterminateEndDate AS DATETIME) AS SegmentIndeterminateEndDate,
        CAST(SegmentStartDate AS DATETIME) AS SegmentStartDate,
        CAST(SegmentEndDate AS DATETIME) AS SegmentEndDate,
        CAST(SegmentSatisfactionDate AS DATETIME) AS SegmentSatisfactionDate,
        SentenceDetailTypeDesc,
        -- Special circumstances (life/death)
        OffenseSentenceTypeName,
    FROM {scl_Sentence}
    LEFT JOIN {scl_SentenceType}
        USING (SentenceTypeId)
    LEFT JOIN {scl_SentenceStatus}
        USING (SentenceStatusId)
    LEFT JOIN {scl_SentenceDetail}
        USING (SentenceId)
    LEFT JOIN {scl_SentenceDetailType}
        USING (SentenceDetailTypeId)
    LEFT JOIN {scl_OffenseSentenceType}
        USING (OffenseSentenceTypeId)
)"""

RETAINED_JURISDICTION_DETAILS_CTE = """
retained_jurisdiction_details_cte AS (
    SELECT
        SentenceOrderID,
        RetainedJurisdictionId,
        RetainedJurisdictionTypeName,
        CAST(RetentionStartDate AS DATETIME) AS RetentionStartDate,
        CAST(RetentionEndDate AS DATETIME) AS RetentionEndDate,
        CAST(QuashedDate AS DATETIME) AS QuashedDate,
    FROM {scl_RetainedJurisdiction}
    LEFT JOIN {scl_RetainedJurisdictionType}
        USING (RetainedJurisdictionTypeId)
)"""

CURRENT_ATLAS_EMPLOYEE_INFO_CTE = """
    -- compile the most recent info for each employee
    --   recency order prioritizes Inactive = '0' and then most recently inserted record
    current_atlas_employee_info as (
        SELECT * EXCEPT(recency_rnk)
        FROM (
            SELECT
                EmployeeId,
                UPPER(FirstName) as FirstName,
                UPPER(LastName) as LastName,
                UPPER(LocationName) as LocationName,
                UPPER(Email) as Email,
                ROW_NUMBER() 
                    OVER(PARTITION BY UPPER(Email), 
                                    UPPER(LocationName),
                                    UPPER(FirstName),
                                    UPPER(LastName)
                        ORDER BY emp.Inactive,
                                 (DATE(emp.InsertDate)) DESC,
                                 CAST(EmployeeId as INT64) DESC) as recency_rnk
            FROM {ref_Employee} emp
            LEFT JOIN {ref_Location} USING(LocationId)
        ) as recency_ranked
        WHERE recency_rnk = 1
    )
"""

SUPERVISOR_ROSTER_EMPLOYEE_IDS_CTE = """
    -- since the roster currently only comes with officer email and names, we have to link on EmployeeId ourselves
    -- we'll first try matching by email (which works most of the time) and then try matching on name/district
    -- from our matches, we'll prioritize email matches over name/district matches
    employee_ids as (
        SELECT
            OFFICER_FIRST_NAME,
            OFFICER_LAST_NAME,
            DIST,
            COALESCE(EmployeeId_email_match, EmployeeId_name_match) as officer_EmployeeId
        FROM (
            SELECT
                DISTINCT *,
                emp_ref_email.EmployeeId as EmployeeId_email_match,
                emp_ref_name.EmployeeId as EmployeeId_name_match,
                ROW_NUMBER() 
                    OVER(PARTITION BY OFFICER_FIRST_NAME, OFFICER_LAST_NAME, DIST
                         ORDER BY emp_ref_email.EmployeeId NULLS LAST, emp_ref_name.EmployeeId NULLS LAST) as row_number
            FROM {RECIDIVIZ_REFERENCE_supervisor_roster@ALL} roster
            LEFT JOIN current_atlas_employee_info emp_ref_email on UPPER(replace(roster.OFFICER_EMAIL, ',', '.')) = emp_ref_email.Email
            LEFT JOIN current_atlas_employee_info emp_ref_name 
                on UPPER(roster.OFFICER_FIRST_NAME) = emp_ref_name.FirstName
                and UPPER(roster.OFFICER_LAST_NAME) = emp_ref_name.LastName
                and STARTS_WITH(emp_ref_name.LocationName, CONCAT('DISTRICT ', SUBSTR(DIST, 2, 1)))       
        ) sub
        -- filter to the most likely match where either the email or name matched
        WHERE row_number = 1
          AND (EmployeeId_email_match is not null or EmployeeId_name_match is not null)
    )
"""
CLIENT_ADDRESS_CTE = """
    -- we will be using Jurisdiction ID to determine what district the client falls into
    client_addresses_cte AS (
        SELECT 
            JurisdictionId,
            OffenderId,
            DATE(StartDate) AS StartDate,
            DATE(EndDate) AS EndDate
        FROM {ref_Address}
        LEFT JOIN {ind_Offender_Address} using (AddressId)
        WHERE PrimaryAddress = '1'
    )
"""
SUPERVISOR_ROSTER_SUPERVISOR_IDS_CTE = """
    -- now, we'll do the same matching to get supervisor employeeId
    -- we'll first try matching by email (which works most of the time) and then try matching on name/dist
    -- from our matches, we'll prioritize email matches over name/dist matches
    -- NOTE: there is a small possibility that there's a supervisor who has an email that doesn't match 
    --       AND is part of state/regional leadership, in which they wouldn't be matched here since their district 
    --       wouldn't match with their location in the ref_Employee.  Since they wouldn't match, anyone with that 
    --       supervisor would have a NULL supervisor for this supervisor period.  BUT I think it's overall still fine
    --       because the supervisor would be getting a regional manager or state leadership view of outliers anyways,
    --       and not a supervisor view.
    supervisor_ids as (
        SELECT
            SUPERVISOR_FIRST_NAME,
            SUPERVISOR_LAST_NAME,
            DIST,
            COALESCE(EmployeeId_email_match, EmployeeId_name_match) as supervisor_EmployeeId
        FROM (
            SELECT
                DISTINCT *,
                emp_ref_email.EmployeeId as EmployeeId_email_match,
                emp_ref_name.EmployeeId as EmployeeId_name_match,
                ROW_NUMBER() 
                    OVER(PARTITION BY SUPERVISOR_FIRST_NAME, SUPERVISOR_LAST_NAME
                         ORDER BY emp_ref_email.EmployeeId NULLS LAST, emp_ref_name.EmployeeId NULLS LAST) as row_number
            FROM {RECIDIVIZ_REFERENCE_supervisor_roster@ALL} roster
            LEFT JOIN current_atlas_employee_info emp_ref_email on UPPER(replace(roster.SUPERVISOR_EMAIL, ',', '.')) = emp_ref_email.Email
            LEFT JOIN current_atlas_employee_info emp_ref_name 
                on UPPER(roster.SUPERVISOR_FIRST_NAME) = emp_ref_name.FirstName
                and UPPER(roster.SUPERVISOR_LAST_NAME) = emp_ref_name.LastName 
                and STARTS_WITH(emp_ref_name.LocationName, CONCAT('DISTRICT ', SUBSTR(DIST, 2, 1)))     
        ) sub
        -- filter to the most likely match where either the email or name/dist matched
        WHERE row_number = 1
          AND (EmployeeId_email_match is not null or EmployeeId_name_match is not null)
    )
"""

STATE_STAFF_SUPERVISOR_PERIODS_CTES = f"""
   {CURRENT_ATLAS_EMPLOYEE_INFO_CTE},
   {SUPERVISOR_ROSTER_EMPLOYEE_IDS_CTE},
   {SUPERVISOR_ROSTER_SUPERVISOR_IDS_CTE},
    -- join employee IDs back onto full roster information
    all_periods as (
        SELECT
            officer_EmployeeId,
            supervisor_EmployeeId,
            ACTIVE,
            update_datetime as start_date,
            LEAD(update_datetime) over(PARTITION BY officer_EmployeeId ORDER BY update_datetime) as end_date,
            MAX(update_datetime) OVER(PARTITION BY officer_EmployeeId) as last_appearance_date,
            MAX(update_datetime) OVER(PARTITION BY TRUE) as last_file_update_datetime
        FROM {{RECIDIVIZ_REFERENCE_supervisor_roster@ALL}} roster
        LEFT JOIN employee_ids e_ids USING(OFFICER_FIRST_NAME, OFFICER_LAST_NAME, DIST)
        LEFT JOIN supervisor_ids s_ids USING(SUPERVISOR_FIRST_NAME, SUPERVISOR_LAST_NAME, DIST)
    ),
    preliminary_periods as (
        SELECT
            officer_EmployeeId,
            supervisor_EmployeeId,
            ACTIVE,
            start_date,
            end_date
        FROM all_periods
        WHERE (start_date < last_appearance_date or start_date = last_file_update_datetime)
    ),
    aggregated_periods as (
        {aggregate_adjacent_spans(
            table_name="preliminary_periods",
            attribute=["supervisor_EmployeeId", "ACTIVE"],
            index_columns=["officer_EmployeeId"])}
    ),
    final_supervisor_periods as (
        SELECT
            officer_EmployeeId,
            supervisor_EmployeeId,
            start_date,
            end_date
        FROM aggregated_periods
        WHERE ACTIVE = 'Y' and supervisor_EmployeeId is not null and officer_EmployeeId is not null
    )
"""
