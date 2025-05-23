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
"""Query containing incarceration period information extracted from the output of the `OffenderMovement` table.
The table contains one row per movement from facility to facility, and this query takes each MovementType and
MovementReason and ultimately maps it into incarceration periods.
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
    -- First, we clean any characters in movement details or locationIDs to be able to join them cleanly later on
    remove_extraneous_chars as (
        SELECT
            OffenderID,
            MovementDateTime,
             -- Remove extraneous unexpected characters.
            REGEXP_REPLACE(MovementType, r'[^A-Z]', '') as MovementType, 
            REGEXP_REPLACE(MovementReason, r'[^A-Z0-9]', '') as MovementReason,
            REGEXP_REPLACE(FromLocationID, r'[^A-Z0-9]', '') as FromLocationID,
            REGEXP_REPLACE(ToLocationID, r'[^A-Z0-9]', '') as ToLocationID,
         FROM {OffenderMovement}
    ),
    -- It is a known data issue that some people will have a final movement in OffenderMovement, but we will 
    -- still see them having unit movements in CellBedAssignment or Custody Level info in Clssification. This CTE finds 
    -- people whose last movement in OffenderMovement is a Discharge, movement to supervision, or court and then sets the MaxOutDate so that we don't 
    -- add in info from CellBedAssignment or Classification that would create open periods when the person's should be closed out. 
    -- If someone's most recent movement in OffenderMovement is NOT a discharge/supervision release/court, we don't restrict any data, ensuring that all updates 
    -- for actively incarcerated are tracked. We have included court as well because sometimes the last movement is to court
    -- but if there is any offendermovement after movement to court they will have the correct unit and custody information
    find_max_offender_discharge_movement_date AS (
        SELECT 
            OffenderID, 
            CASE 
                WHEN MovementType LIKE '%DI' THEN MovementDateTime 
                WHEN MovementType LIKE '%PA' THEN MovementDateTime 
                WHEN MovementType LIKE '%PR' THEN MovementDateTime 
                WHEN MovementType LIKE '%CC' THEN MovementDateTime 
                WHEN MovementType LIKE '%DV' THEN MovementDateTime 
                WHEN MovementType LIKE '%CT' THEN MovementDateTime 
                ELSE NULL 
            END as MaxOutDate
        FROM (
            SELECT 
            OffenderID,
            MovementType,
            MovementDateTime,
            ROW_NUMBER() OVER (PARTITION BY OffenderID ORDER BY MovementDateTime DESC) as max_movement_entry
            FROM {OffenderMovement}) AS a
        WHERE max_movement_entry = 1 AND RIGHT(MovementType, 2) IN ('DI', 'PA', 'PR', 'CC', 'DV', 'CT')
    ),
    -- Next, we pull information from the Classification data table to hydrate custody level information for a person throughout their periods
    classification_filter AS (	
        SELECT DISTINCT 	
            OffenderId, ClassificationDate, RecommendedCustody, ClassificationDecision, ClassificationSequenceNumber,	
            ROW_NUMBER() OVER (PARTITION BY OffenderId, ClassificationDate ORDER BY ClassificationSequenceNumber DESC,
            (CASE WHEN RecommendedCustody = 'MAX' THEN 1 	
            WHEN RecommendedCustody = 'CLS' THEN 2 	
            WHEN RecommendedCustody = 'MED' THEN 3  	
            WHEN RecommendedCustody = 'MIR' THEN 4 	
            WHEN RecommendedCustody = 'MID' THEN 5  	
            WHEN RecommendedCustody = 'MIT' THEN 6  END)) AS CustLevel --Should all have unique Seq Number for date, but adding this to handle when they dont	
        FROM {Classification}
        LEFT JOIN find_max_offender_discharge_movement_date USING (OffenderID)
        WHERE ClassificationDecision = 'A'  # denotes final custody level	
        AND EXTRACT(YEAR FROM CAST(ClassificationDate AS DATETIME)) > 2002  # Data is not reliable before 	
        AND ClassificationDate <= COALESCE(MaxOutDate, '9999-12-31') # Filter out any data linked to someone after they have been discharged 	
    ), 
    -- Then, we pull information from CellBedAssignment to hydrate housing unit information. Note that this table sometimes does not match with the OffenderMovement location information, so we incorporate some logic to help deal with edge cases
    cell_bed_assignment_setup AS (
            SELECT DISTINCT
            OffenderID, 
            AssignmentDateTime as MovementDateTime,
            'CELL_MOVEMENT' AS MovementReason,
            RequestedSiteID AS FromLocationID,
            RequestedSiteID AS ToLocationID,
            'CELL_MOVEMENT' as PeriodEvent, 
            RequestedUnitID AS HousingUnit,
            LAG(RequestedUnitID) OVER(PARTITION BY OFFENDERID ORDER BY AssignmentDateTime) AS LastHousingUnit,
            CASE WHEN EndDate IS NOT NULL THEN 'CLOSED' ELSE 'OPEN' END as AssignmentStatus,
            COUNTIF(EndDate IS NULL) OVER (PARTITION BY OffenderID ORDER BY AssignmentDateTime range between UNBOUNDED preceding and UNBOUNDED FOLLOWING) AS HasOpenPeriod, # This determines if a given person has any open housing unit periods in this table (regardless of it being the last one chronologically by StartDate
            LAST_VALUE(AssignmentDateTime ignore nulls) OVER (PARTITION BY OffenderID ORDER BY AssignmentDateTime range between UNBOUNDED preceding and UNBOUNDED FOLLOWING) AS LastAssignmentDate, # This determines when the last assignment was given in CellBedAssignment for a given person
            LAST_VALUE(EndDate) OVER (PARTITION BY OffenderID ORDER BY AssignmentDateTime range between UNBOUNDED preceding and UNBOUNDED FOLLOWING) AS LastAssignmentEndDate # This determines if the last period assigned to a person has an end date or is an open period
        FROM (
            -- Some records have an assignment end date on or before the assignment start
            -- date. These are omitted when constructing incarceration periods because the
            -- dates don't make sense and we don't know what the correct ones are.
            SELECT *
            FROM {CellBedAssignment}
            WHERE EndDate IS NULL OR EndDate > AssignmentDateTime
        )
        LEFT JOIN find_max_offender_discharge_movement_date USING (OffenderID)
        WHERE RequestedUnitID IS NOT NULL and AssignmentDateTime <= COALESCE(MaxOutDate, '9999-12-31') # Filter out any data linked to someone after they have been discharged 	
        AND EXTRACT(YEAR FROM CAST(AssignmentDateTime AS DATETIME)) > 2002  # Data is not reliable before 2002
    
    ),
    -- Here, we filter to only movements in the OffenderMovement table related to incarceration stays and then join the housing unit information to make a ledger of all critical dates for a person, with the exception of custody changes which will be added later.
    filter_to_only_incarceration_no_custody as (
        SELECT
            OffenderID,
            CAST(MovementDateTime AS DATETIME) AS MovementDateTime,
            MovementType, 
            MovementReason,
            FromLocationID,
            ToLocationID,
            CAST(null AS STRING) AS RecommendedCustody,
            null AS HousingUnit
        FROM remove_extraneous_chars
        -- Filter to only rows that are to/from/within incarceration facilities. There are a few cases from pre-1993 where 
        -- someone follows unintuitive OffenderMovements and we do not see them transfer from FA to supervision but then see 
        -- a movement for supervision to discharge (such as PADI). We include 'DI' movements then to ensure that all these periods are properly closed out.
        WHERE RIGHT(MovementType, 2) IN ('FA', 'FH', 'CT','DI') OR LEFT(MovementType, 2) IN ('FA', 'FH', 'CT') 

        UNION ALL # to pull in housing unit changes from CellBedAssignment table

        SELECT 
            OffenderID,
            CAST(MovementDateTime AS DATETIME) AS MovementDateTime,
            'UNITMOVEMENTFH' as MovementType, # Generated so last two are FH and query handles correctly	
            'UNIT_MOVEMENT' AS MovementReason, 
            FromLocationID, 	
            ToLocationID,
            CAST(null AS STRING) AS RecommendedCustody,
            HousingUnit
        FROM cell_bed_assignment_setup
        WHERE HousingUnit != COALESCE(LastHousingUnit, 'NoHousingUnit')
        
        UNION ALL # to create special periods for CellBedAssignment changes when someone's open assingment period is not the last period noted, since they kepy their assigned bed, completed some other cell changes, and then returned back to the original assignment

        SELECT 
            OffenderID,
            DATE_ADD(CAST(LastAssignmentEndDate AS DATETIME), INTERVAL 1 DAY) as MovementDateTime,
            'UNITMOVEMENTFH' as MovementType, # Generated so last two are FH and query handles correctly	
            'UNIT_MOVEMENT' AS MovementReason, 
            FromLocationID, 	
            ToLocationID,
            CAST(null AS STRING) AS RecommendedCustody,
            HousingUnit
        FROM cell_bed_assignment_setup
        WHERE HasOpenPeriod > 0 AND AssignmentStatus = 'OPEN' AND LastAssignmentEndDate IS NOT NULL
    ), 
    -- Certain movement codes (PRDI, CCDI, PAFA, PRFA, and PADI) can represent a status change without a physical movement.
    -- We need to filter these out, without impacting the cases where these codes do represent a physical movement.
    -- When a movement has one of these codes and meets the following criteria, we assume that it's being used to 
    -- represent a non-movement status change and filter it out.
    -- 1. The 'to' and 'from' locations of the movement don't match the locations in the preceding and subsequent movements
    -- 2. The 'to' location of the preceding movement matches the 'from' location of the subsequent movement,
    --    meaning that the movement in between is interrupting an otherwise logical sequence of locations.
    add_adjacent_location_flags AS (
        SELECT
            *,
            MovementType IN ('PRDI', 'CCDI', 'PAFA', 'PRFA', 'PADI', 'CTPR') AND
                -- Because of the COALESCEs, we will never filter out a movement this way
                -- if it is someone's first or final movement.
                COALESCE(last_location_to,'NONE') = COALESCE(next_location_from,'NONE') AND 
                (
                    -- TODO(#40569): This condition can't be changed to use an OR without
                    -- substantially increasing external accuracy validation errors, but we
                    -- should figure out a way to handle the rare cases where one of these
                    -- non-physical movements does have a matching location with one of the adjacent movements.
                    FromLocationID != COALESCE(last_location_to,FromLocationID) AND 
                    ToLocationID != COALESCE(next_location_from,ToLocationID) 
                )
            AS is_non_physical_movement
        FROM (
            SELECT 
                *,
                LAG(ToLocationID) OVER ordered_movements AS last_location_to,
                LEAD(FromLocationID) OVER ordered_movements AS next_location_from
            FROM filter_to_only_incarceration_no_custody
            WINDOW ordered_movements AS (PARTITION BY OffenderID ORDER BY MovementDateTime)
        )
    ),
    -- Use the flags from the previous CTE to remove movements that look more like status changes. Then, union in custody information 
    -- (this couldn't be done with the previous unions, because custody movements lack location data and therefore create errors with the LAG and LEAD in the previous CTE)
    remove_non_physical_movements_add_custody AS (
        SELECT 
            * EXCEPT(is_non_physical_movement,last_location_to,next_location_from)
        FROM add_adjacent_location_flags
        WHERE NOT is_non_physical_movement

        UNION ALL # to pull in custody level changes as periods	

        SELECT  	
            OffenderId, 	
            CAST(ClassificationDate AS DATETIME) AS MovementDateTime,	
            'CUSTCHANGEFH' as MovementType, # Generated so last two are FH and query handles correctly	
            'CUST_CHANGE' AS MovementReason, 	
            CAST(null AS STRING) AS FromLocationID, 	
            CAST(null AS STRING) AS ToLocationID,	
            RecommendedCustody,
            null AS HousingUnit	
        FROM Classification_filter	
        WHERE CustLevel = 1
    ),
    -- This determines the previous MovementType and MovementReason
    Lag_MovementType AS (
        SELECT *, 
            LAG(MovementType) OVER (PARTITION BY OffenderID ORDER BY MovementDateTime) AS LAST_MovementType,
            LAG(MovementReason) OVER (PARTITION BY OffenderID ORDER BY MovementDateTime) AS LAST_MovementReason,
        FROM remove_non_physical_movements_add_custody
    ), 
    -- This sets up our initial period creation from the ledger of critical dates 
    initial_setup as (
        SELECT
            OffenderID,
            MovementDateTime,
            MovementType, 
            MovementReason,
            FromLocationID,
            ToLocationID,
            RecommendedCustody,
            -- Based on the MovementType, determine the type of period event that is commencing.
            CASE
                -- Movements whose destinations are associated with incarceration facilities.
                WHEN RIGHT(MovementType, 2) IN ('FA', 'FH', 'CT', 'OJ') THEN 'INCARCERATION'

                -- Movements whose destinations are not associated with incarceration facilities (i.e. terminations 
                -- or supervision).
                WHEN RIGHT(MovementType, 2) IN (
                    -- Ends in supervision.
                    'CC', 'PA', 'PR', 'DV', 
                    -- Ends in termination.
                    'DI', 'EI', 'ES', 'NC', 'AB', 'FU', 'BO', 'WR'
                ) THEN 'TERMINATION'

                -- There shouldn't be any PeriodEvents that fall into `UNCATEGORIZED`
                ELSE 'UNCATEGORIZED'
            END AS PeriodEvent,
            -- Only pull in the Death Date if it is properly formatted. The DeathDate should look like: YYYY-MM-DD.
            CAST(CASE 
                WHEN REGEXP_CONTAINS(attributes.DeathDate, r'[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]') THEN DeathDate
            END AS DATETIME) AS DeathDate,
            HousingUnit
        FROM Lag_MovementType 
        LEFT JOIN {OffenderAttributes} AS attributes
            USING (OffenderID)
        -- Filter out rows that are not associated physical movements from facilities.
        WHERE MovementReason not in ('CLASP', 'CLAST', 'APPDI')
    ),
    -- This filters out any movements after someone's death date
    filter_out_movements_after_death_date AS (
        SELECT 
            *,
            ROW_NUMBER() OVER 
            (PARTITION BY OffenderID 
                ORDER BY 
                MovementDateTime ASC, 
                MovementType,
                HousingUnit) AS MovementSequenceNumber
        FROM initial_setup
        WHERE 
            (DeathDate IS NULL
            -- Filter out all rows whose start date is after the death date, if present.
            OR MovementDateTime <= DeathDate)
    ),
    -- This CTE will be joined last to make sure we only assign HousingUnits to periods that we confirmed match the facility that person is located in
    confirm_housing_unit_in_actual_facility AS (
        SELECT DISTINCT RequestedSiteID AS Site, RequestedUnitID AS SiteUnit 
        FROM {CellBedAssignment}
    ),
    -- We then add all movement information from the next movement to the current movement so we can construct the periods
    append_next_movement_information_and_death_dates AS (
        SELECT 
            OffenderID,
            PeriodEvent as StartPeriodEvent,
            MovementSequenceNumber,
            MovementDateTime as StartMovementDateTime,
            MovementReason as StartMovementReason,
            MovementType as StartMovementType,
            FromLocationID as StartFromLocationID,
            ToLocationID as StartToLocationID,
            LEAD(MovementDateTime) OVER person_sequence AS NextMovementDateTime,
            LEAD(MovementType) OVER person_sequence AS NextMovementType,
            LEAD(MovementReason) OVER person_sequence AS NextMovementReason,
            LEAD(FromLocationID) OVER person_sequence AS NextFromLocationID,
            LAG(ToLocationID) OVER person_sequence AS PreviousToLocationID,
            LEAD(PeriodEvent) OVER person_sequence AS NextPeriodEvent,
            DeathDate,
            RecommendedCustody AS CustodyLevel,
            HousingUnit
        FROM filter_out_movements_after_death_date
        WINDOW person_sequence AS (PARTITION BY OffenderID ORDER BY MovementSequenceNumber)
    ),
    -- We clean up any movements related to revocation statuses or transfers to other jurisdictions that are not Courts
    clean_up_movements AS (
        SELECT 
            OffenderID,
            StartPeriodEvent,
            MovementSequenceNumber,
            StartMovementDateTime,
            StartMovementReason,
            StartMovementType,
            -- Revocation status codes don't represent actual movements, and their locations often indicate which district
            -- a person was revoked from rather than the facility in which a person is currently located. For these 
            -- statuses, we set the StartFromLocationID to be the PreviousToLocationID and the StartToLocationID to be 
            -- the NextFromLocationID to ensure that the rows create valid periods with the adjacent statuses.
            IF(StartMovementReason in ('PAVOK', 'REVOK', 'PRVOK', 'PTVOK'), COALESCE(PreviousToLocationID, StartFromLocationID), StartFromLocationID) AS StartFromLocationID,
            IF(StartMovementReason in ('PAVOK', 'REVOK', 'PRVOK', 'PTVOK'), COALESCE(NextFromLocationID, StartToLocationID), StartToLocationID) AS StartToLocationID,
            NextMovementDateTime,
            NextMovementType,
            NextMovementReason,
            NextFromLocationID,
            NextPeriodEvent,
            DeathDate,
            CustodyLevel,
            HousingUnit
        FROM append_next_movement_information_and_death_dates
        WHERE RIGHT(StartMovementType, 2) != 'OJ' OR StartMovementReason = 'OUTCT'
        WINDOW person_sequence AS (PARTITION BY OffenderID ORDER BY MovementSequenceNumber)
    ),
    -- Then filter out any erroneous rows related to incorrect edges or when someone does not actually move as expected through a facility 
    filter_out_erroneous_rows AS (
        SELECT 
            * 
        FROM clean_up_movements
        WHERE 
           -- Filter out all periods that start with a TERMINATION. 
            StartPeriodEvent != 'TERMINATION'  
    ),
    -- We then hydrate all values of the housing unit, custody level, and faciltiy information as well as clean up any rough end date edges
hydrate_all_values AS (
    SELECT 
        OffenderID,
        MovementSequenceNumber,
        StartMovementDateTime as StartDateTime,
        IF ( 
            -- If we do not have an end date and do have a death date, then set the end date to be the death date. 
            -- If we do not have an end date and do not have a death date, then the EndDateTime will be null. 
            -- Ignore the timestamp when doing the comparison between StartDateTime and DeathDate.
            NextMovementDateTime IS NULL AND DeathDate IS NOT NULL, 
            DeathDate, 
            -- If there is an end date, use that, otherwise use the death date.
            COALESCE(NextMovementDateTime, DeathDate)
        ) AS EndDateTime,
        DeathDate,
        LAST_VALUE(StartToLocationID ignore nulls) OVER (person_sequence range between UNBOUNDED preceding and current row) AS Site,	
        LAST_VALUE(SiteType ignore nulls) OVER (person_sequence range between UNBOUNDED preceding and current row) AS SiteType,
        StartMovementType,
        StartMovementReason,
        NextMovementType as EndMovementType,
        NextMovementReason as EndMovementReason,
        LAST_VALUE(CustodyLevel ignore nulls) OVER (person_sequence range between UNBOUNDED preceding and current row) AS CustodyLevel,
        LAST_VALUE(HousingUnit ignore nulls) OVER (person_sequence range between UNBOUNDED preceding and current row) AS HousingUnit
    FROM filter_out_erroneous_rows
    LEFT JOIN 
        {Site} site_info
    ON StartToLocationID = SiteID 
        WINDOW person_sequence AS (PARTITION BY OffenderID ORDER BY MovementSequenceNumber)
),
-- adding flag for when the last_movement ends in a code for release to supervision/from system 
-- and a flag for when an open period starts with a unit_movement
cleaning_extra_movements AS (
    SELECT 
        OffenderID,
        StartDateTime,
        EndDateTime,
        Site,
        SiteType,
        DeathDate,
        StartMovementType,
        StartMovementReason,
        EndMovementType,
        MovementSequenceNumber,
        EndMovementReason,
        CustodyLevel, 
        HousingUnit,
        IF(RIGHT(LAG(EndMovementType) OVER (PARTITION BY OFFENDERID ORDER BY MovementSequenceNumber), 2) IN ('CC', 'PA', 'PR', 'DV', 'DI', 'EI', 'ES', 'NC', 'AB', 'FU', 'BO', 'WR'), 
            'release', 
            'stay'
        ) AS last_movement,
        IF((StartMovementReason ='UNIT_MOVEMENT' OR StartMovementReason ='CUST_CHANGE')  AND EndDatetime IS NULL, 'yes', 'no') as open_with_unit_or_custody_change
    FROM hydrate_all_values
), 
-- Most MovementType/MovementReason combinations are classified as temporary (T) or
-- permanent (P). We make this distinction using RECIDIVIZ_REFERENCE_TEMP_PERM_MOVEMENTS,
-- and append the T or P flag to the admission/release reason raw text. Movement type/reason
-- combinations that lack a flag in the reference table will not have anything appended.
add_movement_type AS (
SELECT 
    final_cleaning.*,
    -- TODO(#39694): Add logic using these flags in IP normalization.
    adm_temp_perm_flag.TorP AS AdmissionTempPermFlag,
    rel_temp_perm_flag.TorP AS ReleaseTempPermFlag
FROM (
    SELECT
        OffenderID,
        StartDateTime,
        EndDateTime,
        cleaning_extra_movements.Site,
        SiteType,
        StartMovementType,
        StartMovementReason,
        CASE WHEN EndDateTime = DeathDate THEN 'DEATH' ELSE EndMovementType END AS EndMovementType,
        CASE WHEN EndDateTime = DeathDate THEN 'DEATH' ELSE EndMovementReason END AS EndMovementReason,
        -- When someone transfers from an incarceration facility to Jail or other SiteType, the CustodyLevel from their previous 
        -- assessment no longer applies so we do not want to carry over that CustodyLevel when it is no longer relevant. 
        -- Therefore, we apply logic here to only attach the CustodyLevel to periods when someone 
        -- is in an incarceration facility (SiteType = 'IN'). We may need to revisit this choice in the future if we decide to 
        -- include other SiteTypes that the CustodyLevel still applies to. 
        CASE WHEN SiteType = 'IN' THEN CustodyLevel ELSE Null END AS CustodyLevel, 
        CASE WHEN cleaning_extra_movements.HousingUnit = confirm_housing_unit_in_actual_facility.SiteUnit THEN HousingUnit ELSE NULL END AS HousingUnit,
        ROW_NUMBER() OVER (PARTITION BY OffenderID ORDER BY MovementSequenceNumber ASC) AS IncarcerationPeriodSequenceNumber
    FROM cleaning_extra_movements
    -- We use this final join to null out any HousingUnit assignments that are not updated once someone is is a different facility in OffenderMovement
    LEFT JOIN confirm_housing_unit_in_actual_facility ON SiteUnit = HousingUnit AND confirm_housing_unit_in_actual_facility.Site = cleaning_extra_movements.Site
    -- filtering out when last open period start movement is unit change after a period ending with discharge codes 
    WHERE last_movement != 'release' OR open_with_unit_or_custody_change != 'yes'
) final_cleaning
LEFT JOIN {RECIDIVIZ_REFERENCE_TEMP_PERM_MOVEMENTS} adm_temp_perm_flag
ON CONCAT(StartMovementType,StartMovementReason) = adm_temp_perm_flag.MovementTypeMovementReason
LEFT JOIN {RECIDIVIZ_REFERENCE_TEMP_PERM_MOVEMENTS} rel_temp_perm_flag
ON CONCAT(EndMovementType,EndMovementReason) = rel_temp_perm_flag.MovementTypeMovementReason
)
-- IF someone does not have a movement in OffenderMovement, they are not active in TN Data
SELECT * FROM add_movement_type
WHERE OffenderID IN (SELECT DISTINCT OffenderID FROM {OffenderMovement}) 
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_tn",
    ingest_view_name="OffenderMovementIncarcerationPeriod_v3",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
