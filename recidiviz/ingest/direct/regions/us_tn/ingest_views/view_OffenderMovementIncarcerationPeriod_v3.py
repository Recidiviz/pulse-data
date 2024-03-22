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
WITH all_incarceration_periods AS ( 
WITH 
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
    find_max_offender_discharge_movement_date AS (
    -- It is a known data issue that some people will have a final movement of Discharge in OffenderMovement, but we will 
    -- still see them having unit movements in CellBedAssignment or Custody Level info in Clssification. This CTE finds 
    -- people whose last movement in OffenderMovement is a Discharge and then sets the MaxDischargeDate so that we don't 
    -- add in info from CellBedAssignment or Classification that would create open periods when the person's should be closed out. 
    -- If someone's most recent movement in OffenderMovement is NOT a discharge, we don't restrict any data, ensuring that all updates 
    -- for actively incarcerated are tracked. 
        SELECT 
            OffenderID, 
            CASE WHEN MovementType LIKE '%DI' THEN MovementDateTime ELSE NULL END as MaxDischargeDate
        FROM (
            SELECT 
            OffenderID,
            MovementType,
            MovementDateTime,
            ROW_NUMBER() OVER (PARTITION BY OffenderID ORDER BY MovementDateTime DESC) as max_movement_entry
            FROM {OffenderMovement}) AS a
        WHERE max_movement_entry = 1 AND RIGHT(MovementType, 2) = 'DI'
    ),
    classification_filter AS (	
        SELECT DISTINCT 	
            OffenderId, ClassificationDecisionDate, RecommendedCustody, ClassificationDecision, ClassificationSequenceNumber,	
            ROW_NUMBER() OVER (PARTITION BY OffenderId, ClassificationDecisionDate ORDER BY ClassificationSequenceNumber DESC, 	
            (CASE WHEN RecommendedCustody = 'MAX' THEN 1 	
            WHEN RecommendedCustody = 'CLS' THEN 2 	
            WHEN RecommendedCustody = 'MED' THEN 3  	
            WHEN RecommendedCustody = 'MIR' THEN 4 	
            WHEN RecommendedCustody = 'MID' THEN 5  	
            WHEN RecommendedCustody = 'MIT' THEN 6  END)) AS CustLevel --Should all have unique Seq Number for date, but adding this to handle when they dont	
        FROM {Classification}
        LEFT JOIN find_max_offender_discharge_movement_date USING (OffenderID)
        WHERE ClassificationDecision = 'A'  # denotes final custody level	
        AND EXTRACT(YEAR FROM CAST(ClassificationDecisionDate AS DATETIME)) > 2002  # Data is not reliable before 	
        AND ClassificationDecisionDate <= COALESCE(MaxDischargeDate, '9999-12-31') # Filter out any data linked to someone after they have been discharged 	
    ), 
    cell_bed_assignment_setup AS (
            SELECT DISTINCT
            OffenderID, 
            AssignmentDateTime as MovementDateTime,
            'CELL_MOVEMENT' AS MovementReason,
            RequestedSiteID AS FromLocationID,
            RequestedSiteID AS ToLocationID,
            'CELL_MOVEMENT' as PeriodEvent, # do we still need this?
            RequestedUnitID AS HousingUnit,
            LAG(RequestedUnitID) OVER(PARTITION BY OFFENDERID ORDER BY AssignmentDateTime) AS LastHousingUnit
        FROM {CellBedAssignment}
        LEFT JOIN find_max_offender_discharge_movement_date USING (OffenderID)
        WHERE RequestedUnitID IS NOT NULL and AssignmentDateTime <= COALESCE(MaxDischargeDate, '9999-12-31') # Filter out any data linked to someone after they have been discharged 	
        AND EXTRACT(YEAR FROM CAST(AssignmentDateTime AS DATETIME)) > 2002  # Data is not reliable before 2002
    
    ),
    filter_to_only_incarceration as (
        SELECT
            OffenderID,
            MovementDateTime,
            MovementType, 
            MovementReason,
            FromLocationID,
            ToLocationID,
            null AS RecommendedCustody,
            null AS HousingUnit
        FROM remove_extraneous_chars
        -- Filter to only rows that are to/from/within incarceration facilities. There are a few cases from pre-1993 where 
        -- someone follows unintuitive OffenderMovements and we do not see them transfer from FA to supervision but then see 
        -- a movement for supervision to discharge (such as PADI). We include 'DI' movements then to ensure that all these periods are properly closed out.
        WHERE RIGHT(MovementType, 2) IN ('FA', 'FH', 'CT','DI') OR LEFT(MovementType, 2) IN ('FA', 'FH', 'CT') 

        UNION ALL # to pull in custody level changes as periods	

        SELECT  	
            OffenderId, 	
            ClassificationDecisionDate || '.000000' AS MovementDateTime, ## add this so date format the same	
            'CUSTCHANGEFH' as MovementType, # Generated so last two are FH and query handles correctly	
            'CUST CHANGE' AS MovementReason, 	
            null AS FromLocationID, 	
            null AS ToLocationID,	
            RecommendedCustody,
            null AS HousingUnit	
        FROM Classification_filter	
        WHERE CustLevel = 1

        UNION ALL # to pull in housing unit changes as periods	

        SELECT 
            OffenderID,
            MovementDateTime,
            'UNITMOVEMENTFH' as MovementType, # Generated so last two are FH and query handles correctly	
            'UNIT_MOVEMENT' AS MovementReason, 
            FromLocationID, 	
            ToLocationID,
            null AS RecommendedCustody,
            HousingUnit
        FROM cell_bed_assignment_setup
        WHERE HousingUnit != LastHousingUnit

    ), Lag_MovementType AS (
        SELECT *, 
            LAG(MovementType) OVER (PARTITION BY OffenderID ORDER BY MovementDateTime) AS LAST_MovementType,
        FROM filter_to_only_incarceration
    ), clean_custody_and_unit_changes AS (
        SELECT * 
        FROM Lag_MovementType
        WHERE (MovementType != 'CUSTCHANGEFH' OR RIGHT(LAST_MovementType, 2) IN  ('FA', 'FH', 'CT'))
    ),
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
                WHEN RIGHT(MovementType, 2) IN ('FA', 'FH', 'CT') THEN 'INCARCERATION'

                -- Movements whose destinations are not associated with incarceration facilities (i.e. terminations 
                -- or supervision).
                WHEN RIGHT(MovementType, 2) IN (
                    -- Ends in supervision.
                    'CC', 'PA', 'PR', 'DV', 
                    -- Ends in termination.
                    'DI', 'EI', 'ES', 'NC', 'AB', 'FU', 'BO', 'WR', 'OJ'
                ) THEN 'TERMINATION'

                -- There shouldn't be any PeriodEvents that fall into `UNCATEGORIZED`
                ELSE 'UNCATEGORIZED'
            END AS PeriodEvent,
            -- Only pull in the Death Date if it is properly formatted. The DeathDate should look like: YYYY-MM-DD.
            CASE 
                WHEN REGEXP_CONTAINS(attributes.DeathDate, r'[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]') THEN DeathDate
            END AS DeathDate,
            HousingUnit
        FROM clean_custody_and_unit_changes --clean_custody_changes
        LEFT JOIN {OffenderAttributes} AS attributes
            USING (OffenderID)
        -- Filter out rows that are not associated physical movements from facilities.
        WHERE MovementReason not in ('CLASP', 'CLAST', 'APPDI')
    ),
    filter_out_movements_after_death_date AS (
        SELECT 
            *
        FROM initial_setup
        WHERE 
            (DeathDate IS NULL
            -- Filter out all rows whose start date is after the death date, if present.
            OR SUBSTRING(MovementDateTime, 0, 10) <= DeathDate)
    ),
    # Then we add the official facility info to all periods so we can filter out any seg periods that were erroneously continued in a facility for a person despite them having been transferred
    all_official_facility_info_to_all_periods AS (
    SELECT 
        *,
        ROW_NUMBER() OVER 
        (PARTITION BY OffenderID 
            ORDER BY 
            MovementDateTime ASC, 
            CASE WHEN MovementReason='CELL_END' THEN 1
                WHEN MovementReason='CELL_START' THEN 2
                WHEN MovementReason='INCARCERATION' THEN 3
                ELSE 4
            END ASC,
            MovementType,
            HousingUnit) AS MovementSequenceNumber # We rank the ordering to end a seg period before an official movement to a new facility when they occur on the same day 
    FROM filter_out_movements_after_death_date
    WHERE MovementDateTime IS NOT NULL
    ),
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
        FROM all_official_facility_info_to_all_periods
        WINDOW person_sequence AS (PARTITION BY OffenderID ORDER BY MovementSequenceNumber)
    ),
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
        WINDOW person_sequence AS (PARTITION BY OffenderID ORDER BY MovementSequenceNumber)
    ),
    filter_out_erroneous_rows AS (
        SELECT 
            * 
        FROM clean_up_movements
        WHERE 
            -- If the locations don't match and facility type in the end of StartMovementType matches the facility type 
            -- of the start of NextMovementType (ex: PAFA -> FADI), then that likely means there was an error in the 
            -- system. To facilitate things, filter out this row.
            (NextMovementType IS NULL OR RIGHT(StartMovementType, 2) != LEFT(NextMovementType, 2) OR StartToLocationID = NextFromLocationID)
            -- Filter out all periods that start with a TERMINATION. 
            AND 
            StartPeriodEvent != 'TERMINATION'  
    )
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
        # CASE WHEN StartMovementReason = 'CELL_END' AND NextMovementReason IS NULL THEN 'GENERAL' ELSE HousingUnit END as HousingUnit,
        LAST_VALUE(HousingUnit ignore nulls) OVER (person_sequence range between UNBOUNDED preceding and current row) AS HousingUnit
    FROM filter_out_erroneous_rows
    LEFT JOIN 
        {Site} site_info
    ON StartToLocationID = SiteID 
        WINDOW person_sequence AS (PARTITION BY OFFENDERID ORDER BY MovementSequenceNumber)
)
SELECT 
    OffenderID,
    StartDateTime,
    EndDateTime,
    Site,
    SiteType,
    StartMovementType,
    StartMovementReason,
    CASE WHEN EndDateTime = DeathDate THEN 'DEATH' ELSE EndMovementType END AS EndMovementType,
    CASE WHEN EndDateTime = DeathDate THEN 'DEATH' ELSE EndMovementReason END AS EndMovementReason,
    # When someone transfers from an incarceration facility to Jail or other SiteType, the CustodyLevel from their previous 
    # assessment no longer applies so we do not want to carry over that CustodyLevel when it is no longer relevant. 
    # Therefore, we apply logic here to only attach the CustodyLevel to periods when someone 
    # is in an incarceration facility (SiteType = 'IN'). We may need to revisit this choice in the future if we decide to 
    # include other SiteTypes that the CustodyLevel still applies to. 
    CASE WHEN SiteType = 'IN' THEN CustodyLevel ELSE Null END AS CustodyLevel, 
    HousingUnit,
    ROW_NUMBER() OVER (PARTITION BY OffenderID ORDER BY MovementSequenceNumber ASC) AS IncarcerationPeriodSequenceNumber
FROM all_incarceration_periods
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_tn",
    ingest_view_name="OffenderMovementIncarcerationPeriod_v3",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
