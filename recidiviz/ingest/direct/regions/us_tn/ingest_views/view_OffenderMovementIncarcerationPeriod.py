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

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH all_incarceration_periods AS ( 
    WITH remove_extraneous_chars as (
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
    filter_to_only_incarceration as (
        SELECT
            OffenderID,
            MovementDateTime,
            MovementType, 
            MovementReason,
            FromLocationID,
            ToLocationID,
        FROM remove_extraneous_chars
        -- Filter to only rows that are to/from with incarceration facilities.
        WHERE RIGHT(MovementType, 2) IN ('FA', 'FH', 'CT') OR LEFT(MovementType, 2) IN ('FA', 'FH', 'CT')
    ),
    initial_setup as (
        SELECT
            OffenderID,
            MovementDateTime,
            MovementType, 
            MovementReason,
            FromLocationID,
            ToLocationID,
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
            ROW_NUMBER() OVER (PARTITION BY OffenderID ORDER BY MovementDateTime ASC) AS MovementSequenceNumber,
        FROM filter_to_only_incarceration
        LEFT JOIN {OffenderAttributes} as attributes
            USING (OffenderID)
        -- Filter out rows that are not associated physical movements from facilities.
        WHERE MovementReason not in ('CLASP', 'CLAST', 'APPDI')
    ),
    filter_out_movements_after_death_date AS (
        SELECT *
        FROM initial_setup
        WHERE 
            DeathDate IS NULL
            -- Filter out all rows whose start date is after the death date, if present.
            OR SUBSTRING(MovementDateTime, 0, 10) <= DeathDate
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
        FROM filter_out_movements_after_death_date
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
        FROM append_next_movement_information_and_death_dates
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
            AND StartPeriodEvent != 'TERMINATION'    
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
        StartToLocationID as Site,
        site_info.SiteType,
        StartMovementType,
        StartMovementReason,
        NextMovementType as EndMovementType,
        NextMovementReason as EndMovementReason,
    FROM filter_out_erroneous_rows
    LEFT JOIN 
        {Site} site_info
    ON StartToLocationID = SiteID
)
SELECT 
    OffenderID,
    StartDateTime,
    EndDateTime,
    Site,
    SiteType,
    StartMovementType,
    StartMovementReason,
    EndMovementType,
    EndMovementReason,
    ROW_NUMBER() OVER (PARTITION BY OffenderID ORDER BY MovementSequenceNumber ASC) AS IncarcerationPeriodSequenceNumber
FROM all_incarceration_periods
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_tn",
    ingest_view_name="OffenderMovementIncarcerationPeriod",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="OffenderID ASC",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
