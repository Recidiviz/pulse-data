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
"""Query containing offender movement information.
The table contains one row per movement from facility to facility, and this query takes each MovementType and
MovementReason and ultimately maps it into incarceration and supervision periods. The output of this query is used by
`view_OffenderMovementIncarcerationPeriod` and `view_OffenderMovementSupervisionPeriod`.
"""
# pylint: disable=line-too-long

ALL_PERIODS_QUERY = """
WITH initial_setup as (
    SELECT
        OffenderID,
        MovementDateTime,
         -- Remove extraneous unexpected characters.
        REGEXP_REPLACE(MovementType, r'[^A-Z]', '') as MovementType, 
        REGEXP_REPLACE(MovementReason, r'[^A-Z]', '') as MovementReason,
        REGEXP_REPLACE(FromLocationID, r'[^A-Z0-9]', '') as FromLocationID,
        REGEXP_REPLACE(ToLocationID, r'[^A-Z0-9]', '') as ToLocationID,
        -- Based on the MovementType, determine the type of period event that is commencing.
        CASE
            -- Movements whose destinations are associated with incarceration facilities.
            WHEN RIGHT(MovementType, 2) IN ('FA', 'OJ', 'FH', 'CT') THEN 'INCARCERATION'
            
            -- Movements whose destinations are associated with supervision facilities.
            WHEN RIGHT(MovementType, 2) IN ('CC', 'PA', 'PR', 'DV') THEN 'SUPERVISION'
            
            -- Movements whose destinations are not associated with supervision 
            -- or incarceration facilities.
            WHEN RIGHT(MovementType, 2) IN ('DI', 'EI', 'ES', 'NC', 'AB', 'FU', 'BO', 'WR') THEN 'TERMINATION'

            -- There shouldn't be any PeriodEvents that fall into `UNCATEGORIZED`
            ELSE 'UNCATEGORIZED'
        END AS PeriodEvent,
        -- Only pull in the Death Date if it is properly formatted. The DeathDate should look like: YYYY-MM-DD HH:MM:SS.
        CASE 
            WHEN REGEXP_CONTAINS(attributes.DeathDate, r'[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9]') THEN DeathDate
        END AS DeathDateTime,
        ROW_NUMBER() OVER (PARTITION BY OffenderID ORDER BY MovementDateTime ASC) AS MovementSequenceNumber,
    FROM {OffenderMovement}
    LEFT JOIN {OffenderAttributes} as attributes
        USING (OffenderID)
    -- Filter out rows that are not associated physical movements from facilities.
    WHERE MovementReason not in ('CLASP', 'CLAST')
),
filter_out_movements_after_death_date AS (
    SELECT *
    FROM initial_setup
    WHERE 
        DeathDateTime IS NULL
        -- Filter out all rows whose start date is after the death date, if present.
        OR SUBSTRING(MovementDateTime, 0, 10) <= SUBSTRING(DeathDateTime, 0, 10)
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
        LEAD(MovementDateTime) OVER (PARTITION BY OffenderID ORDER BY MovementSequenceNumber) AS NextMovementDateTime,
        LEAD(MovementType) OVER (PARTITION BY OffenderID ORDER BY MovementSequenceNumber) AS NextMovementType,
        LEAD(MovementReason) OVER (PARTITION BY OffenderID ORDER BY MovementSequenceNumber) AS NextMovementReason,
        LEAD(FromLocationID) OVER (PARTITION BY OffenderID ORDER BY MovementSequenceNumber) AS NextFromLocationID,
        LEAD(PeriodEvent) OVER (PARTITION BY OffenderID ORDER BY MovementSequenceNumber) AS NextPeriodEvent,
        DeathDateTime,
    FROM filter_out_movements_after_death_date
),
clean_up_movements AS (
    SELECT 
        *,
    FROM append_next_movement_information_and_death_dates 
    WHERE 
        -- If the locations don't match and facility type in the end of StartMovementType matches the facility type 
        -- of the start of NextMovementType (ex: PAFA -> FADI), then that likely means there was an error in the 
        -- system. To facilitate things, filter out the row.
        (NextMovementType IS NULL OR StartToLocationID = NextFromLocationID OR RIGHT(StartMovementType, 2) = LEFT(NextMovementType, 2))
        -- Filter out all periods that start with a TERMINATION. 
        AND StartPeriodEvent != 'TERMINATION'
    
)
SELECT 
    OffenderID,
    StartPeriodEvent as InferredPeriodType,
    MovementSequenceNumber,
    StartMovementDateTime as StartDateTime,
    IF ( 
        -- If we do not have an end date, then set the end date to be the death date. Ignore the timestamp when doing
        -- the comparison between StartDateTime and DeathDateTime.
        NextMovementDateTime IS NULL AND DeathDateTime IS NOT NULL, 
        DeathDateTime, 
        -- Otherwise: if there is an end date, use that, but otherwise use the death date.
        COALESCE(NextMovementDateTime, DeathDateTime)
    ) AS EndDateTime,
    StartToLocationID as Site,
    StartMovementType,
    StartMovementReason,
    NextMovementType as EndMovementType,
    NextMovementReason as EndMovementReason,
FROM clean_up_movements
"""
