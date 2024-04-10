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
"""Query containing MDOC supervision periods information."""

# pylint: disable=anomalous-backslash-in-string
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

##########
# NOTES: #
##########
# Supervision periods information is determined from five data sources:
# 1. ADH_OFFENDER_EXTERNAL_MOVEMENT: table that contains data on all movements associated with an individual
# 2. ADH_OFFENDER_SUPERVISION: table that contains data on all supervision levels assigned to an individual
# 3. ADH_LEGAL_ORDER: table that contains data on all legal orders associated with an individual
# 4. ADH_EMPLOYEE_BOOKING_ASSIGNMENT: table that contains data on all employee assignments associated with an individual
# 5. ADH_SUPERVISION_CONDITION: table that contains data on all conditions associated with a legal order
#
# The dates from each of these sources do not align in most cases, so the general strategy of this query is to construct
# all possible time intervals associated with an individual's time with MDOC and map on all applicable information we have
# about supervision for each time interval


# held in custody movement reasons
held_in_custody_movement_reason_ids = "'105', '128', '104', '106', '103', '102', '156'"

# movement reason ids associated with supervision period starts
# 9/20: remove 27, 28 as movement start reasons since these are releases from SAI where they're released from MDOC jurisdiction
start_movement_reason_ids = (
    "'112', '119', '77', '141', '145', '86', '2', '110', '130', '88', '120', '157', '89', '151', '150', '152', '87', '127', '90', '5', "
    + held_in_custody_movement_reason_ids
)

# movement reason ids associated with supervision period ends
end_movement_reason_ids = "'112', '119', '29', '122', '21', '148', '118', '25', '22', '38', '78', '39', '31', '126', '40', '146', '143', '134', '147', '124', '123', '115', '23', '42', '32', '121', '129', '142', '41', '36', '30', '125', '140', '106', '103', '102', '105', '128', '104', '133', '137', '136', '91', '13', '11', '159', '158', '12', '15', '14', '10', '16', '111', '19', '92', '138', '156', '89', '151', '37', '108', '18', '17', '79', '24', '139', '154', '131', '149', '144', '114', '153', '113', '95', '5'"

# 9/20: location types that are supervision offices
supervision_locs = "'221', '7965', '2145', '224'"

ALL_MOVEMENTS_CTE = """

-- Join together all tables necessary for movements information  
-- and filter out duplicate movements that have the same source and location

all_movements as (
    SELECT 
        offender_id,
        movement_datetime,
        movement_reason_id,
        source_location_type_id,
        dest_location_type_id,
        dest_name,
        offender_external_movement_id,
    FROM (
        SELECT 
            offender_id,
            super.offender_booking_id,
            cast(movement_date as datetime) as movement_datetime,
            (date(cast(movement_date as datetime))) as movement_date,
            movement_reason_id, 
            loc1.location_type_id as source_location_type_id,
            loc2.location_type_id as dest_location_type_id,
            loc1.name as source_name,
            loc2.name as dest_name,
            offender_external_movement_id,
            LAG(movement_reason_id) OVER(PARTITION BY offender_id ORDER BY cast(movement_date as datetime), offender_external_movement_id) as prev_movement_reason_id,
            LAG(loc1.name) OVER(PARTITION BY offender_id ORDER BY cast(movement_date as datetime), offender_external_movement_id) as prev_source_name ,
            LAG(loc2.name) OVER(PARTITION BY offender_id ORDER BY cast(movement_date as datetime), offender_external_movement_id) as prev_dest_name ,
        FROM {ADH_OFFENDER_EXTERNAL_MOVEMENT} super
            LEFT JOIN {ADH_LOCATION} loc1 on super.source_location_id = loc1.location_id
            LEFT JOIN {ADH_LOCATION} loc2 on super.destination_location_id = loc2.location_id
            INNER JOIN {ADH_OFFENDER_BOOKING} book on super.offender_booking_id = book.offender_booking_id
        ) sub
    WHERE
        -- remove duplicate movements from the same exact source location to the same exact destination location, keeping the earliest one
        not (prev_movement_reason_id is not null
                and movement_reason_id = prev_movement_reason_id
                and source_name = prev_source_name 
                and dest_name = prev_dest_name 
                )
)
"""


SUPERVISON_MOVEMENT_PERIODS_CTE = f"""

-- Identify all movements that indicate the start of a supervision period

supervision_period_starts as (
    select 
        *,
        'start' as supervision_period_status
    FROM all_movements
    where 
        movement_reason_id in ({start_movement_reason_ids})
        or 
        -- movement reason is TRANSFER OUT STATE and source location type is STATE or Outside of MDOC Jurisdiction and destination location type is a supervision office
        -- 9/20: add Outside of MDOC Jurisdiction (5772) and supervision_locs restraint
        (movement_reason_id in ('4') and source_location_type_id in ('2155', '5772') and dest_location_type_id in ({supervision_locs}))
        or 
        -- movement reason is Other Movement or Report to Office and destination location type is a supervision office
        (movement_reason_id in ('3', '1') and dest_location_type_id in ({supervision_locs}))
),

-- Identify all movements that indicate the end of a supervision period

supervision_period_ends as (
    select 
        *,
        'end' as supervision_period_status
    from all_movements
    where 
        movement_reason_id in ({end_movement_reason_ids})
        or 
        -- movement reason is Other Movement or TRANSFER OUT STATE and destination location type is STATE or Outside of MDOC Jurisdiction
        -- 9/20: add Outside of MDOC Jurisdiction (5772)
        (movement_reason_id in ('3', '4') and dest_location_type_id in ('2155', '5772'))
),

-- Create supervision periods by stacking all start movements and end movements, creating intervals, and taking all intervals that starts with a supervision start movement

movement_periods as (
    select 
        offender_id,
        (date(movement_datetime)) as movement_start, 
        (date(next_movement_date)) as movement_end,
        movement_reason_id,
        next_movement_reason_id,
        dest_name,
        dest_location_type_id,
        offender_external_movement_id
    from (
        select *,
            -- 9/20: revise order by to order by offender_external_movement_id (which hopefully is generated in the same order as the movement record is entered) before supervision_period_status
            LEAD(supervision_period_status) OVER(PARTITION BY offender_id ORDER BY movement_datetime, offender_external_movement_id, supervision_period_status ASC) as next_supervision_period_status,
            LEAD(movement_datetime) OVER(PARTITION BY offender_id ORDER BY movement_datetime, offender_external_movement_id, supervision_period_status ASC) as next_movement_date,
            LEAD(movement_reason_id) OVER(PARTITION BY offender_id ORDER BY movement_datetime, offender_external_movement_id, supervision_period_status ASC) as next_movement_reason_id,
            LEAD(movement_reason_id, 2) OVER(PARTITION BY offender_id ORDER BY movement_datetime, offender_external_movement_id, supervision_period_status ASC) as next_next_movement_reason_id
        from
        (
            (select * from supervision_period_starts)
            union all
            (select * from supervision_period_ends)
        ) sub1
    ) sub
    where supervision_period_status = 'start'
)
"""

SUPERVISION_LEVELS_CTE = """

-- Create supervision level periods

-- Notes: 1. small proportion of times where next supervision effective date < supervision expiration date
--              solution: in the cases where there are two supervision level periods that map to the same tiny span, let's take the one with the later last_update_date
--        2. small proportion of supervision level periods where expiration date is null, 
--           and only 5 occurences where this record with a null expiration date is succeeded by another supervision_level record
--              solution: case when null, take next effective date if exist

OMNI_levels as (
    SELECT 
           'OMNI' as source,
           offender_id,
           level_start_date,
           case when level_end_date is null and next_effective_date is not null then next_effective_date
                else level_end_date
                end as level_end_date,
           supervision_level_id as supervision_level_value,
           last_update_date,
           offender_supervision_id as record_id,
    FROM(
        SELECT  
            offender_id,
            (date(effective_date)) as level_start_date,
            (date(expiration_date)) as level_end_date,
            LEAD(date(effective_date)) OVER(PARTITION BY offender_id ORDER BY (date(effective_date)), offender_supervision_id) as next_effective_date,
            supervision_level_id,
            offender_supervision_id,
            (date(last_update_date)) as last_update_date
        FROM {ADH_OFFENDER_SUPERVISION} super
        INNER JOIN {ADH_OFFENDER_BOOKING} book on super.offender_booking_id = book.offender_booking_id
    ) sub
),
-- COMS supervision level are composed of two parts:
--   1. A general supervision level category from the table COMS_Supervision_Level
--   2. A more specific supervision level category from the table COMS_Supervision_Schedules

-- This CTE compiles the periods based on the general COMS levels
coms_levels_a AS (
  SELECT DISTINCT
    Offender_Number,
    (DATE(Start_Date)) AS level_start_date,
    COALESCE((DATE(End_Date)), DATE(9999,9,9)) AS level_end_date,
    Supervision_Level AS supervision_level_value,
    Supervision_Level_Id AS record_id,
    CAST(Entered_Date as DATETIME) AS last_update_date
  FROM {COMS_Supervision_Levels}
),
-- This CTE compiles the periods based on the specific COMS supervision schedules
coms_levels_b as (
  SELECT DISTINCT
    Offender_Number,
    Supervision_Activity_Panel AS supervision_level_value,
    (DATE(Schedule_Start_Date)) AS level_start_date,
    COALESCE((DATE(End_Date)), DATE(9999,9,9)) AS level_end_date,
    Supervision_Schedule_Id AS record_id,
    CAST(Entered_Date as DATETIME) AS last_update_date
  FROM {COMS_Supervision_Schedules} coms
), 
coms_level_periods_base as (
  SELECT 
      Offender_Number,
      coms_date AS coms_start_date,
      LEAD(coms_date) OVER(PARTITION BY Offender_Number ORDER BY coms_date) AS coms_end_date
  FROM (
    SELECT DISTINCT 
        Offender_Number,
        level_start_date AS coms_date
    FROM coms_levels_b
    
    UNION DISTINCT 

    SELECT  
        Offender_Number,
        level_end_date AS coms_date
    FROM coms_levels_b

    UNION DISTINCT 

    SELECT 
        Offender_Number,
        level_start_date AS coms_date
    FROM coms_levels_a

    UNION DISTINCT 

    SELECT  
        Offender_Number,
        level_end_date AS coms_date
    FROM coms_levels_a
  ) unioned
),
COMS_levels AS (
    SELECT
        'COMS' AS source,  
        offender_id,
        coms_start_date AS level_start_date,
        coms_end_date AS level_end_date,
        supervision_level_value,
        record_id,
        last_update_date
    FROM (
        SELECT
            offender_id,
            coms_start_date,
            CASE 
                WHEN coms_end_date = DATE(9999,9,9) 
                then NULL 
                else coms_end_date 
                end as coms_end_date,
            CONCAT(
                COALESCE(a.supervision_level_value, 'NONE'), 
                '_', 
                COALESCE(b.supervision_level_value, 'NONE')
                ) as supervision_level_value,
            ROW_NUMBER() 
                OVER(PARTITION BY p.Offender_Number, coms_start_date, coms_end_date 
                     ORDER BY a.last_update_date desc, b.last_update_date desc, a.record_id, b.record_id) as rnk,
            CASE
                WHEN a.last_update_date is null or b.last_update_date is null THEN COALESCE(a.last_update_date, b.last_update_date)
                WHEN a.last_update_date < b.last_update_date THEN b.last_update_date
                ELSE a.last_update_date
                END AS last_update_date,
            CONCAT(COALESCE(a.record_id, 'NONE'), '_', COALESCE(b.record_id, 'NONE')) as record_id
        FROM coms_level_periods_base p
        LEFT JOIN coms_levels_a a 
            on p.Offender_Number = a.Offender_Number
                AND a.level_start_date <= p.coms_start_date AND p.coms_end_date <= a.level_end_date
        LEFT JOIN coms_levels_b b 
            on p.Offender_Number = b.Offender_Number
                AND b.level_start_date <= p.coms_start_date AND p.coms_end_date <= b.level_end_date
        INNER JOIN {ADH_OFFENDER} off on LTRIM(p.Offender_Number, '0') = off.offender_number
        INNER JOIN {ADH_OFFENDER_BOOKING} USING(offender_id)
        WHERE 
            p.coms_start_date <> DATE(9999,9,9)
            AND (a.supervision_level_value IS NOT NULL OR b.supervision_level_value IS NOT NULL)
    ) sub
    WHERE rnk = 1
),
offender_supervision_periods as (
    SELECT
        offender_id,
        level_start_date,
        -- we only want to considers supervision levels from OMNI up until the migration date so let's end all supervision levels from omni that
        -- are still active on that day
        CASE WHEN 
                level_end_date >= DATE(2023,8,14) 
                OR
                level_end_date IS NULL
            THEN DATE(2023,8,14)
            ELSE level_end_date 
            END AS level_end_date,
        supervision_level_value,
        last_update_date,
        source,
        record_id 
    FROM OMNI_levels 

    UNION ALL
    
    SELECT 
        offender_id,
        level_start_date,
        level_end_date,
        supervision_level_value,
        last_update_date,
        source,
        record_id
    FROM COMS_levels
)
"""

MODIFIERS_CTE = """

-- Create modifiers periods (where modifiers from COMS give us information about supervision type and level)
modifiers_periods AS (
    SELECT 
    offender_id,
    Modifier_Id,
    (DATE(Start_Date)) AS modifier_start_date,
    COALESCE(DATE(End_Date), DATE(9999,9,9)) AS modifier_end_date,
    Modifier,
    (DATE(Entered_Date)) as Entered_Date
    FROM {COMS_Modifiers} mod
    INNER JOIN {ADH_OFFENDER} off ON LTRIM(mod.Offender_Number, '0') = off.offender_number
)
"""

SPECIALTIES_CTE = """
-- This CTE compiles the periods based on the specific COMS specialties
specialties_periods as (
  SELECT DISTINCT
    offender_id,
    Specialty_Id,
    (DATE(Start_Date)) AS specialty_start_date,
    COALESCE((DATE(End_Date)), DATE(9999,9,9)) AS specialty_end_date,
    Specialty,
    (DATE(Entered_Date)) as Entered_Date
  FROM {COMS_Specialties} spec
  INNER JOIN {ADH_OFFENDER} off ON LTRIM(spec.Offender_Number, '0') = off.offender_number
) 
"""

EMPLOYEE_ASSIGNMENTS_CTE = """

-- Create supervision officer assignment periods

OMNI_assignments as (
    select 
        'OMNI' as source,
        offender_id,
        employee_id,
        priority,
        employee_assignment_date as employee_start,
        case when employee_closure_date > next_assignment_date then next_assignment_date
            when employee_closure_date is null and next_assignment_date is not null then next_assignment_date
            else employee_closure_date
            end as employee_end
    from(
        select 
            offender_id,
            ass.employee_id,
            -- 3/23/23: We can't use the raw sequence number from the table to sort anymore cause that's partitioned by offender_booking_id, not offender_id
            ROW_NUMBER() OVER (PARTITION BY offender_id ORDER BY (date(assignment_date)), (date(closure_date)) NULLS LAST, ass.offender_booking_id, ass.sequence_number) as priority,
            (date(assignment_date)) as employee_assignment_date,
            -- we only want to considers employee assignments from omni up until the migration date so let's close out all open employee assignments on the migration date
            CASE WHEN (date(closure_date)) >= DATE(2023,8,14) or (date(closure_date)) is NULL
                 THEN DATE(2023,8,14)
                 ELSE (date(closure_date))
                 END AS employee_closure_date,
            LEAD(date(assignment_date)) OVER(PARTITION BY offender_id ORDER BY (date(assignment_date)), (date(closure_date)) NULLS LAST, ass.offender_booking_id, ass.sequence_number) as next_assignment_date
        from {ADH_EMPLOYEE_BOOKING_ASSIGNMENT} ass
          INNER JOIN {ADH_OFFENDER_BOOKING} book on ass.offender_booking_id = book.offender_booking_id
        where
            -- 9/20: remove filter on position type to account for issue where someone used to be a supervision officer but changed positions in later data
            -- ignore records where employee_id = 0 (which is not a real employee ID and MI uses to track internal transfers or something)
            ass.employee_id <> '0'
            -- ignore cases where someone is assigned and closed on the same day (cause I see that some in the data and I think those are errors)
            and ((date(assignment_date)) <> (date(closure_date)) or (date(closure_date)) is null)
            -- only include employee assignments with assignment_type_id = '1305' (aka Intake).  The other assignment type is 912 (aka Report Investigation) but those are only temporary assignments
            and assignment_type_id = '1305'
    ) sub
)
,
COMS_assignments as (
    select
        source,
        offender_id,
        employee_id,
        ROW_NUMBER() OVER (PARTITION BY offender_id ORDER BY employee_start DESC, employee_end NULLS FIRST, Entered_Date DESC, Case_Manager_Id) as priority,
        employee_start,
        employee_end
    from (
        select DISTINCT
            'COMS' as source,
            offender_id,
            Case_Manager_Id,
            Case_Manager_Omnni_Employee_Id as employee_id,
            (date(coms.Start_Date)) AS employee_start,
            (date(coms.End_Date)) AS employee_end,
            CAST(Entered_Date as DATETIME) as Entered_Date
        from {COMS_Case_Managers} coms
        inner join {ADH_OFFENDER} off on LTRIM(coms.Offender_Number, '0') = off.offender_number
        INNER JOIN {ADH_OFFENDER_BOOKING} USING(offender_id)
        WHERE Case_Manager_Omnni_Employee_Id is not NULL -- not sure if this is a concern for real data, but it's sometimes null in sample data
    ) sub
), 
offender_booking_assignment as (
    select 
        offender_id,
        combined.employee_id,
        position,
        first_name,
        middle_name,
        last_name,
        name_suffix,
        ref.description as employee_county,
        source,
        priority,
        employee_start,
        employee_end 
    from (
        select * from OMNI_assignments
        union all 
        select * from COMS_assignments
    ) combined
    inner join {ADH_EMPLOYEE} emp on combined.employee_id = emp.employee_id
    left join {ADH_LOCATION} loc on emp.default_location_id = loc.location_id
    left join {ADH_REFERENCE_CODE} ref on loc.county_id = ref.reference_code_id
)
"""

LEGAL_ORDERS_CTE = """

-- Create legal order periods

-- Notes:
--  1. 44 supervision legal order records where expiration date is null and all but one before 2003
--     solution: ignore for now since we likely won't be using such old data
--  2. Some cases where closing date and expiration date are not the same
--     solution: when closing_date < expiration_date using closing date as legal order period end date
--  3. Some cases where probation and parole legal orders overlap
--     solution: when this happens, map supervision type to DUAL

legal_orders as (
    select 
        offender_id,
        order_type_id,
        (date(effective_date)) as legal_start,
        case when (date(closing_date)) < (date(expiration_date)) then (date(closing_date))
             else (date(expiration_date))
             end as legal_end,
        legal_order_id
    FROM {ADH_LEGAL_ORDER} legal
    INNER JOIN {ADH_OFFENDER_BOOKING} book on legal.offender_booking_id = book.offender_booking_id
    -- filter down to just parole and probation related legal orders
    where order_type_id in ('1719','1726','1720','1727')
)
"""

MISC_CTE = """

-- Grab remaining tables that we need that don't need to be transformed in any way, 
-- which include supervision conditions

supervision_conditions as (
    select cond.*, offender_id from {ADH_SUPERVISION_CONDITION} cond
    INNER JOIN {ADH_OFFENDER_BOOKING} book on cond.offender_booking_id = book.offender_booking_id
)
"""

PERIOD_COLUMNS = """
    period_start,
    period_end
"""

MOVEMENT_COLUMNS = """
    offender_external_movement_id,
    movement_reason_id,
    next_movement_reason_id,
    dest_name,
    dest_location_type_id
    """

SUPERVISION_LEVEL_COLUMNS = """
    record_id,
    supervision_level_value
"""

MODIFIERS_COLUMNS = """
    Modifier
"""

SPECIALTIES_COLUMNS = """
    specialties
"""

LEGAL_ORDER_COLUMNS = """
    legal_order_id_combined,
    order_type_id_list,
    special_condition_ids
"""

EMPLOYEE_COLUMNS = """
    employee_id,
    position,
    first_name,
    middle_name,
    last_name,
    name_suffix,
    employee_county
"""

VIEW_QUERY_TEMPLATE = f"""

WITH {ALL_MOVEMENTS_CTE},
{SUPERVISON_MOVEMENT_PERIODS_CTE},
{SUPERVISION_LEVELS_CTE},
{MODIFIERS_CTE},
{SPECIALTIES_CTE},
{EMPLOYEE_ASSIGNMENTS_CTE},
{LEGAL_ORDERS_CTE},
{MISC_CTE},

-- Create all posible date intervals for each offender_booking_id based on each of the five data sources

tiny_spans as (
    select *,
        LEAD(period_start) OVER(PARTITION BY offender_id ORDER BY period_start) as period_end
    from (
        (
        select 
            distinct
            offender_id,
            dte as period_start
        from 
        (
            (
            select 
                distinct offender_id,
                movement_start as dte
            from movement_periods
            )
            union all
            (
            select 
                distinct offender_id,
                movement_end as dte
            from movement_periods
            )
            union all
            (
            select 
                distinct offender_id,
                legal_start as dte
            from legal_orders
            )
            union all 
            (
            select 
                distinct offender_id,
                legal_end as dte
            from legal_orders
            )
            union all
            (
            -- only need to worry about closing dates from the supervision conditions table 
            -- because the starting dates will be the same as the legal order starting dates
            select 
                distinct offender_id,
                (date(closing_date)) as dte
            from supervision_conditions
            )
            union all
            (
            select 
                distinct offender_id,
                level_start_date as dte
            from offender_supervision_periods
            )
            union all
            (
            select 
                distinct offender_id,
                level_end_date as dte
            from offender_supervision_periods
            )
            union all
            (
            select 
                distinct offender_id,
                employee_start as dte
            from offender_booking_assignment
            )
            union all
            (
            select 
                distinct offender_id,
                employee_end as dte
            from offender_booking_assignment
            )
            union all
            (
            select
                distinct offender_id,
                modifier_start_date as dte
            from modifiers_periods
            )
            union all
            (
            select
                distinct offender_id,
                modifier_end_date as dte
            from modifiers_periods
            )
            union all
            (
            select
                distinct offender_id,
                specialty_start_date as dte
            from specialties_periods
            )
            union all
            (
            select
                distinct offender_id,
                specialty_end_date as dte
            from specialties_periods
            )
        ) unioned
        -- don't include periods in the future
        where dte is not null and dte <= @update_timestamp
        )  
        -- 9/20 revision: realized that the original query was missing same day periods (where two movements happen on the same day) so adding that in here
        union all 
        (
            select distinct offender_id,
                    movement_start as dte
            from movement_periods
            where movement_start = movement_end
        )
        ) unioned2
),

-- Join movement periods data onto all tiny spans
spans_with_movements as (
    (select 
        sp.offender_id,
        {PERIOD_COLUMNS},
        {MOVEMENT_COLUMNS}
    from tiny_spans sp
        left join movement_periods move on (sp.offender_id = move.offender_id 
                                            and (move.movement_start <= sp.period_start)
                                            and 
                                                (
                                                    (sp.period_end is not null and move.movement_end >= sp.period_end)
                                                    or
                                                    (sp.period_end is null and move.movement_end > sp.period_start)
                                                    or
                                                    (move.movement_end is null)
                                                )
                                            )
    )
),

-- Join supervision level periods data onto all tiny spans
-- Note: small proportion of times where next supervision effective date < supervision expiration date
--       solution: in the cases where there are two supervision level periods that map to the same tiny span, let's prioritizing those coming from COMS, and then take the one with the later update_date

spans_movements_supervision as (
  select *
  from (
    select
        sp.offender_id,
        {PERIOD_COLUMNS},
        {MOVEMENT_COLUMNS},
        {SUPERVISION_LEVEL_COLUMNS},
        RANK() OVER(PARTITION BY sp.offender_id, period_start, period_end ORDER BY source, last_update_date desc, record_id) as supervision_rnk
    from spans_with_movements sp
        left join offender_supervision_periods level on (
            sp.offender_id = level.offender_id 
            and (level.level_start_date <= sp.period_start)
            and 
                (
                    (sp.period_end is not null and level.level_end_date >= sp.period_end)
                    or
                    (sp.period_end is null and level.level_end_date > sp.period_start)
                    or
                    (level.level_end_date is null)
                )
            )  
  ) sub
  where supervision_rnk = 1
),

-- Join modifiers periods data onto all tiny spans
-- In the cases where there are two modifiers that map to the same tiny span, let's prioritize whichever one was most recently entered

spans_movements_supervision_modifiers as (
  select *
  from (
    select
        sp.offender_id,
        {PERIOD_COLUMNS},
        {MOVEMENT_COLUMNS},
        {SUPERVISION_LEVEL_COLUMNS},
        {MODIFIERS_COLUMNS},
        RANK() OVER(PARTITION BY sp.offender_id, period_start, period_end ORDER BY mod.Entered_Date DESC, Modifier_Id) as modifier_rnk
    from spans_movements_supervision sp
        left join modifiers_periods mod on (
            sp.offender_id = mod.offender_id 
            and (mod.modifier_start_date <= sp.period_start)
            and 
                (
                    (sp.period_end is not null and mod.modifier_end_date >= sp.period_end)
                    or
                    (sp.period_end is null and mod.modifier_end_date > sp.period_start)
                    or
                    (mod.modifier_end_date is null)
                )
            )  
  ) sub
  where modifier_rnk = 1
),

-- Join legal order periods data onto all tiny spans

-- Note: 1. Since overlapping legal orders would indicate a person is both on parole and probation, we'll use STRING_AGG to concatenate legal order info for each period
--       2. Also joining on supervision conditions here since they link with legal orders.  
--       3. There are cases where supervision condition closing date is before legal order end date, so only joining on supervision conditions onto a time span if it's before the condition closing date

spans_supervision_legal_with_specialties as (
  select 
    offender_id,
    {PERIOD_COLUMNS},
    {MOVEMENT_COLUMNS},
    {SUPERVISION_LEVEL_COLUMNS},
    {MODIFIERS_COLUMNS},
    STRING_AGG(distinct legal_order_id, ',' ORDER BY legal_order_id) as legal_order_id_combined,
    STRING_AGG(distinct order_type_id, ',' ORDER BY order_type_id) as order_type_id_list,
    STRING_AGG(distinct supervision_condition_id, ',' ORDER BY supervision_condition_id) as supervision_condition_id_combined,
    STRING_AGG(distinct special_condition_id, ',' ORDER BY special_condition_id) as special_condition_ids,
    STRING_AGG(distinct Specialty, '&' ORDER BY Specialty) as specialties
  from (
    select distinct
        sp.offender_id,
        {PERIOD_COLUMNS},
        {MOVEMENT_COLUMNS},
        {SUPERVISION_LEVEL_COLUMNS},
        {MODIFIERS_COLUMNS},
        legal.legal_order_id,
        legal.order_type_id,
        supervision_condition_id,
        cond.special_condition_id,
        spec.Specialty
    from spans_movements_supervision_modifiers sp
        left join legal_orders legal on (
            sp.offender_id = legal.offender_id 
            and (legal.legal_start <= sp.period_start)
            and 
                (
                    (sp.period_end is not null and legal.legal_end >= sp.period_end)
                    or
                    (sp.period_end is null and legal.legal_end > sp.period_start)
                    or
                    (legal.legal_end is null)
                )
            )
        left join supervision_conditions cond on (
            sp.offender_id = cond.offender_id
            and legal.legal_order_id = cond.legal_order_id
            and (date(cond.closing_date) >= sp.period_end or (date(cond.closing_date)) is null)
            )
        left join specialties_periods spec on (
            sp.offender_id = spec.offender_id 
            and (spec.specialty_start_date <= sp.period_start)
            and 
                (
                    (sp.period_end is not null and spec.specialty_end_date >= sp.period_end)
                    or
                    (sp.period_end is null and spec.specialty_end_date > sp.period_start)
                    or
                    (spec.specialty_end_date is null)
                )
            ) 
  ) sub
  group by 
    offender_id,    
    {PERIOD_COLUMNS},
    {MOVEMENT_COLUMNS},
    {SUPERVISION_LEVEL_COLUMNS},
    {MODIFIERS_COLUMNS}
),

-- Join on supervision officer assignment periods as well filter down only to spans with an offender_booking_id in ADH_OFFENDER_BOOKING
-- Note: if multiple supervision officers join on to the same period, take the officer that was last assigned according to their source and priority (which prioritizes assignments based on COMS over assignments based on OMNI)

spans_all_combos as (
  select *
  from (
    select 
      sp.offender_id,
      {PERIOD_COLUMNS},
      {MOVEMENT_COLUMNS},
      {SUPERVISION_LEVEL_COLUMNS},
      {MODIFIERS_COLUMNS},
      {LEGAL_ORDER_COLUMNS},
      {SPECIALTIES_COLUMNS},
      {EMPLOYEE_COLUMNS},
      RANK() OVER(PARTITION BY sp.offender_id, period_start, period_end ORDER BY source, priority) as booking_rnk
    from spans_supervision_legal_with_specialties sp
      left join offender_booking_assignment emp on (sp.offender_id = emp.offender_id 
                                                    and (emp.employee_start <= sp.period_start)
                                                            and 
                                                                (
                                                                    (sp.period_end is not null and emp.employee_end >= sp.period_end)
                                                                    or
                                                                    (sp.period_end is null and emp.employee_end > sp.period_start)
                                                                    or
                                                                    (emp.employee_end is null)
                                                                )
                                                              )

  ) sub
  where booking_rnk = 1
)

-- select final data, including adding columns for a period_id as well as next and previous external movement ids, 
-- and filtering down to only rows where we have information from the movements table about a supervision related movement

select 
    offender_id,
    -- 9/20 revision: now that we've added same day periods, adding period_end to order by
    ROW_NUMBER() OVER (PARTITION BY offender_id ORDER BY period_start, period_end NULLS LAST, offender_external_movement_id) as period_id,
    {PERIOD_COLUMNS},
    {MOVEMENT_COLUMNS},
    -- 9/20 revision: adding in prev_movement_reason_id for intake/investigation movement mapping nuances (NOTE: prev_movement_reason_id is not analogous to next_movement_reason_id due to being constructed differently)
    -- 9/20 revision: now that we've added same day periods, adding period_end to order by
    LAG(movement_reason_id) OVER(PARTITION BY offender_id ORDER BY period_start, period_end NULLS LAST, offender_external_movement_id) as prev_movement_reason_id,
    LEAD(offender_external_movement_id) OVER(PARTITION BY offender_id ORDER BY period_start, period_end NULLS LAST, offender_external_movement_id) as next_offender_external_movement_id,
    LAG(offender_external_movement_id) OVER(PARTITION BY offender_id ORDER BY period_start, period_end NULLS LAST, offender_external_movement_id) as prev_offender_external_movement_id,
    supervision_level_value as supervision_level_id,
    order_type_id_list,
    -- 9/20 add prev_order_type_id_list and next_order_type_id_list to use for intake/investigation movement mapping nuances
    LAG(order_type_id_list) OVER(PARTITION BY offender_id ORDER BY period_start, period_end NULLS LAST, offender_external_movement_id) as prev_order_type_id_list,
    LEAD(order_type_id_list) OVER(PARTITION BY offender_id ORDER BY period_start, period_end NULLS LAST, offender_external_movement_id) as next_order_type_id_list,
    special_condition_ids,
    employee_id,
    first_name,
    middle_name,
    last_name,
    name_suffix,
    employee_county,
    -- 9/20 add position to use in new agent type mapping rule
    position,
    -- grab the most recent supervision level we saw for this person (which could be the supervision level currently active)
    LAST_VALUE(supervision_level_value IGNORE NULLS)
        OVER (PARTITION BY offender_id, legal_order_id_combined ORDER BY period_start, period_end NULLS LAST, offender_external_movement_id
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS most_recent_supervision_level_id,
    modifier,
    specialties
from spans_all_combos
where movement_reason_id is not null;
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mi",
    ingest_view_name="supervision_periods_v2",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
