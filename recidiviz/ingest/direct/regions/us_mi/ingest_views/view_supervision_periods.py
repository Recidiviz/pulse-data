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
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
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


# movement reason ids associated with supervision period starts
start_movement_reason_ids = "'112', '119', '77', '141', '145', '86', '2', '110', '130', '88', '120', '157', '89', '151', '150', '152', '87', '127', '28', '27', '90'"

# movement reason ids associated with supervision period ends
end_movement_reason_ids = "'112', '119', '29', '122', '21', '148', '118', '25', '22', '38', '78', '39', '31', '126', '40', '146', '143', '134', '147', '124', '123', '115', '23', '42', '32', '121', '129', '142', '41', '36', '30', '125', '140', '106', '103', '102', '105', '128', '104', '133', '137', '136', '91', '13', '11', '159', '158', '12', '15', '14', '10', '16', '111', '19', '92', '138', '156', '89', '151', '37', '108', '18', '17', '79', '24', '139', '154', '131', '149', '144', '114', '153', '113', '95'"

# held in custody movement reasons
held_in_custody_movement_reason_ids = "'105', '128', '104', '106', '103', '102', '156'"

# revocation movement reasons
revocation_movement_reason_ids = "'17', '129'"

ALL_MOVEMENTS_CTE = """

-- Join together all tables necessary for movements information  
-- and filter out duplicate movements that have the same source and location

all_movements as (
    SELECT 
        offender_booking_id,
        movement_datetime,
        movement_reason_id,
        source_location_type_id,
        dest_location_type_id,
        dest_name,
        offender_external_movement_id,
    FROM (
        SELECT 
            offender_booking_id,
            cast(movement_date as datetime) as movement_datetime,
            (date(cast(movement_date as datetime))) as movement_date,
            movement_reason_id, 
            loc1.location_type_id as source_location_type_id,
            loc2.location_type_id as dest_location_type_id,
            loc2.name as dest_name,
            offender_external_movement_id,
            LAG(movement_reason_id) OVER(PARTITION BY offender_booking_id ORDER BY cast(movement_date as datetime), offender_external_movement_id) as prev_movement_reason_id,
            LAG(loc1.location_type_id) OVER(PARTITION BY offender_booking_id ORDER BY cast(movement_date as datetime), offender_external_movement_id) as prev_source_location_type_id ,
            LAG(loc2.location_type_id) OVER(PARTITION BY offender_booking_id ORDER BY cast(movement_date as datetime), offender_external_movement_id) as prev_dest_location_type_id ,
        FROM {ADH_OFFENDER_EXTERNAL_MOVEMENT} super
            LEFT JOIN {ADH_LOCATION} loc1 on super.source_location_id = loc1.location_id
            LEFT JOIN {ADH_LOCATION} loc2 on super.destination_location_id = loc2.location_id
        ) sub
    WHERE 
        -- remove duplicate movements from the same source to the same location, keeping the earliest one
        not (prev_movement_reason_id is not null
                and movement_reason_id = prev_movement_reason_id
                and source_location_type_id = prev_source_location_type_id 
                and dest_location_type_id = prev_dest_location_type_id 
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
        -- movement reason is TRANSFER OUT STATE and source location type is STATE
        (movement_reason_id in ('4') and source_location_type_id='2155')
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
        -- movement reason is TRANSFER OUT STATE and destination location type is STATE
        (movement_reason_id in ('4') and dest_location_type_id='2155')
),

-- Create supervision periods by stacking all start movements and end movements, creating intervals, and taking all intervals that starts with a supervision start movement

movement_periods as (
    select 
        offender_booking_id,
        (date(movement_datetime)) as movement_start, 
        (date(next_movement_date)) as movement_end,
        movement_reason_id,
        -- if the end of this period is being held in custody but the movement after that is revocation, replace held in custody with revocation as the next movement reason so that the revocation will be capture in our data
        case when next_movement_reason_id in ({held_in_custody_movement_reason_ids}) and next_next_movement_reason_id in ({revocation_movement_reason_ids}) then next_next_movement_reason_id
             else next_movement_reason_id
            end as next_movement_reason_id,
        dest_name,
        offender_external_movement_id
    from (
        select *,
            LEAD(supervision_period_status) OVER(PARTITION BY offender_booking_id ORDER BY movement_datetime, supervision_period_status ASC, offender_external_movement_id) as next_supervision_period_status,
            LEAD(movement_datetime) OVER(PARTITION BY offender_booking_id ORDER BY movement_datetime, supervision_period_status ASC, offender_external_movement_id) as next_movement_date,
            LEAD(movement_reason_id) OVER(PARTITION BY offender_booking_id ORDER BY movement_datetime, supervision_period_status ASC, offender_external_movement_id) as next_movement_reason_id,
            LEAD(movement_reason_id, 2) OVER(PARTITION BY offender_booking_id ORDER BY movement_datetime, supervision_period_status ASC, offender_external_movement_id) as next_next_movement_reason_id
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
--              solution: case when null, take next effective date if exists

offender_supervision_periods as (
    SELECT offender_booking_id,
           level_start_date,
           case when level_end_date is null and next_effective_date is not null then next_effective_date
                else level_end_date
                end as level_end_date,
           supervision_level_id,
           last_update_date,
           offender_supervision_id
    FROM(
        SELECT  
            offender_booking_id,
            (date(effective_date)) as level_start_date,
            (date(expiration_date)) as level_end_date,
            LEAD(date(effective_date)) OVER(PARTITION BY offender_booking_id ORDER BY (date(effective_date)), offender_supervision_id) as next_effective_date,
            supervision_level_id,
            offender_supervision_id,
            (date(last_update_date)) as last_update_date
        FROM {ADH_OFFENDER_SUPERVISION} super
    ) sub
)
"""

EMPLOYEE_ASSIGNMENTS_CTE = """

-- Create supervision officer assignment periods

-- Note: when a new supervision officer is assigned before the last employee assignment is closed, 
--       take the next employee assignment date as the end of the last employee assignment

offender_booking_assignment as (
    select 
        offender_booking_id,
        employee_id,
        position,
        first_name,
        middle_name,
        last_name,
        name_suffix,
        employee_county,
        sequence_number,
        employee_assignment_date as employee_start,
        case when employee_closure_date > next_assignment_date then next_assignment_date
            when employee_closure_date is null and next_assignment_date is not null then next_assignment_date
            else employee_closure_date
            end as employee_end
    from(
        select 
            offender_booking_id,
            ass.employee_id,
            position,
            first_name,
            middle_name,
            last_name,
            name_suffix,
            ref.description as employee_county,
            CAST(sequence_number as int) as sequence_number,
            (date(assignment_date)) as employee_assignment_date,
            (date(closure_date)) as employee_closure_date,
            LEAD(date(assignment_date)) OVER(PARTITION BY offender_booking_id ORDER BY CAST(sequence_number as int)) as next_assignment_date
        from {ADH_EMPLOYEE_BOOKING_ASSIGNMENT} ass
          left join {ADH_EMPLOYEE} emp on ass.employee_id = emp.employee_id
          left join {ADH_LOCATION} loc on emp.default_location_id = loc.location_id
          left join {ADH_REFERENCE_CODE} ref on loc.county_id = ref.reference_code_id
        where
            -- filter down to only supervision officers (MI only has a text field for position type)
            ( 
                upper(position) like '%PAROLE PROBATION%'
                or 
                upper(position) like '%PAROLE/PRBTN%'
                or 
                upper(position) like '%PAROLE/PROBATION%'
            )
            and
            -- ignore records where employee_id = 0 (which is not a real employee ID and MI uses to track internal transfers or something)
            ass.employee_id <> '0'
            -- ignore cases where someone is assigned and closed on the same day (cause I see that some in the data and I think those are errors)
            and (date(assignment_date)) <> (date(closure_date))
    ) sub
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
    select offender_booking_id,
        order_type_id,
        (date(effective_date)) as legal_start,
        case when (date(closing_date)) < (date(expiration_date)) then (date(closing_date))
             else (date(expiration_date))
             end as legal_end,
        legal_order_id
    FROM {ADH_LEGAL_ORDER} legal
    -- filter down to just parole and probation related legal orders
    where order_type_id in ('1719','1726','1720','1727')
)
"""

MISC_CTE = """

-- Grab remaining tables that we need that don't need to be transformed in any way, 
-- which include supervision conditions and a crosswalk from offender_booking_id to offender_number

supervision_conditions as (
    select * from {ADH_SUPERVISION_CONDITION}
),
offender_number_xwalk as (
      select book.offender_booking_id, off.offender_number
      from {ADH_OFFENDER_BOOKING} book
        left join {ADH_OFFENDER} off on book.offender_id = off.offender_id
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
    dest_name
    """

SUPERVISION_LEVEL_COLUMNS = """
    offender_supervision_id,
    supervision_level_id
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
    sequence_number,
    employee_county
"""

VIEW_QUERY_TEMPLATE = f"""

WITH {ALL_MOVEMENTS_CTE},
{SUPERVISON_MOVEMENT_PERIODS_CTE},
{SUPERVISION_LEVELS_CTE},
{EMPLOYEE_ASSIGNMENTS_CTE},
{LEGAL_ORDERS_CTE},
{MISC_CTE},

-- Create all posible date intervals for each offender_booking_id based on each of the five data sources

tiny_spans as (
    select *,
        LEAD(period_start) OVER(PARTITION BY offender_booking_id ORDER BY period_start) as period_end
    from (
        select 
            distinct
            offender_booking_id,
            dte as period_start
        from 
        (
            (
            select 
                distinct offender_booking_id,
                movement_start as dte
            from movement_periods
            )
            union all
            (
            select 
                distinct offender_booking_id,
                movement_end as dte
            from movement_periods
            )
            union all
            (
            select 
                distinct offender_booking_id, 
                legal_start as dte
            from legal_orders
            )
            union all 
            (
            select 
                distinct offender_booking_id, 
                legal_end as dte
            from legal_orders
            )
            union all
            (
            -- only need to worry about closing dates from the supervision conditions table 
            -- because the starting dates will be the same as the legal order starting dates
            select 
                distinct offender_booking_id, 
                (date(closing_date)) as dte
            from supervision_conditions
            )
            union all
            (
            select 
                distinct offender_booking_id,
                level_start_date as dte
            from offender_supervision_periods
            )
            union all
            (
            select 
                distinct offender_booking_id,
                level_end_date as dte
            from offender_supervision_periods
            )
            union all
            (
            select 
                distinct offender_booking_id,
                employee_start as dte
            from offender_booking_assignment
            )
            union all
            (
            select 
                distinct offender_booking_id,
                employee_end as dte
            from offender_booking_assignment
            )
        ) unioned
        -- don't include periods in the future
        where dte is not null and dte <= @update_timestamp
    ) sub
),

-- Join movement periods data onto all tiny spans
spans_with_movements as (
    (select 
        sp.offender_booking_id,
        {PERIOD_COLUMNS},
        {MOVEMENT_COLUMNS}
    from tiny_spans sp
        left join movement_periods move on (sp.offender_booking_id = move.offender_booking_id 
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
--       solution: in the cases where there are two supervision level periods that map to the same tiny span, let's take the one with the later update_date

spans_movements_supervision as (
  select *
  from (
    select
        sp.offender_booking_id,
        {PERIOD_COLUMNS},
        {MOVEMENT_COLUMNS},
        {SUPERVISION_LEVEL_COLUMNS},
        RANK() OVER(PARTITION BY sp.offender_booking_id, period_start, period_end ORDER BY last_update_date desc, offender_supervision_id) as supervision_rnk
    from spans_with_movements sp
        left join offender_supervision_periods level on (
            sp.offender_booking_id = level.offender_booking_id 
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

-- Join legal order periods data onto all tiny spans

-- Note: 1. Since overlapping legal orders would indicate a person is both on parole and probation, we'll use STRING_AGG to concatenate legal order info for each period
--       2. Also joining on supervision conditions here since they link with legal orders.  
--       3. There are cases where supervision condition closing date is before legal order end date, so only joining on supervision conditions onto a time span if it's before the condition closing date

spans_supervision_legal as (
  select 
    offender_booking_id,
    {PERIOD_COLUMNS},
    {MOVEMENT_COLUMNS},
    {SUPERVISION_LEVEL_COLUMNS},
    STRING_AGG(distinct legal_order_id, ',' ORDER BY legal_order_id) as legal_order_id_combined,
    STRING_AGG(distinct order_type_id, ',' ORDER BY order_type_id) as order_type_id_list,
    STRING_AGG(distinct supervision_condition_id, ',' ORDER BY supervision_condition_id) as supervision_condition_id_combined,
    STRING_AGG(distinct special_condition_id, ',' ORDER BY special_condition_id) as special_condition_ids
  from (
    select distinct
        sp.offender_booking_id,
        {PERIOD_COLUMNS},
        {MOVEMENT_COLUMNS},
        {SUPERVISION_LEVEL_COLUMNS},
        legal.legal_order_id,
        legal.order_type_id,
        supervision_condition_id,
        cond.special_condition_id
    from spans_movements_supervision sp
        left join legal_orders legal on (
            sp.offender_booking_id = legal.offender_booking_id 
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
            sp.offender_booking_id = cond.offender_booking_id
            and legal.legal_order_id = cond.legal_order_id
            and (date(cond.closing_date) >= sp.period_end or (date(cond.closing_date)) is null)
            )
  ) sub
  group by     
    offender_booking_id,
    {PERIOD_COLUMNS},
    {MOVEMENT_COLUMNS},
    {SUPERVISION_LEVEL_COLUMNS}
),

-- Join on supervision officer assignment periods as well as offender number from offender_number_xwalk
-- Note: if multiple supervision officers join on to the same period, take the officer that was last assigned accordingto their sequence_number

spans_all_combos as (
  select *
  from (
    select 
      offender_number,
      sp.offender_booking_id,
      {PERIOD_COLUMNS},
      {MOVEMENT_COLUMNS},
      {SUPERVISION_LEVEL_COLUMNS},
      {LEGAL_ORDER_COLUMNS},
      {EMPLOYEE_COLUMNS},
      RANK() OVER(PARTITION BY sp.offender_booking_id, period_start, period_end ORDER BY sequence_number) as booking_rnk
    from spans_supervision_legal sp
      left join offender_booking_assignment emp on (sp.offender_booking_id = emp.offender_booking_id 
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
      left join offender_number_xwalk xwalk on sp.offender_booking_id = xwalk.offender_booking_id

  ) sub
  where booking_rnk = 1
)

-- select final data, including adding columns for a period_id as well as next and previous external movement ids, 
-- and filtering down to only rows where we have information from the movements table about a supervision related movement

select 
    offender_number,
    offender_booking_id,
    ROW_NUMBER() OVER (PARTITION BY offender_number, offender_booking_id ORDER BY period_start) as period_id,
    {PERIOD_COLUMNS},
    {MOVEMENT_COLUMNS},
    LEAD(offender_external_movement_id) OVER(PARTITION BY offender_number, offender_booking_id ORDER BY period_start) as next_offender_external_movement_id,
    LAG(offender_external_movement_id) OVER(PARTITION BY offender_number, offender_booking_id ORDER BY period_start) as prev_offender_external_movement_id,
    supervision_level_id,
    order_type_id_list,
    special_condition_ids,
    employee_id,
    first_name,
    middle_name,
    last_name,
    name_suffix,
    employee_county
from spans_all_combos
where movement_reason_id is not null
and offender_number is not null;
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_mi",
    ingest_view_name="supervision_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="offender_number, offender_booking_id, period_start, period_end",
    materialize_raw_data_table_views=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
