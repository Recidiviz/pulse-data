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
"""Query containing MDOC incarceration period information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PARTITION_STATEMENT = "OVER(PARTITION BY offender_id ORDER BY movement_date, offender_lock_id, offender_external_movement_id, status)"

OFFENDER_IDS_TO_KEEP = "(select distinct offender_id from {ADH_OFFENDER_BOOKING})"

LOCK_RECORDS_CTE = """
-- Find lock records that indicate that a person is in a certain unit in a facility
-- for a certain time span. For our purposes, we only care about lock records that are
-- permanent.
max_date_outs AS (
    -- There are a non-trivial amount of lock records in which we have duplicate records
    -- where the date_out is different. The first instance of the date_out always matches
    -- the date_in, the second instance indicates the actual time the person left the unit.
    -- Therefore we take the maximum. We treat null date_outs as maximum values because this
    -- indicates that the person is still occupying the unit.
    SELECT
        offender_id,
        location_id,
        unit_lock_id,
        date_in,
        NULLIF(MAX(COALESCE(date_out,'9999-12-31')) OVER lock_window,'9999-12-31') AS date_out
    FROM {ADH_OFFENDER_LOCK}
    WHERE permanent_temporary_flag = '1'
    WINDOW lock_window AS (PARTITION BY offender_id, location_id, unit_lock_id, date_in)
),
deduped_lock_records AS (
    -- After the above deduplication, there are then cases where two deduped lock records now have the same date out and the same unit_lock_id but different date ins
    -- In these cases take the one with the earliest date in
    select *
    from (
        select  *,
            ROW_NUMBER() OVER(PARTITION BY offender_id, location_id, unit_lock_id, date_out ORDER BY date_in ASC) as n
        from max_date_outs 
    ) sub1
    where n=1
),
final_lock_records AS (
    -- Join the deduped records back to the full set in order to obtain the offender_lock_id
    -- which is the primary key.
    SELECT
        o.offender_lock_id,
        o.offender_id,
        o.location_id,
        o.unit_lock_id,
        CAST(o.date_in as DATETIME) AS date_in,
        CAST(o.date_out as DATETIME) AS date_out,
        CAST(o.last_update_date as DATETIME) AS last_update_date
    FROM {ADH_OFFENDER_LOCK} o
    JOIN deduped_lock_records d
    ON d.offender_id = o.offender_id
    AND d.location_id = o.location_id
    AND d.unit_lock_id = o.unit_lock_id
    AND d.date_in = o.date_in
    AND COALESCE(d.date_out,'9999-12-31') = COALESCE(o.date_out, '9999-12-31')
    WHERE o.permanent_temporary_flag = '1'
),
internal_movements AS (
    -- Obtain all of the metadata for the lock records, including location information.
    SELECT
        o.offender_id,
        o.offender_lock_id,
        o.unit_lock_id,
        o.last_update_date,
        rs.reporting_station_id,
        rs.name AS reporting_station_name,
        l.location_id,
        l.location_code,
        l.name,
        l.location_type_id,
        r1.unique_code,
        r3.description AS county,
        o.date_in,
        COALESCE((CAST(o.date_out as DATETIME)), CAST( DATE(9999,9,9) as DATETIME)) as date_out,
        u.cell_type_id,
        u.security_level_id,
        u.cell_lock,
        wing.name as wing_name
    FROM final_lock_records o
	JOIN {ADH_OFFENDER} o1	
    ON o.offender_id = o1.offender_id
    JOIN {ADH_LOCATION} l
    ON o.location_id = l.location_id
    JOIN {ADH_REFERENCE_CODE} r1
    ON l.location_type_id = r1.reference_code_id
    JOIN {ADH_REFERENCE_CODE} r3
    ON l.county_id = r3.reference_code_id
    JOIN {ADH_UNIT_LOCK} u
    ON u.unit_lock_id = o.unit_lock_id
    LEFT JOIN {ADH_REPORTING_STATION} rs
    ON u.reporting_station_id = rs.reporting_station_id
    LEFT JOIN {ADH_LOCATION_WING} wing
    ON u.location_wing_id = wing.location_wing_id
),"""

MOVEMENTS_CTE = """
deduped_movement_records AS (
    -- For a given offender_booking_id, which indicates a person on a certain jurisdiction
    -- with MDOC, we see duplicates on the same day of movements that go from a source
    -- to a destination, particularly in transfers between facilities. So we pick the 
    -- max offender_external_movement_id to stabilize an ID.
    SELECT
        offender_booking_id,
        source_location_id,
        destination_location_id,
        MAX(CAST(offender_external_movement_id AS INT64)) AS offender_external_movement_id
    FROM {ADH_OFFENDER_EXTERNAL_MOVEMENT} o
    GROUP BY
        offender_booking_id,
        source_location_id,
        destination_location_id,
        (DATE(movement_date))
),
final_movement_records AS (
    -- We join back to the original table in order to obtain the movement reason id
    -- for this given movement.
    SELECT
        o.offender_booking_id,
        o.source_location_id,
        o.destination_location_id,
        o.offender_external_movement_id,
        o.movement_reason_id,
        o.movement_date
    FROM deduped_movement_records
    JOIN {ADH_OFFENDER_EXTERNAL_MOVEMENT} o
    ON CAST(o.offender_external_movement_id AS INT64) = deduped_movement_records.offender_external_movement_id
),
final_movements AS (
    -- Obtain all of the needed metadata for the movements, including the location info
    -- for both the source and destination.
    SELECT
        o.offender_id,
        e.offender_external_movement_id,
        m.movement_reason_id,
        CAST(e.movement_date as DATETIME) AS movement_date,
        lb.location_code AS destination_location_code,
        lb.name AS destination_location_name,
        lb.location_type_id AS destination_location_type_id,
        rc2.description AS destination_location_county,
        COALESCE(
            LEAD(CAST(e.movement_date as DATETIME)) 
            OVER(partition by o.offender_id ORDER BY e.movement_date, e.offender_external_movement_id), 
            CAST( DATE(9999,9,9) as DATETIME)
            ) as next_movement_date,
        LEAD(m.movement_reason_id)
            OVER(partition by o.offender_id ORDER BY e.movement_date, e.offender_external_movement_id) 
            as next_movement_reason_id,
        CAST(m.last_update_date as DATETIME) AS last_update_date
    FROM final_movement_records e
    JOIN {ADH_OFFENDER_EXTERNAL_MOVEMENT} m
    ON e.offender_external_movement_id = m.offender_external_movement_id
    JOIN {ADH_OFFENDER_BOOKING} b
    ON e.offender_booking_id = b.offender_booking_id
    JOIN {ADH_OFFENDER} o
    ON b.offender_id = o.offender_id
    JOIN {ADH_LOCATION} lb
    ON m.destination_location_id = lb.location_id
    JOIN {ADH_REFERENCE_CODE} rb
    ON lb.location_type_id = rb.reference_code_id
    LEFT JOIN {ADH_REFERENCE_CODE} rc2
    ON lb.county_id = rc2.reference_code_id
    -- Omitting the 2 (Intake) and 131 (Sentenced to Prison) because it's rarely used nowadays and when it is, it's duplicate with the New Commitment movements
    WHERE m.movement_reason_id NOT IN ('2', '131')
),"""

# In MI, individuals are designated into different types of segregation based on a committee's decision.
# We'll pull in each person's designation to use as part of determining housing unit type.
AD_SEG_CTE = """
ad_seg_designation as (
    select 
        offender_designation_id, 
        offender_id,
        offender_designation_code_id,
        CAST(start_date as DATETIME) as start_date, 
        COALESCE(CAST(end_date as DATETIME), CAST( DATE(9999,9,9) as DATETIME)) as end_date,
        CAST(last_update_date as DATETIME) as last_update_date
    from {ADH_OFFENDER_DESIGNATION}
    -- ignore designations related to PREA, holding cell, or writ
    where offender_designation_code_id not in ('13214', '11388', '16134', '16181', '16133', '16132', '16131', '16130')
),
"""


VIEW_QUERY_TEMPLATE = f"""
WITH
{LOCK_RECORDS_CTE}
{MOVEMENTS_CTE}
{AD_SEG_CTE}
all_dates as (
  select offender_id, CAST(date_in as DATETIME) as period_date from internal_movements
  union all  
  select offender_id, CAST(date_out as DATETIME) as period_date from internal_movements
  union all 
  select offender_id, CAST(movement_date as DATETIME) as period_date from final_movements
  union all 
  select offender_id, CAST(next_movement_date as DATETIME) as period_date from final_movements
  union all
  select offender_id, CAST(start_date as DATETIME) as period_date from ad_seg_designation
  union all 
  select offender_id, CAST(end_date as DATETIME) as period_date from ad_seg_designation
),

periods_basic as (
  select
    offender_id,
    period_date as start_date,
    lead(period_date) over(partition by offender_id order by period_date) as end_date
  from (select distinct * from all_dates) sub
),

periods_with_info as (
  select
    distinct
    basic.offender_id,
    basic.start_date,
    move.offender_external_movement_id,
    CASE WHEN basic.end_date = CAST( DATE(9999,9,9) as DATETIME) THEN NULL else basic.end_date end as end_date,
    CASE WHEN move.movement_date = basic.start_date then move.movement_reason_id else NULL end as movement_reason_id,
    CASE WHEN move.next_movement_date = basic.end_date then move.next_movement_reason_id else NULL end as next_movement_reason_id,
    internal.offender_lock_id,
    internal.cell_lock,
    internal.reporting_station_name,
    -- Sometimes we see movement information before unit information, so in those cases we need the location type of the movement destination
    -- Otherwise we want to use the location type from the unit information
    COALESCE(internal.location_type_id, destination_location_type_id) as location_type_id,
    internal.county,
    internal.location_code,
    internal.cell_type_id,
    internal.security_level_id,
    internal.wing_name,
    ad_seg.offender_designation_id,
    ad_seg.offender_designation_code_id,
    ROW_NUMBER() 
        OVER(PARTITION BY basic.offender_id, basic.start_date, basic.end_date 
             ORDER BY internal.last_update_date desc,
                      ad_seg.last_update_date desc,
                      move.last_update_date desc,
                      internal.offender_lock_id desc,
                      ad_seg.offender_designation_id desc,
                      move.offender_external_movement_id desc) as rnk
  from periods_basic basic
  left join final_movements move 
    on basic.offender_id = move.offender_id and
       move.movement_date <= basic.start_date and basic.end_date <= move.next_movement_date
  left join internal_movements internal
    on basic.offender_id = internal.offender_id and
       internal.date_in <= basic.start_date and basic.end_date <= internal.date_out
  left join ad_seg_designation ad_seg
    on basic.offender_id = ad_seg.offender_id and
       ad_seg.start_date <= basic.start_date and basic.end_date <= ad_seg.end_date  
  inner join (select distinct offender_id from {OFFENDER_IDS_TO_KEEP} sub) book on basic.offender_id = book.offender_id
  where basic.start_date is not null and basic.start_date <> CAST( DATE(9999,9,9) as DATETIME)
        and destination_location_type_id in ('225', '226', '14294')
)

select
    offender_id,
    start_date,
    end_date,
    movement_reason_id,
    next_movement_reason_id,
    cell_lock,
    reporting_station_name,
    location_code,
    location_type_id,
    county,
    cell_type_id,
    security_level_id,
    wing_name,
    ROW_NUMBER() OVER (PARTITION BY offender_id 
                       ORDER BY start_date, 
                                end_date NULLS LAST, 
                                offender_lock_id, offender_external_movement_id) as period_id,
    offender_designation_code_id
from periods_with_info
where rnk=1

"""

# TODO(#13970) Add ingest view tests.
VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mi",
    ingest_view_name="incarceration_periods_v3",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
