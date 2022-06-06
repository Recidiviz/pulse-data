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

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PARTITION_STATEMENT = "OVER(PARTITION BY offender_number ORDER BY movement_date, offender_lock_id, offender_external_movement_id, status)"

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
    -- There are a non-trivial amount of duplication of lock records. In order to have a 
    -- stable ID, we take the maximum unit lock id out of the date_in, date_out combinations.
    SELECT
        o.offender_id,
        o.location_id,
        o.date_in,
        o.date_out,
        MAX(o.unit_lock_id) AS unit_lock_id
    FROM {ADH_OFFENDER_LOCK} o
    JOIN max_date_outs
    ON max_date_outs.offender_id = o.offender_id
    AND max_date_outs.location_id = o.location_id
    AND max_date_outs.date_in = o.date_in
    AND COALESCE(max_date_outs.date_out,'9999-12-31') = COALESCE(o.date_out, '9999-12-31')
    WHERE o.permanent_temporary_flag = '1'
    GROUP BY
        offender_id,
        location_id,
        date_in,
        date_out
),
final_lock_records AS (
    -- Join the deduped records back to the full set in order to obtain the offender_lock_id
    -- which is the primary key.
    SELECT
        o.offender_lock_id,
        o.offender_id,
        o.location_id,
        o.unit_lock_id,
        DATE(o.date_in) AS date_in,
        DATE(o.date_out) AS date_out
    FROM {ADH_OFFENDER_LOCK} o
    JOIN deduped_lock_records
    ON deduped_lock_records.offender_id = o.offender_id
    AND deduped_lock_records.location_id = o.location_id
    AND deduped_lock_records.unit_lock_id = o.unit_lock_id
    AND COALESCE(deduped_lock_records.date_out,'9999-12-31') = COALESCE(o.date_out, '9999-12-31')
    WHERE o.permanent_temporary_flag = '1'
),
internal_movements AS (
    -- Obtain all of the metadata for the lock records, including location information.
    SELECT
        o.offender_id,
        o1.offender_number,
        o.offender_lock_id,
        o.unit_lock_id,
        l.location_id,
        l.location_code,
        l.name,
        l.location_type_id,
        r1.unique_code,
        r3.description AS county,
        o.date_in,
        o.date_out
    FROM final_lock_records o
    JOIN {ADH_OFFENDER} o1
    ON o.offender_id = o1.offender_id
    JOIN {ADH_LOCATION} l
    ON o.location_id = l.location_id
    JOIN {ADH_REFERENCE_CODE} r1
    ON l.location_type_id = r1.reference_code_id
    JOIN {ADH_REFERENCE_CODE} r3
    ON l.county_id = r3.reference_code_id
),"""

MOVEMENTS_CTE = """
-- Find external movements that indicate a person is going in and out of MDOC facilities.
deduped_movement_records AS (
    -- For a given offender_booking_id, which indicates a person on a certain jurisdiction
    -- with MDOC, we see duplicates on the same day of movements that go from a source
    -- to a destination, particularly in transfers between facilities. So we pick the 
    -- max offender_external_movement_id to stabilize an ID.
    SELECT
        offender_booking_id,
        source_location_id,
        destination_location_id,
        MAX(offender_external_movement_id) AS offender_external_movement_id
    FROM {ADH_OFFENDER_EXTERNAL_MOVEMENT} o
    GROUP BY
        offender_booking_id,
        source_location_id,
        destination_location_id,
        DATE(movement_date)
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
    USING (offender_external_movement_id)
),
final_movements AS (
    -- Obtain all of the needed metadata for the movements, including the location info
    -- for both the source and destination.
    SELECT
        o.offender_id,
        o.offender_number,
        e.offender_external_movement_id,
        m.movement_reason_id,
        mr.description AS movement_description,
        DATE(e.movement_date) AS movement_date,
        m.source_location_id,
        la.location_code AS source_location_code,
        la.name AS source_location_name,
        la.location_type_id AS source_location_type_id,
        ra.unique_code AS source_location_unique_code,
        rc1.description AS source_location_county,
        m.destination_location_id,
        lb.location_code AS destination_location_code,
        lb.name AS destination_location_name,
        lb.location_type_id AS destination_location_type_id,
        rb.unique_code AS destination_location_unique_code,
        rc2.description AS destination_location_county
    FROM final_movement_records e
    JOIN {ADH_OFFENDER_EXTERNAL_MOVEMENT} m
    ON e.offender_external_movement_id = m.offender_external_movement_id
    JOIN {ADH_OFFENDER_BOOKING} b
    ON e.offender_booking_id = b.offender_booking_id
    JOIN {ADH_OFFENDER} o
    ON b.offender_id = o.offender_id
    JOIN {ADH_LOCATION} la
    ON m.source_location_id = la.location_id
    JOIN {ADH_REFERENCE_CODE} ra
    ON la.location_type_id = ra.reference_code_id
    JOIN {ADH_LOCATION} lb
    ON m.destination_location_id = lb.location_id
    JOIN {ADH_REFERENCE_CODE} rb
    ON lb.location_type_id = rb.reference_code_id
    JOIN {ADH_MOVEMENT_REASON} mr
    ON mr.movement_reason_id = m.movement_reason_id
    LEFT JOIN {ADH_REFERENCE_CODE} rc1
    ON la.county_id = rc1.reference_code_id
    LEFT JOIN {ADH_REFERENCE_CODE} rc2
    ON lb.county_id = rc2.reference_code_id
    ORDER BY
        o.offender_number,
        e.offender_external_movement_id,
        DATE(begin_date),
        movement_date,
        DATE(end_date)
),"""

VIEW_QUERY_TEMPLATE = f"""
WITH
{LOCK_RECORDS_CTE}
{MOVEMENTS_CTE}
external_movements_in AS (
    -- By joining internal movements (lock spans) with movements of the day,
    -- where the destination is the same as the lock span's location, we proxy
    -- the exact reason that causes a person to move into a facility.
    -- These records may have a slight difference in date entered for both, so we have
    -- to match by proxy date difference being as close as possible.
    SELECT
        i.offender_lock_id,
        i.offender_id,
        i.offender_number,
        i.unit_lock_id,
        i.location_id,
        i.location_code,
        i.name,
        i.location_type_id,
        i.unique_code,
        i.county,
        i.date_in AS movement_date,
        e.offender_external_movement_id,
        e.movement_reason_id,
        e.movement_description,
        e.destination_location_id,
        e.destination_location_code,
        e.destination_location_name,
        e.destination_location_type_id,
        e.destination_location_unique_code,
        e.destination_location_county,
        'IN' AS status
    FROM internal_movements i
    JOIN final_movements e
    ON i.offender_id = e.offender_id
    AND i.location_id = e.destination_location_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY 
        i.offender_id, e.offender_external_movement_id, i.location_id 
        ORDER BY ABS(DATE_DIFF(i.date_in, e.movement_date, DAY)) ASC) = 1
),
external_movements_out AS (
    -- By joining internal movements (lock spans) with movements of the day,
    -- where the source location is the same as the lock span's location,
    -- we proxy the exact reason that causes a person to move out of a facility.
    -- These records may have a slight difference in date entered for both, so we have
    -- to match by proxy date difference being as close as possible.
    -- This CTE does not include lock spans in which the end date is null, given that 
    -- that indicates a person is still occupying that unit.
    SELECT 
        i.offender_lock_id,
        i.offender_id,
        i.offender_number,
        i.unit_lock_id,
        i.location_id,
        i.location_code,
        i.name,
        i.location_type_id,
        i.unique_code,
        i.county,
        i.date_out AS movement_date,
        e.offender_external_movement_id,
        e.movement_reason_id,
        e.movement_description,
        e.destination_location_id,
        e.destination_location_code,
        e.destination_location_name,
        e.destination_location_type_id,
        e.destination_location_unique_code,
        e.destination_location_county,
        'OUT' AS status
    FROM internal_movements i
    JOIN final_movements e
    ON i.offender_id = e.offender_id
    AND i.date_out IS NOT NULL
    AND i.location_id = e.source_location_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY 
        i.offender_id, e.offender_external_movement_id, i.location_id
        ORDER BY ABS(DATE_DIFF(i.date_out, e.movement_date, DAY)) ASC) = 1
),
internal_movements_without_external_movements_in AS (
    -- A person may transfer units within facilities, therefore, these movements would
    -- not have an external movement associated with them in terms of moving into the 
    -- new unit.
    SELECT 
        i.offender_lock_id,
        i.offender_id,
        i.offender_number,
        i.unit_lock_id,
        i.location_id,
        i.location_code,
        i.name,
        i.location_type_id,
        i.unique_code,
        i.county,
        i.date_in AS movement_date,
        e1.offender_external_movement_id,
        e1.movement_reason_id,
        e1.movement_description,
        e1.destination_location_id,
        e1.destination_location_code,
        e1.destination_location_name,
        e1.destination_location_type_id,
        e1.destination_location_unique_code,
        e1.destination_location_county,
        'IN' AS status
    FROM internal_movements i
    LEFT JOIN external_movements_in e1
    ON i.offender_lock_id = e1.offender_lock_id
    WHERE e1.offender_lock_id IS NULL
),
internal_movements_without_external_movements_out AS (
    -- A person may transfer units within facilities, therefore, this would not have an
    -- external movement associated with them in terms of moving out from the old unit. 
    -- This CTE does not include lock spans in which the end date is null, given that 
    -- that indicates a person is still occupying that unit.
    SELECT 
        i.offender_lock_id,
        i.offender_id,
        i.offender_number,
        i.unit_lock_id,
        i.location_id,
        i.location_code,
        i.name,
        i.location_type_id,
        i.unique_code,
        i.county,
        i.date_out AS movement_date,
        e2.offender_external_movement_id,
        e2.movement_reason_id,
        e2.movement_description,
        e2.destination_location_id,
        e2.destination_location_code,
        e2.destination_location_name,
        e2.destination_location_type_id,
        e2.destination_location_unique_code,
        e2.destination_location_county,
        'OUT' AS status
    FROM internal_movements i
    LEFT JOIN external_movements_out e2
    ON i.offender_lock_id = e2.offender_lock_id
    WHERE e2.offender_lock_id IS NULL
    AND i.date_out IS NOT NULL
),
external_movements_without_internal_movements_out AS (
    -- In a nontrivial amount of cases, lock units were entered with a date_out that is
    -- prior to the date_in, or do not exist at all for someone, but an external movement
    -- exists where the person is now no longer in a facility in MDOC. Therefore, we
    -- need to look for dangling external movements without associated internal movements
    -- that indicate a person has left a facility.
    SELECT
        i.offender_lock_id,
        e.offender_id,
        e.offender_number,
        i.unit_lock_id,
        e.source_location_id AS location_id,
        e.source_location_code AS location_code,
        e.source_location_name AS name,
        e.source_location_type_id AS location_type_id,
        e.source_location_unique_code AS unique_code,
        e.source_location_county AS county,
        e.movement_date,
        e.offender_external_movement_id,
        e.movement_reason_id,
        e.movement_description,
        e.destination_location_id,
        e.destination_location_code,
        e.destination_location_name,
        e.destination_location_type_id,
        e.destination_location_unique_code,
        e.destination_location_county,
        'OUT' AS status
    FROM final_movements e
    LEFT JOIN external_movements_out i
    ON i.offender_external_movement_id = e.offender_external_movement_id
    -- Filter where the destination is not State Prison, Residential Reentry Program
    WHERE e.destination_location_type_id NOT IN ('225', '14294')
    AND i.offender_external_movement_id IS NULL
),
edges AS (
    SELECT * FROM external_movements_in
    UNION ALL
    SELECT * FROM external_movements_out
    UNION ALL
    SELECT * FROM internal_movements_without_external_movements_in
    UNION ALL
    SELECT * FROM internal_movements_without_external_movements_out
    UNION ALL
    SELECT * FROM external_movements_without_internal_movements_out
    ORDER BY offender_number, movement_date, offender_lock_id, offender_external_movement_id, status
),
periods AS (
    SELECT
        offender_number,
        offender_lock_id,
        offender_external_movement_id,
        status,
        movement_date,
        unit_lock_id,
        location_id,
        location_code,
        name,
        location_type_id,
        unique_code,
        county,
        movement_reason_id,
        movement_description,
        destination_location_id,
        destination_location_name,
        destination_location_type_id,
        destination_location_unique_code,
        destination_location_county,
        LEAD(offender_lock_id) {PARTITION_STATEMENT} as next_offender_lock_id,
        LEAD(offender_external_movement_id) {PARTITION_STATEMENT} AS next_offender_external_movement_id,
        LEAD(status) {PARTITION_STATEMENT} AS next_status,
        LEAD(movement_date) {PARTITION_STATEMENT} AS next_movement_date,
        LEAD(unit_lock_id) {PARTITION_STATEMENT} AS next_unit_lock_id,
        LEAD(location_id) {PARTITION_STATEMENT} AS next_location_id,
        LEAD(location_code) {PARTITION_STATEMENT} AS next_location_code,
        LEAD(name) {PARTITION_STATEMENT} AS next_name,
        LEAD(location_type_id) {PARTITION_STATEMENT} AS next_location_type_id,
        LEAD(unique_code) {PARTITION_STATEMENT} AS next_unique_code,
        LEAD(county) {PARTITION_STATEMENT} AS next_county,
        LEAD(movement_reason_id) {PARTITION_STATEMENT} AS next_movement_reason_id,
        LEAD(movement_description) {PARTITION_STATEMENT} AS next_movement_description,
        LEAD(destination_location_id) {PARTITION_STATEMENT} AS next_destination_location_id,
        LEAD(destination_location_name) {PARTITION_STATEMENT} AS next_destination_location_name,
        LEAD(destination_location_type_id) {PARTITION_STATEMENT} AS next_destination_location_type_id,
        LEAD(destination_location_unique_code) {PARTITION_STATEMENT} AS next_destination_location_unique_code,
        LEAD(destination_location_county) {PARTITION_STATEMENT} AS next_destination_location_county
    FROM edges
)
SELECT
  offender_number,
  offender_lock_id,
  offender_external_movement_id,
  movement_date,
  unit_lock_id,
  location_code,
  location_type_id,
  county,
  movement_reason_id,
  next_movement_date,
  next_movement_reason_id
FROM periods
WHERE status = 'IN' AND (next_status = 'OUT' OR next_status IS NULL)
"""

# TODO(#12834) Add ingest view tests.
VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_mi",
    ingest_view_name="incarceration_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
    materialize_raw_data_table_views=True,
    order_by_cols="offender_number, movement_date, next_movement_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
