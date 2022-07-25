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
"""Query containing external movements information."""

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH
movements_with_direction as (
    SELECT
        *,
        # CO has movements below 40 considered in and above as out, but based on how they count their population (ex: 
        # someone escaped counts towards population/facility) accuracy is improved by increasing IN movements.
        CASE EXTERNALMOVEMENTCODE
            WHEN '01' THEN 'IN' # New CO Commitment
            WHEN '03' THEN 'IN' # YOS Commitment
            WHEN '06' THEN 'IN' # Recommitment
            WHEN '07' THEN 'IN' # ICC Commitment
            WHEN '10' THEN 'IN' # Returned from Parole
            WHEN '13' THEN 'IN' # Terminated from ISP-I
            WHEN '20' THEN 'IN' # Returned from Escape
            WHEN '21' THEN 'IN' # Returned from Court
            WHEN '22' THEN 'IN' # Returned from Escorted Leave
            WHEN '23' THEN 'IN' # Returned from Hospital
            WHEN '24' THEN 'IN' # Returned from Bond
            WHEN '25' THEN 'IN' # Returned from ICC
            WHEN '26' THEN 'IN' # Returned from Furlough
            WHEN '2B' THEN 'IN' # Returned from Work Crew
            WHEN '2C' THEN 'IN' # Returned from AWOL
            WHEN '2D' THEN 'IN' # Returned from Other Jurisdiction
            WHEN '30' THEN 'IN' # Received at DOC Facility
            WHEN '31' THEN 'IN' # Received at Community Center
            WHEN '32' THEN 'IN' # Received at County/City Jail
            WHEN '33' THEN 'IN' # Escapee In Custody - Other Jurisdiction
            WHEN '36' THEN 'IN' # Offender Situation Changed
            WHEN '37' THEN 'IN' # Halfway-In (at CCC)
            WHEN '39' THEN 'IN' # Day Trip (to)
            WHEN '3A' THEN 'IN' # Reassigned to another Facility
            WHEN '40' THEN 'OUT' # Discharge
            WHEN '50' THEN 'OUT' # Commutation 
            WHEN '52' THEN 'OUT' # Released by Court
            WHEN '55' THEN 'OUT' # Released after Erroneous Admission
            WHEN '56' THEN 'IN' # Unauthorized Release
            WHEN '60' THEN 'OUT' # Paroled
            WHEN '63' THEN 'IN' # TO Intensive Supervision (ISP)
            WHEN '74' THEN 'OUT' # Transferred Interstate
            WHEN '75' THEN 'OUT' # Death
            WHEN '78' THEN 'OUT' # Returned to Parole 
            WHEN '79' THEN 'IN' # Returned to ISP-I
            WHEN '80' THEN 'IN' # Escaped
            WHEN '81' THEN 'IN' # Out To Court
            WHEN '82' THEN 'IN' # Out on Escorted Leave
            WHEN '83' THEN 'IN' # Out to Hospital
            WHEN '84' THEN 'OUT' # Out on Bond
            WHEN '85' THEN 'OUT' # Transferred Out-of-State (ICC)
            WHEN '86' THEN 'IN' # Out on Furlough
            WHEN '88' THEN 'OUT' # Absconded from Parole 
            WHEN '8B' THEN 'IN' # Out on Work Crew
            WHEN '8C' THEN 'IN' # Absent without Leave (AWOL)
            WHEN '8D' THEN 'OUT' # Out to Other Jurisdiction
            WHEN '8E' THEN 'IN' # AWOL from ISP-I
            WHEN '90' THEN 'OUT' # Transferred to DOC Facility
            WHEN '91' THEN 'OUT' # Transferred to Community Center
            WHEN '92' THEN 'OUT' # Transferred to County/City Jail
        END as direction
    FROM {eomis_externalmovement} movement
), movements as (
    SELECT
        OFFENDERID,
        direction,
        CONCAT(EXTERNALMOVEMENTDATE, 'T', EXTERNALMOVEMENTTIME) as movement_datetime,
        LOCATIONREPORTMOVEMENT,
        EXTERNALMOVEMENTCODE,
        OTHERLOCATIONCODE,
        REASONFORMOVEMENT,
        JURISDICTIONALAGENCY,
        IF(area.PARTYID IS NOT NULL, area.ORGCOMMONID, org.ORGCOMMONID) as facility_id,
        IF(area.PARTYID IS NOT NULL, area.ORGANIZATIONNAME, org.ORGANIZATIONNAME) as facility_name,
        IF(area.PARTYID IS NOT NULL, area.ORGANIZATIONTYPE, org.ORGANIZATIONTYPE) as facility_type,
        IF(area.PARTYID is not NULL, org.ORGCOMMONID, NULL) as unit_id,
        IF(area.PARTYID is not NULL, org.ORGANIZATIONNAME, NULL) as unit_name,
        IF(area.PARTYID is not NULL, org.ORGANIZATIONTYPE, NULL) as unit_type,
    FROM movements_with_direction movement
    LEFT JOIN {eomis_organizationprof} org on (
        org.PARTYID = IF(direction = 'IN' AND EXTERNALMOVEMENTCODE NOT IN ('63','8C'), movement.LOCATIONREPORTMOVEMENT, movement.OTHERLOCATIONCODE)
    )
    LEFT JOIN {eomis_organizationprof} area on (
        -- TODO(#12372): is there a better way to tell whether the location was initially a unit?
        -- Logic here is if the area is already the same then don't join again for duplicate information but otherwise join if partyid = orgareacode
        org.PARTYID != org.ORGAREACODE AND
        area.PARTYID = org.ORGAREACODE
    )
), isp as (
SELECT OFFENDERID, direction, movement_datetime, LOCATIONREPORTMOVEMENT, OTHERLOCATIONCODE, REASONFORMOVEMENT, JURISDICTIONALAGENCY, facility_id, facility_name, facility_type, unit_id, unit_name, unit_type,
    CASE 
        WHEN EXTERNALMOVEMENTCODE = '63' # To Intensive Supervision Program (ISP)
        AND (REASONFORMOVEMENT = 'PG' # ISP - Inmate
        OR (REASONFORMOVEMENT = '99' # Not Specified
            AND unit_type IN ('B0', 'BK') # Incarceration Facilities
        )) THEN '63-I'
        WHEN EXTERNALMOVEMENTCODE = '63' # To Intensive Supervision Program (ISP)
        THEN '63-P' # Mark any ISP that doesn't match the conditions above for ISP-I as ISP-P instead
        ELSE EXTERNALMOVEMENTCODE
    END AS EXTERNALMOVEMENTCODE
    FROM movements
),

classified_movements as (
  SELECT *,
  CASE 
    WHEN unit_name LIKE '%Intake%' THEN 'TEMPORARY'
    WHEN EXTERNALMOVEMENTCODE IN ('36','39','81','82','83','84','85','86','88','8B','8D', '8E') THEN 'TEMPORARY'
    WHEN unit_type LIKE 'D%' THEN 'TEMPORARY'
    WHEN unit_type IS NULL THEN 'TEMPORARY'
    WHEN facility_type LIKE 'D%' THEN 'TEMPORARY' 
    WHEN EXTERNALMOVEMENTCODE IN ('01', '03', '06', '07', '10') THEN 'PERMANENT' #So location is never taken from previous incarceration
    ELSE 'PERMANENT'
    # Offender Situation Changed
    # Day Trip (to)
    # Out To Court
    # Out on Escorted Leave
    # Out to Hospital
    # Out on Bond
    # Transferred Out-of-State (ICC)
    # Out on Furlough
    # Absconded from Parole 
    # Out on Work Crew
    # Out to Other Jurisdiction
    # AWOL from ISP-I
  END as torpmovement
FROM isp
), ordered_movements as (
    select
        *,
        ROW_NUMBER() OVER (PARTITION BY OFFENDERID ORDER BY movement_datetime) as move_seq
    from classified_movements
), periods as (
    SELECT
        IF(arrival.move_seq IS NOT NULL, arrival.OFFENDERID, departure.OFFENDERID) as OFFENDERID,
        IF(arrival.move_seq IS NOT NULL, arrival.move_seq, departure.move_seq) as move_seq,
        IF(arrival.move_seq IS NOT NULL, arrival.movement_datetime, departure.movement_datetime) as start_datetime,
        IF(arrival.move_seq IS NOT NULL, arrival.EXTERNALMOVEMENTCODE, departure.EXTERNALMOVEMENTCODE) as start_code, 
        IF(arrival.move_seq IS NOT NULL, arrival.REASONFORMOVEMENT, departure.REASONFORMOVEMENT) as start_reason, 
        IF(arrival.move_seq IS NOT NULL, departure.movement_datetime, NULL) as end_datetime, 
        IF(arrival.move_seq IS NOT NULL, departure.EXTERNALMOVEMENTCODE, NULL) as end_code, 
        IF(arrival.move_seq IS NOT NULL, departure.REASONFORMOVEMENT, NULL) as end_reason,
        IF(arrival.move_seq IS NOT NULL, arrival.JURISDICTIONALAGENCY, departure.JURISDICTIONALAGENCY) as JURISDICTIONALAGENCY,
        IF(arrival.move_seq IS NOT NULL, arrival.facility_id, departure.facility_id) as facility_id,
        IF(arrival.move_seq IS NOT NULL, arrival.facility_name, departure.facility_name) as facility_name,
        IF(arrival.move_seq IS NOT NULL, arrival.facility_type, departure.facility_type) as facility_type,
        IF(arrival.move_seq IS NOT NULL, arrival.unit_id, departure.unit_id) as unit_id,
        IF(arrival.move_seq IS NOT NULL, arrival.unit_name, departure.unit_name) as unit_name,
        IF(arrival.move_seq IS NOT NULL, arrival.unit_type, departure.unit_type) as unit_type,
        IF(arrival.move_seq IS NOT NULL, arrival.LOCATIONREPORTMOVEMENT, departure.LOCATIONREPORTMOVEMENT) AS LOCATIONREPORTMOVEMENT,
        IF(arrival.move_seq IS NOT NULL, arrival.OTHERLOCATIONCODE, departure.OTHERLOCATIONCODE) AS OTHERLOCATIONCODE,
        IF(arrival.move_seq IS NOT NULL, arrival.torpmovement, departure.torpmovement) as temporaryorpermanent
    FROM (
        SELECT *
        FROM ordered_movements
        WHERE direction = 'IN'
    ) arrival
    FULL OUTER JOIN (
        SELECT *
        FROM ordered_movements
        WHERE direction = 'OUT'
    ) departure
    ON arrival.OFFENDERID = departure.OFFENDERID
    AND arrival.move_seq = (departure.move_seq - 1)
    AND arrival.LOCATIONREPORTMOVEMENT = departure.LOCATIONREPORTMOVEMENT

), permanent_moves AS (
    SELECT *, 
    IF(temporaryorpermanent='PERMANENT',unit_name, null) as p_unit_name,
    IF(temporaryorpermanent='PERMANENT', unit_id,  null) as p_unit_id, 
    IF(temporaryorpermanent='PERMANENT', unit_type,  null) as p_unit_type1, 
    IF(temporaryorpermanent='PERMANENT', facility_id,  null) as p_facility_id, 
    IF(temporaryorpermanent='PERMANENT', facility_type,  null) as p_facility_type, 
    IF(temporaryorpermanent='PERMANENT', facility_name,  null) as p_facility_name, 
    FROM periods
), 
missing_releases_handled AS (
    SELECT OFFENDERID, move_seq, start_datetime, start_code, start_reason, JURISDICTIONALAGENCY, LOCATIONREPORTMOVEMENT, OTHERLOCATIONCODE,
        -- If there's no release but we know that the movement is not the person's most
        -- recent movement then use the next admission as the release
        IFNULL(end_datetime, LEAD(start_datetime) OVER periods_for_person) as end_datetime,
        IFNULL(end_code, LEAD(start_code) OVER periods_for_person) as end_code,
        IFNULL(end_reason, LEAD(start_reason) OVER periods_for_person) as end_reason,
        LAST_VALUE(p_unit_name ignore nulls) OVER (periods_for_person range between UNBOUNDED preceding and current row) as p_unit_name,
        LAST_VALUE(p_unit_id ignore nulls) OVER (periods_for_person range between UNBOUNDED preceding and current row) as p_unit_id,
        LAST_VALUE(p_unit_type1 ignore nulls) OVER (periods_for_person range between UNBOUNDED preceding and current row) as p_unit_type,
        LAST_VALUE(p_facility_name ignore nulls) OVER (periods_for_person range between UNBOUNDED preceding and current row) as p_facility_name,
        LAST_VALUE(p_facility_id ignore nulls) OVER (periods_for_person range between UNBOUNDED preceding and current row) as p_facility_id,
        LAST_VALUE(p_facility_type ignore nulls) OVER (periods_for_person range between UNBOUNDED preceding and current row) as p_facility_type,
    FROM permanent_moves
    WINDOW periods_for_person AS (PARTITION BY OFFENDERID ORDER BY move_seq)
), final as (
SELECT *, 
FROM missing_releases_handled
WHERE start_code IN (
    '01', # New CO Commitment
    '03', # YOS Commitment
    '06', # Recommitment
    '07', # ICC Commitment
    '10', # Returned from Parole
    '13', # Terminated from ISP-I
    '20', # Returned from Escape
    '21', # Returned from Court
    '22', # Returned from Escorted Leave
    '23', # Returned from Hospital
    '24', # Returned from Bond
    '25', # Returned from ICC
    '26', # Returned from Furlough
    '2B', # Returned from Work Crew
    '2C', # Returned from AWOL
    '2D', # Returned from Other Jurisdiction
    '30', # Received at DOC Facility
    '31', # Received at Community Center
    '32', # Received at County/City Jail
    '33', # Escapee In Custody - Other Jurisdiction
    '36', # Offender Situation Changed
    '37', # Halfway-In (at CCC)
    '39', # Day Trip (to)
    '3A', # Reassigned to another Facility
    '56', # Unauthorized Release
    '63-I', # ISP-INMATE
    '79', # Returned to ISP-I
    '80', # Escaped
    '81', # Out To Court
    '82', # Out on Escorted Leave
    '83', # Out to Hospital
    '84', # Out on Bond
    '86', # Out on Furlough
    '8B', # Out on Work Crew
    '8C', # Absent without Leave (AWOL)
    '8E', # AWOL from ISP-I
    '90', # Transferred to DOC Facility
    '91', # Transferred to Community Center
    '92' # Transferred to County/City Jail
) 
)
select ROW_NUMBER() OVER (PARTITION BY OFFENDERID ORDER BY start_datetime) AS period_id, OFFENDERID, final.start_datetime, final.start_code, final.start_reason, final.end_datetime, final.end_code, final.end_reason, final.JURISDICTIONALAGENCY, final.OTHERLOCATIONCODE, final.LOCATIONREPORTMOVEMENT, final.p_facility_id, final.p_facility_name, final.p_facility_type, final.p_unit_id, final.p_unit_name, final.p_unit_type
FROM final;
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_co",
    ingest_view_name="IncarcerationPeriod",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="OFFENDERID, period_id, start_datetime, end_datetime",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
