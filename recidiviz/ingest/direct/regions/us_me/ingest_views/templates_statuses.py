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
"""Helper templates for the US_ME current status and transfers query used
in incarceration and supervision periods."""
from typing import Optional

from recidiviz.ingest.direct.regions.us_me.ingest_views.us_me_view_query_fragments import (
    CURRENT_STATUS_ORDER_BY,
    REGEX_TIMESTAMP_NANOS_FORMAT,
    VIEW_CLIENT_FILTER_CONDITION,
)


def statuses_cte(
    status_filter_condition: Optional[str] = "",
) -> str:
    """This returns a filtered and ordered set of adult statuses that have the associated transfer reason and type. The
    resulting CTE is called `statuses`.

    This CTE filters out:

    1. Statuses associated with cancelled transfers
    2. Statuses with physical or jurisdiction locations in juvenile facilities
    3. Statuses with a start date in the year 1900
    4. Status codes that are for juveniles or have open date ranges that cause issues when joining other tables
    5. Inactive statuses that have date ranges that encompass other status rows

    The statuses are ordered by:

    1. Effective date
    2. Ineffective date - Future and NULL dates are transformed to 9999-12-31
    3. Transfer date - This could be NULL if no transfer is associated with the status change
    4. Status code ordered by incarceration to supervision status types
    5. Current_Status_Hist_Id
    """

    return f"""
    transfers AS (
        SELECT
            Cis_100_Client_Id AS client_id,
            Transfer_Id AS transfer_id,
            type.E_Trans_Type_Desc AS transfer_type,
            reason.E_Transfer_Reason_Desc AS transfer_reason,
            jur_from.Cis_9080_Ccs_Location_Type_Cd AS transfer_from_jur_location_type,
            jur_from.Location_Name as transfer_from_jur_location,
            IF(Cancelled_Ind = 'Y', TRUE, NULL) AS cancelled,
            PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', REGEXP_REPLACE(Transfer_Date, r'{REGEX_TIMESTAMP_NANOS_FORMAT}', '')) AS transfer_datetime,
        FROM {{CIS_314_TRANSFER}}

        LEFT JOIN {{CIS_3140_TRANSFER_TYPE}} type
        ON Cis_3140_Transfer_Type_Cd = type.Transfer_Type_Cd

        LEFT JOIN {{CIS_3141_TRANSFER_REASON}} reason
        ON Cis_3141_Transfer_Reason_Cd = reason.Transfer_Reason_Cd
        
        LEFT JOIN {{CIS_908_CCS_LOCATION}}  jur_from
        on jur_from.Ccs_Location_Id = Cis_908_Ccs_Loc_Jur_From_Id
    ),
    statuses_and_transfers_with_parsed_dates AS (
        SELECT
            Current_Status_Hist_Id AS status_history_id,
            Cis_100_Client_Id AS client_id,
            status_code.E_Current_Status_Desc AS current_status,
            l.Location_Name AS current_status_location,
            l.Cis_9080_Ccs_Location_Type_Cd AS location_type,
            juris_loc.Cis_9080_Ccs_Location_Type_Cd AS jurisdiction_location_type,
            juris_loc.Location_Name AS current_jurisdiction_location,
            unit.Name_Tx AS housing_unit,
            Cis_314_Transfer_Id AS transfer_id,
            t.transfer_datetime,
            t.transfer_type,
            t.transfer_reason,
            t.transfer_from_jur_location_type,
            t.transfer_from_jur_location,
            PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', REGEXP_REPLACE(Effct_Date, r'{REGEX_TIMESTAMP_NANOS_FORMAT}', '')) AS effective_datetime,
            PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', REGEXP_REPLACE(Ineffct_Date, r'{REGEX_TIMESTAMP_NANOS_FORMAT}', '')) AS ineffective_datetime
        FROM {{CIS_125_CURRENT_STATUS_HIST}} status

        INNER JOIN {{CIS_100_CLIENT}} client
        ON status.Cis_100_Client_Id = client.Client_Id
        AND {VIEW_CLIENT_FILTER_CONDITION}

        LEFT JOIN {{CIS_1000_CURRENT_STATUS}} status_code
        ON Cis_1000_Current_Status_Cd = status_code.Current_Status_Cd

        LEFT JOIN {{CIS_908_CCS_LOCATION}} l
        ON Cis_908_Ccs_Location_2_Id = l.Ccs_Location_Id

        LEFT JOIN {{CIS_908_CCS_LOCATION}} juris_loc
        on Cis_908_Ccs_Location_Id = juris_loc.Ccs_Location_Id

        LEFT JOIN {{CIS_912_UNIT}} unit
        ON Cis_912_Unit_Id = unit.Unit_Id

        LEFT JOIN transfers t
        ON Cis_314_Transfer_Id = t.transfer_id

        -- Only include statuses associated with non-cancelled transfers
        WHERE t.cancelled IS NULL 
        
        -- Only include statuses at Adult facilities
        AND NOT (
            juris_loc.Cis_9080_Ccs_Location_Type_Cd IN ('3','15') 
                OR l.Cis_9080_Ccs_Location_Type_Cd IN ('3', '15')
        )

        -- Remove rows with effective dates in year 1900
        AND EXTRACT(YEAR FROM PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', REGEXP_REPLACE(Effct_Date, r'{REGEX_TIMESTAMP_NANOS_FORMAT}', ''))) != 1900

        -- Remove statuses that either do not mean anything or can cause issues when ordering by effective_date
        {status_filter_condition}
    ),
    order_status_dates_chronologically AS (
        -- Client statuses with effective and end dates are swapped if they are out of order chronologically.
        -- Except for rows where the effective date matches the transfer date, those are assumed to be correct 
        -- effective dates, and we do not know the correct end dates.
        SELECT
            status_history_id,
            client_id,
            current_status,
            transfer_datetime,
            effective_datetime,
            -- If ineffective_datetime is earlier than effective_datetime, then set the effective_datetime as the
            -- ineffective_date, essentially creating a 1 day status period because we do not know the actual
            -- ineffective_date.
            CASE 
                WHEN ineffective_datetime < effective_datetime
                THEN effective_datetime
                ELSE ineffective_datetime
            END AS end_datetime,
        FROM statuses_and_transfers_with_parsed_dates
    ),
    select_next_effective_datetime AS (
        SELECT
            client_id,
            status_history_id,
            current_status,
            LEAD(effective_datetime) OVER status_seq AS next_effective_datetime,
            effective_datetime,
            -- Close out open statuses that span the date range of the next status
            CASE WHEN LEAD(effective_datetime) OVER status_seq IS NOT NULL
                AND LEAD(effective_datetime) OVER status_seq < end_datetime
                AND LEAD(effective_datetime) OVER status_seq < LEAD(end_datetime) OVER status_seq
                THEN LEAD(effective_datetime) OVER status_seq
                ELSE end_datetime 
            END AS end_datetime,                
            transfer_datetime,
        FROM order_status_dates_chronologically

        WINDOW status_seq AS (
            PARTITION BY client_id
            ORDER BY 
                effective_datetime, 
                end_datetime, 
                {CURRENT_STATUS_ORDER_BY}, 
                transfer_datetime, 
                status_history_id
        )
    ),
    statuses AS (
        SELECT
            s2.client_id,
            s2.status_history_id,
            s2.current_status,
            s2.current_status_location,
            s2.location_type,
            s2.jurisdiction_location_type,
            s2.current_jurisdiction_location,
            s2.housing_unit,
            s2.transfer_id,
            s2.transfer_type,
            s2.transfer_reason,
            s2.transfer_from_jur_location_type,
            s2.transfer_from_jur_location,
            s1.transfer_datetime,
            s1.effective_datetime,
            s1.end_datetime,         
            EXTRACT(DATE FROM s1.effective_datetime) AS effective_date,
            EXTRACT(DATE FROM s1.end_datetime) AS end_date,
        FROM select_next_effective_datetime s1
        
        JOIN statuses_and_transfers_with_parsed_dates s2
        ON s1.status_history_id = s2.status_history_id
    )
"""
