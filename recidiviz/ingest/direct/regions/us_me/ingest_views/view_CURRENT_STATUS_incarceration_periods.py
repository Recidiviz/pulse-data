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
# TODO(#11586): Remove this view once it is deprecated
"""Query containing MDOC client incarceration periods."""
from recidiviz.ingest.direct.regions.us_me.ingest_views.us_me_view_query_fragments import (
    VIEW_CLIENT_FILTER_CONDITION,
)
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    UPDATE_DATETIME_PARAM_NAME,
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REGEX_TIMESTAMP_NANOS_FORMAT = r"\.\d+"

# A quick analysis showed that a 7 day look back period captured most of instances when a person transitioned from
# supervision to incarceration because of a revocation.
# TODO(#10573): Investigate using sentencing data to determine revocation admission reasons
NUM_DAYS_STATUS_LOOK_BACK = 7

VIEW_QUERY_TEMPLATE = f"""
    WITH transfers AS (
        SELECT
            Cis_100_Client_Id AS client_id,
            Transfer_Id AS transfer_id,
            type.E_Trans_Type_Desc AS transfer_type,
            reason.E_Transfer_Reason_Desc AS transfer_reason,
            IF(Cancelled_Ind = 'Y', TRUE, NULL) AS cancelled,
            PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', REGEXP_REPLACE(Transfer_Date, r'{REGEX_TIMESTAMP_NANOS_FORMAT}', '')) AS transfer_datetime,
        FROM {{CIS_314_TRANSFER}}

        LEFT JOIN {{CIS_3140_TRANSFER_TYPE}} type
        ON Cis_3140_Transfer_Type_Cd = type.Transfer_Type_Cd

        LEFT JOIN {{CIS_3141_TRANSFER_REASON}} reason
        ON Cis_3141_Transfer_Reason_Cd = reason.Transfer_Reason_Cd
    ),
    all_bed_assignments AS (
        SELECT
            Cis_100_Client_Id AS client_id,
            PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', REGEXP_REPLACE(Start_Date, r'{REGEX_TIMESTAMP_NANOS_FORMAT}', '')) AS bed_assignment_start_date,
            PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', REGEXP_REPLACE(End_Date, r'{REGEX_TIMESTAMP_NANOS_FORMAT}', '')) AS bed_assignment_end_date,
        FROM {{CIS_916_ASSIGN_BED}}
    ),
    all_statuses AS (
        SELECT
            Cis_100_Client_Id AS client_id,
            status_code.E_Current_Status_Desc AS current_status,
            l.Location_Name AS current_status_location,
            l.Cis_9080_Ccs_Location_Type_Cd AS location_type,
            juris_loc.Cis_9080_Ccs_Location_Type_Cd AS jurisdiction_location_type,
            unit.Name_Tx AS housing_unit,
            Cis_314_Transfer_Id AS transfer_id,
            t.transfer_datetime,
            t.transfer_type,
            t.transfer_reason,
            PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', REGEXP_REPLACE(Effct_Date, r'{REGEX_TIMESTAMP_NANOS_FORMAT}', '')) AS effective_datetime,
            PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', REGEXP_REPLACE(End_Date, r'{REGEX_TIMESTAMP_NANOS_FORMAT}', '')) AS end_datetime,
            PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', REGEXP_REPLACE(Ineffct_Date, r'{REGEX_TIMESTAMP_NANOS_FORMAT}', '')) AS ineffective_datetime
        FROM {{CIS_125_CURRENT_STATUS_HIST}}

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

        -- Remove statuses that either do not mean anything or can cause issues when ordering by effective_date
        WHERE Cis_1000_Current_Status_Cd NOT IN (
            '31', -- Active
            '23', -- Referral
            '20', -- Petition Authorized
            '18', -- Informal Adjustment
            '32', -- No Further Action
            '19', -- Sole Sanction, Juvenile only
            '21', -- Detention, Juvenile only
            '22', -- Shock Sentence, Juvenile only
            '24', -- Drug Court Sanction, juvenile only
            '25', -- Drug Court Participant, Juvenile only
            '26', -- Conditional Release, Juvenile only
            '3' -- Committed - In Custody, Juvenile only
        )
        -- Only include statuses associated with non-cancelled transfers
        AND t.cancelled IS NULL
    ),
    bed_assignment_periods AS (
        -- Select date ranges for continuous periods of assignment
        SELECT
          client_id,
          MIN(EXTRACT(DATE FROM bed_assignment_start_date)) AS bed_period_start_date, 
          IF(
            -- NULL end dates or end dates in the future mean the period is still active
            LOGICAL_OR(bed_assignment_end_date IS NULL), DATE(9999, 12, 31), MAX(EXTRACT(DATE FROM bed_assignment_end_date))
          ) AS bed_period_end_date
        FROM (
          SELECT 
              client_id,
              bed_assignment_start_date,
              bed_assignment_end_date,
              COUNT(CASE WHEN IFNULL(is_new_range, TRUE) THEN 1 ELSE NULL END) OVER (
                PARTITION BY client_id 
                ORDER BY bed_assignment_start_date, bed_assignment_end_date
              ) AS range_id
          FROM (
            SELECT 
                client_id, 
                bed_assignment_start_date, 
                bed_assignment_end_date, 
                DATE_DIFF(bed_assignment_start_date, LAG(bed_assignment_end_date) OVER bed_assignment_order, DAY) > 1 AS is_new_range
            FROM all_bed_assignments
            WINDOW bed_assignment_order AS (
                PARTITION BY client_id 
                ORDER BY bed_assignment_start_date, bed_assignment_end_date
            )
          ) AS identify_new_ranges
        ) AS order_and_count_new_ranges
        GROUP BY client_id, range_id
    ),
    cleaned_up_bed_assignment_periods AS (
        SELECT
            client_id,
            -- Fix dates that do not make sense chronologically
            CASE
                WHEN bed_period_start_date > bed_period_end_date
                THEN bed_period_end_date
                ELSE bed_period_start_date
            END AS bed_period_start_date,
            CASE
                WHEN bed_period_end_date < bed_period_start_date
                THEN bed_period_start_date
                ELSE bed_period_end_date
            END AS bed_period_end_date,
        FROM bed_assignment_periods
    ),
    cleaned_up_statuses AS (
        SELECT
            client_id,
            current_status,
            current_status_location,
            location_type,
            jurisdiction_location_type,
            housing_unit,
            transfer_id,
            transfer_type,
            transfer_reason,
            LEAD(effective_datetime) OVER status_seq AS next_effective_datetime,
            -- Fix dates that do not make sense chronologically 
            CASE 
                WHEN effective_datetime > end_datetime and effective_datetime != transfer_datetime
                THEN end_datetime 
                ELSE effective_datetime 
            END AS effective_datetime,
            CASE 
                WHEN effective_datetime > end_datetime and effective_datetime != transfer_datetime
                THEN EXTRACT(DATE FROM end_datetime) 
                ELSE EXTRACT(DATE FROM effective_datetime) 
            END AS effective_date,
            CASE 
                WHEN end_datetime < effective_datetime AND EXTRACT(YEAR FROM ineffective_datetime) = 9999
                THEN NULL
                WHEN end_datetime < effective_datetime and effective_datetime != transfer_datetime
                THEN effective_datetime
                ELSE end_datetime
            END AS end_datetime,
            CASE 
                WHEN end_datetime < effective_datetime AND EXTRACT(YEAR FROM ineffective_datetime) = 9999
                THEN NULL
                WHEN end_datetime < effective_datetime and effective_datetime != transfer_datetime
                THEN EXTRACT(DATE FROM effective_datetime)
                ELSE EXTRACT(DATE FROM end_datetime)
            END AS end_date,
            ineffective_datetime,
            EXTRACT(DATE FROM ineffective_datetime) AS ineffective_date
        FROM all_statuses

        -- Remove statuses with effective dates in 1900
        WHERE EXTRACT(YEAR FROM effective_datetime) != 1900

        -- Order by effective_datetime and end_datetime to get the accurate next_effective_datetime for the status
        -- window. This is used to filter out Inactive statuses that are following by an active status that has an
        -- effective_datetime that's within the Inactive status date range.
        WINDOW status_seq AS (
            PARTITION BY client_id 
            ORDER BY CASE 
                WHEN effective_datetime > end_datetime and effective_datetime != transfer_datetime
                THEN end_datetime
                ELSE effective_datetime
            END,
            CASE 
                WHEN end_datetime < effective_datetime AND EXTRACT(YEAR FROM ineffective_datetime) = 9999
                THEN NULL
                WHEN end_datetime < effective_datetime and effective_datetime != transfer_datetime
                THEN effective_datetime
                ELSE end_datetime
            END,
            ineffective_datetime
        )
    ),
    join_statuses_and_bed_assignments AS (
        SELECT 
            s.client_id,
            current_status,
            current_status_location,
            location_type,
            jurisdiction_location_type,
            housing_unit,
            transfer_id,
            transfer_type,
            transfer_reason,
            next_effective_datetime,
            effective_datetime,
            effective_date,
            end_datetime,
            end_date,
            ineffective_datetime,
            ineffective_date,
            -- If multiple bed assignments are joined to one status period, take the latest bed assignment period.
            MAX(b.bed_period_start_date) as bed_period_start_date, 
            MAX(b.bed_period_end_date) as bed_period_end_date
        FROM cleaned_up_statuses s
        LEFT JOIN cleaned_up_bed_assignment_periods b
        ON s.client_id = b.client_id
        AND (
            -- Bed period starts before status ends and the status starts before the bed period ends
            (b.bed_period_start_date < s.end_date AND s.effective_date <= b.bed_period_end_date)
          -- Join bed assignments that happen on the same day as the status start and end dates
          OR (b.bed_period_start_date = s.effective_date AND b.bed_period_end_date = s.end_date)
      )
      GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
    ),
    all_movements AS (
        SELECT
            Cis_Client_Id AS client_id,
            Cis_314_Transfer_Id AS transfer_id,
            type.E_Movement_Type_Desc AS movement_type,
            direction.E_Mvmt_Direction_Desc AS movement_direction,
            status.E_Mvmt_Status_Desc AS movement_status,
            PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', REGEXP_REPLACE(Movement_Date, r'{REGEX_TIMESTAMP_NANOS_FORMAT}', '')) AS movement_datetime
        FROM {{CIS_309_MOVEMENT}}

        LEFT JOIN {{CIS_3090_MOVEMENT_TYPE}} type
        ON Cis_3090_Movement_Type_Cd = type.Movement_Type_Cd

        LEFT JOIN {{CIS_3095_MVMT_DIRECTION}} direction
        ON Cis_3095_Mvmt_Direction_Cd = direction.Mvmt_Direction_Cd

        LEFT JOIN {{CIS_3093_MVMT_STATUS}} status
        ON Cis_3093_Mvmt_Status_Cd = Mvmt_Status_Cd

        -- Filter to movements with status 'Complete' and remove 'Transport' and 'Community Transition' movements
        WHERE (Cis_3093_Mvmt_Status_Cd = '3' AND Cis_3090_Movement_Type_Cd NOT IN ('5', '9'))
        -- Keep "Discharge" and "Release" movement types with a "Pending" status
        OR (Cis_3090_Movement_Type_Cd in ('7', '8') AND Cis_3093_Mvmt_Status_Cd = '1')
    ),
    ranked_movements AS (
        SELECT
              m.client_id,
              m.transfer_id,
              m.movement_type,
              m.movement_direction,
              m.movement_status,
              m.movement_datetime,
              EXTRACT(DATE FROM m.movement_datetime) AS movement_date,
              t.transfer_type,
              t.transfer_reason,
              -- Transfer movement types have 2 rows: one for movement 'In' to Cis_908_Ccs_Location_2_Id and one
              -- for movement 'Out' of Cis_908_Ccs_Location_Id. This orders the rows so we only use the 'In' direction.
              ROW_NUMBER() OVER (
                PARTITION BY m.client_id, EXTRACT(DATE FROM m.movement_datetime), m.movement_type, m.transfer_id
                ORDER BY (m.movement_direction = 'In') DESC
              ) AS movement_dedup_rank,
        FROM all_movements m

        LEFT JOIN transfers t
        ON t.transfer_id = m.transfer_id

        -- Only keep movements associated with non-cancelled transfers
        WHERE t.cancelled IS NULL
    ),
    filtered_movements_with_next_type AS (
        SELECT
            client_id,
            transfer_id,
            movement_type,
            LEAD(movement_type) OVER movement_seq AS next_movement_type,
            LEAD(movement_status) OVER movement_seq AS next_movement_status,
            movement_direction,
            movement_date,
            movement_datetime,
            transfer_type,
            transfer_reason
        FROM ranked_movements

        -- Filter the "Out" movements for Transfers. This filter will keep the "Out" movements for all other movement
        -- types so that we can get an accurate next and previous movement date and type in the next CTE.
        WHERE movement_dedup_rank = 1

        -- Order by movement_direction DESC because Furlough and Escape types should be ordered Out then In
        WINDOW movement_seq AS (
            PARTITION BY client_id 
            ORDER BY movement_date, movement_direction = 'Out' DESC, movement_datetime,
                -- "Release and "Discharge" movements have a corresponding "Transfer" movement. This orders the release
                -- and discharge movement first so we have an accurate next_movement_type and status.
                CASE WHEN movement_type IN ('Release', 'Discharge') THEN 1
                ELSE 2
            END
        )
    ),
    join_statuses_and_movements AS (
        SELECT 
            s.client_id,
            COALESCE(s.transfer_id, m.transfer_id) AS transfer_id,
            -- Prioritize using the movement_date over the status's effective_date since there can be multiple
            -- movements within a status period.
            COALESCE(m.movement_date, s.effective_date) AS start_date,
            s.bed_period_start_date,
            s.bed_period_end_date,
            m.movement_type,
            m.movement_direction,
            m.next_movement_status,
            m.next_movement_type,
            -- It is expected that some of these will be NULL when there are status changes without movements
            -- We want to keep the NULL movement dates so that we correctly use the status end_date for these periods
            -- instead of the next_movement_date 
            LAG(movement_date) OVER movement_seq AS previous_movement_date,
            LEAD(movement_date) OVER movement_seq AS next_movement_date,
            s.current_status,
            LAG(s.current_status) OVER movement_seq AS previous_status,
            LAG(s.current_status, 2) OVER movement_seq AS previous_previous_status,
            IF(m.next_movement_status = 'Pending' AND LEAD(movement_date) OVER movement_seq > @{UPDATE_DATETIME_PARAM_NAME}, NULL, LEAD(s.current_status) OVER movement_seq) AS next_status,
            s.current_status_location,
            s.location_type,
            IF(m.next_movement_status = 'Pending' AND LEAD(movement_date) OVER movement_seq > @{UPDATE_DATETIME_PARAM_NAME}, NULL, LEAD(s.location_type) OVER movement_seq) AS next_location_type,
            s.jurisdiction_location_type,
            s.housing_unit,
            s.end_date,
            LAG(s.end_date, 2) OVER movement_seq AS previous_previous_end_date,
            LAG(s.end_date) OVER movement_seq AS previous_end_date,
            s.ineffective_date AS ineffective_date,
            -- If the previous status has the same transfer_id, do not carry over transfer info to the next status
            -- This prevents Furloughs and Escapes from repeating the previous movement's transfer reasons
            IF(LAG(s.transfer_id) OVER movement_seq = s.transfer_id, m.transfer_type, s.transfer_type) AS transfer_type,
            IF(LAG(s.transfer_id) OVER movement_seq = s.transfer_id, m.transfer_reason, s.transfer_reason) AS transfer_reason,
        FROM join_statuses_and_bed_assignments s

        LEFT JOIN filtered_movements_with_next_type m
            ON m.client_id = s.client_id
            AND (
                movement_date >= effective_date
                AND (
                    s.end_date IS NOT NULL AND m.movement_date < s.end_date
                    OR (
                        s.end_date IS NULL AND s.ineffective_date = DATE(9999, 12, 31)
                    )
                )
            )
        -- Remove rows where the status is Inactive and the end date is later than the next status's start date
        -- This helps to avoid joining multiple movements to an invalid status date range.  
        WHERE NOT (current_status = 'Inactive' 
            AND (
                next_effective_datetime IS NOT NULL AND next_effective_datetime < s.end_datetime
            )
        )

        WINDOW movement_seq AS (
            PARTITION BY s.client_id 
            ORDER BY CASE WHEN m.movement_date = s.effective_date
                     THEN s.effective_datetime 
                     ELSE COALESCE(m.movement_datetime, s.effective_datetime) END, s.end_datetime, s.ineffective_datetime
        )
    ),             
    incarceration_periods AS (
        SELECT 
            s.client_id,
            start_date,
            CASE 
                -- Deceased location type
                WHEN next_location_type IN ('14')
                    THEN ineffective_date
                WHEN next_movement_type IN ('Discharge', 'Release') AND next_movement_status = 'Pending' 
                  AND next_movement_date > @{UPDATE_DATETIME_PARAM_NAME}
                    THEN NULL
                -- Prioritize an earlier bed assignment date over a status end date or next movement date
                WHEN bed_period_end_date < s.end_date 
                    AND (bed_period_end_date < next_movement_date OR next_movement_date IS NULL)
                    AND bed_period_end_date < @{UPDATE_DATETIME_PARAM_NAME}
                    THEN bed_period_end_date
                WHEN next_movement_date IS NULL
                -- Convert all future dates to NULL
                THEN COALESCE(
                    IF(end_date > @{UPDATE_DATETIME_PARAM_NAME}, NULL, end_date), 
                    IF(ineffective_date > @{UPDATE_DATETIME_PARAM_NAME}, NULL, ineffective_date)
                )
                ELSE IF(s.next_movement_date > @{UPDATE_DATETIME_PARAM_NAME}, NULL, s.next_movement_date) 
            END AS end_date,
            CASE
                -- If the previous status is Inactive, check the status end date preceding Inactive and if it 
                -- is within the look back period, use it as the previous_status value.
                WHEN s.previous_status = 'Inactive'
                    AND DATE_DIFF(s.start_date, s.previous_previous_end_date, DAY) <= CAST({NUM_DAYS_STATUS_LOOK_BACK} AS integer)
                THEN previous_previous_status
                WHEN DATE_DIFF(s.start_date, s.previous_end_date, DAY) <= CAST({NUM_DAYS_STATUS_LOOK_BACK} AS integer)
                THEN s.previous_status
                ELSE NULL
            END AS previous_status,
            current_status,
            next_status,
            current_status_location,
            location_type,
            jurisdiction_location_type,
            next_location_type,
            housing_unit,
            movement_type,
            next_movement_type,
            next_movement_status,
            transfer_type,
            transfer_reason,
        FROM join_statuses_and_movements s

        -- Filter out periods for test client IDs that have been removed from the client table
        INNER JOIN {{CIS_100_CLIENT}} c
        ON s.client_id = c.Client_Id
        AND {VIEW_CLIENT_FILTER_CONDITION}

        WHERE current_status IN (
            'Incarcerated',
            'Partial Revocation - incarcerated',
            'Interstate Compact In',
            'County Jail'
        )
        -- Filter to periods at Adult DOC Facilities and County Jails
        AND location_type IN ('2','7','9')
        -- Filter out periods with a Juvenile DOC jurisdiction
        AND jurisdiction_location_type NOT IN ('3','15')

        -- Include periods that either do not have a movement_type, or have a Transfer "In" movement
        AND (
            movement_type IS NULL OR (
                movement_type = 'Transfer'
                -- Select the "In" movements for all other movement types
                OR (movement_type != 'Transfer' AND movement_direction = 'In')
            )
            -- Filter out time periods after discharges and releases
            AND movement_type NOT IN ('Discharge', 'Release')
        )
    )
    SELECT
        client_id,
        start_date,
        end_date,
        previous_status,
        current_status,
        next_status,
        current_status_location,
        location_type,
        next_location_type,
        jurisdiction_location_type,
        housing_unit,
        movement_type,
        next_movement_type,
        transfer_type,
        transfer_reason,
        CAST(ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY start_date ASC) AS STRING) AS incarceration_period_id,
    FROM incarceration_periods
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_me",
    ingest_view_name="CURRENT_STATUS_incarceration_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="client_id",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
