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
"""Query containing MDOC client incarceration periods to hydrate StateIncarcerationPeriod."""
from recidiviz.ingest.direct.regions.us_me.ingest_views.templates_statuses import (
    statuses_cte,
)
from recidiviz.ingest.direct.regions.us_me.ingest_views.us_me_view_query_fragments import (
    CURRENT_STATUS_ORDER_BY,
    MOVEMENT_TYPE_ORDER_BY,
    REGEX_TIMESTAMP_NANOS_FORMAT,
)
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    UPDATE_DATETIME_PARAM_NAME,
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

STATUS_FILTER_CONDITION = """
    AND Cis_1000_Current_Status_Cd NOT IN (
        '3', -- Committed - In Custody, Juvenile only
        '14', -- Community Reintegration, Juvenile only
        '15', -- Interstate Compact, juvenile only
        '16', -- Partial Revocation - probation to continue, juvenile only and discontinued
        '17', -- Partial Revocation - probation to terminate, juvenile only and discontinued
        '18', -- Informal Adjustment
        '19', -- Sole Sanction, Juvenile only
        '20', -- Petition Authorized
        '21', -- Detention, Juvenile only
        '22', -- Shock Sentence, Juvenile only
        '23', -- Referral
        '24', -- Drug Court Sanction, juvenile only
        '25', -- Drug Court Participant, Juvenile only
        '26', -- Conditional Release, Juvenile only
        '27', -- Federal Hold, never been used
        '31', -- Active
        '32'  -- No Further Action
    )
"""

TRANSFER_ID_ORDER_BY = (
    "CASE WHEN movement_type NOT IN ('Release', 'Discharge') THEN transfer_id END"
)
MOVEMENT_ID_ORDER_BY = (
    "CASE WHEN movement_type NOT IN ('Release', 'Discharge') THEN movement_id END"
)

MOVEMENT_DIRECTION_ORDER_BY = """
    CASE WHEN movement_type in ('Furlough', 'Furlough Hospital', 'Escape', 'Release', 'Discharge') 
        THEN movement_direction = 'Out'
    END DESC
"""
# This returns all bed assignment periods for a client, fixing any start/end dates that do not
# follow chronologically.
BED_ASSIGNMENTS_CTE = f"""
    all_bed_assignments AS (
        SELECT
            Cis_100_Client_Id AS client_id,
            PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', REGEXP_REPLACE(Start_Date, r'{REGEX_TIMESTAMP_NANOS_FORMAT}', '')) AS bed_assignment_start_date,
            PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', REGEXP_REPLACE(End_Date, r'{REGEX_TIMESTAMP_NANOS_FORMAT}', '')) AS bed_assignment_end_date,
        FROM {{CIS_916_ASSIGN_BED}}
    ),
    continuous_bed_assignment_periods AS (
        -- Returns a set of continuous periods of assignments
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
    bed_assignment_periods AS (
        SELECT
            client_id,
            -- Swap the start/end dates if they do not make sense chronologically
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
        FROM continuous_bed_assignment_periods
    )
"""


MOVEMENTS_CTE = f"""
    -- Returns movements_with_next_values CTE which is a set of movements with the following filters:
    -- 1. Removes Cancelled and Pending movements, except for pending Releases and Discharges
    -- 2. Removes movements that are associated with cancelled transfers
    -- 3. Removes movements to a juvenile facility or location
    -- 4. Merges Release/Transfer pairs, takes movement type from Release row and transfer info from Transfer row
    -- 5. Removes duplicate Transfer rows that have all the same info except for In/Out directions
    -- 6. Removes Community Transition and Transport movement types
    
    -- Orders the movements by movement date, direction, and a list of ordered movement types. 
    
    movements AS (
        SELECT
            Movement_Id AS movement_id,
            Cis_Client_Id AS client_id,
            Cis_314_Transfer_Id AS transfer_id,
            t.transfer_type,
            t.transfer_reason,
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

        LEFT JOIN {{CIS_908_CCS_LOCATION}} to_location
        on to_location.Ccs_Location_Id = Cis_908_Ccs_Location_2_Id

        LEFT JOIN transfers t
        ON t.transfer_id = Cis_314_Transfer_Id

        WHERE (
            -- Include movements with status 'Complete'
            -- Filter out 'Transport' and 'Community Transition' movement types
            (Cis_3093_Mvmt_Status_Cd = '3' AND Cis_3090_Movement_Type_Cd NOT IN ('5', '9'))

            -- Keep 'Discharge' and 'Release' movement types with a 'Pending' status
            OR (Cis_3093_Mvmt_Status_Cd = '1' AND Cis_3090_Movement_Type_Cd IN ('7', '8'))
        )

        -- Filter out movements to juvenile locations
        AND to_location.Cis_9080_Ccs_Location_Type_Cd not in ('3', '15')

        -- Only keep movements associated with non-cancelled transfers
        AND t.cancelled IS NULL
    ),
    previous_movements_for_deduplication AS (
        SELECT
          m.movement_id,
          m.client_id,
          m.transfer_id,
          m.movement_type,
          m.movement_direction,
          m.movement_status,
          m.movement_datetime,
          EXTRACT(DATE FROM m.movement_datetime) AS movement_date,
          transfer_type,
          transfer_reason,
          LAG(
            EXTRACT(DATE FROM m.movement_datetime)
            ) OVER movement_seq as previous_movement_date,
          LAG(m.movement_direction) OVER movement_seq as previous_movement_direction,
          LAG(m.movement_status) OVER movement_seq as previous_movement_status,
          LAG(m.transfer_id) OVER movement_seq as previous_transfer_id,
        FROM movements m
        
        WINDOW movement_seq AS (
            PARTITION BY m.client_id
            -- This order by **must** match the order in the following CTE for setting the client_release_movement_id.
            -- This movement_seq is only for identifying Release/Transfer pairs that should be merged.
            -- For movements that have the same movement_date, we order by the transfer_id, followed by the 
            -- movement_direction and the movement_id. This may not necessarily be the correct order for all movement
            -- types, but we need to have a movement_direction ordering here for an accurate count in the next CTE
            -- for the client_release_movement_id. The Transfer rows are deduplicated in the ranked_movements
            -- CTE with a different order condition, and other movement type rows are correctly ordered in the next
            -- CTEs once the duplicate Release/Transfer and Transfer In/Out rows are all merged. 
            ORDER BY EXTRACT(DATE FROM m.movement_datetime), transfer_id, movement_direction, movement_id
        )
    ),
    ranked_movements AS (
        SELECT
          client_id,
          movement_id,
          movement_type,
          movement_direction,
          movement_status,
          movement_datetime,
          movement_date,
          transfer_id,
          transfer_type,
          transfer_reason,
          -- Releases have 2 rows, one for the Release and one for the Transfer. The movement date, direction, and 
          -- status will all be the same value.
          -- This column increments when the preceding row has a different date, direction, status, and
          -- transfer_id, unless it is a Release row, then it does not check the transfer_id. This will allow us to
          -- group together Release/Transfer rows so that we can merge them later on.
          COUNTIF(movement_date != previous_movement_date
            OR movement_direction != previous_movement_direction
            OR movement_status != previous_movement_status
            OR CASE WHEN movement_type != 'Release' THEN transfer_id != previous_transfer_id ELSE NULL END) OVER (
                PARTITION BY client_id
                -- See comment in previous CTE for this ORDER BY's description
                ORDER BY movement_date, transfer_id, movement_direction, movement_id
            ) AS client_release_movement_id,
          -- Some Transfers have 2 rows: one for movement 'In' and one for movement 'Out'.
          -- This ranks the rows so we can filter to only use the 'In' direction when both exist.
          ROW_NUMBER() OVER (
            PARTITION BY client_id, movement_date, movement_type, transfer_id
            -- This orders all movement types, but we will only care about Transfer movement types when filtering on 
            -- this column. This will order Transfer movements types as In/Out, because we only keep the In
            -- rows to determine admission dates. 
            ORDER BY (movement_direction = 'In') DESC
          ) AS transfer_dedup_rank,
        FROM previous_movements_for_deduplication
    ),
    ranked_movements_release AS (
        SELECT
            client_id,
            movement_direction,
            movement_status,
            movement_datetime,
            movement_date,
            FIRST_VALUE(movement_id) OVER (
                PARTITION BY client_id, client_release_movement_id
                -- This order by selects the Transfer's movement_id instead of the Release's movement_id
                -- This is because Release movement_id's are auto-generated before they actually are completed, so
                -- the Transfer ID will be more accurate when ordering later on.
                ORDER BY CASE WHEN movement_type = 'Transfer' THEN 1 ELSE 2 END
            ) as movement_id,
            FIRST_VALUE(transfer_id) OVER (
                PARTITION BY client_id, client_release_movement_id
                -- This order by selects the Transfer row's transfer_id because the Release will have a NULL value
                ORDER BY transfer_id IS NULL
            ) as transfer_id,
            FIRST_VALUE(movement_type) OVER (
                PARTITION BY client_id, client_release_movement_id
                -- This order by selects the Release row's movement_type
                ORDER BY CASE WHEN movement_type = 'Transfer' THEN 2 ELSE 1 END
            ) as movement_type,
            FIRST_VALUE(transfer_type) OVER (
                PARTITION BY client_id, client_release_movement_id
                -- This order by selects the Transfer row's transfer_type because the Release will have a NULL value
                ORDER BY transfer_type IS NULL
            ) as transfer_type,
            FIRST_VALUE(transfer_reason) OVER (
                PARTITION BY client_id, client_release_movement_id
                -- This order by selects the Transfer row's transfer_reason because the Release will have a NULL value
                ORDER BY transfer_reason IS NULL
            ) as transfer_reason,
            ROW_NUMBER() OVER (
                PARTITION BY client_id, client_release_movement_id
                -- This order by will count the Release row as 1 and the Transfer row as 2 so we can filter out
                -- the transfer rows.
                ORDER BY CASE WHEN movement_type = 'Transfer' THEN 2 ELSE 1 END
            ) as release_dedup_rank
        FROM ranked_movements
        
        -- Remove the "Out" movements for Transfers. This filter will keep the "Out" movements for all other movement
        -- types so that we can get an accurate next and previous movement date and type in the next CTE.
        WHERE (
            movement_type != 'Transfer' OR transfer_dedup_rank = 1
        )
    ),
    movements_with_next_values AS (
        SELECT
            client_id,
            movement_id,
            movement_type,
            LEAD(movement_type) OVER movement_seq AS next_movement_type,
            LEAD(movement_status) OVER movement_seq AS next_movement_status,
            movement_direction,
            movement_date,
            movement_datetime,
            transfer_id,
            transfer_type,
            transfer_reason
        FROM ranked_movements_release
        
        -- Remove the duplicate Transfer movement type in Release pairs
        WHERE release_dedup_rank = 1
        
        WINDOW movement_seq AS (
            PARTITION BY client_id
            ORDER BY 
                movement_date, 
                {TRANSFER_ID_ORDER_BY},
                -- The order by ordered rows with movement_type 'Furlough', 'Furlough Hospital', 'Escape', 'Release', 
                -- and 'Discharge' Out then In. 
                {MOVEMENT_DIRECTION_ORDER_BY},
                {MOVEMENT_TYPE_ORDER_BY},
                movement_datetime,
                {MOVEMENT_ID_ORDER_BY}
        )
    )
"""

VIEW_QUERY_TEMPLATE = f"""
    WITH {statuses_cte(status_filter_condition=STATUS_FILTER_CONDITION)},
    {BED_ASSIGNMENTS_CTE},
    {MOVEMENTS_CTE},
    join_statuses_to_bed_assignments AS (
        SELECT 
            s.client_id,
            s.status_history_id,
            s.transfer_id,
            s.location_type,
            s.current_status,
            s.effective_datetime,
            s.effective_date,
            s.end_datetime,
            s.end_date,
            jurisdiction_location_type,
            housing_unit,
            current_status_location,
            transfer_type,
            transfer_reason,
            -- If multiple bed assignments are joined to one status period, take the latest bed assignment period.
            MAX(b.bed_period_start_date) as bed_period_start_date, 
            MAX(b.bed_period_end_date) as bed_period_end_date,
        FROM statuses s
        
        LEFT JOIN bed_assignment_periods b
        ON s.client_id = b.client_id
        AND (
            -- Join when status and bed periods overlap and same day periods
            (b.bed_period_start_date < s.end_date AND s.effective_date <= b.bed_period_end_date)
            OR 
            (b.bed_period_start_date = s.effective_date AND b.bed_period_end_date = s.end_date)
      )
      GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
    ),
    join_statuses_to_movements AS (
        SELECT 
            s.client_id,
            s.status_history_id,
            s.current_status,
            -- Use movement_date over status date because there can be multiple movements within a status period.
            COALESCE(m.movement_date, s.effective_date) AS start_date,
            s.end_date,
            LAG(s.current_status) OVER movement_seq AS previous_status,
            LAG(s.location_type) OVER movement_seq AS previous_location_type,
            -- Only set the next status and location type if the next movement is completed
            IF(m.next_movement_status = 'Pending' AND LEAD(movement_date) OVER movement_seq > @{UPDATE_DATETIME_PARAM_NAME}, NULL, LEAD(s.current_status) OVER movement_seq) AS next_status,
            IF(m.next_movement_status = 'Pending' AND LEAD(movement_date) OVER movement_seq > @{UPDATE_DATETIME_PARAM_NAME}, NULL, LEAD(s.location_type) OVER movement_seq) AS next_location_type,
            s.bed_period_end_date,
            m.movement_id,
            m.movement_type,
            m.movement_direction,
            m.next_movement_status,
            m.next_movement_type,
            -- It is expected that some of these will be NULL when there are status changes without movements
            -- We want to keep the NULL movement dates so that we correctly use the status end_date for these periods
            -- instead of the next_movement_date 
            LEAD(movement_date) OVER movement_seq AS next_movement_date,
            COALESCE(s.transfer_id, m.transfer_id) AS transfer_id,
            -- If the previous status has the same transfer_id, do not carry over transfer info to the next status
            -- This prevents Furloughs and Escapes from repeating the previous movement's transfer reasons
            IF(LAG(s.transfer_id) OVER movement_seq = s.transfer_id, m.transfer_type, s.transfer_type) AS transfer_type,
            IF(LAG(s.transfer_id) OVER movement_seq = s.transfer_id, m.transfer_reason, s.transfer_reason) AS transfer_reason,
            current_status_location,
            jurisdiction_location_type,
            housing_unit,
            location_type,
        FROM join_statuses_to_bed_assignments s

        LEFT JOIN movements_with_next_values m
            ON m.client_id = s.client_id

            -- If there is a movement transfer_id, use it to join, otherwise join by date range
            AND CASE WHEN m.transfer_id = s.transfer_id
                THEN m.transfer_id = s.transfer_id
                -- Join when status and movements overlap and same day movements/status changes
                -- Movements with null transfer_ids include Release, Discharge, Furlough, Furlough Hospital, Escape
                WHEN m.transfer_id IS NULL THEN (
                    (movement_date >= effective_date AND m.movement_date < s.end_date)
                    OR (movement_date = effective_date AND movement_date = end_date)
                )
                WHEN m.transfer_id IS NOT NULL AND movement_date != effective_date
                THEN (movement_date >= effective_date AND m.movement_date < s.end_date)
            END

        WINDOW movement_seq AS (
            PARTITION BY s.client_id 
            ORDER BY COALESCE(m.movement_date, s.effective_date), 
                     s.end_datetime,
                     {CURRENT_STATUS_ORDER_BY},
                     {MOVEMENT_DIRECTION_ORDER_BY},
                     {MOVEMENT_TYPE_ORDER_BY},
                     {MOVEMENT_ID_ORDER_BY},
                     status_history_id
        )
    ),
    get_status_end_dates AS (
        SELECT 
            client_id,
            status_history_id,
            movement_id,
            transfer_id,
            start_date,
            CASE 
                WHEN next_movement_type IN ('Discharge', 'Release') AND next_movement_status = 'Pending' 
                  AND next_movement_date > @{UPDATE_DATETIME_PARAM_NAME}
                    THEN NULL
                -- Prioritize an earlier bed assignment date over a status end date or next movement date
                WHEN bed_period_end_date <= end_date
                    AND (bed_period_end_date <= next_movement_date OR next_movement_date IS NULL)
                    AND bed_period_end_date < @{UPDATE_DATETIME_PARAM_NAME}
                    THEN bed_period_end_date
                WHEN next_movement_date IS NULL
                -- Convert all future dates to NULL
                THEN IF(end_date > @{UPDATE_DATETIME_PARAM_NAME}, NULL, end_date)
                ELSE IF(next_movement_date > @{UPDATE_DATETIME_PARAM_NAME}, NULL, next_movement_date)
            END AS end_date,
            movement_type,
            movement_direction,
            previous_status,
            previous_location_type,
            current_status,
            next_status,
            next_movement_type,
            next_location_type,
            location_type,
            transfer_type,
            transfer_reason,
            current_status_location,
            jurisdiction_location_type,
            housing_unit,
        FROM join_statuses_to_movements
    ),
    get_next_and_prev_statuses AS (
        SELECT
            client_id,
            status_history_id,
            movement_id,
            start_date,
            end_date,
            current_status,
            movement_type,
            movement_direction,
            next_movement_type,
            IF(ABS(DATE_DIFF(LEAD(start_date) OVER status_seq, end_date, DAY)) <= 7, next_status, NULL) AS next_status,
            IF(ABS(DATE_DIFF(LEAD(start_date) OVER status_seq, end_date, DAY)) <= 7, next_location_type, NULL) AS next_location_type,
            IF(ABS(DATE_DIFF(start_date, LAG(end_date) OVER status_seq, DAY)) <= 7, previous_status, NULL) AS previous_status,
            IF(ABS(DATE_DIFF(start_date, LAG(end_date) OVER status_seq, DAY)) <= 7, previous_location_type, NULL) AS previous_location_type,
            location_type,
            transfer_id,
            transfer_type,
            transfer_reason,
            jurisdiction_location_type,
            housing_unit,
            current_status_location,
        FROM get_status_end_dates
        WINDOW status_seq AS (
            PARTITION BY client_id
            ORDER BY start_date, end_date NULLS LAST, 
                    {CURRENT_STATUS_ORDER_BY},
                    {MOVEMENT_DIRECTION_ORDER_BY},
                    {TRANSFER_ID_ORDER_BY}, 
                    {MOVEMENT_ID_ORDER_BY},
                    {MOVEMENT_TYPE_ORDER_BY}, 
                    status_history_id
        )
    ),
    incarceration_periods AS (
        SELECT
            client_id,
            start_date,
            end_date,
            previous_status,
            current_status,
            next_status,
            current_status_location,
            location_type,
            jurisdiction_location_type,
            previous_location_type,
            next_location_type,
            housing_unit,
            movement_type,
            next_movement_type,
            transfer_type,
            transfer_reason,
        FROM get_next_and_prev_statuses
        
        -- Filter to incarceration statuses
        WHERE current_status IN (
            'Incarcerated',
            'Partial Revocation - incarcerated',
            'Interstate Compact In',
            'County Jail'
        )

        -- Filter to periods at Adult DOC Facilities and County Jails
        AND location_type IN ('2','7','9')
        
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
        previous_location_type,
        next_location_type,
        jurisdiction_location_type,
        housing_unit,
        movement_type,
        next_movement_type,
        transfer_type,
        transfer_reason,
        CAST(ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY start_date ASC, end_date NULLS LAST) AS STRING) AS incarceration_period_id,
    FROM incarceration_periods
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_me",
    ingest_view_name="CURRENT_STATUS_incarceration_periods_v2",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="client_id",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
