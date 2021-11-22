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
"""Query containing MDOC client incarceration periods."""

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
    WITH transfers AS (
        SELECT
            Cis_100_Client_Id AS client_id,
            Transfer_Id AS transfer_id,
            type.E_Trans_Type_Desc AS transfer_type,
            reason.E_Transfer_Reason_Desc AS transfer_reason
        FROM {CIS_314_TRANSFER}
        
        LEFT JOIN {CIS_3140_TRANSFER_TYPE} type
        ON Cis_3140_Transfer_Type_Cd = type.Transfer_Type_Cd
        
        LEFT JOIN {CIS_3141_TRANSFER_REASON} reason
        ON Cis_3141_Transfer_Reason_Cd = reason.Transfer_Reason_Cd

        WHERE Cancelled_Ind != "Y"
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
            -- TODO(#9968) Update timestamp format when we receive SFTP transfer
            EXTRACT(DATE FROM PARSE_TIMESTAMP("%m/%d/%Y %r", Effct_Date)) AS effective_date,
            EXTRACT(DATE FROM PARSE_TIMESTAMP("%m/%d/%Y %r", End_Date)) AS end_date,
            EXTRACT(DATE FROM PARSE_TIMESTAMP("%m/%d/%Y %r", Ineffct_Date)) AS ineffective_date
        FROM {CIS_125_CURRENT_STATUS_HIST}
        
        LEFT JOIN {CIS_1000_CURRENT_STATUS} status_code
        ON Cis_1000_Current_Status_Cd = status_code.Current_Status_Cd
        
        LEFT JOIN {CIS_908_CCS_LOCATION} l
        ON Cis_908_Ccs_Location_2_Id = l.Ccs_Location_Id
        
        LEFT JOIN {CIS_908_CCS_LOCATION} juris_loc
        on Cis_908_Ccs_Location_Id = juris_loc.Ccs_Location_Id
        
        LEFT JOIN {CIS_912_UNIT} unit
        ON Cis_912_Unit_Id = unit.Unit_Id
    ),
    statuses_with_next_and_prev AS (
        SELECT
            client_id,
            current_status,
            LAG(current_status) OVER status_seq AS previous_status,
            LEAD(current_status) OVER status_seq AS next_status,
            current_status_location,
            location_type,
            jurisdiction_location_type,
            housing_unit,
            LEAD(location_type) OVER status_seq AS next_location_type,
            transfer_id,
            effective_date,
            end_date,
            ineffective_date
        FROM all_statuses
        WINDOW status_seq AS (PARTITION BY client_id ORDER BY effective_date ASC)
    ),
    all_movements AS (
        SELECT
                Cis_Client_Id AS client_id,
                Cis_314_Transfer_Id AS transfer_id,
                type.E_Movement_Type_Desc AS movement_type,
                status.E_Mvmt_Status_Desc AS movement_status,
                direction.E_Mvmt_Direction_Desc AS movement_direction,
                -- TODO(#9968) Update timestamp format when we receive SFTP transfer
                EXTRACT(DATE FROM PARSE_TIMESTAMP("%m/%d/%Y %r", Movement_Date)) AS movement_date
            FROM {CIS_309_MOVEMENT}
            
            LEFT JOIN {CIS_3090_MOVEMENT_TYPE} type
            ON Cis_3090_Movement_Type_Cd = type.Movement_Type_Cd
            
            LEFT JOIN {CIS_3093_MVMT_STATUS} status
            ON  Cis_3093_Mvmt_Status_Cd = status.Mvmt_Status_Cd
            
            LEFT JOIN {CIS_3095_MVMT_DIRECTION} direction
            ON Cis_3095_Mvmt_Direction_Cd = direction.Mvmt_Direction_Cd
    
            -- Filter to movements with status "Complete" and remove "Transport" movements
            WHERE Cis_3093_Mvmt_Status_Cd = '3' AND Cis_3090_Movement_Type_Cd != "5"
    ),
    ranked_movements AS (
        SELECT
            client_id,
            transfer_id,
            movement_type,
            movement_status,
            movement_direction,
            movement_date,
            -- Transfer movement types have 2 rows: one for movement "In" to Cis_908_Ccs_Location_2_Id and one
            -- for movement "Out" of Cis_908_Ccs_Location_Id. This orders the rows so we only use the "In" direction.
            ROW_NUMBER() OVER (PARTITION BY client_id, movement_date, movement_type, transfer_id ORDER BY movement_direction ASC) AS movement_dedup_rank
        FROM all_movements
    ),
    movements_with_next_and_prev AS (
        SELECT
            client_id,
            transfer_id,
            movement_type,
            LAG(movement_type) OVER movement_seq AS previous_movement_type,
            LEAD(movement_type) OVER movement_seq AS next_movement_type,
            movement_status,
            movement_direction,
            movement_date,
            LAG(movement_date) OVER movement_seq AS previous_movement_date,
            LEAD(movement_date) OVER movement_seq AS next_movement_date,
            movement_dedup_rank
        FROM ranked_movements
        -- Order by movement_direction DESC because Furlough and Escape types should be ordered Out then In
        WINDOW movement_seq AS (PARTITION BY client_id ORDER BY movement_date, movement_direction DESC)
    ),
    ranked_movements_with_next_prev AS (
        SELECT * FROM movements_with_next_and_prev 
          -- Select the first ranked Transfer movement
        WHERE (movement_dedup_rank = 1 AND movement_type = 'Transfer') 
            -- Select the "In" movements for types Escape, Furlough, Furlough Hospital
            OR (movement_type != 'Transfer' AND movement_direction = 'In')
            -- Discharge and Release movement types only have "Out" directions
            OR (movement_type IN ('Discharge', 'Release'))
    ),
    movements_with_incarceration_statuses AS (
        SELECT
            m.client_id,
            m.previous_movement_type,
            m.movement_type,
            m.next_movement_type,
            m.movement_status,
            m.movement_direction,
            m.previous_movement_date,
            m.movement_date AS start_date,
            CASE 
                -- Deceased location type
                WHEN next_location_type IN ('14')
                THEN s.ineffective_date
                WHEN m.next_movement_date IS NULL AND s.ineffective_date IS NOT NULL AND s.ineffective_date != DATE(9999, 12, 31)
                THEN s.ineffective_date
            ELSE m.next_movement_date END AS end_date,
            t.transfer_type,
            t.transfer_reason,
            s.previous_status,
            s.current_status,
            s.next_status,
            s.current_status_location,
            s.location_type,
            s.housing_unit,
            s.next_location_type,
            s.jurisdiction_location_type
        FROM ranked_movements_with_next_prev m
        
        LEFT JOIN transfers t
        ON t.transfer_id = m.transfer_id
        AND t.client_id = m.client_id
        
        LEFT JOIN statuses_with_next_and_prev s
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
        
        -- Filter to statuses related to adult prison incarceration
        -- County Jail status means the client was not sentenced to a DOC facility, but is being held at one
        -- for the county because of resourcing needs, primarily for mental health resources. 
        WHERE current_status IN (
            "Incarcerated",
            "Partial Revocation - incarcerated",
            "Interstate Compact In",
            "County Jail"
        )
        -- Filter out periods at juvenile facilities
        AND location_type NOT IN ("3","15")
        AND jurisdiction_location_type NOT IN ("3", "15")
        AND movement_type NOT IN ('Discharge', 'Release')
    )
    SELECT
        client_id,
        previous_status,
        current_status,
        next_status,
        current_status_location,
        location_type,
        next_location_type,
        jurisdiction_location_type,
        housing_unit,
        start_date,
        end_date,
        movement_type,
        next_movement_type,
        transfer_type,
        transfer_reason,
        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY start_date ASC) AS incarceration_period_id
    FROM movements_with_incarceration_statuses
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
