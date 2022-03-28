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
"""Query containing MDOC client supervision periods for hydrating StateSupervisionPeriod and StateAgent entities."""
from recidiviz.ingest.direct.regions.us_me.ingest_views.templates_statuses import (
    statuses_cte,
)
from recidiviz.ingest.direct.regions.us_me.ingest_views.us_me_view_query_fragments import (
    CURRENT_STATUS_ORDER_BY,
    NUM_DAYS_STATUS_LOOK_BACK,
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
        '24', -- Drug Court Sanction, juvenile only
        '25', -- Drug Court Participant, Juvenile only
        '26', -- Conditional Release, Juvenile only
        '27', -- Federal Hold, never been used
        '32'  -- No Further Action
    )
"""
VIEW_QUERY_TEMPLATE = f"""
    WITH {statuses_cte(status_filter_condition=STATUS_FILTER_CONDITION)},
    supervision_officer_assignments AS (
        SELECT
            Cis_100_Client_Id AS client_id,
            PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', REGEXP_REPLACE(Assignment_Date, r'{REGEX_TIMESTAMP_NANOS_FORMAT}', '')) AS supervision_start_datetime,
            IFNULL(
                PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', REGEXP_REPLACE(Supervision_End_Date, r'{REGEX_TIMESTAMP_NANOS_FORMAT}', '')),
                TIMESTAMP('9999-12-31')
            ) AS supervision_end_datetime,
            Cis_900_Employee_Id AS supervising_officer_id,
            status.Supervision_Status_Desc AS officer_status,
            l.Location_Name AS supervision_location,
            l.Cis_9080_Ccs_Location_Type_Cd AS supervision_location_type,
            employee.First_Name as officer_first_name,
            employee.Middle_Name as officer_middle_name,
            employee.Last_Name as officer_last_name,
        FROM {{CIS_124_SUPERVISION_HISTORY}} sh
        
        LEFT JOIN {{CIS_908_CCS_LOCATION}} l
        ON sh.Cis_908_Ccs_Location_Id = l.Ccs_Location_Id
        
        LEFT JOIN {{CIS_1241_SUPERVISION_STATUS}} status
        ON sh.Cis_1241_Super_Status_Cd = status.Supervision_Status_Cd
        
        LEFT JOIN {{CIS_900_EMPLOYEE}} employee 
        ON employee.Employee_Id = sh.Cis_900_Employee_Id

        -- Filter out future probation periods
        WHERE (DATE( 
            PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', REGEXP_REPLACE(Assignment_Date, r'{REGEX_TIMESTAMP_NANOS_FORMAT}', ''))
        ) <= @{UPDATE_DATETIME_PARAM_NAME})
        
        -- Filter out juvenile locations
        AND l.Cis_9080_Ccs_Location_Type_Cd NOT IN ('3', '15')
        
        -- Filter out "Victim Services" assignments
        AND Cis_900_Employee_Id NOT IN ('3628')

        -- Filter out null officer IDs
        AND Cis_900_Employee_Id IS NOT NULL
    ),
    supervision_officer_assignments_dates AS (
        SELECT 
            client_id,
            supervision_start_datetime,
            EXTRACT(DATE FROM supervision_start_datetime) AS supervision_start_date,
            supervision_end_datetime,
            EXTRACT(DATE FROM supervision_end_datetime) AS supervision_end_date,
            supervising_officer_id,
            officer_status,
            officer_first_name,
            officer_middle_name,
            officer_last_name,
            supervision_location,
            supervision_location_type,
        FROM supervision_officer_assignments
    ),
    join_statuses_and_officers AS (
        -- This CTE will join supervision history and statuses, using the supervision table as the base to left join 
        -- statuses to. The join condition tries to join a status row to supervision row by finding overlapping date
        -- ranges between the status and supervision start and end dates. The result will select the greater of
        -- the two values for the start date, and the lesser of the two values for the end date. This CTE should not
        -- return any NULL values for start dates or end dates. Ongoing periods will have the date 9999-12-31 which will
        -- be updated in the next CTE as NULL.
        SELECT
            statuses.client_id,
            statuses.jurisdiction_location_type AS current_jurisdiction_location_type,
            statuses.current_status,
            statuses.transfer_id,
            statuses.transfer_type,
            statuses.transfer_reason,
            statuses.transfer_from_jur_location_type,
            statuses.transfer_from_jur_location,       
            statuses.current_status_location,
            statuses.current_jurisdiction_location,
            statuses.location_type AS current_physical_location_type,
            statuses.location_type AS status_location_type,
            officers.supervising_officer_id AS officer_external_id,
            officers.supervision_location_type,
            officers.supervision_location,
            officers.officer_status,
            officers.officer_first_name,
            officers.officer_middle_name,
            officers.officer_last_name,
            GREATEST(IFNULL(officers.supervision_start_datetime, TIMESTAMP('1000-12-31')), statuses.effective_datetime) AS start_datetime,
            GREATEST(IFNULL(officers.supervision_start_date, (DATE('1000-12-31'))), statuses.effective_date) AS start_date,
            LEAST(IFNULL(officers.supervision_end_datetime, TIMESTAMP('9999-12-31')), statuses.end_datetime) AS end_datetime,
            LEAST(IFNULL(officers.supervision_end_date, (DATE('9999-12-31'))), statuses.end_date) AS end_date,
        FROM statuses
            
        LEFT JOIN supervision_officer_assignments_dates officers
        ON statuses.client_id = officers.client_id
        AND ((
            -- Capture all overlapping time periods between statuses and officer assignments
            supervision_start_date <= statuses.end_date AND statuses.effective_date < supervision_end_date
            )
            -- Capture single-day statuses that line up with supervision assignments
            OR (
                supervision_start_date = statuses.effective_date OR statuses.end_date = supervision_end_date
            ) 
        )
    ),
    statuses_and_officers_with_prev_and_next AS (
        SELECT
            client_id,
            start_date,
            -- Set the next start_date as the end_date if it exists, otherwise use the end_date as is
            -- and transform 9999-12-31 to null.
            CASE 
                WHEN (end_date > @{UPDATE_DATETIME_PARAM_NAME} OR end_date IS NULL) AND LEAD(start_date) OVER status_seq IS NOT NULL
                THEN LEAD(start_date) OVER status_seq
                ELSE IF(end_date > @{UPDATE_DATETIME_PARAM_NAME}, NULL, end_date)
            END AS end_date,
            current_status,
            IF(
                DATE_DIFF(start_datetime, LAG(end_datetime) OVER status_seq, DAY) <= CAST({NUM_DAYS_STATUS_LOOK_BACK} AS integer), 
                LAG(current_status) OVER status_seq, 
                NULL
            ) AS previous_status,
            IF(
                DATE_DIFF(start_datetime, LAG(end_datetime) OVER status_seq, DAY) <= CAST({NUM_DAYS_STATUS_LOOK_BACK} AS integer), 
                LAG(current_jurisdiction_location_type) OVER status_seq, 
                NULL
            ) AS previous_jurisdiction_location_type,
            IF(
                DATE_DIFF(start_datetime, LAG(end_datetime) OVER status_seq, DAY) <= CAST({NUM_DAYS_STATUS_LOOK_BACK} AS integer), 
                LAG(current_jurisdiction_location) OVER status_seq, 
                NULL
            ) AS previous_jurisdiction_location,
            IF(
                DATE_DIFF(LEAD(start_datetime) OVER status_seq, end_datetime, DAY) <= CAST({NUM_DAYS_STATUS_LOOK_BACK} AS integer), 
                LEAD(current_status) OVER status_seq, 
                NULL
            ) AS next_status,
            IF(
                DATE_DIFF(LEAD(start_datetime) OVER status_seq, end_datetime, DAY) <= CAST({NUM_DAYS_STATUS_LOOK_BACK} AS integer), 
                LEAD(current_jurisdiction_location_type) OVER status_seq, 
                NULL
            ) AS next_jurisdiction_location_type,
            IF(
                DATE_DIFF(LEAD(start_datetime) OVER status_seq, end_datetime, DAY) <= CAST({NUM_DAYS_STATUS_LOOK_BACK} AS integer), 
                LEAD(current_jurisdiction_location) OVER status_seq, 
                NULL
            ) AS next_jurisdiction_location,
            current_jurisdiction_location_type,
            current_physical_location_type,
            -- Very few cases have "Central Office,IT" as the supervision location, in these cases we want to use
            -- the jurisdiction location.
            IF(supervision_location_type = '16', current_jurisdiction_location, supervision_location) AS supervision_location,
            IF(supervision_location_type = '16', current_jurisdiction_location_type, supervision_location_type) AS supervision_location_type,
             -- If the previous status has the same transfer_id, do not carry over transfer info to the next status
            IF(LAG(transfer_id) OVER status_seq = transfer_id, NULL, transfer_type) AS transfer_type,
            IF(LAG(transfer_id) OVER status_seq = transfer_id, NULL, transfer_reason) AS transfer_reason,
            IF(LAG(transfer_id) OVER status_seq = transfer_id, NULL, transfer_from_jur_location_type) AS transfer_from_jur_location_type,
            IF(LAG(transfer_id) OVER status_seq = transfer_id, NULL, transfer_from_jur_location) AS transfer_from_jur_location,
            officer_external_id,
            officer_status,
            officer_first_name,
            officer_middle_name,
            officer_last_name,
        FROM join_statuses_and_officers
    
        WINDOW status_seq AS (
            PARTITION BY client_id 
            ORDER BY start_date, end_date, 
            {CURRENT_STATUS_ORDER_BY},
            transfer_type,
            transfer_reason
        )
    ),
    supervision_periods AS (
        SELECT 
            client_id,
            start_date,
            end_date,
            officer_external_id,
            officer_status,
            officer_first_name,
            officer_middle_name,
            officer_last_name,
            supervision_location,
            previous_status,
            current_status,
            next_status,
            CASE 
                -- Previously not in ME jurisdiction
                WHEN previous_status = 'Inactive' OR previous_status IS NULL
                THEN COALESCE(transfer_from_jur_location_type, previous_jurisdiction_location_type)
                ELSE previous_jurisdiction_location_type 
            END AS previous_jurisdiction_location_type,
            CASE 
                -- Previously not in the ME jurisdiction
                WHEN previous_status = 'Inactive' or previous_status is null
                THEN COALESCE(transfer_from_jur_location, previous_jurisdiction_location)
                ELSE previous_jurisdiction_location 
            END AS previous_jurisdiction_location,
            current_jurisdiction_location_type,
            next_jurisdiction_location_type,
            next_jurisdiction_location,
            transfer_type,
            transfer_reason,
        FROM statuses_and_officers_with_prev_and_next
            
        WHERE (
            -- Filter to supervision periods
            (current_status in (
                'Probation',
                'SCCP',
                'Parole',
                'Warrant Absconded',
                'Pending Violation',
                'Pending Violation - Incarcerated',
                'Partial Revocation - County Jail',
                'Interstate Compact In',
                'Interstate Compact Out',
                'Interstate Active Detainer'
            )
                -- Remove periods where the supervision site, jurisdiction location, and physical location
                -- are all either DOC Facility, County Jail, one of Maine's counties, one of the US States, 
                -- or a Federal facility.
                AND NOT (
                    supervision_location_type IN ('2', '7', '9', '8', '19', '13') 
                    AND current_jurisdiction_location_type IN ('2', '7', '9', '8', '19', '13')
                    AND current_physical_location_type IN ('2', '7', '9', '8', '19', '13')
                )
            )
            # Look into County Jail, Incarcerated, with supervision_location_type = 2
            -- This will result in supervision periods with non-supervision statuses. Based on the current validation
            -- data this seems to be expected
            OR current_jurisdiction_location_type = '4' -- Adult Supervision Locations
        )
        -- Filter out periods after death
        AND (previous_jurisdiction_location_type IS NULL OR previous_jurisdiction_location_type NOT IN ('14'))

        -- Filter out periods where location is in a DOC Facility or County Jail
        AND current_physical_location_type NOT IN ('2', '7', '9')

        -- Filter out periods where no officer is assigned
        AND officer_external_id IS NOT NULL
    )
    SELECT
        client_id,
        start_date,
        end_date,
        previous_status,
        current_status,
        next_status,
        supervision_location,
        previous_jurisdiction_location,
        previous_jurisdiction_location_type,
        current_jurisdiction_location_type,
        next_jurisdiction_location_type,
        next_jurisdiction_location,
        transfer_type,
        transfer_reason,
        officer_external_id,
        officer_status,
        officer_first_name,
        officer_middle_name,
        officer_last_name,
        CAST(ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY start_date, end_date NULLS LAST) AS STRING) AS supervision_period_id
    FROM supervision_periods
"""


VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_me",
    ingest_view_name="supervision_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="client_id",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
