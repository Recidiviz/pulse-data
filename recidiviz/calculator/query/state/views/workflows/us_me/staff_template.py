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
"""View logic to prepare US_ME supervision staff data for Workflows"""

US_ME_STAFF_TEMPLATE = """
    WITH 
    caseload_staff_ids AS (
        SELECT DISTINCT
            officer_id AS id,
            state_code,
            false AS has_caseload,
            true AS has_facility_caseload,
        FROM `{project_id}.{workflows_dataset}.resident_record_materialized`
        WHERE state_code = "US_ME"
        AND officer_id IS NOT NULL
        
        UNION ALL
        
        -- Supervision staff
        SELECT DISTINCT
            officer_id AS id,
            state_code,
            true AS has_caseload,
            false AS has_facility_caseload,
        FROM `{project_id}.{workflows_dataset}.client_record_materialized`
        WHERE state_code = "US_ME"
        AND officer_id IS NOT NULL
    )
    , caseload_staff AS (
        SELECT DISTINCT
            ids.id,
            ids.state_code,
            UPPER(state_table.First_Name || " " || state_table.Last_Name) AS name,
            CAST(NULL AS STRING) AS district,
            LOWER(state_table.Email_Tx) AS email,
            has_caseload,
            has_facility_caseload,
            UPPER(state_table.First_Name) as given_names,
            UPPER(state_table.Last_Name) as surname,
        FROM caseload_staff_ids ids
        LEFT JOIN `{project_id}.{us_me_raw_data_up_to_date_dataset}.CIS_900_EMPLOYEE_latest` state_table
            ON state_table.Employee_Id = ids.id
    )
    , leadership_staff AS (
        SELECT DISTINCT
            # There are two US_ME staff who have multiple ids, which are mapped here to a single id for user management
            IFNULL(ids.external_id_mapped, state_table.Employee_Id) AS id,
            lu.state_code,
            UPPER(lu.first_name || " " || lu.last_name) AS name,
            CAST(NULL AS STRING) AS district,
            email_address AS email,
            false AS has_caseload,
            false AS has_facility_caseload,
            UPPER(lu.first_name) as given_names,
            UPPER(lu.last_name) as surname,
        FROM `{project_id}.{static_reference_tables_dataset}.us_me_leadership_users` lu
        LEFT JOIN `{project_id}.{us_me_raw_data_up_to_date_dataset}.CIS_900_EMPLOYEE_latest` state_table
            ON LOWER(state_table.Email_Tx) = LOWER(lu.email_address)
        LEFT JOIN {project_id}.{static_reference_tables_dataset}.agent_multiple_ids_map ids
            ON state_table.Employee_Id = ids.external_id_to_map AND lu.state_code = ids.state_code 
    ), staff_without_caseloads AS (
        SELECT DISTINCT 
            CAST(roster.external_id AS string) as id,
            "US_ME" as state_code,
            UPPER(employee_name) as name,
            district as district,
            email_address as email,
            false AS has_caseload,
            false AS has_facility_caseload,
            UPPER(state_table.first_name) as given_names,
            UPPER(state_table.last_name) as surname,
        FROM `{project_id}.{static_reference_tables_dataset}.us_me_roster` roster
        LEFT JOIN `{project_id}.{us_me_raw_data_up_to_date_dataset}.CIS_900_EMPLOYEE_latest` state_table
            ON LOWER(state_table.Email_Tx) = LOWER(roster.email_address)
        WHERE CAST(roster.external_id AS STRING) NOT IN (SELECT id from caseload_staff_ids)
    )
    SELECT 
        {columns}
    FROM caseload_staff
    
    UNION ALL
    
    SELECT 
        {columns}
    FROM leadership_staff
    
    UNION ALL
    
    SELECT 
        {columns}
    FROM staff_without_caseloads
"""
