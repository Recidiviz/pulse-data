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

US_ME_INCARCERATION_STAFF_TEMPLATE = """
    WITH 
    caseload_staff_ids AS (
        SELECT DISTINCT
            officer_id AS id,
            state_code,
        FROM `{project_id}.{workflows_dataset}.resident_record_materialized`
        WHERE state_code = "US_ME"
        AND officer_id IS NOT NULL
    )
    , caseload_staff AS (
        SELECT DISTINCT
            ids.id,
            ids.state_code,
            UPPER(roster.First_Name || " " || roster.Last_Name) AS name,
            CAST(NULL AS STRING) AS district,
            LOWER(roster.Email_Tx) AS email,
            false AS has_caseload,
            true AS has_facility_caseload,
            UPPER(roster.First_Name) as given_names,
            UPPER(roster.Last_Name) as surname,
        FROM caseload_staff_ids ids
        LEFT JOIN `{project_id}.{us_me_raw_data_dataset}.CIS_900_EMPLOYEE` roster
            ON roster.Employee_Id = ids.id
    )
    , leadership_staff AS (
        SELECT DISTINCT
            roster.Employee_Id AS id,
            state_code,
            UPPER(lu.first_name || " " || lu.last_name) AS name,
            CAST(NULL AS STRING) AS district,
            email_address AS email,
            false AS has_caseload,
            false AS has_facility_caseload,
            UPPER(lu.first_name) as given_names,
            UPPER(lu.last_name) as surname,
        FROM `{project_id}.{static_reference_tables_dataset}.us_me_leadership_users` lu
        LEFT JOIN `{project_id}.{us_me_raw_data_dataset}.CIS_900_EMPLOYEE` roster
            ON LOWER(roster.Email_Tx) = lu.email_address
    )
    SELECT 
        {columns}
    FROM caseload_staff
    
    UNION ALL
    
    SELECT 
        {columns}
    FROM leadership_staff
"""
