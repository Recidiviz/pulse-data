# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""View logic to prepare US_MO incarceration staff data for Workflows"""

US_MO_INCARCERATION_STAFF_TEMPLATE = """
    WITH 

    -- In MO, we treat facilities as staff with caseloads instead of using individual case managers.
    caseload_staff AS (
        SELECT DISTINCT
            rr.facility_id AS id,
            rr.state_code,
            IFNULL(locations.level_1_incarceration_location_name, rr.facility_id) AS name,
            -- TODO(#19062) Make it more clear where facility IDs go
            rr.facility_id AS district,
            CAST(NULL AS STRING) AS email,
            false AS has_caseload,
            true AS has_facility_caseload,
            IFNULL(locations.level_1_incarceration_location_name, rr.facility_id) AS given_names,
            "" as surname,
        FROM `{project_id}.{workflows_dataset}.resident_record_materialized` rr
        LEFT JOIN `{project_id}.{reference_views_dataset}.incarceration_location_ids_to_names` locations
        ON rr.facility_id = locations.level_1_incarceration_location_external_id
          AND rr.state_code = locations.state_code
        WHERE rr.state_code = "US_MO"
        AND facility_id IS NOT NULL
    ),
    staff_without_caseloads AS (
        SELECT DISTINCT 
            IFNULL(id, LOWER(email)) as id,
            "US_MO" as state_code,
            UPPER(first_name || " " || last_name) as name,
            -- TODO(#19062) Make it more clear where facility IDs go
            IF(LOWER(facility)="all", CAST(NULL as STRING), facility) as district,
            LOWER(email) AS email,
            false AS has_caseload,
            false AS has_facility_caseload,
            UPPER(first_name) as given_names,
            UPPER(last_name) as surname,
        -- This table is intentionally named to be specific to this workflow and is not intended to
        -- be the long-term place where we manage MO workflows users (the new users-in-admin-panel
        -- infrastructure will be).
        FROM `{project_id}.{static_reference_tables_dataset}.us_mo_rh_workflows_roster`
    )
    SELECT 
        {columns}
    FROM caseload_staff

    UNION ALL
    
    SELECT 
        {columns}
    FROM staff_without_caseloads
"""
