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

"""View logic to prepare US_MI supervision staff data for Workflows"""
import os

import yaml

MICHIGAN_COUNTIES_BY_DISTRICT_FILE = os.path.join(
    os.path.dirname(__file__), "us_mi_counties_by_district.yaml"
)


def get_district_clause(county_column: str) -> str:
    with open(
        MICHIGAN_COUNTIES_BY_DISTRICT_FILE, "r", encoding="utf-8"
    ) as counties_file:
        counties_by_district = yaml.safe_load(counties_file.read())

    when_clauses = []
    for district, counties in counties_by_district["districts"].items():
        counties_clause = ",".join([repr(f"{county} County") for county in counties])
        when_clauses.append(
            f"WHEN {county_column} IN ({counties_clause}) THEN {repr(district)}"
        )

    when_clause = "\n".join(when_clauses)

    return f"""
        CASE 
        {when_clause}
        ELSE {county_column}
        END
    """


US_MI_SUPERVISION_STAFF_TEMPLATE = f"""
    WITH officers_with_caseload AS (
        SELECT DISTINCT officer_id
        FROM `{{project_id}}.{{workflows_dataset}}.client_record_materialized`
        WHERE state_code = 'US_MI'
    )
    SELECT
        "US_MI" AS state_code,
        employee.employee_id AS id,
        UPPER(employee.first_name || " " || IF(employee.middle_name is null, "", employee.middle_name || " ") || employee.last_name) AS name,
        {get_district_clause("county_reference.description")} AS district,
        LOWER(additional_info.email_address) as email,
        TRUE AS has_caseload,
        FALSE AS has_facility_caseload,
        employee.first_name AS given_names,
        employee.last_name AS surname,
        CAST(NULL AS STRING) AS role_subtype,
    FROM `{{project_id}}.{{us_mi_raw_data_up_to_date_views_dataset}}.ADH_EMPLOYEE_latest` employee
    LEFT JOIN `{{project_id}}.{{us_mi_raw_data_up_to_date_views_dataset}}.ADH_EMPLOYEE_ADDITIONAL_INFO_latest`
        additional_info USING (employee_id)
    LEFT JOIN `{{project_id}}.{{us_mi_raw_data_up_to_date_views_dataset}}.ADH_LOCATION_latest`
        location ON location.location_id = employee.default_location_id
    LEFT JOIN `{{project_id}}.{{us_mi_raw_data_up_to_date_views_dataset}}.ADH_REFERENCE_CODE_latest`
        county_reference ON  county_reference.reference_code_id = location.county_id
    LEFT JOIN `{{project_id}}.{{us_mi_raw_data_up_to_date_views_dataset}}.ADH_REFERENCE_CODE_latest`
        location_reference_code ON location_reference_code.reference_code_id = employee.work_site_id
    WHERE
        employee.employee_id in (SELECT officer_id FROM officers_with_caseload)
        AND employee.termination_date IS NULL
        AND employee.employee_type_id IN (
            "2104", # Supervisor
            "2105", # Agent
            "2106", # Field Services Assistant
            "2109", # Parole Manager
            "2110", # Probation Manager
            "2111", # Center Manager
            "13744" # Parole Probation Specialist
        )
 """
