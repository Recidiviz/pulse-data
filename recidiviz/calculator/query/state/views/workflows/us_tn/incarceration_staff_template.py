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
"""View logic to prepare US_TN incarceration staff data for Workflows. Needed for user identification, even if
a facility tool is search-by-location"""

US_TN_INCARCERATION_STAFF_TEMPLATE = """
    SELECT DISTINCT sei.external_id AS id,
                    "US_TN" AS state_code,
                    location_external_id AS district,
                    COALESCE(staff.email,r.email_address) AS email,
                    FALSE AS has_caseload,
                    TRUE AS has_facility_caseload,
                    JSON_EXTRACT_SCALAR(full_name,'$.given_names') AS given_names,
                    JSON_EXTRACT_SCALAR(full_name,'$.surname') AS surname,
                    CAST(NULL AS STRING) AS role_subtype,
        FROM `{project_id}.normalized_state.state_staff_role_period` role
        INNER JOIN `{project_id}.normalized_state.state_staff_external_id` sei
            USING(staff_id)
        INNER JOIN (
                SELECT staff_id, location_external_id
                FROM `{project_id}.normalized_state.state_staff_location_period`
                QUALIFY ROW_NUMBER() OVER(PARTITION BY staff_id ORDER BY start_date DESC) = 1
            ) location
            USING(staff_id)
        INNER JOIN `{project_id}.normalized_state.state_staff` staff
            USING(staff_id)
        LEFT JOIN `{project_id}.reference_views.product_roster_materialized` r
            ON sei.external_id = r.external_id
            AND r.state_code = 'US_TN'
        WHERE role.state_code = "US_TN"
            AND role.end_date IS NULL
            AND role.role_subtype_raw_text = "CNSK"
            
"""
