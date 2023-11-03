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
"""View logic to prepare US_IX supervision staff data for Workflows"""

US_IX_SUPERVISION_STAFF_TEMPLATE = """
    WITH 
    caseload_staff_ids AS (
        SELECT DISTINCT
            IFNULL(ids.external_id_mapped, officer_id) AS id,
            client.state_code,
        FROM `{project_id}.{workflows_dataset}.client_record_materialized` client
        LEFT JOIN `{project_id}.{static_reference_tables_dataset}.agent_multiple_ids_map` ids
            ON officer_id = ids.external_id_to_map AND client.state_code = ids.state_code 
        WHERE client.state_code = "US_IX"
    )
    , caseload_staff AS (
        SELECT
            UPPER(ids.id) AS id,
            ids.state_code,
            COALESCE(districts.district_name, r.district) AS district,
            email_address AS email,
            true AS has_caseload,
            false AS has_facility_caseload,
            COALESCE(names.given_names, first_name) as given_names,
            COALESCE(names.surname, last_name) as surname,
            CAST(NULL AS STRING) AS role_subtype,
        FROM caseload_staff_ids ids
        LEFT JOIN `{project_id}.{reference_views_dataset}.product_roster_materialized` r
            ON UPPER(ids.id) = UPPER(r.external_id)
            # The users in the roster all have US_ID state code
            AND r.state_code = 'US_ID'
        LEFT JOIN `{project_id}.reference_views.state_staff_with_names` names
            ON UPPER(ids.id) = UPPER(names.legacy_supervising_officer_external_id) 
            AND ids.state_code = names.state_code
        LEFT JOIN `{project_id}.{vitals_report_dataset}.supervision_officers_and_districts_materialized` districts
            ON ids.state_code = districts.state_code 
            AND ids.id = districts.supervising_officer_external_id
    )
    
    SELECT 
        {columns}
    FROM caseload_staff
"""
