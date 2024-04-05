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
"""View logic to prepare US_ND supervision staff data for Workflows"""

US_ND_SUPERVISION_STAFF_TEMPLATE = """
    WITH 
    caseload_staff_ids AS (
        SELECT DISTINCT
            # There is one US_ND staff who has multiple ids, which are mapped here to a single id for user management
            IFNULL(ids.external_id_mapped, officer_id) AS id,
            client.state_code,
            CAST(officers.status AS STRING) = "(1)" AS is_active,
            CONCAT(LOWER(loginname), "@nd.gov") AS email_address,
        FROM `{project_id}.{workflows_dataset}.client_record_materialized` client
        LEFT JOIN `{project_id}.{us_nd_raw_data_up_to_date_dataset}.docstars_officers_latest` officers
            ON officer_id = officers.OFFICER
        LEFT JOIN `{project_id}.{static_reference_tables_dataset}.agent_multiple_ids_map` ids
            ON officer_id = ids.external_id_to_map AND client.state_code = ids.state_code 
        WHERE client.state_code = "US_ND"
    )
    , leadership_staff_with_caseload AS (
        SELECT
            external_id AS id,
            r.state_code,
            districts.district_name AS district,
            email_address AS email,
            TRUE AS has_caseload,
            FALSE AS has_facility_caseload,
            UPPER(first_name) as given_names,
            UPPER(last_name) as surname,
            CAST(NULL AS STRING) AS role_subtype,
        FROM `{project_id}.{reference_views_dataset}.product_roster_materialized` r
        LEFT JOIN `{project_id}.{vitals_report_dataset}.supervision_officers_and_districts_materialized` districts
            ON r.state_code = districts.state_code 
            AND r.external_id = districts.supervising_officer_external_id
        WHERE r.state_code = 'US_ND'
        AND r.role IN ('leadership_role', 'supervision_leadership')
        AND external_id IN (SELECT id FROM caseload_staff_ids)
    )
    , caseload_staff AS (
        SELECT
            ids.id,
            ids.state_code,
            districts.district_name AS district,
            ids.email_address AS email,
            true AS has_caseload,
            false as has_facility_caseload,
            names.given_names as given_names,
            names.surname as surname,
            CAST(NULL AS STRING) AS role_subtype,
        FROM caseload_staff_ids ids
        LEFT JOIN `{project_id}.reference_views.state_staff_with_names` names
            ON ids.id = names.legacy_supervising_officer_external_id 
            AND ids.state_code = names.state_code
        LEFT JOIN `{project_id}.{vitals_report_dataset}.supervision_officers_and_districts_materialized` districts
            ON ids.state_code = districts.state_code 
            AND ids.id = districts.supervising_officer_external_id
        WHERE is_active
            AND ids.id NOT IN (SELECT id from leadership_staff_with_caseload)
    )
    SELECT 
        {columns_minus_supervisor_id}
    FROM caseload_staff
    
    UNION ALL
    
    SELECT 
        {columns_minus_supervisor_id}
    FROM leadership_staff_with_caseload
"""
