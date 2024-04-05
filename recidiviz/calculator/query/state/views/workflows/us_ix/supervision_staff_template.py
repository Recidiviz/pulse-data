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
            COALESCE(
                CASE s.supervision_district_id
                    WHEN "DISTRICT 1" THEN "DISTRICT OFFICE 1, COEUR D'ALENE"
                    WHEN "DISTRICT 2" THEN "DISTRICT OFFICE 2, LEWISTON"
                    WHEN "DISTRICT 3" THEN "DISTRICT OFFICE 3, CALDWELL"
                    WHEN "DISTRICT 4" THEN "DISTRICT OFFICE 4, BOISE"
                    WHEN "DISTRICT 5" THEN "DISTRICT OFFICE 5, TWIN FALLS"
                    WHEN "DISTRICT 6" THEN "DISTRICT OFFICE 6, POCATELLO"
                    WHEN "DISTRICT 7" THEN "DISTRICT OFFICE 7, IDAHO FALLS"
                    END,
                -- Fall back to district from the product roster if we don't have it in our data
                r.district) AS district,
            s.email,
            true AS has_caseload,
            false AS has_facility_caseload,
            s.given_names,
            s.surname,
            CAST(NULL AS STRING) AS role_subtype
        FROM caseload_staff_ids ids
        LEFT JOIN `{project_id}.reference_views.current_staff_materialized` s
            ON UPPER(ids.id) = UPPER(s.external_id)
            AND ids.state_code = s.state_code
        LEFT JOIN `{project_id}.reference_views.product_roster_materialized` r
            ON UPPER(ids.id) = UPPER(r.external_id)
            -- The users in the roster all have US_ID state code
            AND r.state_code = 'US_ID'
    )
    
    SELECT 
        {columns_minus_supervisor_id}
    FROM caseload_staff
"""
