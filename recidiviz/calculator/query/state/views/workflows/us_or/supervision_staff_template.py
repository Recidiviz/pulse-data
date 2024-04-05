#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2024 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""View logic to generate staff records for US_OR workflows"""

US_OR_SUPERVISION_STAFF_TEMPLATE = """
    WITH 
    caseload_staff_ids AS (
        SELECT DISTINCT
            officer_id AS external_id,
            client.state_code,
        FROM `{project_id}.{workflows_dataset}.client_record_materialized` client
        LEFT JOIN `{project_id}.{static_reference_tables_dataset}.agent_multiple_ids_map` ids
            ON officer_id = ids.external_id_to_map AND client.state_code = ids.state_code 
        WHERE client.state_code = "US_OR"
    )
    , caseload_staff AS (
        SELECT external_id AS id,
          state_code,
          CAST(NULL AS STRING) AS district,
          email,
          true AS has_caseload,
          false AS has_facility_caseload,
          JSON_VALUE(full_name, "$.given_names") AS given_names,
          JSON_VALUE(full_name, "$.surname") AS surname,
          CAST(NULL AS STRING) AS role_subtype,
        FROM caseload_staff_ids
        LEFT JOIN `{project_id}.{normalized_state_dataset}.state_staff_external_id`
            USING (state_code, external_id)
        LEFT JOIN `{project_id}.{normalized_state_dataset}.state_staff`
            USING (state_code, staff_id)
        WHERE state_code = "US_OR"
            AND id_type = "US_OR_CASELOAD"
    )
    
    SELECT {columns_minus_supervisor_id}
    FROM caseload_staff
"""
