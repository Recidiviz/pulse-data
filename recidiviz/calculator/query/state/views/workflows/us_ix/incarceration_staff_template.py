# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""View logic to prepare US_IX incarceration staff data for Workflows"""


from recidiviz.calculator.query.bq_utils import get_pseudonymized_id_query_str

US_IX_INCARCERATION_STAFF_TEMPLATE = f"""
    WITH
    caseload_staff_ids AS (
        SELECT DISTINCT
            officer_id AS external_id,
            state_code,
        FROM `{{project_id}}.{{workflows_dataset}}.resident_record_materialized`
        WHERE state_code = "US_IX"
        AND officer_id IS NOT NULL
    )
    , caseload_staff AS (
        SELECT DISTINCT
            ids.external_id AS id,
            ids.state_code,
            CAST(NULL AS STRING) AS district,
            LOWER(staff_with_names.email) AS email,
            UPPER(staff_with_names.given_names) AS given_names,
            UPPER(staff_with_names.surname) AS surname,
            "COUNSELOR" AS role_subtype,
            {get_pseudonymized_id_query_str("'US_ID' || ids.external_id")} AS pseudonymized_id 
        FROM caseload_staff_ids ids
        INNER JOIN `{{project_id}}.normalized_state.state_staff_external_id` sei
            USING(external_id, state_code)
        INNER JOIN `{{project_id}}.reference_views.state_staff_with_names` staff_with_names
            USING(staff_id)
    )
    SELECT 
        {{columns}}
    FROM caseload_staff
"""
