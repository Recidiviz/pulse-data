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
            LOWER(email) AS email,
            UPPER(given_names) AS given_names,
            UPPER(surname) AS surname,
            incarceration_staff_assignment_role_subtype AS role_subtype,
            {get_pseudonymized_id_query_str("'US_ID' || ids.external_id")} AS pseudonymized_id 
        FROM caseload_staff_ids ids
        INNER JOIN `{{project_id}}.normalized_state.state_staff_external_id`
            USING(external_id, state_code)
        INNER JOIN `{{project_id}}.reference_views.state_staff_with_names`
            USING(staff_id)
        LEFT JOIN `{{project_id}}.sessions.us_ix_incarceration_staff_assignment_sessions_preprocessed` isa
            ON ids.external_id = isa.incarceration_staff_assignment_external_id
    )
    SELECT 
        {{columns}}
    FROM caseload_staff
"""
