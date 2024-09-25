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
"""Helpers for querying normalized_state.state_staff and other staff-related tables"""
from recidiviz.calculator.query.bq_utils import (
    get_pseudonymized_id_query_str,
    list_to_query_string,
    today_between_start_date_and_nullable_end_date_clause,
)
from recidiviz.calculator.query.state.views.outliers.outliers_enabled_states import (
    get_outliers_enabled_states_for_bigquery,
)
from recidiviz.outliers.outliers_configs import get_outliers_backend_config


def staff_query_template(role: str) -> str:
    """
    Returns the full query for the given role, which is used in the Outliers product views.
    """
    state_queries = []

    if role == "SUPERVISION_OFFICER":
        source_tbl = f"""
        -- A supervision officer in the Outliers product is anyone that has open session 
        -- in supervision_officer_sessions, in which they are someone's supervising officer 
        SELECT DISTINCT
            state_code,
            supervising_officer_external_id AS external_id
        FROM `{{project_id}}.sessions.supervision_officer_sessions_materialized`
        WHERE
            -- Only include officers who have open officer sessions
            {today_between_start_date_and_nullable_end_date_clause("start_date", "end_date")}
    """
    elif role == "SUPERVISION_OFFICER_SUPERVISOR":
        source_tbl = f"""
        -- A supervision supervisor in the Outliers product is anyone that has an open supervisor period as a  
        -- supervisor. 
        SELECT DISTINCT state_code, supervisor_staff_external_id AS external_id
        FROM `{{project_id}}.normalized_state.state_staff_supervisor_period` sp
        WHERE {today_between_start_date_and_nullable_end_date_clause("start_date", "end_date")}
    """
    else:
        raise ValueError(f"Unexpected role provided: {'None' if not role else role}")

    for state in get_outliers_enabled_states_for_bigquery():
        config = get_outliers_backend_config(state)
        state_queries.append(
            f"""
    SELECT
        attrs.officer_id AS external_id,
        attrs.staff_id,
        attrs.state_code,
        staff.full_name,
        -- This pseudonymized_id will match the one for the user in the auth0 roster. Hashed
        -- attributes must be kept in sync with recidiviz.auth.helpers.generate_pseudonymized_id.
        {get_pseudonymized_id_query_str("IF(attrs.state_code = 'US_IX', 'US_ID', attrs.state_code) || attrs.officer_id")} AS pseudonymized_id,
        staff.email,
        COALESCE(attrs.supervision_district_name,attrs.supervision_district_name_inferred) AS supervision_district,
        -- TODO(#29942): Deprecate once array is deployed and fully in use
        attrs.supervisor_staff_external_id_array[SAFE_OFFSET(0)] AS supervisor_external_id,
        attrs.specialized_caseload_type_primary AS specialized_caseload_type,
        attrs.supervisor_staff_external_id_array AS supervisor_external_ids,
        IF(attrs.state_code = 'US_CA', attrs.supervision_office_name, attrs.supervision_unit_name) AS supervision_unit,
        assignment.earliest_person_assignment_date
    FROM ({source_tbl}) supervision_staff
    INNER JOIN attrs
        ON attrs.state_code = supervision_staff.state_code AND attrs.officer_id = supervision_staff.external_id 
    INNER JOIN `{{project_id}}.normalized_state.state_staff` staff 
        ON attrs.staff_id = staff.staff_id AND attrs.state_code = staff.state_code
    LEFT JOIN (
        SELECT state_code, officer_id, MIN(assignment_date) AS earliest_person_assignment_date
        FROM `{{project_id}}.aggregated_metrics.supervision_officer_metrics_person_assignment_sessions_materialized`
        GROUP BY 1,2
    ) assignment
        ON attrs.officer_id = assignment.officer_id AND attrs.state_code = assignment.state_code
    WHERE staff.state_code = '{state}' 
      {f"AND {config.supervision_staff_exclusions}" if config.supervision_staff_exclusions else ""}
"""
        )

    all_state_queries = "\n      UNION ALL\n".join(state_queries)

    return f"""
    WITH 
    attrs AS (
        -- Use the most recent session to get the staff's attributes from the most recent session
        SELECT *
        FROM `{{project_id}}.sessions.supervision_staff_attribute_sessions_materialized` 
        WHERE state_code IN ({list_to_query_string(get_outliers_enabled_states_for_bigquery(), quoted=True)})
        QUALIFY ROW_NUMBER() OVER(PARTITION BY state_code, officer_id ORDER BY COALESCE(end_date_exclusive, "9999-01-01") DESC) = 1
    )
    {all_state_queries}
"""
