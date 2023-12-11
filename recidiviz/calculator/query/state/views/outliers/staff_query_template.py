#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2023 Recidiviz, Inc.
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
    today_between_start_date_and_nullable_end_date_clause,
    today_between_start_date_and_nullable_end_date_exclusive_clause,
)
from recidiviz.calculator.query.state.views.outliers.outliers_enabled_states import (
    get_outliers_enabled_states,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.outliers.outliers_configs import OUTLIERS_CONFIGS_BY_STATE


def staff_query_template(role: str) -> str:
    """
    Returns the full query for the given role, which is used in the Outliers product views.
    """
    state_queries = []

    if role == "SUPERVISION_OFFICER":
        source_tbl = f"""
        -- A supervision officer in the Outliers product is anyone that has open person assignment periods
        -- and metrics calculated, in which case we can assume they have an active caseload. 
        SELECT DISTINCT
            state_code,
            officer_id AS external_id
        FROM `{{project_id}}.aggregated_metrics.supervision_officer_metrics_person_assignment_sessions_materialized`
        WHERE
            -- Only include officers who have open person assignment periods
            {today_between_start_date_and_nullable_end_date_clause("assignment_date", "end_date")}
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

    for state in get_outliers_enabled_states():
        config = OUTLIERS_CONFIGS_BY_STATE[StateCode(state)]
        state_queries.append(
            f"""
    SELECT
        attrs.officer_id AS external_id,
        attrs.staff_id,
        attrs.state_code,
        staff.full_name,
        -- Note that the below hash will be different from the hash id for the user in the product_rosters view
        {get_pseudonymized_id_query_str("attrs.state_code || attrs.officer_id")} AS pseudonymized_id,
        staff.email,
        COALESCE(attrs.supervision_district_name,attrs.supervision_district_name_inferred) AS supervision_district,
        attrs.supervisor_staff_external_id AS supervisor_external_id,
        attrs.specialized_caseload_type_primary AS specialized_caseload_type,
    FROM ({source_tbl}) supervision_staff
    INNER JOIN `{{project_id}}.sessions.supervision_officer_attribute_sessions_materialized` attrs
        ON attrs.state_code = supervision_staff.state_code AND attrs.officer_id = supervision_staff.external_id
    INNER JOIN `{{project_id}}.normalized_state.state_staff` staff 
        ON attrs.staff_id = staff.staff_id AND attrs.state_code = staff.state_code
    WHERE staff.state_code = '{state}' 
      -- Get the staff's attributes from the current session
      AND {today_between_start_date_and_nullable_end_date_exclusive_clause("attrs.start_date", "attrs.end_date_exclusive")}
      {f"AND {config.supervision_staff_exclusions}" if config.supervision_staff_exclusions else ""}
"""
        )

    return "\n      UNION ALL\n".join(state_queries)
