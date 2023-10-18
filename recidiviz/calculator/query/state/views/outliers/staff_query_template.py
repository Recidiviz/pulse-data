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
    today_between_start_date_and_nullable_end_date_exclusive_clause,
)
from recidiviz.calculator.query.state.views.outliers.outliers_enabled_states import (
    get_outliers_enabled_states,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.outliers.outliers_configs import OUTLIERS_CONFIGS_BY_STATE


def staff_query_template(role: str) -> str:

    state_queries = []

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
        COALESCE(attrs.supervision_district,attrs.supervision_district_inferred) AS supervision_district,
        attrs.supervisor_staff_external_id AS supervisor_external_id,
        attrs.specialized_caseload_type_primary AS specialized_caseload_type,
    FROM `{{project_id}}.sessions.supervision_officer_attribute_sessions_materialized` attrs
    INNER JOIN `{{project_id}}.normalized_state.state_staff` staff 
        USING (staff_id, state_code)
    WHERE staff.state_code = '{state}' 
      AND {today_between_start_date_and_nullable_end_date_exclusive_clause("start_date", "end_date_exclusive")}
      AND '{role}' IN UNNEST(attrs.role_subtype_array)
      {f"AND {config.supervision_staff_exclusions}" if config.supervision_staff_exclusions else ""}
"""
        )

    return "\n      UNION ALL\n".join(state_queries)
