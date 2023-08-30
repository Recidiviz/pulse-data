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
    list_to_query_string,
    today_between_start_date_and_nullable_end_date_exclusive_clause,
)
from recidiviz.calculator.query.state.views.outliers.outliers_enabled_states import (
    get_outliers_enabled_states,
)
from recidiviz.calculator.query.state.views.outliers.utils import (
    get_all_config_based_query_filters_query_str,
)


def staff_query_template(role: str) -> str:
    return f"""
SELECT
    attrs.officer_id AS external_id,
    attrs.staff_id,
    attrs.state_code,
    staff.full_name,
    staff.email,
    COALESCE(attrs.supervision_district,attrs.supervision_district_inferred) AS supervision_district,
    attrs.supervisor_staff_external_id AS supervisor_external_id,
    attrs.specialized_caseload_type_primary AS specialized_caseload_type,
FROM `{{project_id}}.{{sessions_dataset}}.supervision_officer_attribute_sessions_materialized` attrs
INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_staff` staff 
    USING (staff_id, state_code)
WHERE staff.state_code IN ({list_to_query_string(get_outliers_enabled_states(), quoted=True)}) 
  AND {today_between_start_date_and_nullable_end_date_exclusive_clause("start_date", "end_date_exclusive")}
  AND '{role}' IN UNNEST(attrs.role_subtype_array)
  AND {get_all_config_based_query_filters_query_str()}
    """
