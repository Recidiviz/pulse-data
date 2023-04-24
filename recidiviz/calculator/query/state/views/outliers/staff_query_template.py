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

OUTLIERS_STATES = ["US_PA"]


def staff_query_template(role: str) -> str:
    return f"""
WITH staff AS (
  SELECT * except (full_name),
    JSON_EXTRACT_SCALAR(full_name, '$.given_names')  AS given_names,
    JSON_EXTRACT_SCALAR(full_name, '$.surname')  AS surname
  FROM `{{project_id}}.{{normalized_state_dataset}}.state_staff` staff
)

SELECT 
  ids.external_id,
  staff.staff_id,
  staff.state_code,
  TRIM(CONCAT(COALESCE(given_names, ''), ' ', COALESCE(surname, ''))) AS full_name,
  staff.email,
  location.location_external_id
FROM staff
INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_staff_external_id` ids 
  USING (state_code, staff_id)
INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_staff_location_period` location
  USING (state_code, staff_id)
INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_staff_role_period` role
  USING (state_code, staff_id)
WHERE staff.state_code IN ({list_to_query_string(OUTLIERS_STATES, quoted=True)}) 
  AND {today_between_start_date_and_nullable_end_date_exclusive_clause("location.start_date", "location.end_date")}
  AND {today_between_start_date_and_nullable_end_date_exclusive_clause("role.start_date", "role.end_date")}
  AND role.role_subtype = '{role}'
    """
