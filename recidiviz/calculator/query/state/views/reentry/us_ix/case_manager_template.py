#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2025 Recidiviz, Inc.
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
"""Template for listing out Idaho Case Managers."""

US_IX_REENTRY_CASE_MANAGER_QUERY_TEMPLATE = """
WITH
  assignments AS (
  SELECT
    incarceration_staff_assignment_id,
    ARRAY_AGG(person_ext.external_id
    ORDER BY
      external_id) AS client_ids
  FROM
    `{project_id}.sessions.incarceration_staff_assignment_sessions_preprocessed_materialized` asa
  LEFT JOIN
    `{project_id}.{normalized_state_dataset}.state_person_external_id` person_ext
  ON
    asa.person_id = person_ext.person_id
  WHERE
    asa.state_code = "US_IX"
    AND end_date_exclusive IS NULL
    AND person_ext.id_type = 'US_IX_DOC'
  GROUP BY
    incarceration_staff_assignment_id)
SELECT
  "US_IX" AS state_code,
  external_id,
  staff.email,
  full_name,
  client_ids,
FROM
  assignments
LEFT JOIN
  `{project_id}.{normalized_state_dataset}.state_staff` staff
ON
  assignments.incarceration_staff_assignment_id = staff.staff_id
LEFT JOIN
  `{project_id}.{normalized_state_dataset}.state_staff_external_id` staff_ext
ON
  staff.staff_id = staff_ext.staff_id
  AND id_type = 'US_IX_EMPLOYEE'
"""
