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
"""Template for listing out Utah Case Managers."""

US_UT_REENTRY_CASE_MANAGER_QUERY_TEMPLATE = """
WITH
  assignments AS (
  SELECT
    case_wrkr_usr_id AS staff_external_id,
    ARRAY_AGG(person_ext.external_id
    ORDER BY
      external_id) AS client_ids
  FROM
    `{project_id}.{us_ut_raw_data_up_to_date_dataset}.case_mgr_ofndr_latest` case_mgr_ofndr
  LEFT JOIN
    `{project_id}.{normalized_state_dataset}.state_person_external_id` person_ext
  ON
    case_mgr_ofndr.ofndr_num = person_ext.external_id
  WHERE
    person_ext.id_type = 'US_UT_DOC'
  GROUP BY
    staff_external_id)
SELECT
  "US_UT" AS state_code,
  external_id,
  staff.email,
  full_name,
  client_ids,
FROM
  assignments
LEFT JOIN
  `{project_id}.{normalized_state_dataset}.state_staff_external_id` staff_ext
ON
  LOWER(assignments.staff_external_id) = LOWER(staff_ext.external_id)
  AND id_type = "US_UT_USR_ID"
LEFT JOIN
  `{project_id}.{normalized_state_dataset}.state_staff` staff
ON
  staff_ext.staff_id = staff.staff_id
"""
