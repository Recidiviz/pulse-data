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
"""Template for listing out Utah Parole Officers."""

US_UT_REENTRY_SUPERVISION_OFFICER_QUERY_TEMPLATE = """
WITH
  assignments AS (
  SELECT
    supervising_officer_staff_external_id AS external_id,
    ARRAY_AGG(DISTINCT person_ext.external_id
    ORDER BY
      person_ext.external_id) AS client_ids
  FROM
    `{project_id}.{normalized_state_dataset}.state_supervision_period` supervision
  LEFT JOIN
    `{project_id}.{normalized_state_dataset}.state_person_external_id` person_ext
  ON
    supervision.person_id = person_ext.person_id
  WHERE
    supervision.state_code = "US_UT"
    AND supervision.termination_date IS NULL
    AND supervision.supervising_officer_staff_external_id IS NOT NULL
    AND person_ext.id_type = 'US_UT_DOC'
  GROUP BY
    supervision.supervising_officer_staff_external_id)
SELECT
  "US_UT" AS state_code,
  assignments.external_id,
  staff.email,
  full_name,
  client_ids
FROM
  assignments
LEFT JOIN
  `{project_id}.{normalized_state_dataset}.state_staff_external_id` staff_ext
ON
  assignments.external_id = staff_ext.external_id
  AND id_type = "US_UT_USR_ID"
LEFT JOIN
  `{project_id}.{normalized_state_dataset}.state_staff` staff
ON
  staff_ext.staff_id = staff.staff_id
"""
