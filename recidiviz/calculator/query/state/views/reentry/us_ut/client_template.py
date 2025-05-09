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
"""Template for listing out Utah clients (Incarcerated and Supervised JII)."""

US_UT_REENTRY_CLIENT_QUERY_TEMPLATE = """
WITH
  current_facilities_assignments AS (
    -- Join with `state_person_external_id` and select the person_id here
    -- so that the union below is always using internal person ids,
    -- making the logic below easier to understand
  SELECT
    UPPER(case_wrkr_usr_id) AS staff_id,
    person_ext.person_id AS person_id
  FROM
    `{project_id}.{us_ut_raw_data_up_to_date_dataset}.case_mgr_ofndr_latest` case_mgr_ofndr
  LEFT JOIN
    `{project_id}.{normalized_state_dataset}.state_person_external_id` person_ext
  ON
    case_mgr_ofndr.ofndr_num = person_ext.external_id
    AND person_ext.id_type = 'US_UT_DOC'
  WHERE
    end_dt IS NULL),
  all_staff_assignments AS (
  SELECT
    *
  FROM (
      -- UNION incarceration assignments and supervision assignments
      (
      SELECT
        staff_id,
        person_id
      FROM
        current_facilities_assignments)
    UNION ALL (
      SELECT
        supervising_officer_staff_external_id AS staff_id,
        person_id,
      FROM
        `{project_id}.{normalized_state_dataset}.state_supervision_period`
      WHERE
        state_code = "US_UT"
        AND termination_date IS NULL
        AND supervising_officer_staff_external_id IS NOT NULL )))
SELECT
  "US_UT" AS state_code,
  external_id,
  full_name,
  gender,
  birthdate,
  current_address,
  staff_id,
FROM
  all_staff_assignments
LEFT JOIN
  `{project_id}.{normalized_state_dataset}.state_person` person
ON
  all_staff_assignments.person_id = person.person_id
LEFT JOIN
  `{project_id}.{normalized_state_dataset}.state_person_external_id` person_ext
ON
  person.person_id = person_ext.person_id
  AND person_ext.id_type = 'US_UT_DOC'
"""
