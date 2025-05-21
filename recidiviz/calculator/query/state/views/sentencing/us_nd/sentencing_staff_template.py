# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""View logic to prepare US_ND SENTENCING staff data for PSI tools

This view pulls in the following pieces of information about each client that appears in 
ND's PSI data: 
- External ID
- Full name
- Email
- Case IDs
"""

US_ND_SENTENCING_STAFF_TEMPLATE = """
WITH
  -- Select their names and external_ids because only cases after April 2025 have the PSI_WRITER_OFFICER_ID field set
  psi AS (
  SELECT
    UPPER(TRIM(SPLIT(NAME, ',')[
      OFFSET
        (0)])) AS surname,
    UPPER(TRIM(SPLIT(NAME, ',')[SAFE_OFFSET(1)])) AS given_names,
    PSI_WRITER_OFFICER_ID AS external_id,
    REPLACE(RecId, ',', '') AS case_id
  FROM
    `{project_id}.{us_nd_raw_data_up_to_date_dataset}.docstars_psi_latest`),
  staff AS (
  SELECT
    *
  FROM (
    SELECT
      full_name,
      JSON_EXTRACT_SCALAR(s.full_name, '$.surname') AS surname,
      JSON_EXTRACT_SCALAR(s.full_name, '$.given_names') AS given_names,
      external_id,
      email,
      ROW_NUMBER() OVER(PARTITION BY email ORDER BY external_id ASC) AS rn
    FROM
      `{project_id}.{normalized_state_dataset}.state_staff` s
    JOIN
      `{project_id}.{normalized_state_dataset}.state_staff_external_id` sei
    ON
      (s.staff_id = sei.staff_id
        AND sei.state_code = "US_ND"
        AND sei.id_type = "US_ND_DOCSTARS_OFFICER") )
  WHERE
    rn = 1)
SELECT
  "US_ND" AS state_code,
  s.external_id,
  s.full_name,
  s.email,
  CONCAT('[', STRING_AGG(CONCAT('"',case_id,'"'), ','
    ORDER BY
      case_id),']') AS case_ids,
  CAST(NULL AS STRING) AS supervisor_id,
FROM
  psi
JOIN
  staff s
ON
  -- JOIN on the external id if it is available, otherwise try a fuzzy matching to the name
  ( psi.external_id IS NOT NULL
    AND psi.external_id = s.external_id )
  OR ( psi.surname = s.surname
    -- This catches instances where PSI full name = MITCH and ingested full name = MITCHEL
    -- as well as instances where PSI full name = VALERIE and ingested full name = VAL
    -- We may want to work in similar protections for surname?
    AND( psi.given_names LIKE CONCAT(UPPER(s.given_names), '%')
      OR UPPER(s.given_names) LIKE CONCAT(psi.given_names, '%')))
  -- It's safe to group by all of these because they are all coming from the same row in the staff table anyways and it makes the query simpler
GROUP BY
  s.external_id,
  s.full_name,
  s.email
"""
