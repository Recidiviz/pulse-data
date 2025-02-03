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
  psi AS (
  SELECT
    DISTINCT UPPER(TRIM(SPLIT(NAME, ',')[
      OFFSET
        (0)])) AS surname,
    UPPER(TRIM(SPLIT(NAME, ',')[SAFE_OFFSET(1)])) AS given_names,
    STRING_AGG(CONCAT('"',REPLACE(RecId, ',', ''),'"'), ','
    ORDER BY
      CAST(RecDate AS DATETIME)) AS case_ids,
  FROM
    `{project_id}.{us_nd_raw_data_up_to_date_dataset}.docstars_psi_latest`
  WHERE
    -- Only pick cases that have been completed in the last three months or are not yet completed
    -- AND were ordered within the past year (there are some very old cases that were never completed)
    (DATE(DATE_COM) > DATE_SUB(CURRENT_DATE, INTERVAL 3 MONTH)
      OR DATE_COM IS NULL)
    AND (DATE(DATE_DUE) > DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR))
  GROUP BY
    surname,
    given_names),
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
  external_id,
  s.full_name,
  s.email,
  case_ids
FROM
  psi
LEFT JOIN
  staff s
ON
  ( psi.surname = s.surname
    -- This catches instances where PSI full name = MITCH and ingested full name = MITCHEL
    -- as well as instances where PSI full name = VALERIE and ingested full name = VAL
    -- We may want to work in similar protections for surname?
    AND( psi.given_names LIKE CONCAT(UPPER(s.given_names), '%')
      OR UPPER(s.given_names) LIKE CONCAT(psi.given_names, '%')))
"""
