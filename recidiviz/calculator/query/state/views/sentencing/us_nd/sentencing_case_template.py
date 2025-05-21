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
"""View logic to prepare US_ND Sentencing case data for PSI tools.

It's possible an investigation has multiple court cases associated with it, and therefore multiple counties of sentencing. In reality, this rarely happens, so we just pick the first non-null county.
"""

US_ND_SENTENCING_CASE_TEMPLATE = """
WITH
  psi AS (
  SELECT
    *,
    UPPER(TRIM(SPLIT(NAME, ',')[
      OFFSET
        (0)])) AS surname,
    UPPER(TRIM(SPLIT(NAME, ',')[SAFE_OFFSET(1)])) AS given_names,
  FROM
    `{project_id}.{us_nd_raw_data_up_to_date_dataset}.docstars_psi_latest`),
  staff AS (
  SELECT
    *
  FROM (
    SELECT
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
    rn = 1),
  offense AS (
  SELECT
    COURT_NUMBER,
    COUNTY
  FROM
    `{project_id}.{us_nd_raw_data_up_to_date_dataset}.docstars_offensestable_latest`
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY COURT_NUMBER ORDER BY IF (COUNTY IS NULL, 1, 0),
      RecDate DESC) = 1),
  court_to_county AS (
  SELECT
    COURT_NUMBER,
    location_name AS county
  FROM
    offense
  LEFT JOIN
    `{project_id}.reference_views.location_metadata_materialized` lmm
  ON
    -- strip the leading zeros an format as 'COUNTY-X'
    CONCAT('COUNTY-', LTRIM(COUNTY, '0')) = lmm.location_external_id
    AND lmm.state_code = "US_ND" )
SELECT
  "US_ND" AS state_code,
  REPLACE(psi.RecID,',','') AS external_id,
  REPLACE(psi.SID,',','') AS client_id,
  s.external_id AS staff_id,
  DATE(DATE_DUE) AS due_date,
  DATE(DATE_COM) AS completion_date,
  CAST(NULL AS DATE) AS sentence_date,
  DATE(DATE_ORD) AS assigned_date,
  NULL AS lsir_score,
  CAST(NULL AS STRING) AS lsir_level,
  CAST(NULL AS STRING) AS report_type,
  -- Pick the first non-null county, if one exists
  COALESCE(court_to_county1.county, court_to_county2.county, court_to_county3.county) AS county,
  CAST(NULL AS STRING) AS district,
  CAST(NULL AS STRING) AS investigation_status,
  "0" AS employee_inactive
FROM
  psi
LEFT JOIN
  staff s
ON
  -- JOIN on the external id if it is available, otherwise try a fuzzy matching to the name
  -- This is because only cases after April 2025 have the PSI_WRITER_OFFICER_ID field set
  ( psi.PSI_WRITER_OFFICER_ID IS NOT NULL
    AND psi.PSI_WRITER_OFFICER_ID = s.external_id )
  OR ( psi.surname = s.surname
    -- This catches instances where PSI full name = MITCH and ingested full name = MITCHEL
    -- as well as instances where PSI full name = VALERIE and ingested full name = VAL
    -- We may want to work in similar protections for surname?
    AND( psi.given_names LIKE CONCAT(UPPER(s.given_names), '%')
      OR UPPER(s.given_names) LIKE CONCAT(psi.given_names, '%')))
LEFT JOIN
  court_to_county AS court_to_county1
ON
  (COURT1 = court_to_county1.COURT_NUMBER)
LEFT JOIN
  court_to_county AS court_to_county2
ON
  (COURT2 = court_to_county2.COURT_NUMBER)
LEFT JOIN
  court_to_county AS court_to_county3
ON
  (COURT3 = court_to_county3.COURT_NUMBER)
"""
