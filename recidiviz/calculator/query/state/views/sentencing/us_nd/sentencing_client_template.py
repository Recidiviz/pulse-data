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
"""View logic to prepare US_ND Sentencing clients data for PSI tools

This view pulls in the information about each client that appears in ND's PSI data.

We use the RecID as the case id because each report is a "case" for us (even though the report can cover multiple charges/cases).
"""

US_ND_SENTENCING_CLIENT_TEMPLATE = """
WITH
  psi AS (
  SELECT
    REPLACE(SID, ',', '') AS SID,
    STRING_AGG(CONCAT('"',REPLACE(RecId, ',', ''),'"'), ','
    ORDER BY
      LAST_UPDATE) AS case_ids
  FROM
    `{project_id}.{us_nd_raw_data_up_to_date_dataset}.docstars_psi_latest`
  WHERE
    -- Only pick cases that have been completed in the last three months or are not yet completed
    -- AND were ordered within the past year (there are some very old cases that were never completed)
    (DATE(DATE_COM) > DATE_SUB(CURRENT_DATE, INTERVAL 3 MONTH)
      OR DATE_COM IS NULL)
    AND (DATE(DATE_DUE) > DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR))
  GROUP BY
    SID)
SELECT
  DISTINCT "US_ND" AS state_code,
  psi.SID AS external_id,
  COALESCE( p.full_name, TO_JSON_STRING(STRUCT( "UNKNOWN" AS given_names,
        "" AS middle_names,
        "" AS name_suffix,
        "UNKNOWN" AS surname)) ) AS full_name,
  p.birthdate AS birth_date,
  p.gender AS gender,
  -- If someone does not live in ND, their "County of Residence" will be null.
  CASE
    WHEN JSON_EXTRACT_SCALAR(a.address_metadata, '$.state_residence') != 'ND' THEN null
  -- Counties are stored in the format "US_ND_COUNTY_NAME". This cleans that so the output is only "COUNTY NAME".
    ELSE REGEXP_REPLACE(REGEXP_REPLACE(a.address_county, r'US_ND_', ''), '_', ' ')
END
  AS county,
  CONCAT('[', case_ids,']') AS case_ids,
  CAST(NULL AS STRING) AS district,
FROM
  psi
LEFT JOIN
  `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
ON
  (psi.SID = external_id
    AND id_type = 'US_ND_SID')
LEFT JOIN
  `{project_id}.{normalized_state_dataset}.state_person` p
USING
  (person_id)
LEFT JOIN
  `{project_id}.{normalized_state_dataset}.state_person_address_period` a
USING
  (person_id)
  -- Choose the most recent county of residence
QUALIFY
  ROW_NUMBER() OVER (w) = 1
WINDOW
  w AS (
  PARTITION BY
    psi.SID
  ORDER BY
    IFNULL(address_end_date, '9999-12-31') DESC)
"""
