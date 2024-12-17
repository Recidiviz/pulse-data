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
 offense AS
 (SELECT
    COURT_NUMBER,
    COUNTY
  FROM
    `{project_id}.{us_nd_raw_data_up_to_date_dataset}.docstars_offensestable_latest`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY COURT_NUMBER ORDER BY IF(COUNTY IS NULL, 1, 0), RecDate DESC) = 1)
SELECT
  "US_ND" AS state_code,
  REPLACE(psi.RecID,',','') AS external_id,
  REPLACE(psi.SID,',','') AS client_id,
  -- TODO(#35882): Use real external ids once available
  UPPER(NAME) AS staff_id,
  DATE_DUE AS due_date,
  null as lsir_score,
  CAST(null AS STRING) as lsir_level,
  CAST(null AS STRING) as report_type,
  COALESCE(offense1.COUNTY, offense2.COUNTY, offense3.COUNTY) AS county,
  CAST(null AS STRING) as district,
FROM
  `{project_id}.{us_nd_raw_data_up_to_date_dataset}.docstars_psi_latest` psi
LEFT JOIN
  offense as offense1
ON
  (COURT1 = offense1.COURT_NUMBER)
LEFT JOIN
  offense as offense2
ON
  (COURT2 = offense2.COURT_NUMBER)
LEFT JOIN
  offense as offense3
ON
  (COURT3 = offense3.COURT_NUMBER)
WHERE
  -- Make sure that case is either not completed or completed within the last three months, and it has psi staff assigned to it (this should be true for reports ordered in the past ~7 years)
  (DATE(psi.DATE_COM) > DATE_SUB(CURRENT_DATE, INTERVAL 3 MONTH) OR psi.DATE_COM IS NULL) AND NAME IS NOT NULL
"""
