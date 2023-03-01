# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Query containing offenses table information.

The raw data refers to offenses by either the legacy NCIC code or the Common Statute
NCIC Code. These are mututally exclusive - there are currently no rows in the raw data
that have both fields populated. The descriptions and violence indicators for these codes
come from distinct reference files. Because the legacy NCIC code reference is shared 
across states and the CST code reference file is ND-specific, information for rows with
CST codes is populated in the view; info for legacy NCIC codes is populated in the mapping.

The Common_Statute_Number field is populated in the raw data if and only if the CST code
is used, so it can be used to determine if a given row uses the legacy or CST system."""

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH base_offenses AS (
SELECT ot.*,
oc.JUDGE,
FROM {docstars_offensestable} ot
LEFT JOIN {docstars_offendercasestable} oc
USING (CASE_NUMBER)
),
offenses_with_desc AS (
  SELECT
    base.*,
    UPPER(cst.DESCRIPTION) AS description
  FROM base_offenses base
  LEFT JOIN {recidiviz_docstars_cst_ncic_code} cst
  ON base.Common_Statute_NCIC_Code = cst.CODE
),
offenses_with_violence_flag AS (
  SELECT off.*,
  CASE 
  WHEN Common_Statute_NCIC_Code IS NOT NULL AND CrimCodeTxt IS NULL AND description IS NOT NULL THEN 'N'
  WHEN Common_Statute_NCIC_Code IS NOT NULL AND CrimCodeTxt IS NOT NULL AND description IS NOT NULL THEN 'Y'
  ELSE NULL
  END AS violence_flag
FROM offenses_with_desc off
LEFT JOIN {RECIDIVIZ_REF_violent_crimes_codes} vc
ON off.Common_Statute_NCIC_Code = vc.CrimCodeTxt
)
SELECT  LEVEL, COUNTS, OFFENSEDATE, COUNTY, SID, CASE_NUMBER, RecID, COUNT, COURT_NUMBER, 
INACTIVEDATE, RecDate, YEAR, LAST_UPDATE, CREATED_BY, MASTER_OFFENSE_IND,REQUIRES_REGISTRATION,
Common_Statute_Number, JUDGE, description, violence_flag,
COALESCE(Common_Statute_NCIC_Code, CODE) as ncic_code
FROM offenses_with_violence_flag
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_nd",
    ingest_view_name="docstars_offensestable",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="RecID",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
