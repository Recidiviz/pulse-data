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
that have both fields populated. The Common_Statute_NCIC_Code field stores legacy NCIC
codes, so these two fields are coalesced before being passed to the ingest mapping. 

The Common_Statute_Number field is populated in the raw data if and only if the CST code
is used, so it can be used to determine if a given row uses the legacy or CST system."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT  
  ot.SID, 
  ot.CASE_NUMBER, 
  ot.RecID, 
  ot.LEVEL, 
  ot.COUNTS, 
  ot.OFFENSEDATE, 
  ot.COUNTY, 
  ot.REQUIRES_REGISTRATION,
  -- Exactly one of these values is populated in each row. 
  COALESCE(ot.Common_Statute_NCIC_Code, ot.CODE) as ncic_code,
  oc.JUDGE,
  COALESCE(cst.DROPDOWN_DESCRIPTION, xref.DESCRIPTION) AS charge_description
FROM {docstars_offensestable} ot
LEFT JOIN {docstars_offendercasestable} oc
USING (CASE_NUMBER)
LEFT JOIN {recidiviz_docstars_cst_ncic_code} xref
ON (COALESCE(ot.Common_Statute_NCIC_Code, ot.CODE) = xref.CODE)  
LEFT JOIN {RECIDIVIZ_REFERENCE_common_statute_table} cst
USING (Common_Statute_Number)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_nd",
    ingest_view_name="docstars_offensestable",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
