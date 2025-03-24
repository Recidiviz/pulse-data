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
"""Query that generates info for drug screens in the Utah DOC."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT
  t.ofndr_num,
  t.tst_id,
  CAST(LEFT(t.tst_dt, 10) AS DATE) AS tst_dt,
  st.smpl_typ_desc,
  s.sbstnc_sbstnc_desc,
  r.sbstnc_found_flg,
  r.admit_use_flg,
  r.med_invalidate_flg
FROM
  {sbstnc_tst} t
JOIN
  {sbstnc_rslt} r
USING
  (tst_id)
JOIN
  {sbstnc_sbstnc_cd} s
ON
  (r.sbstnc_cd = s.sbstnc_sbstnc_cd)
LEFT JOIN
  {smpl_typ_cd} st
USING
  (smpl_typ_cd)
WHERE 
  tst_dt IS NOT NULL
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ut",
    ingest_view_name="drug_screen",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
