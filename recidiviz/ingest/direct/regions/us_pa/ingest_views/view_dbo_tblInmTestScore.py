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
"""Query containing information about various assessments."""

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import \
    DirectIngestPreProcessedIngestViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH all_test_scores AS (
    SELECT 
      Test_Id,
      Control_Number,
      Test_Desc,
      Inmate_number,
      Test_Dt,
      Fac_Cd,
      Test_Score,
      ModBy_EmpNum,
      LstMod_Dt,
      AsmtVer_Num,
      Fab_ind,
      RSTRvsd_Flg
    FROM {dbo_tblInmTestScore}
    UNION DISTINCT
    SELECT 
      Test_Id,
      Control_Number,
      Test_Desc,
      Inmate_number,
      Test_Dt,
      Fac_Cd,
      Test_Score,
      ModBy_EmpNum,
      LstMod_Dt,
      AsmtVer_Num,
      Fab_ind,
      RSTRvsd_Flg
    FROM {dbo_tblInmTestScoreHist}
)
SELECT *
FROM all_test_scores
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region='us_pa',
    ingest_view_name='dbo_tblInmTestScore',
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols='Control_Number, Inmate_number',
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
