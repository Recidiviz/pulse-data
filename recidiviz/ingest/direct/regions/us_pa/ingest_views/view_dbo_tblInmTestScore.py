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

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
    WITH all_test_scores as (
      SELECT Test_Id, Control_Number, Test_Desc, Inmate_number, Test_Dt, Fac_Cd, Test_Score, ModBy_EmpNum, LstMod_Dt, AsmtVer_Num, Fab_ind, RSTRvsd_Flg
  FROM (
    SELECT 
      *, 
      ROW_NUMBER() OVER (PARTITION BY Control_Number, Inmate_number, Test_Id, AsmtVer_Num ORDER BY is_history_row ASC) AS entry_priority
    FROM (
      SELECT
        t.Test_Id,
        t.Control_Number,
        t.Test_Desc,
        t.Inmate_number,
        t.Test_Dt,
        t.Fac_Cd,
        t.Test_Score,
        t.ModBy_EmpNum,
        t.LstMod_Dt,
        t.AsmtVer_Num,
        t.Fab_ind,
        t.RSTRvsd_Flg,
        0 AS is_history_row
    FROM {dbo_tblInmTestScore} t
      UNION ALL
      SELECT
        ht.Test_Id,
        ht.Control_Number,
        ht.Test_Desc,
        ht.Inmate_number,
        ht.Test_Dt,
        ht.Fac_Cd,
        ht.Test_Score,
        ht.ModBy_EmpNum,
        ht.LstMod_Dt,
        ht.AsmtVer_Num,
        ht.Fab_ind,
        ht.RSTRvsd_Flg,
        1 AS is_history_row
    FROM {dbo_tblInmTestScoreHist} ht
    ) as programs
  ) as programs_with_priority
  WHERE entry_priority = 1
)
SELECT *
FROM all_test_scores
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_pa",
    ingest_view_name="dbo_tblInmTestScore",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
