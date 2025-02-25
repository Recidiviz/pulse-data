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
"""Query that generates information for all violation reports from the deprecated US_ID report form (test_id = 204).
This is necessary to gather historical violation report info.
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
  WITH stacked as (
    SELECT
      ofndr_num,
      body_loc_cd,
      ofndr_tst_id, 
      assess_tst_id,
      tst_dt,
      score_by_name,
      CONCAT(assess_qstn_num, '-', tst_sctn_num) AS qstn_num, 
      STRING_AGG(qstn_choice_desc ORDER BY qstn_choice_desc) AS qstn_answer
    FROM 
      {ofndr_tst}
    LEFT JOIN 
      {tst_qstn_rspns}
    USING 
      (ofndr_tst_id, assess_tst_id)
    LEFT JOIN 
      {assess_qstn_choice}
    USING 
      (assess_tst_id, tst_sctn_num, qstn_choice_num, assess_qstn_num)
    WHERE 
      assess_tst_id in ('204', '210')
    GROUP BY 
      ofndr_num, 
      body_loc_cd,
      ofndr_tst_id, 
      assess_tst_id,
      tst_dt,
      score_by_name,
      assess_qstn_num, 
      tst_sctn_num
  )

  SELECT 
    ofndr_num,
    body_loc_cd,
    ofndr_tst_id,
    assess_tst_id,
    tst_dt,
    score_by_name,
    # Question 1-1 is violation date, which we already have access to from tst_dt
    MAX(IF((qstn_num='1-2' and assess_tst_id='204') or (qstn_num='2-2' and assess_tst_id='210'), qstn_answer, NULL)) AS violation_types,
    MAX(IF((qstn_num='1-3' and assess_tst_id='204') or (qstn_num='3-3' and assess_tst_id='210'), qstn_answer, NULL)) AS new_crime_types,
    MAX(IF((qstn_num='1-4' and assess_tst_id='204'), qstn_answer, NULL)) AS pv_initiated_by_prosecutor,
    MAX(IF((qstn_num='1-5' and assess_tst_id='204') or (qstn_num='4-4' and assess_tst_id='210'), qstn_answer, NULL)) AS parolee_placement_recommendation,
    MAX(IF((qstn_num='1-6' and assess_tst_id='204') or (qstn_num='5-5' and assess_tst_id='210'), qstn_answer, NULL)) AS probationer_placement_recommendation,
    MAX(IF((qstn_num='1-7' and assess_tst_id='204') or (qstn_num='6-6' and assess_tst_id='210'), qstn_answer, NULL)) AS legal_status,
  FROM 
    stacked
  GROUP BY
    ofndr_num,
    body_loc_cd,
    ofndr_tst_id,
    assess_tst_id,
    tst_dt,
    score_by_name
    
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ix",
    ingest_view_name="supervision_violation_legacy",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
