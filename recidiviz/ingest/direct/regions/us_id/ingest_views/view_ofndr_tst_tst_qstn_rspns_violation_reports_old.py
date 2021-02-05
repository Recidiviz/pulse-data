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

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import \
    DirectIngestPreProcessedIngestViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.ingest.direct.regions.us_id.ingest_views.templates_test_questions import \
    question_numbers_with_descriptive_answers_view_fragment

VIEW_QUERY_TEMPLATE = f"""WITH
    {question_numbers_with_descriptive_answers_view_fragment(test_id='204')}
    SELECT 
      ofndr_num,
      body_loc_cd,
      ofndr_tst_id,
      assess_tst_id,
      tst_dt,
      score_by_name,
      # Question 1-1 is violation date, which we already have access to from tst_dt
      MAX(IF(qstn_num='1-2', qstn_answer, NULL)) AS violation_types,
      MAX(IF(qstn_num='1-3', qstn_answer, NULL)) AS new_crime_types,
      MAX(IF(qstn_num='1-4', qstn_answer, NULL)) AS pv_initiated_by_prosecutor,
      MAX(IF(qstn_num='1-5', qstn_answer, NULL)) AS parolee_placement_recommendation,
      MAX(IF(qstn_num='1-6', qstn_answer, NULL)) AS probationer_placement_recommendation,
      MAX(IF(qstn_num='1-7', qstn_answer, NULL)) AS legal_status,
    FROM 
      qstn_nums_with_descriptive_answers
    GROUP BY
      ofndr_num,
      body_loc_cd,
      ofndr_tst_id,
      assess_tst_id,
      tst_dt,
      score_by_name
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region='us_id',
    ingest_view_name='ofndr_tst_tst_qstn_rspns_violation_reports_old',
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols='ofndr_num, ofndr_tst_id',
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
