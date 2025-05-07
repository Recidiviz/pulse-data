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

"""Query containing assessment information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- Collect all questions and answers from the "Alcohol/Drug Problem" section of the LS/RNR
-- assessment to include as metadata where applicable.
lsrnr_drug_and_alcohol_metadata AS (
  SELECT 
  ot.ofndr_num,
  ot.ofndr_tst_id,
  MAX(COALESCE(case WHEN aqc.tst_sctn_num = '6' AND aqc.assess_qstn_num = "28" THEN aq.assess_qstn END,CAST(NULL AS STRING))) AS LSRNR_Q28,
  MAX(COALESCE(case WHEN aqc.tst_sctn_num = '6' AND aqc.assess_qstn_num = "29" THEN aq.assess_qstn END,CAST(NULL AS STRING))) AS LSRNR_Q29,
  MAX(COALESCE(case WHEN aqc.tst_sctn_num = '6' AND aqc.assess_qstn_num = "30" THEN aq.assess_qstn END,CAST(NULL AS STRING))) AS LSRNR_Q30,
  MAX(COALESCE(case WHEN aqc.tst_sctn_num = '6' AND aqc.assess_qstn_num = "31" THEN aq.assess_qstn END,CAST(NULL AS STRING))) AS LSRNR_Q31,
  MAX(COALESCE(case WHEN aqc.tst_sctn_num = '6' AND aqc.assess_qstn_num = "32" THEN aq.assess_qstn END,CAST(NULL AS STRING))) AS LSRNR_Q32,
  MAX(COALESCE(case WHEN aqc.tst_sctn_num = '6' AND aqc.assess_qstn_num = "33" THEN aq.assess_qstn END,CAST(NULL AS STRING))) AS LSRNR_Q33,
  MAX(COALESCE(case WHEN aqc.tst_sctn_num = '6' AND aqc.assess_qstn_num = "34" THEN aq.assess_qstn END,CAST(NULL AS STRING))) AS LSRNR_Q34,
  MAX(COALESCE(case WHEN aqc.tst_sctn_num = '6' AND aqc.assess_qstn_num = "35" THEN aq.assess_qstn END,CAST(NULL AS STRING))) AS LSRNR_Q35,
  MAX(COALESCE(case WHEN aqc.tst_sctn_num = '6' AND aqc.assess_qstn_num = "28" THEN aqc.qstn_choice_desc END,CAST(NULL AS STRING))) AS LSRNR_Q28_ANSWER,
  MAX(COALESCE(case WHEN aqc.tst_sctn_num = '6' AND aqc.assess_qstn_num = "29" THEN aqc.qstn_choice_desc END,CAST(NULL AS STRING))) AS LSRNR_Q29_ANSWER,
  MAX(COALESCE(case WHEN aqc.tst_sctn_num = '6' AND aqc.assess_qstn_num = "30" THEN aqc.qstn_choice_desc END,CAST(NULL AS STRING))) AS LSRNR_Q30_ANSWER,
  MAX(COALESCE(case WHEN aqc.tst_sctn_num = '6' AND aqc.assess_qstn_num = "31" THEN aqc.qstn_choice_desc END,CAST(NULL AS STRING))) AS LSRNR_Q31_ANSWER,
  MAX(COALESCE(case WHEN aqc.tst_sctn_num = '6' AND aqc.assess_qstn_num = "32" THEN aqc.qstn_choice_desc END,CAST(NULL AS STRING))) AS LSRNR_Q32_ANSWER,
  MAX(COALESCE(case WHEN aqc.tst_sctn_num = '6' AND aqc.assess_qstn_num = "33" THEN aqc.qstn_choice_desc END,CAST(NULL AS STRING))) AS LSRNR_Q33_ANSWER,
  MAX(COALESCE(case WHEN aqc.tst_sctn_num = '6' AND aqc.assess_qstn_num = "34" THEN aqc.qstn_choice_desc END,CAST(NULL AS STRING))) AS LSRNR_Q34_ANSWER,
  MAX(COALESCE(case WHEN aqc.tst_sctn_num = '6' AND aqc.assess_qstn_num = "35" THEN aqc.qstn_choice_desc END,CAST(NULL AS STRING))) AS LSRNR_Q35_ANSWER,
  MAX(COALESCE(case WHEN aqc.tst_sctn_num = '6' AND aqc.assess_qstn_num = "28" THEN tqr.cmt END,CAST(NULL AS STRING))) AS LSRNR_Q28_CMT,
  MAX(COALESCE(case WHEN aqc.tst_sctn_num = '6' AND aqc.assess_qstn_num = "29" THEN tqr.cmt END,CAST(NULL AS STRING))) AS LSRNR_Q29_CMT,
  MAX(COALESCE(case WHEN aqc.tst_sctn_num = '6' AND aqc.assess_qstn_num = "30" THEN tqr.cmt END,CAST(NULL AS STRING))) AS LSRNR_Q30_CMT,
  MAX(COALESCE(case WHEN aqc.tst_sctn_num = '6' AND aqc.assess_qstn_num = "31" THEN tqr.cmt END,CAST(NULL AS STRING))) AS LSRNR_Q31_CMT,
  MAX(COALESCE(case WHEN aqc.tst_sctn_num = '6' AND aqc.assess_qstn_num = "32" THEN tqr.cmt END,CAST(NULL AS STRING))) AS LSRNR_Q32_CMT,
  MAX(COALESCE(case WHEN aqc.tst_sctn_num = '6' AND aqc.assess_qstn_num = "33" THEN tqr.cmt END,CAST(NULL AS STRING))) AS LSRNR_Q33_CMT,
  MAX(COALESCE(case WHEN aqc.tst_sctn_num = '6' AND aqc.assess_qstn_num = "34" THEN tqr.cmt END,CAST(NULL AS STRING))) AS LSRNR_Q34_CMT,
  MAX(COALESCE(case WHEN aqc.tst_sctn_num = '6' AND aqc.assess_qstn_num = "35" THEN tqr.cmt END,CAST(NULL AS STRING))) AS LSRNR_Q35_CMT,
FROM {ofndr_tst} ot
JOIN {tst_qstn_rspns} tqr
  ON(ot.ofndr_tst_id = tqr.ofndr_tst_id)
JOIN {assess_qstn_choice} aqc
  ON(
    aqc.qstn_choice_num = tqr.qstn_choice_num
    and aqc.assess_qstn_num = tqr.assess_qstn_num
    and aqc.tst_sctn_num = tqr.tst_sctn_num
    and aqc.assess_tst_id = tqr.assess_tst_id)
JOIN {assess_qstn} aq
  ON (aq.assess_qstn_num = tqr.assess_qstn_num
  AND aq.tst_sctn_num = tqr.tst_sctn_num
  AND aq.assess_tst_id = tqr.assess_tst_id)
WHERE aqc.assess_tst_id = '48' -- LS/RNR
  AND aqc.tst_sctn_num = '6' -- "Alcohol/Drug Problem" section
  AND aqc.assess_qstn_num != '999' -- Always null
GROUP BY ofndr_num, ofndr_tst_id
)
SELECT
  ot.ofndr_num,
  ot.ofndr_tst_id,
  atc.assess_cat_desc,
  atl.tst_title,
  -- We null out any dates not between 1900 and 2040 because they probably are
  -- meaningless and they confuse bigquery
  IF(DATE(tst_dt) BETWEEN DATE('1900-01-01') AND DATE('2040-01-01'), CAST(LEFT(tst_dt, 10) AS DATE), NULL) tst_dt,
  ote.tot_score,
  UPPER(ote.eval_desc) as eval_desc,
  m.* EXCEPT (ofndr_num, ofndr_tst_id)
FROM {ofndr_tst} ot

-- Get score information
LEFT JOIN {ofndr_tst_eval} ote USING (ofndr_tst_id)

-- Get information about the assessment type
LEFT JOIN {assess_tst} atl USING (assess_tst_id)
LEFT JOIN {assess_tst_cat_cd} atc USING (assess_cat_cd)

-- Join in metadata for rows where it is applicable
LEFT JOIN lsrnr_drug_and_alcohol_metadata m USING (ofndr_num, ofndr_tst_id)

WHERE 
  -- Only removes 23 rows.
  DATE(tst_dt) <= @update_timestamp
  
  -- There are several tests that have NOTE in the title. These don't seem to be actual
  -- tests, but rather a note about a test. I'm not sure how to retrieve that note yet,
  -- but they aren't actually tests so they can be filtered out. TODO(#37453) for more
  -- filters that could be applied this way
  AND tst_title NOT LIKE '%NOTE'
  
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="US_UT",
    ingest_view_name="assessment",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
