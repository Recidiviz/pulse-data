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
"""Query that collects information about sentences and charges in UT.

TODO(#37588): This view is based on the following assumptions, which need to be confirmed with the 
state:
- One case can have multiple associated offenses
- Each offense can have its own associated sentence
- Sentences can involve three components: prison time, probation time, and jail time
- The maximum sentence length can be found by adding the maximum possible duration of all of those components together

It is likely that this logic will need to be revisited once we have clarity on the
sentencing and related data storage practices in Utah. 
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- Collect all sentences and charges that are valid for ingest.
valid_sentences AS (
SELECT DISTINCT
  crt.ofndr_num,
  ofnse.ofnse_id,
  ofnse_viol_cd AS statute,
  oc.ofnse_desc AS charge_description,
  ofnse.ofnse_svrty_cd AS offense_severity_code,
  ofnse.atmpt_flg AS attempt_flag,
  CAST(ofnse_dt AS DATETIME) AS offense_date,
  cd.crime_degree_desc AS charge_classification_type_raw_text,
  CAST(crt.sent_dt AS DATETIME) AS sent_dt,
  cl.crt_loc_desc AS sentencing_court,
  NULLIF(CAST(crt.sent_jail_day AS INT64), 0) as jail_days_at_sentencing,
  NULLIF(CAST(prob.prob_sent_days AS INT64), 0) as probation_days,
  NULLIF(CAST(prsn.max_days AS INT64), 0) as max_prison_days,
FROM 
  {crt_case} crt
JOIN 
  {rfrd_ofnse} ofnse
USING 
  (intr_case_num)
LEFT JOIN 
  {ofnse_cd} oc
USING
  (ofnse_viol_cd)
LEFT JOIN 
  {crime_degree_cd} cd
ON
  (cd.crime_degree_cd = ofnse.crime_degree_cd)
LEFT JOIN 
  {crt_loc} cl
USING
  (crt_loc_cd, crt_typ_cd)
LEFT JOIN 
  {prob_sent} prob
USING
  (intr_case_num)
LEFT JOIN 
  {prsn_sent_len} prsn
USING
  (ofnse_id)
-- Exclude sentences with no imposed date. These are all in the past, and make up 5% of closed cases.
WHERE sent_dt IS NOT NULL),
-- Collect all sentences listed as consecutive to another sentence. Filter out sentences
-- that do not otherwise exist in the base table to avoid linking a case that does not 
-- exist as consecutive to a case that does exist.
consecutive_sentences AS (
SELECT DISTINCT 
  NULLIF(prsn_consec.consec_to_ofnse,'0') AS consec_to_ofnse, valid_sentences.ofnse_id
FROM
  prsn_sent_len_generated_view prsn_consec
JOIN
  valid_sentences
ON
  (prsn_consec.consec_to_ofnse = valid_sentences.ofnse_id)
)
SELECT
  *
FROM
  valid_sentences
LEFT JOIN
  consecutive_sentences
USING
  (ofnse_id)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ut",
    ingest_view_name="sentence",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
