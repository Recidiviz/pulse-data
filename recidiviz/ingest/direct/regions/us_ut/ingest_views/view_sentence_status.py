# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Query that collects information about sentences statuses over time in UT.

TODO(#37588): This view is based on the following assumptions, which need to be confirmed with the 
state:
- One case can have multiple associated offenses
- Each offense can have its own associated sentence

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
  cast(sent_dt AS DATETIME) as sent_dt,
  cast(crt_trmn_dt AS DATETIME) as crt_trmn_dt,
  CAST(crt.updt_dt AS DATETIME) AS updt_dt
FROM 
  {crt_case@ALL} crt
JOIN 
  {rfrd_ofnse} ofnse
USING 
  (intr_case_num)
-- Exclude sentences with no imposed date. These are all in the past, and make up 5% of closed cases.
WHERE sent_dt IS NOT NULL),
-- For the time being, assign statuses to cases based only on whether they have a 
-- termination date included in the crt_case table.
sentences_with_status AS (
SELECT 
  ofndr_num,
  ofnse_id,
  CASE 
    WHEN sent_dt IS NOT NULL AND crt_trmn_dt IS NULL THEN 'SERVING'
    WHEN sent_dt IS NOT NULL AND crt_trmn_dt IS NOT NULL THEN 'COMPLETE'
    ELSE 'UNKNOWN'
  END AS status,
  updt_dt
FROM valid_sentences
)
SELECT 
  ofndr_num,
  ofnse_id,
  status,
  updt_dt,
  -- There are 6 sentences with multiple statuses entered into the UT database with the 
  -- exact same timestamp. Order these manually so that the SERVING status comes before
  -- the COMPLETE status.
  ROW_NUMBER() OVER (PARTITION BY OFNDR_NUM, OFNSE_ID ORDER BY UPDT_DT, IF(STATUS='COMPLETE',1,0)) as sequence
  FROM (
    SELECT *, 
    LAG(status) OVER (partition by ofndr_num, ofnse_id ORDER BY UPDT_DT, IF(STATUS='COMPLETE',1,0)) as prev_status
    FROM sentences_with_status
  )
  WHERE 
  -- Status changed
    ((status != prev_status) 
  -- Last status
    OR (status IS NULL AND prev_status IS NOT NULL) 
  -- First status
    OR (status IS NOT NULL AND prev_status IS NULL))
  AND 
  -- This is the case in 87 out of 591,669 resulting rows. 
  -- It does not seem like the norm, and appears to be a data entry error in a majority
  -- of cases. Exclude these cases for the time being and revisit once we have
  -- more context from UT. TODO(#38008)
    (NOT (status = 'SERVING' AND prev_status = 'COMPLETE') OR prev_status IS NULL)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ut",
    ingest_view_name="sentence_status",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
