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
"""Query for sentence status snapshot information in NE."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- pkOffenseId in NE indicates each individual sentence as well, but all sentences get rolled up into 
-- 1 aggregate sentence per serving period.
offenses AS (
  SELECT 
    pkOffenseId,
    inmateNumber,
  FROM {Offense}
), 
-- aggregate sentence information for each person's cycle of incarceration, each inmateNumber will only have one
agg_sentences AS (
  SELECT 
    pkAggregateSentenceId,
    inmateNumber,
    adminStatusCd,
 FROM {AggregateSentence}
), 
-- admission status lookup table
admission_status AS (
  SELECT 
    pkCode,
    description
  FROM {LKAdmissionStatus}
), 
-- joining sentences and charges and dropping some sentence types not managed by NDCS
joined_sentence_and_charge AS (
SELECT 
    pkOffenseId,
    pkAggregateSentenceId,
    inmateNumber,
    a.description AS sentence_admission,
  FROM agg_sentences s
  INNER JOIN offenses o
  USING (inmateNumber)
  LEFT JOIN admission_status a 
  ON adminStatusCd = a.pkCode 
WHERE a.description NOT IN ("NINETY DAY EVALUATOR", "WORK ETHIC CAMP ADMISSION","INS DETAINEE"," KANSAS JUVENILES", "SARPY COUNTY WORK RELEASE")
),
-- NE does not have sentence status information. The closest we can get is looking at their last movement in LOCATION_HIST table and
-- determining if it's an open or closed period, and if the last movement is closed and from prison
-- to liberty then sometimes there will be additional information
location_history AS (
SELECT 
  LocationHistId,
  ID_NBR,
  EFFECTIVE_DT,
  ADMIS_TYPE_DSC,
  REC_CNTR_DSC, 
  LOCT_PRFX_DESC, -- indicates discharge, absconsion, etc
  LOCT_SUFX_DSC, -- 
  INST_RLSE_TYPE_DSC,
  INST_RLSE_DT, -- date left facility/institution
  IF(LOCT_RELEASE_DT IS NULL AND LOCT_PRFX_DESC = "DISCHARGE", EFFECTIVE_DT, LOCT_RELEASE_DT) AS LOCT_RELEASE_DT,  -- date left location - could be "leaving" facility or ending parole, don't always close the discharge periods
  IFNULL(LAST_ACTIVITY_TS, CREATION_TS) AS LAST_ACTIVITY_TS, 
FROM {LOCATION_HIST@ALL} lh 
LEFT JOIN  {E04_LOCT_PRFX}  lp
ON lh.LOCT_PRFX_CD = lp.LOCT_PRFX_CD
LEFT JOIN {E03_REC_CNTR} rc
ON lh.REC_CNTR_CD = rc.REC_CNTR_CD
LEFT JOIN {A16_INST_RLSE_TYPE}  rt
ON lh.INST_RLSE_TYPE_CD = rt.INST_RLSE_TYPE_CD
LEFT JOIN {A03_ADMIS_TYPE} ad
ON lh.ADMIS_TYPE_CD = ad.ADMIS_TYPE_CD
WHERE REC_CNTR_DSC != 'CENTRAL RECORDS OFFICE' -- this is transfer of records, not person, and usually has no end date
QUALIFY ROW_NUMBER() OVER (PARTITION BY ID_NBR ORDER BY EFFECTIVE_DT DESC, LOCT_RELEASE_DT DESC NULLS FIRST) = 1
), 
-- joining the latst movement information with sentence information
loc_and_sent AS (
SELECT 
  LocationHistId,
  pkOffenseId,
  pkAggregateSentenceId,
  ID_NBR,
  EFFECTIVE_DT,
  ADMIS_TYPE_DSC,
  REC_CNTR_DSC, 
  LOCT_PRFX_DESC,
  LOCT_SUFX_DSC, 
  INST_RLSE_TYPE_DSC, -- will only be populated if last movement is from facility to full release
  LOCT_RELEASE_DT,
  LAST_ACTIVITY_TS,  
FROM  location_history lh
-- there are a lot of movements from people whose sentences are not managed by NDCS so want to filter those out
INNER JOIN joined_sentence_and_charge sc
ON lh.ID_NBR = sc.inmateNumber
) 
SELECT 
  ID_NBR AS inmateNumber,
  pkAggregateSentenceId,
  pkOffenseId,
  ROW_NUMBER() OVER (PARTITION BY ID_NBR, pkAggregateSentenceId, pkOffenseId ORDER BY LAST_ACTIVITY_TS) AS sequenceNum,
  INST_RLSE_TYPE_DSC, 
  LOCT_RELEASE_DT,
  LAST_ACTIVITY_TS,  
FROM loc_and_sent
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ne",
    ingest_view_name="sentence_status_snapshot",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
