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
"""Query for sentence and offense information in NE."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- admission status lookup table, used for sentence type primarily
-- we exclude some sentences not relevant and not controlled by NDCS
admission_status AS (
  SELECT 
    pkCode,
    description
  FROM {LKAdmissionStatus}
), 
-- paroleEarnedDischargeDate is in it's own table, and only assigned after paroled
earned_parole_date AS (
  SELECT 
  pkParoleEarnedDischargeDateId, 
  inmateNumber, 
  NULLIF(paroleEarnedDischargeDate, "NULL") AS paroleEarnedDischargeDate
 FROM {ParoleEarnedDischargeDate}
),
--aggregate sentence information for each person's cycle of incarceration
get_sentences AS (
  SELECT 
    pkAggregateSentenceId,
    inmateNumber,
    adminStatusCd,
    beginDate,
    paroleEligibilityDate,
    tentativeReleaseDate,
    determinantSentence,
    IFNULL(modifiedDate, createdDate) AS modifiedDate, -- modified date if record has not been updated
    update_datetime
 FROM {AggregateSentence@ALL}
), 
-- only need one row per inmateNumber, pkAggregateSentenceId, and modifiedDate
agg_sentences AS (
  SELECT * 
  FROM get_sentences
  QUALIFY ROW_NUMBER() OVER (PARTITION BY inmateNumber, pkAggregateSentenceId, modifiedDate ORDER BY update_datetime) = 1
),
-- joining all tables and cleaning  
joined_tables AS (
SELECT 
    s.inmateNumber,
    CONCAT(s.inmateNumber, '-', s.pkAggregateSentenceId) AS sentenceGroupId,
    ROW_NUMBER() OVER (PARTITION BY s.inmateNumber, s.pkAggregateSentenceId ORDER BY s.modifiedDate) AS sequenceNum,
    determinantSentence,
    DATE(paroleEligibilityDate) AS paroleEligibilityDate,
    DATE(tentativeReleaseDate) AS tentativeReleaseDate,
    DATE(pedd.paroleEarnedDischargeDate) AS paroleEarnedDischargeDate,
    a.description AS sentence_admission,
    s.modifiedDate
  FROM agg_sentences s
  LEFT JOIN admission_status a 
  ON adminStatusCd = a.pkCode 
  LEFT JOIN earned_parole_date pedd
  USING (inmateNumber)
  WHERE inmateNumber IN (SELECT DISTINCT INMATENUMBER FROM {Offense})
)
SELECT 
  inmateNumber, 
  sentenceGroupId, 
  sequenceNum,
  IF(determinantSentence = '1', NULL, paroleEligibilityDate) AS paroleEligibilityDate, 
  -- TRD should always come after PEDD but this is not always the case in the data,
  -- in cases when this isn't true I am setting the TRD TO PEDD since in these cases 
  -- full release from custody generally falls on PEDD
  IF(paroleEarnedDischargeDate > tentativeReleaseDate OR tentativeReleaseDate IS NULL, paroleEarnedDischargeDate, tentativeReleaseDate) AS tentativeReleaseDate,
  paroleEarnedDischargeDate,
  modifiedDate,
FROM joined_tables 
WHERE sentence_admission NOT IN ("NINETY DAY EVALUATOR", "WORK ETHIC CAMP ADMISSION","INS DETAINEE"," KANSAS JUVENILES", "SARPY COUNTY WORK RELEASE")
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ne",
    ingest_view_name="sentence_group_length",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
