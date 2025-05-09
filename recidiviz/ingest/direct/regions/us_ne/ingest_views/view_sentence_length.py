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
"""Query for sentence length information in NE."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
--offense/individual sentence information for each person's cycle of incarceration
offenses AS (
  SELECT 
    pkOffenseId,
    inmateNumber,
    arrestDate,   
    fkOffenseArrestCode,
    minimumTerm, -- to determine life, death, etc
    maximumTerm, -- to determine life, death, etc
    minimumYear,
    minimumMonth,
    minimumDay,
    maximumYear,
    maximumMonth,
    maximumDay,
    COALESCE(beginDate, offenseBeginDate, offenseCommittedDate, createdDate) AS beginDate, -- sometimes is populated here and not in sentence table
  FROM {Offense}
), 
-- admission status lookup table, indicates sentence type
admission_status AS (
  SELECT 
    pkCode,
    description
  FROM {LKAdmissionStatus}
), 
-- paroleEarnedDischateDate is in it's own table, only receive once paroled
earned_parole_date AS (
  SELECT 
  pkParoleEarnedDischargeDateId, 
  inmateNumber, 
  NULLIF(paroleEarnedDischargeDate, "NULL") AS paroleEarnedDischargeDate,
 FROM {ParoleEarnedDischargeDate}
),
-- arrest code lookup table for offense_type 
arrest_codes AS (
  SELECT 
    pkCode,
    description
  FROM {LKOffenseArrest}
), 
--aggregate sentence information for each person's cycle of incarceration, each inmateNumber 
--should only have one record as it also operates as a custody cycle number, all individual sentence 
--information is rolled up into one aggregate sentence
get_sentences AS (
  SELECT 
    pkAggregateSentenceId,
    inmateNumber,
    adminStatusCd,
    beginDate,
    minimumTerm, -- to determine life, death, etc
    maximumTerm, -- to determine life, death, etc
    minimumYear,
    minimumMonth,
    minimumDay,
    maximumYear,
    maximumMonth,
    maximumDay,
    paroleEligibilityDate,
    tentativeReleaseDate,
    determinantSentence,
    IFNULL(modifiedDate, createdDate) AS modifiedDate, -- modified date if record has not been updated
    update_datetime
 FROM {AggregateSentence@ALL}
), 
-- only need one row per inmateNumber, pkAggregateSentenceId, and modifiedDate to track as dates change
agg_sentences AS (
  SELECT * 
  FROM get_sentences
  QUALIFY ROW_NUMBER() OVER (PARTITION BY inmateNumber, pkAggregateSentenceId, modifiedDate ORDER BY update_datetime) = 1
),
-- joining tables and populating individual sentence length information with aggSentence if null
-- thougth we except these numbers to line up because Offense table dates are edited to reflect the same as aggSentence
joined_tables AS (
SELECT 
    inmateNumber,
    -- if offenseID is null we only have aggregate sentence information for that person
    -- currently using inner join to avoid sentences without charges, but this will be relevant if we change schema
    CONCAT(inmateNumber, '-', pkAggregateSentenceId, '-', IFNULL(pkOffenseId, "AggSentOnly")) AS sentenceId,
    ROW_NUMBER() OVER (PARTITION BY inmateNumber, pkAggregateSentenceId, pkOffenseId ORDER BY modifiedDate) AS sequenceNum,
    determinantSentence,
    o.minimumYear,
    o.minimumMonth, 
    o.minimumDay,
    o.maximumYear,
    o.maximumMonth,
    o.maximumDay,
    DATE(paroleEligibilityDate) AS paroleEligibilityDate,
    DATE(tentativeReleaseDate) AS tentativeReleaseDate,
    DATE(pedd.paroleEarnedDischargeDate) AS paroleEarnedDischargeDate,
    DATE(IFNULL(s.beginDate, o.beginDate)) AS beginDate,
    a.description AS sentence_admission,
    modifiedDate
  FROM agg_sentences s
  -- using inner join because we have a number of AggSentences without charges which our schema doesn't allow
  -- TODO(#37688): change to LEFT JOIN to allow sentences without charge information 
  INNER JOIN offenses o
  USING (inmateNumber)
  LEFT JOIN admission_status a 
  ON adminStatusCd = a.pkCode 
  LEFT JOIN arrest_codes ac
  ON fkOffenseArrestCode = ac.pkCode
  LEFT JOIN earned_parole_date pedd
  USING (inmateNumber)
)
SELECT 
  inmateNumber,
  sentenceId,
  sequenceNum,
  minimumYear,
  minimumMonth,
  minimumDay,
  maximumYear,
  maximumMonth,
  maximumDay,
  IF(determinantSentence = '1', NULL, paroleEligibilityDate) AS paroleEligibilityDate,
  -- TRD should always come after PEDD but this is not always the case in the data,
  -- in cases when this isn't true I am setting the TRD TO PEDD since in these cases 
  -- full release from custody generally falls on PEDD
  IF(paroleEarnedDischargeDate > tentativeReleaseDate OR tentativeReleaseDate IS NULL, paroleEarnedDischargeDate, tentativeReleaseDate) AS tentativeReleaseDate,
  paroleEarnedDischargeDate,
  beginDate,
  modifiedDate
FROM joined_tables 
WHERE beginDate IS NOT NULL
AND sentence_admission NOT IN ("NINETY DAY EVALUATOR", "WORK ETHIC CAMP ADMISSION","INS DETAINEE"," KANSAS JUVENILES", "SARPY COUNTY WORK RELEASE")
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ne",
    ingest_view_name="sentence_length",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
