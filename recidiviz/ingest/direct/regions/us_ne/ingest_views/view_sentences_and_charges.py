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
-- offense information for each person's cycle of incarceration
offenses AS (
  SELECT 
    pkOffenseId,
    inmateNumber,
    fkOffenseTypeCode,
    offenseCount,
    arrestDate,   
    fkFelonyMisdemeanorCode,
    fkOffenseAttemptCode,
    fkOffenseArrestCode,
    fkOffenseRunCode, -- indicates concurrent, consecutive, both
    fkCountyCode, -- county
    convictedSexualOffense,
    COALESCE(beginDate, offenseBeginDate, offenseCommittedDate, createdDate) AS beginDate, -- sometimes is populated here and not in sentence table
    convictedDescription,
    controlling, --is_controlling
    offenseCommittedDate, -- offense_date
    statute, 
    courtType,
  FROM {Offense}
), 
-- aggregate sentence information for each person's cycle of incarceration
agg_sentences AS (
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
    jailTimeDays, --initial_time_served_days
    deadTimeDays, 
    paroleEligibilityDate,
    tentativeReleaseDate,
    determinantSentence,
    CASE
      WHEN 
        paroleEligibilityDate is NULL
        AND minimumYear = maximumYear
        AND minimumMonth = maximumMonth
        AND minimumDay = maximumDay
        AND minimumTerm NOT IN ('LFE', 'DTH')
      THEN 'Y' 
      ELSE 'N'
    END AS parole_possible
 FROM {AggregateSentence}
), 
-- admission status lookup table
admission_status AS (
  SELECT 
    pkCode,
    description
  FROM {LKAdmissionStatus}
), 
-- county lookup table 
county_codes AS (
  SELECT 
    pkCode, 
    description,
    fkStateCode,
  FROM {LKCounty}
),
-- arrest code lookup table for offense_type 
arrest_codes AS (
  SELECT 
    pkCode,
    description
  FROM {LKOffenseArrest}
), 
-- joining all tables, getting degree, and getting non null sentence admission and begin date
joined_tables AS (
SELECT 
    pkOffenseId,
    inmateNumber,
    fkOffenseTypeCode,
    offenseCount, 
    fkFelonyMisdemeanorCode,
    fkOffenseAttemptCode,
    ac.description AS arrest_code,
    c.description AS county_name,
    convictedSexualOffense,
    convictedDescription, -- crime description
    CASE 
      WHEN convictedDescription LIKE '%1ST DEG%' THEN '1ST DEGREE'
      WHEN convictedDescription LIKE '%2ND DEG%' THEN '2ND DEGREE'
      WHEN convictedDescription LIKE '%3RD DEG%' THEN '3RD DEGREE'
      ELSE null 
    END AS degree,
    controlling, --is_controlling
    offenseCommittedDate, -- offense_date
    statute, -- statute
    pkAggregateSentenceId,
    a.description AS sentence_admission,
    IFNULL(s.beginDate, o.beginDate) AS beginDate,
    minimumTerm, -- to determine life, death, etc
    jailTimeDays, --initial_time_served_days
    IFNULL(determinantSentence, "0") AS determinantSentence,
  FROM agg_sentences s
  -- using inner join because we have a number of AggSentences without charges which our schema doesn't allow
  INNER JOIN offenses o
  USING (inmateNumber)
  LEFT JOIN county_codes c
  ON fkCountyCode = c.pkCode
  LEFT JOIN admission_status a 
  ON adminStatusCd = a.pkCode 
  LEFT JOIN arrest_codes ac
  ON fkOffenseArrestCode = ac.pkCode
)
SELECT * 
FROM joined_tables
WHERE beginDate IS NOT NULL
AND sentence_admission NOT IN ("NINETY DAY EVALUATOR", "WORK ETHIC CAMP ADMISSION","INS DETAINEE"," KANSAS JUVENILES", "SARPY COUNTY WORK RELEASE")

"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ne",
    ingest_view_name="sentences_and_charges",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
