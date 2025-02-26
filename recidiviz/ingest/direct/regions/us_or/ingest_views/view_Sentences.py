# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Query containing sentence information from the following tables:
"""


from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH sentence AS (
  SELECT DISTINCT
    NEXT_NUMBER,
    CHARGE_NEXT_NUMBER,
    RECORD_KEY, 
    CUSTODY_NUMBER,
    ADMISSION_NUMBER, 
    OFFENSE_NUMBER,
    SENTENCE_NUMBER, 
    SENTENCE_TYPE,
    (DATE(SENTENCE_BEGIN_DATE)) AS SENTENCE_BEGIN_DATE,
    (DATE(MINIMUM_DATE)) AS MINIMUM_DATE, 
    (DATE(MAXIMUM_DATE)) AS MAXIMUM_DATE, 
    SENTENCE_LENGTH_YEARS, 
    SENTENCE_LENGTH_MONTHS,
    SENTENCE_LENGTH_DAYS,
    LIFE_OR_DEATH, # (D or L) - for is_life and capital_punishment
    TIME_SERVED, 
    TERMINATION_DATE,
    TERMINATION_CODE,
    EARNED_TIME_DAYS, 
    STATUTORY_GOOD_TIME, 
    CONSEC_TO,
    MERGE_SENTENCE, 
    DANG_OFFENDER_SENT,
    DANG_OFFENDER
  FROM {RCDVZ_PRDDTA_OP054P}
), charge AS ( 
  SELECT DISTINCT
    RECORD_KEY, 
    CUSTODY_NUMBER,
    ADMISSION_NUMBER, 
    OFFENSE_NUMBER, 
    ARREST_DATE, 
    DATE(CONVICTED_DATE) AS CONVICTED_DATE,
    COUNTY, 
    COURT_CASE_NUMBER,
    (DATE(CRIME_COMMITTED_DATE)) AS CRIME_COMMITTED_DATE,
    FELONY_IS_MISDEMEANOR,
    ORS_NUMBER,
    ORS_PARAGRAPH,
    CRIME_ABBREVIATION,
    CRIME_CLASS,
    CRIME_CATEGORY,
    OFF_SEVERITY, 
    COURT_CASE_COUNT, 
    JUDGE
  FROM {RCDVZ_PRDDTA_OP053P}
), offense AS ( 
  SELECT
    RECORD_KEY,
    CUSTODY_NUMBER, 
    ADMISSION_NUMBER,
    COUNTY,
    CURRENT_STATUS,
    SENTENCE_BEGIN_DATE,
    CRIME_CATEGORY
  FROM {RCDVZ_PRDDTA_OPCOUR}
), conditions AS (
  SELECT 
    RECORD_KEY,
    COURT_CASE_NUMBER,
    CUSTODY_NUMBER,
    ADMISSION_NUMBER, 
    STRING_AGG(CONDITION_DESC, ', ' ORDER BY CONDITION_DESC) AS CONDITIONS_LIST, 
    FROM {RCDVZ_CISPRDDTA_OPCOND}
    LEFT JOIN {RCDVZ_DOCDTA_TBCOND}
    USING (CONDITION_CODE, CONDITION_TYPE)
    GROUP BY RECORD_KEY, COURT_CASE_NUMBER, CUSTODY_NUMBER, ADMISSION_NUMBER 
    UNION ALL
  SELECT 
    RECORD_KEY,
    COURT_CASE_NUMBER,
    CUSTODY_NUMBER,
    ADMISSION_NUMBER, 
    STRING_AGG(CONDITION_DESC, ', ' ORDER BY CONDITION_DESC) AS CONDITIONS_LIST, 
    FROM {RCDVZ_CISPRDDTA_OPCONE}
    LEFT JOIN {RCDVZ_DOCDTA_TBCOND}
    USING (CONDITION_CODE, CONDITION_TYPE)
    WHERE RECORD_KEY NOT IN (SELECT RECORD_KEY FROM RCDVZ_CISPRDDTA_OPCOND_generated_view)
    GROUP BY RECORD_KEY, COURT_CASE_NUMBER, CUSTODY_NUMBER, ADMISSION_NUMBER 
), additional_info AS (
  SELECT
    RECORD_KEY, 
    CUSTODY_NUMBER, 
    ADMISSION_NUMBER, 
    CURRENT_STATUS,
    ORS_NUMBER, 
    ORS_PARAGRAPH, 
    CRIME_CLASS, 
    OFF_SEVERITY, 
    DANG_OFFENDER, 
    PROJECTED_RELEASE_DATE,
    IF(PAROLE_RELEASE_DATE = '01/01/1901', null, PAROLE_RELEASE_DATE) AS PAROLE_RELEASE_DATE,
    MAXIMUM_DATE,
    MINIMUM_DATE, 
    RESPONSIBLE_DIVISION
  FROM {RCDVZ_PRDDTA_OP013P}
), crime_info AS (
  SELECT
    ORS_NUMBER,
    ORS_SUBCLASS, 
    ORS_ABBREVIATION,
    IF(ORS_ABBREVIATION = 'ELUDE POLI', 'ELUDE POLICE ATTEMPT - VEHICLE/ON FOOT', ORS_DESCRIPTION) AS ORS_DESCRIPTION, 
    CRIME_TYPE,
    CRIME_CLASS, 
    SEX_ASSAULT_CRIME,
    NCRP_OFFENSE_CODE
  FROM {RCDVZ_DOCDTA_TB209P}
), ncic_info AS (
  SELECT 
    ORS_NUMBER, 
    ORS_SUBCLASS, 
    NCIC_CODE
  FROM {RCDVZ_PBMIS_COMMON_NCIC_CODES}
),
final AS (
  SELECT 
    RECORD_KEY, 
    CUSTODY_NUMBER, 
    ADMISSION_NUMBER, 
    OFFENSE_NUMBER,
    SENTENCE_NUMBER,
    additional_info.CURRENT_STATUS,
    sentence.SENTENCE_TYPE, 
    sentence.SENTENCE_BEGIN_DATE, 
    sentence.MINIMUM_DATE, 
    sentence.MAXIMUM_DATE,
    sentence.SENTENCE_LENGTH_YEARS, 
    sentence.SENTENCE_LENGTH_MONTHS,
    sentence.SENTENCE_LENGTH_DAYS,
    sentence.TERMINATION_DATE,
    sentence.TERMINATION_CODE,
    additional_info.PAROLE_RELEASE_DATE,
    sentence.LIFE_OR_DEATH,
    IF(CRIME_COMMITTED_DATE < '1989-11-01', 'Y', 'N') AS PAROLE_POSSIBLE, # after this date supervision sentenced separately
    sentence.TIME_SERVED,
    sentence.STATUTORY_GOOD_TIME,
    sentence.EARNED_TIME_DAYS,  
    sentence.CONSEC_TO, 
    sentence.MERGE_SENTENCE, 
    CONDITIONS_LIST,
    -- charge --
    charge.CRIME_COMMITTED_DATE, 
    IFNULL(descriptions.ORS_DESCRIPTION, CRIME_ABBREVIATION) AS CRIME_DESCRIPTION,
    IF(charge.CONVICTED_DATE = '1901-01-01', SENTENCE_BEGIN_DATE, charge.CONVICTED_DATE) AS CONVICTED_DATE,
    charge.COUNTY,
    NCIC_CODE,
    charge.ORS_NUMBER,
    charge.CRIME_CLASS, 
    crime_info.ORS_DESCRIPTION, 
    sentence.DANG_OFFENDER, 
    crime_info.SEX_ASSAULT_CRIME, 
    COURT_CASE_COUNT, 
    JUDGE
  FROM sentence
  LEFT JOIN charge 
  USING (RECORD_KEY, CUSTODY_NUMBER, ADMISSION_NUMBER, OFFENSE_NUMBER)
  LEFT JOIN additional_info 
  USING (RECORD_KEY, CUSTODY_NUMBER, ADMISSION_NUMBER)
  LEFT JOIN crime_info 
  ON additional_info.ORS_NUMBER = crime_info.ORS_NUMBER
  AND additional_info.ORS_PARAGRAPH = crime_info.ORS_SUBCLASS
  LEFT JOIN crime_info descriptions # join twice to get crime descriptions since OR_subclass not properly filled in
  ON charge.CRIME_ABBREVIATION = descriptions.ORS_ABBREVIATION
  LEFT JOIN ncic_info 
  ON additional_info.ORS_NUMBER = ncic_info.ORS_NUMBER
  AND additional_info.ORS_PARAGRAPH = ncic_info.ORS_SUBCLASS
  LEFT JOIN conditions
  USING (RECORD_KEY, CUSTODY_NUMBER, ADMISSION_NUMBER, COURT_CASE_NUMBER)
)
SELECT DISTINCT * FROM final
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_or",
    ingest_view_name="Sentences",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="RECORD_KEY, CUSTODY_NUMBER, ADMISSION_NUMBER, OFFENSE_NUMBER",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
