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
WITH base_sanction AS (
  SELECT 
    RECORD_KEY, 
    SANC_NUMBER, 
    CONDITION_CODE,
    SEQUENCE_NO, 
    DATE(SANC_DATE) AS SANC_DATE
  FROM {RCDVZ_CISPRDDTA_CMSACN}
), imposed_sanc AS (
  SELECT 
    RECORD_KEY, 
    SANC_NUMBER, 
    SANCTION_TYPE,
    CUSTODY_NUMBER, 
    ADMISSION_NUMBER, 
    SANCTION_ACTION,
    LOCATION_CODE, 
    CONDITION_CODE,
    SANCTION_CODE, 
    COURT_CASE_NUMBER, 
    COUNTY, 
    LOCAL_SANCTION_FLAG,
    SANC_DATE, 
    HEARING_DATE, 
    DECISION_DATE
  FROM {RCDVZ_CISPRDDTA_CMSAIM}
), court_sanction AS (
  SELECT 
    RECORD_KEY, 
    SANC_NUMBER, 
    COURT_CASE_NUMBER, 
    COUNTY, 
    SEQUENCE_NO, 
    SANCTION_CODE, 
    REC_CUST_UNITS, 
    REC_AUTH_CODE,
  FROM {RCDVZ_CISPRDDTA_CMSACO}
), conditions as (
  SELECT 
    CONDITION_CODE, 
    CONDITION_TYPE,
    CONDITION_DESC
  FROM {RCDVZ_DOCDTA_TBCOND}
), all_sanctions AS (
SELECT 
  base_sanction.RECORD_KEY, 
  base_sanction.SANC_NUMBER, 
  base_sanction.CONDITION_CODE,
  CONDITION_DESC, 
  base_sanction.SEQUENCE_NO, 
  (DATE(base_sanction.SANC_DATE)) AS SANC_DATE,
  court_sanction.COURT_CASE_NUMBER, 
  court_sanction.COUNTY, 
  IFNULL(court_sanction.SANCTION_CODE, imposed_sanc.SANCTION_CODE) AS SANCTION_CODE,
  imposed_sanc.SANCTION_TYPE,
  imposed_sanc.SANCTION_ACTION,
  imposed_sanc.LOCAL_SANCTION_FLAG, 
FROM base_sanction
LEFT JOIN court_sanction
USING (RECORD_KEY, SANC_NUMBER, SEQUENCE_NO)
LEFT JOIN imposed_sanc
USING (RECORD_KEY, SANC_NUMBER, CONDITION_CODE)
LEFT JOIN conditions
USING (CONDITION_CODE)
), aggr AS (
  SELECT
    RECORD_KEY, 
    SANC_NUMBER, 
    CONDITION_CODE,
    CONDITION_DESC, 
    MAX(SEQUENCE_NO) AS SEQUENCE_NO, 
    SANC_DATE,
    COURT_CASE_NUMBER,
    COUNTY, 
    --SANCTION_CODE,
    SANCTION_DESC,
    all_sanctions.SANCTION_TYPE,
    SANCTION_ACTION, --complete, pending, etc
    LOCAL_SANCTION_FLAG, 
  FROM all_sanctions
  LEFT JOIN {RCDVZ_DOCDTA_TBSANC}
  USING (SANCTION_CODE)
  GROUP BY RECORD_KEY, SANC_NUMBER, CONDITION_CODE, CONDITION_DESC, SANC_DATE,COURT_CASE_NUMBER,COUNTY, SANCTION_CODE,SANCTION_DESC,all_sanctions.SANCTION_TYPE,SANCTION_ACTION,LOCAL_SANCTION_FLAG
), final AS (
  SELECT 
    RECORD_KEY, 
    SANC_NUMBER,
    TO_JSON_STRING(
      ARRAY_AGG(STRUCT< RECORD_KEY string,
                        SANC_NUMBER string,
                        CONDITION_CODE string,
                        CONDITION_DESC string, 
                        SEQUENCE_NO string, 
                        SANC_DATE date,
                        COURT_CASE_NUMBER string,
                        COUNTY string,
                        SANCTION_DESC string,
                        SANCTION_TYPE string,
                        SANCTION_ACTION string, 
                        LOCAL_SANCTION_FLAG string>
                (RECORD_KEY,
                SANC_NUMBER,
                CONDITION_CODE,
                CONDITION_DESC,
                SEQUENCE_NO,
                SANC_DATE, 
                COURT_CASE_NUMBER,
                COUNTY,
                SANCTION_DESC,
                SANCTION_TYPE,
                SANCTION_ACTION,
                LOCAL_SANCTION_FLAG) ORDER BY RECORD_KEY, SANC_NUMBER, CONDITION_CODE, SEQUENCE_NO, SANCTION_DESC)
    ) AS SANCTION_DATA
  FROM aggr
  GROUP BY RECORD_KEY, SANC_NUMBER
)

SELECT DISTINCT final.*, SANC_DATE
FROM final
LEFT JOIN base_sanction
USING (RECORD_KEY, SANC_NUMBER)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_or",
    ingest_view_name="Supervision_Violation_Responses",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
