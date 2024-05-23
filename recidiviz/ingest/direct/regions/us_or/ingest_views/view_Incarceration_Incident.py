# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Query containing incarceration incident information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- A lot of reused "rules" in the MTRLMS table so getting the most recent and accurate ones.
distinct_mtrlms AS (
  SELECT t.*
  FROM (SELECT *, ROW_NUMBER() OVER(PARTITION BY RULE_ALLEGED ORDER BY EFFECTIVE_DATE DESC) AS seq 
        FROM {RCDVZ_CISPRDDTA_MTRLMS}) t
  WHERE seq = 1
),
-- Getting all information related to incidents and bucketing specific rule violations to severity number based on documentation
-- provided by OR. This is how they determine if/time of seg stays for incidents. This list could be changed by OR. 
-- This cte also joins in sanction table to see the coresponding response to incidents. 
MISCONDUCTS AS (
  SELECT 
    mto.RECORD_KEY,
    mtr.MISCONDUCT_CHARGE_ID, 
    mtrl.LONG_DESCRIPTION, 
    #mtr.RULE_ALLEGED,
    #mtr.RULE_FOUND,
    #mto.NEXT_NUMBER,
    #DECISION_STATUS,
    mtr.RULE_FINDING,
    mto.EFFECTIVE_DATE, 
    mto.LOCATION_CODE, 
    mto.MISCONDUCT_LOCATION, 
    mts.DISCIPLINARY_SANCTION, 
    mts.MISCONDUCT_SANCTION_ID, 
    DECISION_DATE, 
    mto.SUBMITTED_DATE, 
    mts.START_DATE,
    mts.STOP_DATE,
    mts.ACTUAL_DAYS,
    #mts.SANCTION_AMOUNT,
    #mts.SUSPENSE_DATE, 
    CASE
      WHEN LONG_DESCRIPTION IN ('AIC Assault I','Arson', 'Assault of a Member of Public', 'Compromising an Employee', 'Disturbance', 'Distribution I', 'Drug Possession', 'Escape I', 'Extortion I', 'Hostage Taking', 'Possession a Electronic Device', 'Possession of an Escape Device', 'Possession of a Weapon', 'Racketeering', 'Sexual Assault', 'Staff Assault I', 'Unauthorized Organization I', 'Inmate Assault I', 'Assault I', 'Disturbance I', 'Escape', 'Drug Smuggling', 'Poss Dang/Deadly Weap/Esc Dev', 'Possess a Weapon or Escape Dev', 'Posses of an Electronic Device', 'Possess a Weapon', 'Possess an Escape Device', 'Sexual Assault/Abuse', 'Drug Distribution') THEN '1'
      WHEN LONG_DESCRIPTION IN ('Assault II', 'AIC Assault II', 'Inmate Assault II', 'Contraband I', 'Distribution II', 'Escape II', 'Extortion II', 'Leave Violation', 'Sexual Harassment', 'Staff Assault II', 'Harassment (Rac/Relig/Sex)', 'Harrassment (Rac/Relig/Sex)') THEN '2'
      WHEN LONG_DESCRIPTION IN ('Disobedience of an Order I', 'Disrespect I', 'Unauthorized Area I', 'Unauthorized Area', 'Un Area', 'Disobedience of Order I') THEN '3A'
      WHEN LONG_DESCRIPTION IN ('Assault III', 'AIC Assault III', 'Inmate Assault III', 'Non-Assaultive Sexual Activity', 'Property I', 'Unauthorized Organization II', 'Unauthorized Use Inf System I', 'Unauthorized Use Info System I', 'Non-Assaultive Sex Activity', 'Non-assaultive Sexual Activity', 'Unauthorized Use of Computer') THEN '3B'
      WHEN LONG_DESCRIPTION IN ('Body Modification', 'Contraband II', 'Disobedience of an Order II', 'Disrespect II', 'False Info to Employees I', 'Forgery', 'Fraud', 'Gambling', 'Possess Body Mod Paraphernalia','Unauthorized Use Inf System II', 'Unauthorized Use Info Systm II', 'Tattooing/Body piercing', 'Tattooing' , 'Tattooing/Body Piercing') THEN '4'
      WHEN LONG_DESCRIPTION IN ('Disobedience III', 'Disrespect III', 'Property II', 'Disobedience of an Order III') THEN '5'
      WHEN LONG_DESCRIPTION IN ('Contraband III', 'False Info to Employees II', 'Unauthorized Area II') THEN '6'
      ELSE 'Not Specified'
    END AS MAPPED_VIOLATION_SEVERITY_LEVEL
  FROM {RCDVZ_CISPRDDTA_MTOFDR} mto
  LEFT JOIN {RCDVZ_CISPRDDTA_MTRULE} mtr
  USING (RECORD_KEY, NEXT_NUMBER)
  LEFT JOIN distinct_mtrlms mtrl
  ON mtr.RULE_ALLEGED = mtrl.RULE_ALLEGED
  LEFT JOIN {RCDVZ_CISPRDDTA_MTSANC} mts
  ON mto.RECORD_KEY = mts.RECORD_KEY
  AND mto.NEXT_NUMBER = mts.NEXT_NUMBER
  AND mtr.SEQUENCE_NO = mts.SEQUENCE_NO
  AND mtr.RULE_ALLEGED = mts.RULE_ALLEGED
)
-- Only want where the Miconduct_charge_id is populated
SELECT  *
FROM MISCONDUCTS
WHERE MISCONDUCT_CHARGE_ID IS NOT NULL
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_or",
    ingest_view_name="Incarceration_Incident",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
