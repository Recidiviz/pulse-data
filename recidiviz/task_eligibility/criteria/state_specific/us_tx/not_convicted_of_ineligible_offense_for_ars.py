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
"""
Defines a criteria view that shows spans of time when clients do not have any current or prior convictions
 for Annual Reporting Status-ineligible offenses. List of offenses can be found at https://statutes.capitol.texas.gov/Docs/CR/htm/CR.42A.htm#42A.054
"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TX_NOT_CONVICTED_OF_INELIGIBLE_OFFENSE_FOR_ARS"

_DESCRIPTION = """Defines a criteria view that shows spans of time when clients do not have any current or prior convictions
 for Annual Reporting Status-ineligible offenses. List of offenses can be found at https://statutes.capitol.texas.gov/Docs/CR/htm/CR.42A.htm#42A.054
"""

_REASON_QUERY = f"""
WITH ineligible_statutes AS (
  /* this pulls offense information for ARS-ineligible offenses */
  SELECT DISTINCT statute,
    description
  FROM `{{project_id}}.{{normalized_state_dataset}}.state_charge_v2`
  WHERE state_code = 'US_TX'
    AND TRIM(statute, '0') IN (
        -- '15.03',   -- Citation for Criminal Solicitation if the offense is punishable as a felony of the first degree
        '19.02',   -- Citation for Murder
        '19.03',   -- Citation for Capital Murder
        '20.04',   -- Citation for Aggravated Kidnapping
        '20A.02',  -- Citation for Trafficking of Persons
        '20A.03',  -- Citation for Continuous Trafficking of Persons
        '21.11',   -- Citation for Indecency with a Child
        '22.011',  -- Citation for Sexual Assault
        '22.021',  -- Citation for Aggravated Sexual Assault
        -- '22.04(a)(1)',   -- Citation for Injury to a Child, Elderly Individual, or Disabled Individual if the offense is punishable as a felony of the first degree and the victim of the offense is a child
        '29.03',   -- Citation for Aggravated Robbery
        -- '30.02(d)',   -- Citation for Burglary if the actor committed the offense with the intent to commit a felony under Section 21.02, 21.11, 22.011, 22.021, or 25.02
        '43.04',   -- Citation for Aggravated Promotion of Prostitution
        '43.05',   -- Citation for Compelling Prostitution
        '43.25',   -- Citation for Sexual Performance by a Child
        '43.26',   -- Citation for Possession or Promotion of Child Pornography
        '481.14'  -- Violation of Controlled Substances Act - Use of Child in Commission of Offense (using 14 instead of 140 due to trimming of 0's - there is no 481.14 or 481.014 so this should be ok) 
        -- '481.134(c)(d)(e)(f)', -- Citation for Drug-free Zones if it is shown that the defendant has been previously convicted of an offense for which punishment was increased under any of those subsections
        -- '481.1123(d)(e)(f)' -- Citation for Manufacture or Delivery of Substance in Penalty Group 1-B
        )  
), ineligible_spans AS (
/* this captures spans of time where someone has been convicted of an ARS-ineligible offense. it includes all above statutes,
as well as a few edge cases where the statute is missing or incorrect but the description references an ineligible offense */
  SELECT state_code,
    person_id,
    date_charged AS start_date, 
    CAST(NULL AS DATE) AS end_date,
    False AS meets_criteria,
    CONCAT(statute, '-', description) AS ineligible_offense,
  FROM `{{project_id}}.{{normalized_state_dataset}}.state_charge_v2`
  WHERE state_code = 'US_TX'
    AND (statute IN (SELECT statute FROM ineligible_statutes)
    OR description IN (SELECT description FROM ineligible_statutes))
), {create_sub_sessions_with_attributes('ineligible_spans')},
dedup_cte AS (
    SELECT * except (ineligible_offense),
         TO_JSON(STRUCT(ARRAY_AGG(DISTINCT ineligible_offense ORDER BY ineligible_offense) AS ineligible_offenses)) AS reason,
        ARRAY_AGG(DISTINCT ineligible_offense ORDER BY ineligible_offense) AS ineligible_offenses,
    FROM sub_sessions_with_attributes
    GROUP BY 1,2,3,4,5
)
SELECT * 
FROM dedup_cte
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    criteria_spans_query_template=_REASON_QUERY,
    state_code=StateCode.US_TX,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    meets_criteria_default=True,
    reasons_fields=[
        ReasonsField(
            name="ineligible_offenses",
            type=bigquery.enums.StandardSqlTypeNames.ARRAY,
            description="List of offenses that a client has been convicted of which make them ineligible for ARS",
        )
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
