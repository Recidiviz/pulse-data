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
# ============================================================================
"""Defines a criteria span view that shows spans of time during which someone has served one year under the supervision
authority for a DUI offense and their LSI-R score is LOW. Before the year is up, policy states
that their LSI-R score should be overridden to MODERATE.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    revert_nonnull_end_date_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_IX_SERVED_AT_LEAST_ONE_YEAR_FOR_DUI_IF_LSIR_LEVEL_LOW"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which someone has served one year under the supervision
authority for a DUI offense and their LSI-R score is LOW. Before the year is up, policy states
that their LSI-R score should be overridden to MODERATE.
"""
_QUERY_TEMPLATE = f"""
WITH supervision_starts AS (
  SELECT
    state_code,
    person_id,
    start_date AS supervision_start_date,
  FROM `{{project_id}}.{{sessions_dataset}}.custodial_authority_sessions_materialized`
  WHERE state_code = "US_IX"
  --here we look at custodial authority sessions because we do not want to count time spent on home confinement (XCRC)
  --towards the one year on supervision supervised at LOW
    AND custodial_authority = "SUPERVISION_AUTHORITY"
),
sentences AS (
/* This CTE creates active sentence spans for any DUI sentences starting from the sentence imposed date to the latest
active sentence status session end date.
*/
  SELECT
      state_code,
      person_id,
      sentences.imposed_date AS start_date,
      -- Use the latest active status session end date as the criteria span end date,
      -- and leave NULL for open/current sessions
      {revert_nonnull_end_date_clause(f"MAX({nonnull_end_date_clause('serving_periods.end_date_exclusive')})")} AS end_date,
  FROM `{{project_id}}.sentence_sessions.sentences_and_charges_materialized` sentences
  LEFT JOIN `{{project_id}}.sentence_sessions.sentence_serving_period_materialized` serving_periods
      USING (state_code, person_id, sentence_id)
  WHERE state_code = "US_IX"
    AND (
        LOWER(sentences.description) LIKE '%driving under the influence%'
        OR LOWER(sentences.description) LIKE '%dui%'
    )
    AND sentences.imposed_date <= CURRENT_DATE("US/Pacific")
  GROUP BY 1, 2, 3
),
supervision_starts_with_sentences AS (
  SELECT
    s.*,
    supervision_start_date,
    --take the greatest of the supervision start date and the sentence start date for the date the DUI started
    --to be served. This accounts for incarceration sentences for DUIs, where we want to consider the supervision
    --start date for this sentence, and not the incarceration start date
    GREATEST(s.start_date, supervision_start_date) AS sentence_served_start_date
  FROM sentences s
  INNER JOIN supervision_starts p
    ON p.state_code = s.state_code
    AND p.person_id = s.person_id
    AND p.supervision_start_date < {nonnull_end_date_clause('s.end_date')}
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY s.state_code, s.person_id, s.start_date ORDER BY supervision_start_date DESC
    -- Pick the most recent supervision start date
  ) = 1
),
critical_date_spans AS (
  SELECT
    state_code,
    person_id,
    start_date AS start_datetime,
    end_date AS end_datetime,
    DATE_ADD(sentence_served_start_date, INTERVAL 1 YEAR) AS critical_date
  FROM supervision_starts_with_sentences
),
{critical_date_has_passed_spans_cte()},
lsir_dui_spans AS (
/* This CTE unions sessions of time for which someone is serving a DUI with sessions of time someone has a LOW 
risk score to ultimately identify spans of time where someone has not yet served one year for a DUI and their risk
score is LOW */
    SELECT * 
    EXCEPT(critical_date, critical_date_has_passed), 
    critical_date_has_passed,
    --risk level set to FALSE so we can identify all spans where ANY risk level is TRUE
    FALSE AS risk_level_low
    FROM critical_date_has_passed_spans
    
    UNION ALL 
    
     SELECT 
        state_code, 
        person_id, 
        assessment_date AS start_date, 
        score_end_date_exclusive AS end_date,
        --critical_date_has_passed is set to TRUE so we can identify all spans where ANY risk level is FALSE
        TRUE AS critical_date_has_passed,
        TRUE AS risk_level_low
      FROM `{{project_id}}.{{sessions_dataset}}.assessment_score_sessions_materialized`
      WHERE state_code = "US_IX"
      AND assessment_level = 'LOW' 
),
{create_sub_sessions_with_attributes('lsir_dui_spans')}
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    /* False spans should be:
     1. when any of the sub sessions have a TRUE value for risk_level_low  (LOGICAL_OR) AND
     2. when any value for critical_date_has_passed is FALSE
        (Using LOGICAL_AND should return TRUE only when ALL values are TRUE, so will return FALSE if any of the values
        are FALSE. Therefore NOT LOGICAL_AND() should return TRUE when any value is FALSE)
    Taking the NOT of LOGICAL_OR(risk_level) and NOT LOGICAL_AND(critical_date_has_passed) then returns FALSE
    spans whenever risk levels are low, and the critical date has not passed.
    */
    NOT(LOGICAL_OR(risk_level_low) AND NOT LOGICAL_AND(critical_date_has_passed)) AS meets_criteria,
    TO_JSON(STRUCT(
        start_date AS eligible_date
    )) AS reason,
    start_date AS minimum_time_served_date,
FROM sub_sessions_with_attributes
WHERE start_date != {nonnull_end_date_clause('end_date')}
GROUP BY 1,2,3,4
"""
VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    state_code=StateCode.US_IX,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    meets_criteria_default=True,
    sessions_dataset=SESSIONS_DATASET,
    reasons_fields=[
        ReasonsField(
            name="minimum_time_served_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="The date on which the person has served at least one year for a DUI offense",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
