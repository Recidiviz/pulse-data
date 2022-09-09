# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
Defines a criteria span view that shows spans of time during which
someone in ID has a valid LSIR level for the required number of days:
-LSI-R score at or below the low potential level to reoffend with no increase in risk
 for the 90 days of active superivion
-At or below moderate potential to reoffend can have no increase in risk level 360 days prior
"""
from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    revert_nonnull_end_date_clause,
)
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_ID_LSIR_LEVEL_LOW_MODERATE_FOR_X_DAYS"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which
someone in ID has a valid LSIR level for the required number of days:
-LSI-R score at or below the low potential level to reoffend with no increase in risk 
 for the 90 days of active superivion
-At or below moderate potential to reoffend can have no increase in risk level 360 days prior 
"""

_QUERY_TEMPLATE = f"""
/*{{description}}*/
WITH LSIR_level_gender AS(
  /* This CTE creates a view of LSIR-score level by gender according to updated
  Idaho Supervision Categories (07/21/2020) */
  SELECT
      score.person_id,
      score.state_code,
      score.assessment_date,
      score.score_end_date,
      score.assessment_score,
  #TODO(#15022) add historical ranges 
  #TODO(#15153) move this logic to dataflow
      CASE
          WHEN ((gender != "MALE" OR gender IS NULL) AND assessment_score <=22) THEN "LOW"
          WHEN ((gender != "MALE" OR gender IS NULL)
                                AND (assessment_score BETWEEN 23 AND 30)) THEN "MODERATE"
          WHEN (gender = "MALE" AND assessment_score <=20) THEN "LOW"
          WHEN (gender = "MALE" AND (assessment_score BETWEEN 21 AND 28)) THEN "MODERATE"
          ELSE "HIGH"
      END AS lsir_level
  FROM `{{project_id}}.{{sessions_dataset}}.assessment_score_sessions_materialized` score
  LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person` info
      ON score.state_code = info.state_code
      AND score.person_id = info.person_id
  WHERE score.state_code = 'US_ID' 
      AND assessment_type = 'LSIR'
),
 grp_starts as (
   /* This CTE identifies when the LSI-R level changes
  grp_start is 1 at the beginning of each new start date and score 
  and 0 for consecutive periods with the same LSI-R score */
  SELECT 
      state_code,
      person_id,
      lsir_level, 
      assessment_date,
      score_end_date,
      CASE
        WHEN lsir_level = LAG(lsir_level) OVER(PARTITION BY person_id ORDER BY person_id, assessment_date)
        THEN 0 ELSE 1 
      END grp_start
  FROM LSIR_level_gender
),
  grps as (
    /* This CTE sums grp_starts to group together adjacent periods where the LSI-R 
    score is the same */
    SELECT
        state_code,
        person_id,
        lsir_level, 
        assessment_date,
        score_end_date,
        SUM(grp_start) OVER(ORDER BY person_id, assessment_date, score_end_date) AS grp
    FROM grp_starts
),
  lsir_spans AS (
    /* This CTE actually combines adjacent periods where the LSI-R
     score is the same by choosing min start and max end date*/
    SELECT 
        state_code, 
        person_id,
        lsir_level, 
        MIN(assessment_date) AS start_date,
        {revert_nonnull_end_date_clause(f'''MAX({nonnull_end_date_clause('score_end_date')})''')} AS end_date
    FROM grps
    GROUP BY grp, person_id, lsir_level, state_code
),
  lsir_spans_thresholds AS (
    /* This CTE calculates the days at the current level, as well as the days
    at the previous level and what the previous level was*/
    SELECT
        *, 
        DATE_DIFF(COALESCE(end_date, CURRENT_DATE('US/Pacific')), start_date, DAY) AS days_at_level,
        LAG(lsir_level) OVER(PARTITION BY person_id ORDER BY start_date) AS previous_lsir,
        IF((lsir_level IN ("LOW", "MODERATE") 
            AND LAG(lsir_level) OVER(PARTITION BY person_id ORDER BY start_date) IN ("LOW", "MODERATE")),
        DATE_DIFF(COALESCE(end_date, CURRENT_DATE('US/Pacific')), start_date, DAY) 
                    + LAG(DATE_DIFF(COALESCE(end_date, CURRENT_DATE('US/Pacific')), start_date, DAY))
                            OVER(PARTITION BY person_id ORDER BY start_date), NULL) AS combined_days_at_level
        FROM lsir_spans
)

SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    --include low >= 90, moderate >= 360, or moderate + low >=360
    ((lsir_level = 'LOW' AND days_at_level >= 90) OR
     (lsir_level = 'MODERATE' AND days_at_level>= 360) OR
      (lsir_level = 'LOW' AND previous_lsir = 'MODERATE') AND combined_days_at_level >=360) AS meets_criteria,
    TO_JSON(
            STRUCT(lsir_level AS risk_level, days_at_level AS days_at_level, 
                  combined_days_at_level AS combined_days_at_moderate_or_low_level)
        ) AS reason,
FROM lsir_spans_thresholds
ORDER BY person_id
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_ID,
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        sessions_dataset=SESSIONS_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
