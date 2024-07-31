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
 for the 90 days of active supervision
-At or below moderate potential to reoffend can have no increase in risk level 360 days prior
"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
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

_CRITERIA_NAME = "US_IX_LSIR_LEVEL_LOW_MODERATE_FOR_X_DAYS"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which
someone in ID has a valid LSIR level for the required number of days:
-LSI-R score at or below the low potential level to reoffend with no increase in risk 
 for the 90 days of active supervision
-At or below moderate potential to reoffend can have no increase in risk level 360 days prior 
"""

_QUERY_TEMPLATE = f"""
WITH LSIR_level_gender AS(
  /* This CTE creates a view of LSIR-score level by gender according to updated
  Idaho Supervision Categories (07/21/2020) */
  SELECT
      score.person_id,
      score.state_code,
      score.assessment_date AS score_start_date,
      {nonnull_end_date_clause('score.score_end_date_exclusive')} AS score_end_date,
      ses.start_date AS supervision_start_date,
      ses.end_date_exclusive AS supervision_end_date,
      score.assessment_score,
      CASE
          WHEN ((gender != "MALE" OR gender IS NULL) AND assessment_score <=22) THEN "LOW"
          WHEN ((gender != "MALE" OR gender IS NULL)
                                AND (assessment_score BETWEEN 23 AND 30)) THEN "MODERATE"
          WHEN (gender = "MALE" AND assessment_score <=20) THEN "LOW"
          WHEN (gender = "MALE" AND (assessment_score BETWEEN 21 AND 28)) THEN "MODERATE"
          ELSE "HIGH"
          END AS lsir_level
  FROM `{{project_id}}.{{sessions_dataset}}.assessment_score_sessions_materialized` score
  INNER JOIN `{{project_id}}.{{sessions_dataset}}.supervision_super_sessions_materialized`ses
    ON score.state_code = ses.state_code
    AND score.person_id = ses.person_id
    --only consider scores relevant in the supervision session during which they occur 
    AND ses.start_date < {nonnull_end_date_clause('score.score_end_date_exclusive')}
    AND score.assessment_date < {nonnull_end_date_clause('ses.end_date_exclusive')}
  LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person` info
      ON score.state_code = info.state_code
      AND score.person_id = info.person_id
  WHERE score.state_code = 'US_IX' 
      AND assessment_type = 'LSIR'
),
grp_starts as (
   /* This CTE identifies when the LSI-R level changes within a supervision session. 
   grp_start is 1 at the beginning of each new level span and 0 for consecutive periods with the same LSI-R score. 
  
  This CTE also combines LOW spans with directly previous MODERATE spans to assess when risk level is at or below moderate.
   Therefore, LOW spans will also have grp_start as 0 if preceded by a MODERATE span*/
  SELECT 
      state_code,
      person_id,
      lsir_level, 
      supervision_start_date,
      supervision_end_date,
      score_start_date,
      score_end_date,
      CASE
      --partition by person and supervision start date so only assessments within the same supervision session are counted
        WHEN lsir_level = LAG(lsir_level) OVER supervision_window THEN 0
        --combine low spans with previously adjacent moderate spans
        WHEN lsir_level = "LOW" AND LAG(lsir_level) OVER supervision_window = "MODERATE" THEN 0
        ELSE 1 
      END grp_start
  FROM LSIR_level_gender
  WINDOW supervision_window AS (
    PARTITION BY person_id, supervision_start_date ORDER BY score_start_date
  )
),
grps as (
    /* This CTE sums grp_starts to group together adjacent periods where the LSI-R score is the same
     or when a LOW level is preceded by a MODERATE level */
    SELECT
        state_code,
        person_id,
        lsir_level, 
        supervision_start_date,
        supervision_end_date,
        score_start_date,
        score_end_date,
        SUM(grp_start) OVER(ORDER BY person_id, supervision_start_date, score_start_date) AS grp
    FROM grp_starts
),
 lsir_spans AS (
    /* This CTE actually combines adjacent periods where the LSI-R score is the same by 
    choosing min start and max end date.
    
    This view is unioned to itself once where it is grouped by lsir_level, and once
    where lsir_level is aggregated. This allows for eligibility calculations for
    both the LOW span itself, and the LOW span preceded by a MODERATE span*/
    SELECT 
        state_code, 
        person_id,
        --create 'MODERATE,LOW' category for combined level periods
        STRING_AGG(DISTINCT lsir_level ORDER BY lsir_level DESC) AS lsir_level,
        MIN(score_start_date) AS score_span_start_date,
        --end span date on supervision session end date, or score end date, whichever comes first
        MAX(LEAST(score_end_date,{nonnull_end_date_clause('supervision_end_date')})) AS score_span_end_date
    FROM grps
    GROUP BY grp, state_code, person_id
    UNION DISTINCT
    SELECT 
        state_code, 
        person_id,
        lsir_level,
        MIN(score_start_date) AS score_span_start_date,
        --end span date on supervision session end date, or score end date, whichever comes first
        MAX(LEAST(score_end_date,{nonnull_end_date_clause('supervision_end_date')})) AS score_span_end_date
    FROM grps
    GROUP BY grp, state_code, person_id, lsir_level
),
critical_date_spans AS (
    SELECT
        state_code,
        person_id,
        score_span_start_date AS start_datetime,
        score_span_end_date AS end_datetime,
        CASE 
            WHEN lsir_level = "LOW" THEN DATE_ADD(score_span_start_date, INTERVAL 90 DAY)
            WHEN lsir_level IN ("MODERATE", "MODERATE,LOW") THEN DATE_ADD(score_span_start_date, INTERVAL 360 DAY)
            END AS critical_date
    FROM lsir_spans
    WHERE lsir_level IN ("LOW", "MODERATE", "MODERATE,LOW")
),
{critical_date_has_passed_spans_cte()},
{create_sub_sessions_with_attributes('critical_date_has_passed_spans')}
SELECT 
    ssa.state_code,
    ssa.person_id,
    ssa.start_date,
    ssa.end_date,
    --choose TRUE spans over FALSE 
    LOGICAL_OR(critical_date_has_passed) AS meets_criteria,
    TO_JSON(STRUCT(MIN(critical_date) AS eligible_date, MAX(lsir_level) AS risk_level)) AS reason,
    MIN(critical_date) AS eligible_date,
    MAX(lsir_level) AS risk_level,
FROM sub_sessions_with_attributes ssa
LEFT JOIN LSIR_level_gender ls
  ON ls.state_code = ssa.state_code
  AND ls.person_id = ssa.person_id
  AND ssa.start_date >= ls.score_start_date AND ssa.start_date < ls.score_end_date
GROUP BY 1,2,3,4
ORDER BY person_id, start_date
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    state_code=StateCode.US_IX,
    criteria_name=_CRITERIA_NAME,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    description=_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    reasons_fields=[
        ReasonsField(
            name="eligible_date",
            type=bigquery.enums.SqlTypeNames.DATE,
            description="The date on which the person had a valid LSIR level for the required number of days",
        ),
        ReasonsField(
            name="risk_level",
            type=bigquery.enums.SqlTypeNames.STRING,
            description="The risk level of the client",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
