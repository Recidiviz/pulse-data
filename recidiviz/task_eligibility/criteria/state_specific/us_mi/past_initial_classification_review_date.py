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
"""Defines a criteria span view that shows spans of time during which someone is past
their classification review date.
"""
from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import (
    TASK_COMPLETION_EVENTS_DATASET_ID,
    task_eligibility_criteria_state_specific_dataset,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_MI_PAST_INITIAL_CLASSIFICATION_REVIEW_DATE"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which
someone is past their classification review date 
        - If completed the Special Alternative to Incarceration (SAI), on intensive supervision for 4 months
        - If just completed electronic monitoring, eligible at completion date 
        - If serving a sex offense, and score is Moderate/High on supervision for at 12 months 
        - If serving a sex offense, and score is Moderate/Low and charges are CSC 1st (750.520b) or CSC 2nd (750. 520c), on supervision for 12 months 
        - Else, on supervision for 6 months 
"""
_QUERY_TEMPLATE = f"""
WITH supervision_starts_with_assessments AS (
/* This CTE finds start dates for all active supervision starts and joins the first assessment done within that supervision
 super session*/ 
  SELECT
    sss.state_code,
    sss.person_id,
    sss.start_date,
    sss.end_date_exclusive AS end_date,
    assessment_level_raw_text,
  FROM `{{project_id}}.{{sessions_dataset}}.supervision_super_sessions_materialized` sss
  LEFT JOIN `{{project_id}}.{{sessions_dataset}}.assessment_score_sessions_materialized` sap
    ON sss.state_code = sap.state_code
    AND sss.person_id = sap.person_id 
    AND sap.assessment_date BETWEEN sss.start_date AND {nonnull_end_date_exclusive_clause('sss.end_date_exclusive')}
  WHERE sss.state_code = "US_MI"
  --only choose the first assessment score within a supervision super session
  QUALIFY ROW_NUMBER() OVER(PARTITION BY sss.state_code, sss.person_id, sss.start_date, sss.end_date ORDER BY sap.assessment_date)=1
),
sentence_spans_with_info AS (
/* This CTE groups sentences by sentence span start_date and end_date, and adds relevant information on 
qualifying statutes and sex offenses for determining priority */ 
  SELECT
    span.state_code,
    span.person_id,
    span.start_date,
    span.end_date,
    LOGICAL_OR(statute IN ("750.520B", "750.520C")) AS qualifying_statute,
    LOGICAL_OR(is_sex_offense) AS is_sex_offense,
  FROM `{{project_id}}.{{sessions_dataset}}.sentence_spans_materialized` span,
  UNNEST (sentences_preprocessed_id_array) AS sentences_preprocessed_id
  INNER JOIN `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` sent
    USING (state_code, person_id, sentences_preprocessed_id)
  WHERE state_code = "US_MI"
  GROUP BY 1,2,3,4
),

supervision_starts_with_priority AS (
/* Here, supervision starts are either given a 'b_priority' priority_level or a 'z_default' priority_level based on
whether the offense type and risk level during that supervision session requires 12 months or 6 months on supervision.
If multiple sentence spans from requires 12 months on supervision overlap with a supervision super session, 
the first sentence span is chosen.  */ 
  SELECT
    ss.state_code,
    ss.person_id,
    ss.start_date,
    ss.end_date,
    CASE 
        WHEN (assessment_level_raw_text IN ('HIGH/MEDIUM', 'MEDIUM/HIGH', 'HIGH/LOW', 'LOW/HIGH', 'HIGH/HIGH')
                AND is_sex_offense) THEN 'b_priority'
        WHEN (COALESCE(qualifying_statute, FALSE) 
                AND assessment_level_raw_text IN ('LOW/LOW', 'LOW/MEDIUM', 'MEDIUM/LOW', 'MEDIUM/MEDIUM')
                AND is_sex_offense) THEN 'b_priority'
        ELSE 'z_default' 
        END AS priority_level
  FROM supervision_starts_with_assessments ss
  --sentence spans are joined such that they overlap with the start of a supervision super session
  LEFT JOIN sentence_spans_with_info q
    ON ss.state_code = q.state_code
    AND ss.person_id = q.person_id
    AND q.start_date <= ss.start_date
    AND {nonnull_end_date_clause('q.end_date')} > ss.start_date
),
critical_date_spans_all AS (
/* This CTE sets the critical dates for each supervision super session to 12 months or 6 months from the supervision
 start according to whether the client meets the requires_12_months_on_supervision criteria. The end date is the least of
 the end of the supervision session or the earliest classification review date during that supervision session. */
  SELECT
    sp.state_code,
    sp.person_id,
    sp.start_date,
    LEAST(ce.completion_event_date, end_date) AS end_date,
    IF(priority_level = 'b_priority', DATE_ADD(sp.start_date, INTERVAL 1 YEAR), 
                        DATE_ADD(start_date, INTERVAL 6 MONTH)) AS critical_date,
    priority_level,
   FROM supervision_starts_with_priority sp
   LEFT JOIN `{{project_id}}.{{completion_dataset}}.supervision_classification_review_materialized` ce
        ON sp.person_id = ce.person_id 
        AND sp.state_code = ce.state_code
        AND ce.completion_event_date BETWEEN sp.start_date AND {nonnull_end_date_exclusive_clause('sp.end_date')}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY sp.state_code, sp.person_id, sp.start_date, sp.end_date ORDER BY completion_event_date)=1
  UNION ALL 
    /* This CTE identifies when an individual has recently finished electronic monitoring
and sets the critical date to the start date of their new supervision level session. The end date is the least of 
 the end of their supervision level session or the earliest classification review date during that session.*/
  SELECT
    c.state_code,
    c.person_id,
    c.start_date, 
    LEAST(ce.completion_event_date, end_date) AS end_date, 
    c.start_date AS critical_date,
    'a_priority' AS priority_level
  FROM `{{project_id}}.{{criteria_dataset}}.completed_electronic_monitoring_materialized` c
  LEFT JOIN `{{project_id}}.{{completion_dataset}}.supervision_classification_review_materialized` ce
        ON c.person_id = ce.person_id 
        AND c.state_code = ce.state_code
        AND ce.completion_event_date BETWEEN c.start_date AND {nonnull_end_date_exclusive_clause('c.end_date')}
  WHERE meets_criteria
  QUALIFY ROW_NUMBER() OVER(PARTITION BY c.state_code, c.person_id, c.start_date, c.end_date ORDER BY completion_event_date)=1
  UNION ALL
      /* This CTE identifies when an individual is on supervision after completing the Special Alternative to  
    Incarceration (SAI) and sets the critical date to 4 months after they start SAI. The end date is the least of 
 the end of their supervision level session or the earliest classification review date during that session*/
  SELECT 
     sai.state_code,
     sai.person_id,
     sai.start_date,
     LEAST(ce.completion_event_date, end_date) AS end_date,
     DATE_ADD(sai.start_date, INTERVAL 4 MONTH) AS critical_date,
     'c_priority' AS priority_level
  FROM `{{project_id}}.{{criteria_dataset}}.supervision_level_is_sai_materialized` sai
  LEFT JOIN `{{project_id}}.{{completion_dataset}}.supervision_classification_review_materialized` ce
        ON sai.person_id = ce.person_id 
        AND sai.state_code = ce.state_code
        AND ce.completion_event_date BETWEEN sai.start_date AND {nonnull_end_date_exclusive_clause('sai.end_date')}
  WHERE meets_criteria
  QUALIFY ROW_NUMBER() OVER(PARTITION BY sai.state_code, sai.person_id, sai.start_date, sai.end_date ORDER BY completion_event_date)=1
),
/* For clients that have overlapping spans with different classification review dates, the review date is deduped
according to the following priority: 
    - a: immediately eligible after finishing electronic monitoring
    - b: 12 months required for sex offense and corresponding risk/level 
    - c: 4 months required if on SAI 
    - d: otherwise, 6 months required  */
{create_sub_sessions_with_attributes('critical_date_spans_all')},
critical_date_spans AS (
    SELECT
        state_code,
        person_id,
        start_date AS start_datetime,
        end_date AS end_datetime,
        critical_date,
    FROM sub_sessions_with_attributes
    QUALIFY ROW_NUMBER() OVER(PARTITION BY state_code, person_id, start_date, end_date ORDER BY priority_level)=1
),
{critical_date_has_passed_spans_cte()}
SELECT
    cd.state_code,
    cd.person_id,
    cd.start_date,
    cd.end_date,
    cd.critical_date_has_passed AS meets_criteria,
    TO_JSON(STRUCT(
        cd.critical_date AS eligible_date
    )) AS reason,
FROM critical_date_has_passed_spans cd
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        state_code=StateCode.US_MI,
        sessions_dataset=SESSIONS_DATASET,
        criteria_dataset=task_eligibility_criteria_state_specific_dataset(
            StateCode.US_MI
        ),
        completion_dataset=TASK_COMPLETION_EVENTS_DATASET_ID,
    )
)
if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
