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
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_criteria_state_specific_dataset,
)
from recidiviz.task_eligibility.reasons_field import ReasonsField
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
    sss.session_id_start,
    sss.session_id_end,
    sss.compartment_level_1_super_session_id AS super_session_id,
    assessment_level,
  FROM `{{project_id}}.{{sessions_dataset}}.compartment_level_1_super_sessions_materialized` sss
  LEFT JOIN `{{project_id}}.{{sessions_dataset}}.assessment_score_sessions_materialized` sap
    ON sss.state_code = sap.state_code
    AND sss.person_id = sap.person_id 
    AND sap.assessment_date BETWEEN sss.start_date AND {nonnull_end_date_exclusive_clause('sss.end_date_exclusive')}
  WHERE sss.state_code = "US_MI"
    AND sss.compartment_level_1 = 'SUPERVISION'
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
    -- since both statutes ("750.520B" "750.520C") are considered sex offenses, there will never be a case where
    -- qualifying_statute is TRUE and is_sex_offense is not true. However, is_sex_offense might be true and
    -- qualifying statute my be false. 
    LOGICAL_OR(statute LIKE '750.520B%' OR statute LIKE '750.520C%') AS qualifying_statute,
    LOGICAL_OR(is_sex_offense) AS is_sex_offense,
  FROM `{{project_id}}.{{sessions_dataset}}.sentence_spans_materialized` span,
  UNNEST (sentences_preprocessed_id_array_actual_completion) AS sentences_preprocessed_id
  INNER JOIN `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` sent
    USING (state_code, person_id, sentences_preprocessed_id)
  WHERE state_code = "US_MI"
  GROUP BY 1,2,3,4
),
supervision_starts_with_priority AS (
/* Here, supervision starts are either given a 'a_priority' priority_level or a 'z_default' priority_level based on
whether the offense type and risk level during that supervision session requires 12 months or 6 months on supervision.
Only sentence spans that overlap with the beginning of the supervision super session are considered. */ 
  SELECT
    ss.state_code,
    ss.person_id,
    ss.start_date,
    ss.end_date,
    ss.session_id_start,
    ss.session_id_end,
    ss.super_session_id,
    CASE 
        WHEN (assessment_level IN ('MEDIUM_HIGH', 'MAXIMUM')
                AND is_sex_offense) THEN 'a_priority'
        WHEN (COALESCE(qualifying_statute, FALSE) 
                AND assessment_level IN ('MINIMUM', 'MEDIUM')
                AND is_sex_offense) THEN 'a_priority'
        ELSE 'z_default' 
        END AS priority_level
  FROM supervision_starts_with_assessments ss
  --sentence spans are joined such that they overlap with the start of a supervision super session and therefore
  --an entire supervision super session either has a a_priority level OR a z_default priority level
  LEFT JOIN sentence_spans_with_info q
    ON ss.state_code = q.state_code
    AND ss.person_id = q.person_id
    AND q.start_date <= ss.start_date
    AND {nonnull_end_date_clause('q.end_date')} > ss.start_date
),
active_supervision_population_cumulative_day_spans AS 
/* This CTE analyses compartment_level_1_super_sessions to create a cumulative_inactive_days variable that counts days spent
in inactive supervision, currently defined as compartment_level_2 = `BENCH_WARRANT`. This variable is used to NULL out
critical dates during inactive sub sessions as well as push out the critical date by the number of cumulative_inactive_days
once an active supervision sub session resumes. */ 
(
/*Join sub-sessions to super-sessions*/
  SELECT 
    sub.person_id,
    sub.sub_session_id,
    sub.session_id,
    sub.state_code,
    super.super_session_id AS super_session_id,
    sub.start_date,
    sub.end_date_exclusive AS end_date,
    sub.compartment_level_1,
    sub.compartment_level_2,
    sub.session_length_days,
    
    sub.inactive_session_days,
    SUM(sub.inactive_session_days) OVER(PARTITION BY sub.person_id, sub.state_code, super.super_session_id ORDER BY sub.start_date) AS cumulative_inactive_days,
    super.start_date AS super_session_start_date,
    super.end_date AS super_session_end_date,
    super.priority_level,
  FROM
      (SELECT
          *,
          -- Here is where we define which sessions within a supervision super session should stop the clock
          IF(compartment_level_2 IN ('BENCH_WARRANT') OR 
             correctional_level IN ('ABSCONSION', 'IN_CUSTODY'), session_length_days, 0) AS inactive_session_days,
      FROM `{{project_id}}.{{sessions_dataset}}.compartment_sub_sessions_materialized` 
      ) sub
  JOIN supervision_starts_with_priority super
    ON sub.person_id = super.person_id
    AND sub.state_code = super.state_code
    AND sub.session_id BETWEEN super.session_id_start AND super.session_id_end
),
critical_date_sub_session_spans AS (
/* This CTE sets a NULL critical date for inactive supervision sub sessions and sets the critical date as 6 or
12 months from the supervision super session start depending on the priority level assigned in 
supervision_starts_with_priority for active sub sessions. In addition, for active sub sessions preceded by inactive 
sub sessions, it bumps out the critical date by the number of days spent on inactive supervision. */ 
  SELECT 
    person_id,
    state_code,
    start_date,
    end_date,
    CASE
        WHEN inactive_session_days=0 AND priority_level = 'a_priority' 
            THEN DATE_ADD(DATE_ADD(super_session_start_date, INTERVAL 1 YEAR), INTERVAL cumulative_inactive_days DAY)
        WHEN inactive_session_days=0 AND priority_level = 'z_default' 
            THEN DATE_ADD(DATE_ADD(super_session_start_date, INTERVAL 6 MONTH), INTERVAL cumulative_inactive_days DAY)
        ELSE NULL
    END AS critical_date,
    priority_level,
    super_session_start_date,
    super_session_end_date,
  FROM active_supervision_population_cumulative_day_spans 
),
critical_date_spans_all AS (
/* This CTE first aggregates adjacent spans from critical_date_sub_session_spans based on the critical date and 
priority level. 

Then, critical date spans for clients on supervision after completing the Special Alternative to Incarceration (SAI)
are unioned with the associated priority level (b_priority).

For all spans, the most recent classification review date for each supervision super session is joined so that all 
critical date spans for the supervision super session end when the first classification review date takes place */ 
  SELECT 
    sp.state_code,
    sp.person_id,
    sp.start_date,
    LEAST({nonnull_end_date_clause('ce.completion_event_date')}, {nonnull_end_date_clause('sp.end_date')}) AS end_date, 
    sp.critical_date,
    sp.priority_level,
   FROM ({aggregate_adjacent_spans(table_name='critical_date_sub_session_spans', 
                                  attribute=['critical_date', 'priority_level', 'super_session_start_date', 'super_session_end_date'])}) sp
   LEFT JOIN `{{project_id}}.{{analyst_views_dataset}}.us_mi_supervision_classification_review_materialized` ce
        ON sp.person_id = ce.person_id
        AND sp.state_code = ce.state_code
        AND ce.completion_event_date BETWEEN sp.super_session_start_date AND {nonnull_end_date_clause('sp.super_session_end_date')}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY sp.state_code, sp.person_id, sp.start_date, sp.end_date ORDER BY completion_event_date)=1
  UNION ALL
      /* This CTE identifies when an individual is on supervision after completing the Special Alternative to  
    Incarceration (SAI) and sets the critical date to 4 months after they start SAI. The end date is the least of 
 the end of their supervision level session or the earliest classification review date during that session*/
  SELECT 
     sai.state_code,
     sai.person_id,
     sai.start_date,
     LEAST({nonnull_end_date_clause('ce.completion_event_date')}, {nonnull_end_date_clause('end_date')}) AS end_date,
     DATE_ADD(sai.start_date, INTERVAL 4 MONTH) AS critical_date,
     'b_priority' AS priority_level
  FROM `{{project_id}}.{{criteria_dataset}}.supervision_or_supervision_out_of_state_level_is_sai_materialized` sai
  LEFT JOIN `{{project_id}}.{{analyst_views_dataset}}.us_mi_supervision_classification_review_materialized` ce
        ON sai.person_id = ce.person_id 
        AND sai.state_code = ce.state_code
        AND ce.completion_event_date BETWEEN sai.start_date AND {nonnull_end_date_exclusive_clause('sai.end_date')}
  WHERE meets_criteria
  QUALIFY ROW_NUMBER() OVER(PARTITION BY sai.state_code, sai.person_id, sai.start_date, sai.end_date ORDER BY completion_event_date)=1
),
critical_date_spans_all_deduped AS (
/* For clients that have overlapping spans with different classification review dates, the review date is deduped
according to the following priority: 
    - a: 12 months required for sex offense and corresponding risk/level 
    - b: 4 months required if on SAI 
    - z: otherwise, 6 months required  */
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        critical_date,
        priority_level
    FROM critical_date_spans_all
    --each supervision start should only have one critical date assigned 
    QUALIFY ROW_NUMBER() OVER(PARTITION BY state_code, person_id, start_date ORDER BY priority_level, critical_date DESC)=1
),
{create_sub_sessions_with_attributes('critical_date_spans_all_deduped')},
critical_date_spans AS (
    SELECT
        state_code,
        person_id,
        start_date AS start_datetime,
        end_date AS end_datetime,
        critical_date,
    FROM sub_sessions_with_attributes
    --start date can equal end date when classification reviews are done on the same day a supervision session starts
    --these sessions should be excluded from critical date spans have passed
    WHERE start_date != {nonnull_end_date_clause('end_date')}
    --for overlapping spans that have different start dates (Ie the supervision start vs. the SAI start)
    --spans should be deduped again so that there is only one critical date associated with each span 
    QUALIFY ROW_NUMBER() OVER(PARTITION BY state_code, person_id, start_date, end_date ORDER BY priority_level, critical_date DESC)=1
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
    cd.critical_date AS classification_review_date,
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
        analyst_views_dataset=ANALYST_VIEWS_DATASET,
        reasons_fields=[
            ReasonsField(
                name="classification_review_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Expected classification review date",
            ),
        ],
    )
)
if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
