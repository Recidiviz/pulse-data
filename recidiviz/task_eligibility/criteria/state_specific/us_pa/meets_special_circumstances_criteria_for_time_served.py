# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Describes spans of time during which a candidate is potentially eligible for
    special circumstances supervision due to the time they've served on supervision,
    according to the following logic:
    1. life sentence: must serve 7 years on supervision
    2. non-life sentence for violent case: must serve 5 years on supervision
    3. non-life sentence for non-violent case: must serve 3 years on supervision
    4. special case: must serve 1 year on supervision
"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
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
from recidiviz.task_eligibility.utils.us_pa_query_fragments import (
    case_when_special_case,
    supervision_legal_authority_sessions_excluding_general_incarceration,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_PA_MEETS_SPECIAL_CIRCUMSTANCES_CRITERIA_FOR_TIME_SERVED"

_DESCRIPTION = """Describes spans of time during which a candidate is potentially eligible for
    special circumstances supervision due to the time they've served on supervision,
    according to the following logic:
    1. life sentence: must serve 7 years on supervision
    2. non-life sentence for violent case: must serve 5 years on supervision
    3. non-life sentence for non-violent case: must serve 3 years on supervision
    4. special case: must serve 1 year on supervision
"""

_QUERY_TEMPLATE = f"""
WITH aggregated_supervision_periods_excluding_general_incarceration AS ({supervision_legal_authority_sessions_excluding_general_incarceration()}),
/* this pulls + aggregates all periods where someone is considered on supervision UNLESS they're concurrently in general incarceration */

supervision_starts_with_assessments AS (
/* This CTE finds start dates for all active supervision starts and joins the first assessment done within that supervision
 super session */ 
  SELECT
    sup.state_code,
    sup.person_id,
    sup.start_date,
    sup.end_date_exclusive AS end_date,
    sup.super_session_id,
    assessment_level,
  FROM aggregated_supervision_periods_excluding_general_incarceration sup
  LEFT JOIN `{{project_id}}.{{sessions_dataset}}.assessment_score_sessions_materialized` sap
    ON sup.state_code = sap.state_code
    AND sup.person_id = sap.person_id 
    AND sap.assessment_date BETWEEN sup.start_date AND {nonnull_end_date_exclusive_clause('sup.end_date_exclusive')}
  --only choose the first assessment score within a supervision super session
  QUALIFY ROW_NUMBER() OVER(PARTITION BY sup.person_id, sup.start_date, sup.end_date_exclusive ORDER BY sap.assessment_date)=1
),
sentence_spans_with_info AS (
/* This CTE groups sentences by sentence span start_date and end_date, and adds relevant information on 
    whether the sentence is a life sentence */ 
  SELECT
    span.state_code,
    span.person_id,
    span.start_date,
    span.end_date,
    LOGICAL_OR(life_sentence) AS is_life_sentence,
  FROM `{{project_id}}.{{sessions_dataset}}.sentence_spans_materialized` span,
  UNNEST (sentences_preprocessed_id_array_actual_completion) AS sentences_preprocessed_id
  INNER JOIN `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` sent
    USING (state_code, person_id, sentences_preprocessed_id)
  WHERE state_code = "US_PA"
  GROUP BY 1,2,3,4
),
supervision_starts_with_priority AS (
/* Here, supervision starts are given a case type, which determine how long someone has to spend on supervision
  before being eligible. This type is determined on their status at the beginning of their supervision super session */ 
  SELECT
    ss.state_code,
    ss.person_id,
    ss.start_date,
    ss.end_date,
    ss.super_session_id,
    {case_when_special_case()} THEN 'special probation or parole case'
        WHEN is_life_sentence THEN 'life sentence'
        WHEN assessment_level IN ('MINIMUM', 'MEDIUM') THEN 'non-life sentence (violent case)' # placeholder while we wait for strong-r data 
        ELSE 'non-life sentence (non-violent case)'
        END AS case_type,
  FROM supervision_starts_with_assessments ss
  --sentence spans are joined such that they overlap with the start of a supervision super session and therefore
  --an entire supervision super session has only one case type 
  LEFT JOIN sentence_spans_with_info q
    ON ss.state_code = q.state_code
    AND ss.person_id = q.person_id
    AND q.start_date <= ss.start_date
    AND {nonnull_end_date_clause('q.end_date')} > ss.start_date
  LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_supervision_period` ssp
    ON ss.state_code = ssp.state_code
    AND ss.person_id = ssp.person_id
    AND ssp.start_date <= ss.start_date
    AND {nonnull_end_date_clause('ssp.termination_date')} > ss.start_date
),
critical_date_spans AS (
/* This CTE assigns the critical date as 1, 3, 5, or 7 years from the supervision super session start
    depending on the priority level assigned in supervision_starts_with_priority. It also assigns 
    case type and years required to serve, which are eventually displayed as reasons */
  SELECT 
    person_id,
    state_code,
    start_date as start_datetime,
    end_date as end_datetime,
    case_type,
    CASE
        WHEN case_type = 'special probation or parole case' THEN DATE_ADD(start_date, INTERVAL 1 YEAR)
        WHEN case_type = 'life sentence' THEN DATE_ADD(start_date, INTERVAL 7 YEAR)
        WHEN case_type = 'non-life sentence (violent case)' THEN DATE_ADD(start_date, INTERVAL 5 YEAR)
        WHEN case_type = 'non-life sentence (non-violent case)' THEN DATE_ADD(start_date, INTERVAL 3 YEAR)
        ELSE NULL
    END AS critical_date,
    CASE
        WHEN case_type = 'special probation or parole case' THEN '1'
        WHEN case_type = 'life sentence' THEN '7'
        WHEN case_type = 'non-life sentence (violent case)' THEN '5'
        WHEN case_type = 'non-life sentence (non-violent case)' THEN '3'
        ELSE NULL
    END AS years_required_to_serve,
  FROM supervision_starts_with_priority 
),
{critical_date_has_passed_spans_cte(attributes=['case_type', 'years_required_to_serve'])}
SELECT
    cd.state_code,
    cd.person_id,
    cd.start_date,
    cd.end_date,
    cd.critical_date_has_passed AS meets_criteria,
    TO_JSON(STRUCT(
        cd.case_type AS case_type,
        cd.years_required_to_serve AS years_required_to_serve,
        cd.critical_date AS eligible_date
    )) AS reason,
    cd.case_type AS case_type,
    cd.years_required_to_serve AS years_required_to_serve,
    cd.critical_date AS eligible_date,
FROM critical_date_has_passed_spans cd
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    state_code=StateCode.US_PA,
    sessions_dataset=SESSIONS_DATASET,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    reasons_fields=[
        ReasonsField(
            name="case_type",
            type=bigquery.enums.SqlTypeNames.STRING,
            description="Type of supervision case (special, life, non-life violent, or non-life non-violent)",
        ),
        ReasonsField(
            name="years_required_to_serve",
            type=bigquery.enums.SqlTypeNames.STRING,
            description="Years required to serve on supervision before being eligible for special circumstances supervision. Depends on case type.",
        ),
        ReasonsField(
            name="eligible_date",
            type=bigquery.enums.SqlTypeNames.DATE,
            description="Date where a client will have served enough time on supervision to be eligible for special circumstances supervision",
        ),
    ],
)
if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
