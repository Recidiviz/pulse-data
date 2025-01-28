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
    1. special case: must serve 1 year on supervision
    2. life sentence: must serve 7 years on supervision
    3. non-life sentence for violent case: must serve 5 years on supervision
    4. non-life sentence for non-violent case: must serve 3 years on supervision
"""
# TODO(#37715) - Pull time on supervision from sentencing once sentencing v2 is implemented in PA

from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.task_eligibility.utils.us_pa_query_fragments import (
    case_when_special_case,
    offense_is_violent,
    us_pa_supervision_super_sessions,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_PA_MEETS_SPECIAL_CIRCUMSTANCES_CRITERIA_FOR_TIME_SERVED"

_DESCRIPTION = """Describes spans of time during which a candidate is potentially eligible for
    special circumstances supervision due to the time they've served on supervision,
    according to the following logic:
    1. special case: must serve 1 year on supervision
    2. life sentence: must serve 7 years on supervision
    3. non-life sentence for violent case: must serve 5 years on supervision
    4. non-life sentence for non-violent case: must serve 3 years on supervision
"""

_QUERY_TEMPLATE = f"""
WITH sentence_spans AS (
/* This CTE groups sentences by sentence span start_date and end_date, and adds relevant information on 
    whether the sentence is a life sentence */ 
    SELECT
        span.state_code,
        span.person_id,
        span.start_date,
        span.end_date,
        LOGICAL_OR(life_sentence) AS life_sentence_ind,
        LOGICAL_OR({offense_is_violent()}) AS violent_offense_ind,
    FROM `{{project_id}}.{{sessions_dataset}}.sentence_spans_materialized` span,
    UNNEST (sentences_preprocessed_id_array_actual_completion) AS sentences_preprocessed_id
    INNER JOIN `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` sent
        USING (state_code, person_id, sentences_preprocessed_id)
    WHERE state_code = "US_PA"
    GROUP BY 1,2,3,4
),
us_pa_supervision_super_sessions AS ({us_pa_supervision_super_sessions()}),
supervision_spans AS (
/* This CTE pulls supervision super sessions and joins the sentence span that overlaps with the beginning of each supervision period */
    SELECT
        sup.state_code,
        sup.person_id,
        sup.start_date,
        sup.end_date_exclusive AS end_date,
        CASE WHEN life_sentence_ind THEN 'life sentence'
            WHEN violent_offense_ind THEN 'non-life sentence (violent case)'
            ELSE 'non-life sentence (non-violent case)' 
        END AS case_type,
        sup.release_date,
    FROM us_pa_supervision_super_sessions sup
    LEFT JOIN sentence_spans sent
    --sentence spans are joined such that they overlap with the start of supervision (defined by release date)
        ON sup.state_code = sent.state_code
        AND sup.person_id = sent.person_id
        AND sent.start_date <= sup.release_date
        AND {nonnull_end_date_clause('sent.end_date')} > sup.release_date
    WHERE sup.state_code = "US_PA"
),
special_case_spans AS (
/* This CTE pulls all spans where someone is serving a special case */
    SELECT state_code,
        person_id,
        start_date,
        termination_date AS end_date,
        'special probation or parole case' AS case_type,
        CAST(NULL AS DATE) AS release_date,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_period`
    WHERE state_code = 'US_PA'
        AND {case_when_special_case()} THEN TRUE ELSE FALSE END
),
all_spans AS (
    SELECT * FROM supervision_spans
    UNION ALL  
    SELECT * FROM special_case_spans
),
{create_sub_sessions_with_attributes('all_spans')}, 
sub_sessions_with_priority AS (
/* this CTE de-dupes and assigns priority to one case type if someone is serving multiple in one sub-session */
    SELECT state_code, 
        person_id, 
        start_date, 
        end_date,
        CASE WHEN LOGICAL_OR(case_type = 'special probation or parole case') THEN 'special probation or parole case'
             WHEN LOGICAL_OR(case_type = 'life sentence') THEN 'life sentence'
             WHEN LOGICAL_OR(case_type = 'non-life sentence (violent case)') THEN 'non-life sentence (violent case)'
             WHEN LOGICAL_OR(case_type = 'non-life sentence (non-violent case)') THEN 'non-life sentence (non-violent case)'
        ELSE NULL END AS case_type, 
        MAX(release_date) AS release_date_nonnull -- take non-null value from super sessions, rather than null value from special case spans
    FROM sub_sessions_with_attributes
    GROUP BY 1, 2, 3, 4
    HAVING(MAX(release_date) IS NOT NULL) 
    -- this filters out any spans where there is no release date
),
supervision_spans_with_priority AS (
    {aggregate_adjacent_spans(table_name = 'sub_sessions_with_priority', attribute = ['case_type', 'release_date_nonnull'])}
),
critical_date_spans AS (
/* This CTE assigns the critical date as 1, 3, 5, or 7 years from the supervision super session start
    depending on the case type */
  SELECT 
    person_id,
    state_code,
    start_date as start_datetime,
    end_date as end_datetime,
    case_type,
    CASE WHEN case_type = 'special probation or parole case' THEN DATE_ADD(release_date_nonnull, INTERVAL 1 YEAR)
        WHEN case_type = 'life sentence' THEN DATE_ADD(release_date_nonnull, INTERVAL 7 YEAR)
        WHEN case_type = 'non-life sentence (violent case)' THEN DATE_ADD(release_date_nonnull, INTERVAL 5 YEAR)
        WHEN case_type = 'non-life sentence (non-violent case)' THEN DATE_ADD(release_date_nonnull, INTERVAL 3 YEAR)
        ELSE NULL
    END AS critical_date,
    CASE WHEN case_type = 'special probation or parole case' THEN 1
        WHEN case_type = 'life sentence' THEN 7
        WHEN case_type = 'non-life sentence (violent case)' THEN 5
        WHEN case_type = 'non-life sentence (non-violent case)' THEN 3
        ELSE NULL
    END AS years_required_to_serve,
  FROM supervision_spans_with_priority
),
{critical_date_has_passed_spans_cte(attributes=['case_type', 'years_required_to_serve'])}
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    critical_date_has_passed AS meets_criteria,
    TO_JSON(STRUCT(
        case_type,
        years_required_to_serve,
        critical_date AS eligible_date
    )) AS reason,
    case_type,
    years_required_to_serve,
    critical_date AS eligible_date,
FROM critical_date_has_passed_spans
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    state_code=StateCode.US_PA,
    sessions_dataset=SESSIONS_DATASET,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    us_pa_raw_data_dataset=raw_tables_dataset_for_region(
        state_code=StateCode.US_PA,
        instance=DirectIngestInstance.PRIMARY,
    ),
    reasons_fields=[
        ReasonsField(
            name="case_type",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Type of supervision case (special, life, non-life violent, or non-life non-violent)",
        ),
        ReasonsField(
            name="years_required_to_serve",
            type=bigquery.enums.StandardSqlTypeNames.INT64,
            description="Years required to serve on supervision before being eligible for special circumstances supervision. Depends on case type.",
        ),
        ReasonsField(
            name="eligible_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date where a client will have served enough time on supervision to be eligible for special circumstances supervision",
        ),
    ],
)
if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
