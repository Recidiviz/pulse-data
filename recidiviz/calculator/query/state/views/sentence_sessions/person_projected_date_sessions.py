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
# =============================================================================
"""
View that is unique on person_id, state_code, sentence_inferred_group_id, and start_date. Intersects sentence serving
periods with sentence and sentence group projected dates to create a view that represents the full set of projected
dates associated with a person at a given time. A new session is created if there is any of the following 3 changes in
attributes: (1) date change at the group level, (2) change in a sentence being served within that group, (3) date change
at the sentence level.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SENTENCE_SESSIONS_DATASET
from recidiviz.calculator.query.state.views.sessions.state_sentence_configurations import (
    STATES_NOT_MIGRATED_TO_SENTENCE_V2_SCHEMA,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PERSON_PROJECTED_DATE_SESSIONS_VIEW_ID = "person_projected_date_sessions"

QUERY_TEMPLATE = f"""
WITH serving_periods_and_dates AS
(
-- Hydrates the `is_serving` flag and is used to clip projected date sessions to serving periods
SELECT DISTINCT
    state_code,
    person_id,
    sentence_inferred_group_id,
    sentence_id,
    start_date,
    end_date_exclusive,
    TRUE AS is_serving,
    CAST(NULL AS DATE) AS parole_eligibility_date,
    CAST(NULL AS DATE) AS projected_parole_release_date,
    CAST(NULL AS DATE) AS projected_full_term_release_date_min,
    CAST(NULL AS DATE) AS projected_full_term_release_date_max,
    CAST(NULL AS DATE) AS sentence_parole_eligibility_date,
    CAST(NULL AS DATE) AS sentence_projected_parole_release_date,
    CAST(NULL AS DATE) AS sentence_projected_full_term_release_date_min,
    CAST(NULL AS DATE) AS sentence_projected_full_term_release_date_max,
    CAST(NULL AS INT64) AS sentence_length_days_min,
    CAST(NULL AS INT64) AS sentence_length_days_max,
    CAST(NULL AS INT64) AS good_time_days,
    CAST(NULL AS INT64) AS earned_time_days,
FROM `{{project_id}}.{{sentence_sessions_dataset}}.sentence_serving_period_materialized`sp
JOIN `{{project_id}}.{{sentence_sessions_dataset}}.sentences_and_charges_materialized`
    USING(person_id, state_code, sentence_id)
-- only include migrated states here because we will add in v1 serving periods separately
WHERE state_code NOT IN ({{v2_non_migrated_states}})

UNION ALL

-- Hydrates the group-level projected date fields 
SELECT
    state_code,
    person_id,
    sentence_inferred_group_id,
    CAST(NULL AS INT64) AS sentence_id,
    start_date,
    end_date_exclusive,
    CAST(NULL AS BOOLEAN) AS is_serving,
    parole_eligibility_date,
    projected_parole_release_date,
    projected_full_term_release_date_min,
    projected_full_term_release_date_max,
    CAST(NULL AS DATE) AS sentence_parole_eligibility_date,
    CAST(NULL AS DATE) AS sentence_projected_parole_release_date,
    CAST(NULL AS DATE) AS sentence_projected_full_term_release_date_min,
    CAST(NULL AS DATE) AS sentence_projected_full_term_release_date_max,
    CAST(NULL AS INT64) AS sentence_length_days_min,
    CAST(NULL AS INT64) AS sentence_length_days_max,
    CAST(NULL AS INT64) AS good_time_days,
    CAST(NULL AS INT64) AS earned_time_days,
FROM `{{project_id}}.{{sentence_sessions_dataset}}.sentence_inferred_group_projected_date_sessions_materialized`
-- this view already only has v2 migrated states

UNION ALL

-- Hydrates the sentence-level projected date fields
SELECT DISTINCT
    state_code,
    person_id,
    sentence_inferred_group_id,
    sentence_id,
    start_date,
    end_date_exclusive,
    CAST(NULL AS BOOLEAN) AS is_serving,
    CAST(NULL AS DATE) AS parole_eligibility_date,
    CAST(NULL AS DATE) AS projected_parole_release_date,
    CAST(NULL AS DATE) AS projected_full_term_release_date_min,
    CAST(NULL AS DATE) AS projected_full_term_release_date_max,
    parole_eligibility_date AS sentence_parole_eligibility_date,
    projected_parole_release_date AS sentence_projected_parole_release_date,
    projected_full_term_release_date_min AS sentence_projected_full_term_release_date_min,
    projected_full_term_release_date_max AS sentence_projected_full_term_release_date_max,
    sentence_length_days_min,
    sentence_length_days_max,
    good_time_days,
    earned_time_days,
FROM `{{project_id}}.{{sentence_sessions_dataset}}.sentence_projected_date_sessions_materialized`
-- Join to sentences and charges because we also want the inferred group id for each sentence
JOIN `{{project_id}}.{{sentence_sessions_dataset}}.sentences_and_charges_materialized`
    USING(person_id, state_code, sentence_id)
-- this view already only has v2 migrated states

UNION ALL

-- Unions in v1 states and hydrates all date fields and the `is_serving` field.
-- The person_id temporarily set as the sentence_inferred_group_id. This is because the inferred_group_id concept 
-- doesn't exist for v1 states and because we want this output view to not have overlaps within a person_id
-- TODO(#33402): deprecate `sentences_preprocessed`
SELECT
    state_code,
    person_id,
    person_id AS sentence_inferred_group_id,     
    sentence_id,
    start_date,
    end_date_exclusive,
    TRUE AS is_serving,
    parole_eligibility_date,
    CAST(NULL AS DATE) AS projected_parole_release_date,
    projected_completion_date_min AS projected_full_term_release_date_min,
    projected_completion_date_max AS projected_full_term_release_date_max,
    parole_eligibility_date AS sentence_parole_eligibility_date,
    CAST(NULL AS DATE) AS sentence_projected_parole_release_date,
    projected_completion_date_min AS sentence_projected_full_term_release_date_min,
    projected_completion_date_max AS sentence_projected_full_term_release_date_max,
    min_sentence_length_days_calculated AS sentence_length_days_min,
    max_sentence_length_days_calculated AS sentence_length_days_max,
    CAST(NULL AS INT64) AS good_time_days,
    CAST(NULL AS INT64) AS earned_time_days,
FROM `{{project_id}}.{{sentence_sessions_dataset}}.sentence_serving_period_materialized`
JOIN `{{project_id}}.sessions.sentences_preprocessed_materialized`
  USING(person_id, state_code, sentence_id)
WHERE state_code IN ({{v2_non_migrated_states}})
),
{create_sub_sessions_with_attributes(
    table_name="serving_periods_and_dates",
    end_date_field_name="end_date_exclusive",
    index_columns=['state_code','person_id','sentence_inferred_group_id']
)}
,
_deduped_pre_aggregated_cte AS
(
/*
We have a sub-sessionized view based on serving periods, sentence inferred group dates, and sentence dates. We
ultimately want to aggregate to the person and sentence inferred group level, but in this CTE we make every row have the
necessary set of sentence-level and sentence-group level attributes before aggregating. There are two windows created 
and the sentence group level attributes are assigned to all rows within the group window, and the sentence level
attributes are assigned to all rows within the sentence level. For V2 states, this step just hydrates NULL values that
result from combining the 3 component views in `sub_sessions_with_attributes`, but for V1 states, calling MAX on the 
sentence group level attributes is creating a group value based on the max of all sentences being served.
*/
SELECT DISTINCT
    state_code,
    person_id,
    start_date,
    end_date_exclusive,
    sentence_inferred_group_id,
    MAX(parole_eligibility_date) OVER(group_window) group_parole_eligibility_date,
    MAX(projected_parole_release_date) OVER(group_window) group_projected_parole_release_date,
    MAX(projected_full_term_release_date_min) OVER(group_window) group_projected_full_term_release_date_min,
    MAX(projected_full_term_release_date_max) OVER(group_window) group_projected_full_term_release_date_max,
    
    sentence_id,
    MAX(is_serving) OVER(sentence_window) is_serving,
    MAX(sentence_parole_eligibility_date) OVER(sentence_window) sentence_parole_eligibility_date,
    MAX(sentence_projected_parole_release_date) OVER(sentence_window) sentence_projected_parole_release_date,
    MAX(sentence_projected_full_term_release_date_min) OVER(sentence_window) sentence_projected_full_term_release_date_min,
    MAX(sentence_projected_full_term_release_date_max) OVER(sentence_window) sentence_projected_full_term_release_date_max,
    MAX(sentence_length_days_min) OVER(sentence_window) sentence_length_days_min,
    MAX(sentence_length_days_max) OVER(sentence_window) sentence_length_days_max,
    MAX(good_time_days) OVER(sentence_window) sentence_good_time_days,
    MAX(earned_time_days) OVER(sentence_window) sentence_earned_time_days,
FROM sub_sessions_with_attributes
WINDOW group_window AS (PARTITION BY person_id, state_code, sentence_inferred_group_id, start_date, end_date_exclusive),
 sentence_window AS (PARTITION BY person_id, state_code, sentence_id, start_date, end_date_exclusive)
)
,
_aggregated_cte AS
(
/*
Aggregate to the sentence inferred group level (which is also the person level since sentence inferred groups should
not overlap and is checked by the validation `overlapping_sentence_inferred_group_serving_periods`). The group attributes 
are already the same within a group, and the sentence attributes are constructed as an array of structs
*/
SELECT
    state_code,
    person_id,
    start_date,
    end_date_exclusive,
    IF(state_code IN ({{v2_non_migrated_states}}), NULL, sentence_inferred_group_id) AS sentence_inferred_group_id,
    ANY_VALUE(group_parole_eligibility_date) group_parole_eligibility_date,
    ANY_VALUE(group_projected_parole_release_date) AS group_projected_parole_release_date,
    ANY_VALUE(group_projected_full_term_release_date_min) AS group_projected_full_term_release_date_min,
    ANY_VALUE(group_projected_full_term_release_date_max) AS group_projected_full_term_release_date_max,
    ARRAY_AGG(
        STRUCT(
            sentence_id,
            sentence_parole_eligibility_date,
            sentence_projected_parole_release_date,
            sentence_projected_full_term_release_date_min,
            sentence_projected_full_term_release_date_max,
            sentence_length_days_min,
            sentence_length_days_max,
            sentence_good_time_days,
            sentence_earned_time_days
            )
        ORDER BY
            sentence_id
    ) AS sentence_array
FROM _deduped_pre_aggregated_cte
WHERE is_serving
GROUP BY 1,2,3,4,5
)
SELECT * EXCEPT(date_gap_id, session_id)
FROM (
{aggregate_adjacent_spans(
    table_name='_aggregated_cte',
    index_columns=['state_code','person_id','sentence_inferred_group_id'],
    attribute = [
        'group_parole_eligibility_date',
        'group_projected_parole_release_date',
        'group_projected_full_term_release_date_min',
        'group_projected_full_term_release_date_max',
        'sentence_array'],
    struct_attribute_subset='sentence_array',
    end_date_field_name='end_date_exclusive')})
"""

PERSON_PROJECTED_DATE_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SENTENCE_SESSIONS_DATASET,
    view_id=PERSON_PROJECTED_DATE_SESSIONS_VIEW_ID,
    view_query_template=QUERY_TEMPLATE,
    description=__doc__,
    should_materialize=True,
    sentence_sessions_dataset=SENTENCE_SESSIONS_DATASET,
    v2_non_migrated_states=list_to_query_string(
        string_list=STATES_NOT_MIGRATED_TO_SENTENCE_V2_SCHEMA,
        quoted=True,
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PERSON_PROJECTED_DATE_SESSIONS_VIEW_BUILDER.build_and_print()
