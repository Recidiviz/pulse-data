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
This is the V1 state version of `sentence_projected_date_sessions`. The logic is identical, except for instead of
projected dates being sourced from normalized_state.state_sentence_length, they are sourced from the combination of
dates found in sentence_deadline_spans and sentences_preprocessed. The output is a sentence-level view that captures
the relevant projected date for each sentence over the period of time that that sentence is being served.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import MAGIC_START_DATE, list_to_query_string
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

SENTENCE_PROJECTED_DATE_SESSIONS_V1_STATES_VIEW_ID = (
    "sentence_projected_date_sessions_v1_states"
)

QUERY_TEMPLATE = f"""
-- TODO(#33402): deprecate `sentences_preprocessed`
WITH serving_periods_and_dates AS
(
SELECT
    sp.state_code,
    sp.person_id,
    sp.sentence_id,
    --If row represents first sentence length session, leave the start date open so that we hydrate any sentence serving
    --periods that started prior to the first update with these date values 
    IF(
        ROW_NUMBER() OVER(PARTITION BY sp.state_code, sp.person_id, sp.sentence_id ORDER BY td.start_date) = 1, 
         '{MAGIC_START_DATE}', td.start_date
        ) AS start_date,
    td.end_date_exclusive,
    min_sentence_length_days_calculated AS sentence_length_days_min,
    max_sentence_length_days_calculated AS sentence_length_days_max,
    CAST(NULL AS INT64) AS good_time_days,
    CAST(NULL AS INT64) AS earned_time_days,
    --Consistent with current logic, we prioritize the date in task deadline spans over sentences preprocessed
    COALESCE(td.parole_eligibility_date,sp.parole_eligibility_date) AS parole_eligibility_date,
    CAST(NULL AS DATE) AS projected_parole_release_date,
    sp.projected_completion_date_min AS projected_full_term_release_date_min,
    --Consistent with current logic, we prioritize the date in task deadline spans over sentences preprocessed
    COALESCE(td.projected_supervision_release_date,td.projected_incarceration_release_date,sp.projected_completion_date_max) AS projected_full_term_release_date_max,
    CAST(NULL AS BOOLEAN) AS is_serving,
FROM `{{project_id}}.sessions.sentences_preprocessed_materialized` sp
LEFT JOIN `{{project_id}}.sessions.sentence_deadline_spans_materialized` td
    USING(state_code, person_id, sentences_preprocessed_id)
WHERE state_code IN ({{v2_non_migrated_states}})

UNION ALL

SELECT DISTINCT
    state_code,
    person_id,
    sentence_id,
    start_date,
    end_date_exclusive,
    CAST(NULL AS INT64) AS sentence_length_days_min,
    CAST(NULL AS INT64) AS sentence_length_days_max,
    CAST(NULL AS INT64) AS good_time_days,
    CAST(NULL AS INT64) AS earned_time_days,
    CAST(NULL AS DATE) AS parole_eligibility_date,
    CAST(NULL AS DATE) AS projected_parole_release_date,
    CAST(NULL AS DATE) AS projected_full_term_release_date_min,
    CAST(NULL AS DATE) AS projected_full_term_release_date_max,
    TRUE AS is_serving,
FROM `{{project_id}}.{{sentence_sessions_dataset}}.sentence_serving_period_materialized`
WHERE state_code IN ({{v2_non_migrated_states}})
)
,
{create_sub_sessions_with_attributes(
    table_name="serving_periods_and_dates",
    end_date_field_name="end_date_exclusive",
    index_columns=['state_code','person_id','sentence_id']
)}
,
deduped_cte AS
(
SELECT 
    state_code,
    person_id,
    sentence_id,
    start_date,
    end_date_exclusive,
    --choose the projected date value from the sessionized_sentence_length_cte but dropping any sub-sessions for which 
    --the sentence is not being served.
    ANY_VALUE(sentence_length_days_min) AS sentence_length_days_min,
    ANY_VALUE(sentence_length_days_max) AS sentence_length_days_max,
    ANY_VALUE(good_time_days) AS good_time_days,
    ANY_VALUE(earned_time_days) AS earned_time_days,
    ANY_VALUE(parole_eligibility_date) AS parole_eligibility_date,
    ANY_VALUE(projected_parole_release_date) AS projected_parole_release_date,
    ANY_VALUE(projected_full_term_release_date_min) AS projected_full_term_release_date_min,
    ANY_VALUE(projected_full_term_release_date_max) AS projected_full_term_release_date_max,
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4,5
HAVING ANY_VALUE(is_serving)
)
SELECT
    state_code,
    person_id,
    sentence_id,
    start_date,
    end_date_exclusive,
    sentence_length_days_min,
    sentence_length_days_max,
    good_time_days,
    earned_time_days,
    parole_eligibility_date,
    projected_parole_release_date,
    projected_full_term_release_date_min,
    projected_full_term_release_date_max,
FROM
(
{aggregate_adjacent_spans(
    table_name = 'deduped_cte',
    index_columns = ['state_code','person_id','sentence_id'],
    attribute = ['sentence_length_days_min','sentence_length_days_max','good_time_days','earned_time_days',
                 'parole_eligibility_date','projected_parole_release_date','projected_full_term_release_date_min',
                 'projected_full_term_release_date_max'],
    end_date_field_name='end_date_exclusive')
    }
)

"""

SENTENCE_PROJECTED_DATE_SESSIONS_V1_STATES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SENTENCE_SESSIONS_DATASET,
    view_id=SENTENCE_PROJECTED_DATE_SESSIONS_V1_STATES_VIEW_ID,
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
        SENTENCE_PROJECTED_DATE_SESSIONS_V1_STATES_VIEW_BUILDER.build_and_print()
