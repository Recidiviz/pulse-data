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
View that is unique on person_id, state_code, sentence_inferred_group_id, and start_date and stores the
sentence level and sentence inferred group level projected dates for v1 states
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

PERSON_PROJECTED_DATE_SESSIONS_V1_STATES_VIEW_ID = (
    "person_projected_date_sessions_v1_states"
)

QUERY_TEMPLATE = f"""
WITH serving_periods_and_dates AS
(
-- The person_id temporarily set as the sentence_inferred_group_id. This is because the inferred_group_id concept 
-- doesn't exist for v1 states and because we want this output view to not have overlaps within a person_id
-- TODO(#33402): deprecate `sentences_preprocessed`
SELECT DISTINCT
    state_code,
    person_id,
    person_id AS sentence_inferred_group_id,     
    sentence_id,
    start_date,
    end_date_exclusive,
    parole_eligibility_date AS group_parole_eligibility_date,
    CAST(NULL AS DATE) AS group_projected_parole_release_date,
    projected_completion_date_min AS group_projected_full_term_release_date_min,
    projected_completion_date_max AS group_projected_full_term_release_date_max,
    parole_eligibility_date AS sentence_parole_eligibility_date,
    CAST(NULL AS DATE) AS sentence_projected_parole_release_date,
    projected_completion_date_min AS sentence_projected_full_term_release_date_min,
    projected_completion_date_max AS sentence_projected_full_term_release_date_max,
    min_sentence_length_days_calculated AS sentence_length_days_min,
    max_sentence_length_days_calculated AS sentence_length_days_max,
    CAST(NULL AS INT64) AS good_time_days,
    CAST(NULL AS INT64) AS earned_time_days
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
_aggregated_cte AS
(
SELECT 
    state_code,
    person_id,
    CAST(NULL AS INT64) AS sentence_inferred_group_id,
    start_date,
    end_date_exclusive,
    MAX(group_parole_eligibility_date) AS group_parole_eligibility_date,
    MAX(group_projected_parole_release_date) AS group_projected_parole_release_date,
    MAX(group_projected_full_term_release_date_min) AS group_projected_full_term_release_date_min,
    MAX(group_projected_full_term_release_date_max) AS group_projected_full_term_release_date_max, 
    ARRAY_AGG(
        STRUCT(
            sentence_id,
            sentence_parole_eligibility_date,
            sentence_projected_parole_release_date,
            sentence_projected_full_term_release_date_min,
            sentence_projected_full_term_release_date_max,
            sentence_length_days_min,
            sentence_length_days_max,
            good_time_days,
            earned_time_days
            )
        ORDER BY
            sentence_id
    ) AS sentence_array
FROM sub_sessions_with_attributes
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

PERSON_PROJECTED_DATE_SESSIONS_V1_STATES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SENTENCE_SESSIONS_DATASET,
    view_id=PERSON_PROJECTED_DATE_SESSIONS_V1_STATES_VIEW_ID,
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
        PERSON_PROJECTED_DATE_SESSIONS_V1_STATES_VIEW_BUILDER.build_and_print()
