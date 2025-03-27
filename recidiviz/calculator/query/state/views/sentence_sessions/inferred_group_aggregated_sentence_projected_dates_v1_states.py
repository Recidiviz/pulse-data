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
V1 state version of the view that aggregates sentences to sentence inferred groups. A V1-specific version is required in
order to replicate logic where nulls are respected only for the projected completion date max field when the sentence
is a life sentence.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    list_to_query_string,
    nonnull_end_date_clause,
    revert_nonnull_end_date_clause,
)
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

INFERRED_GROUP_AGGREGATED_SENTENCE_PROJECTED_DATES_V1_STATES_VIEW_ID = (
    "inferred_sentence_group_aggregated_sentence_projected_dates_v1_states"
)

QUERY_TEMPLATE = f"""
-- TODO(#33402): deprecate with `sentences_preprocessed`
WITH sentence_projected_dates AS
(
SELECT DISTINCT
   p.*,
   p.person_id AS sentence_inferred_group_id,
   c.is_life,
FROM `{{project_id}}.{{sentence_sessions_dataset}}.sentence_projected_date_sessions_materialized` p
JOIN `{{project_id}}.{{sentence_sessions_dataset}}.sentences_and_charges_materialized` c
    USING(state_code, person_id, sentence_id)
WHERE state_code IN ({{v2_non_migrated_states}})
)
,
{create_sub_sessions_with_attributes(
    table_name="sentence_projected_dates",
    end_date_field_name="end_date_exclusive",
    index_columns=['state_code','person_id','sentence_inferred_group_id']
)}
,
aggregated_cte AS
(
SELECT 
    state_code,
    person_id,
    sentence_inferred_group_id,
    start_date,
    end_date_exclusive,
    MAX(parole_eligibility_date) AS parole_eligibility_date,
    MAX(projected_parole_release_date) AS projected_parole_release_date,
    MAX(projected_full_term_release_date_min) AS projected_full_term_release_date_min,
    MAX(IF(is_life, {nonnull_end_date_clause('projected_full_term_release_date_max')}, projected_full_term_release_date_max)) AS projected_full_term_release_date_max,   
    ARRAY_AGG(
        STRUCT(
            sentence_id,
            parole_eligibility_date AS sentence_parole_eligibility_date,
            projected_parole_release_date AS sentence_projected_parole_release_date,
            projected_full_term_release_date_min AS sentence_projected_full_term_release_date_min,
            projected_full_term_release_date_max AS sentence_projected_full_term_release_date_max,
            sentence_length_days_min,
            sentence_length_days_max,
            good_time_days AS sentence_good_time_days,
            earned_time_days AS sentence_earned_time_days
            )
        ORDER BY
            sentence_id
    ) AS sentence_array 
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4,5
)
SELECT 
    state_code,
    person_id,
    sentence_inferred_group_id,
    start_date,
    end_date_exclusive,
    parole_eligibility_date,
    projected_parole_release_date,
    projected_full_term_release_date_min, 
    {revert_nonnull_end_date_clause('projected_full_term_release_date_max')} AS projected_full_term_release_date_max,
    sentence_array,
FROM 
(
{aggregate_adjacent_spans(
    table_name = 'aggregated_cte',
    index_columns = ['state_code','person_id','sentence_inferred_group_id'],
    attribute = ['parole_eligibility_date',
                 'projected_parole_release_date',
                 'projected_full_term_release_date_min',
                 'projected_full_term_release_date_max',
                 'sentence_array'],
    struct_attribute_subset = 'sentence_array',
    end_date_field_name='end_date_exclusive')
}
)
"""


INFERRED_GROUP_AGGREGATED_SENTENCE_PROJECTED_DATES_V1_STATES_VIEW_BUILDER = (
    SimpleBigQueryViewBuilder(
        dataset_id=SENTENCE_SESSIONS_DATASET,
        view_id=INFERRED_GROUP_AGGREGATED_SENTENCE_PROJECTED_DATES_V1_STATES_VIEW_ID,
        view_query_template=QUERY_TEMPLATE,
        sentence_sessions_dataset=SENTENCE_SESSIONS_DATASET,
        description=__doc__,
        should_materialize=True,
        v2_non_migrated_states=list_to_query_string(
            string_list=STATES_NOT_MIGRATED_TO_SENTENCE_V2_SCHEMA,
            quoted=True,
        ),
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INFERRED_GROUP_AGGREGATED_SENTENCE_PROJECTED_DATES_V1_STATES_VIEW_BUILDER.build_and_print()
