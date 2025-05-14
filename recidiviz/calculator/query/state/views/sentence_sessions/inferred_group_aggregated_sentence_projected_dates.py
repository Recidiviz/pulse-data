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
Creates a table where each row are the aggregated projected dates for all 
individual sentences in an inferred sentence group. 

Note that this is an aggregation of NormalizedStateSentenceLength values 
and does not include any NormalizedStateSentenceGroupLength data.

Output fields for this view are:

    - sentence_inferred_group_id: 
        The ID for the inferred sentence group. This can be used to link back to the
        constituent sentences and state provided sentence groups.

    - start_date:
        This is the date where the values in this row begin to be valid.

    - end_date_exclusive:
        This is the date where the values in this row become no longer valid.

    - parole_eligibility_date:
        The maximum parole eligibility date across the NormalizedStateSentenceLength
        entities affiliated with this inferred group.

    - projected_parole_release_date:
        The maximum projected parole release date across the NormalizedStateSentenceLength
        entities affiliated with this inferred group. This is when we expect the affiliated 
        person to be released from incarceration to parole.

    - projected_full_term_release_date_min:
        The minimum projected completion date (min) across the NormalizedStateSentenceLength
        entities affiliated with this inferred group. This is earliest time we expect all sentences 
        of the affiliated person to be completed and that person to be released to liberty.

    - projected_full_term_release_date_max
        The maximum projected completion date (max) across the NormalizedStateSentenceLength
        entities affiliated with this inferred group. This is latest time we expect all sentences 
        of the affiliated person to be completed and that person to be released to liberty.
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

INFERRED_GROUP_AGGREGATED_SENTENCE_PROJECTED_DATES_VIEW_ID = (
    "inferred_sentence_group_aggregated_sentence_projected_dates"
)

QUERY_TEMPLATE = f"""
WITH sentence_projected_dates AS
(
SELECT DISTINCT
   p.*,
   IF(state_code IN ({{v2_non_migrated_states}}), p.person_id, s.sentence_inferred_group_id) AS sentence_inferred_group_id,
   s.is_life,
FROM `{{project_id}}.{{sentence_sessions_dataset}}.sentence_projected_date_sessions_materialized` p
JOIN `{{project_id}}.{{sentence_sessions_dataset}}.sentences_and_charges_materialized` s
    USING(state_code, person_id, sentence_id)
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
/*
For periods of time when more than one sentence within an inferred group is being served, we take the max projected date
across sentences. However, if we have a situation where there are more than one sentence and one of those sentences has 
a null projected date and the other has a non-null date, we still want to keep the null date, as we are considering 
those to be intentional updates in normalized_state.state_sentence_group_length and 
normalized_state.state_sentence_length. Therefore, nulls are converted to our MAGIC_END_DATE value before calling max, 
and then reverted back to null afterward. If a state has no hydration of a column, we will also get a null value
*/
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
    SUM(good_time_days) AS good_time_days,
    SUM(earned_time_days) AS earned_time_days,
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
    good_time_days,
    earned_time_days,
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
                 'earned_time_days',
                 'good_time_days',
                 'sentence_array'],
    struct_attribute_subset = 'sentence_array',
    end_date_field_name='end_date_exclusive')
}
)
"""


INFERRED_GROUP_AGGREGATED_SENTENCE_PROJECTED_DATES_VIEW_BUILDER = (
    SimpleBigQueryViewBuilder(
        dataset_id=SENTENCE_SESSIONS_DATASET,
        view_id=INFERRED_GROUP_AGGREGATED_SENTENCE_PROJECTED_DATES_VIEW_ID,
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
        INFERRED_GROUP_AGGREGATED_SENTENCE_PROJECTED_DATES_VIEW_BUILDER.build_and_print()
