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
state provided sentence groups in an inferred sentence group. 

Note that this is an aggregation of NormalizedStateSentenceGroupLength values 
and does not include any NormalizedStateSentenceLength data.

Output fields for this view are:
    - sentence_inferred_group_id:
        The ID for the inferred sentence group. This can be used to link back to the
        constituent sentences and state provided sentence groups.

    - start_date:
        This is the date where the values in this row begin to be valid.

    - end_date_exclusive:
        This is the date where the values in this row become no longer valid.

    - parole_eligibility_date:
        The maximum parole eligibility date across the NormalizedStateSentenceGroupLength
        entities affiliated with this inferred group.

    - projected_parole_release_date:
        The maximum projected parole release date across the NormalizedStateSentenceGroupLength
        entities affiliated with this inferred group. This is when we expect the affiliated
        person to be released from incarceration to parole.

    - projected_full_term_release_date_min:
        The minimum projected completion date (min) across the NormalizedStateSentenceGroupLength
        entities affiliated with this inferred group. This is earliest time we expect all sentences
        of the affiliated person to be completed and that person to be released to liberty.

    - projected_full_term_release_date_max
        The maximum projected completion date (max) across the NormalizedStateSentenceGroupLength
        entities affiliated with this inferred group. This is latest time we expect all sentences
        of the affiliated person to be completed and that person to be released to liberty.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    revert_nonnull_end_date_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SENTENCE_SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

INFERRED_GROUP_AGGREGATED_SENTENCE_GROUP_PROJECTED_DATES_VIEW_ID = (
    "inferred_sentence_group_aggregated_sentence_group_projected_dates"
)

QUERY_TEMPLATE = f"""
WITH sentence_group_projected_dates AS
(
SELECT DISTINCT
    p.*,
    s.sentence_inferred_group_id,
    LOGICAL_OR(s.is_life) OVER(PARTITION BY state_code, person_id, sentence_group_id, start_date) AS is_life,
FROM `{{project_id}}.{{sentence_sessions_dataset}}.sentence_group_projected_date_sessions_materialized` p
JOIN `{{project_id}}.{{sentence_sessions_dataset}}.sentences_and_charges_materialized` s
    USING(state_code, person_id, sentence_group_id)
)
,
{create_sub_sessions_with_attributes(
    table_name="sentence_group_projected_dates",
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
            sentence_group_id,
            parole_eligibility_date AS sentence_group_parole_eligibility_date,
            projected_parole_release_date AS sentence_group_projected_parole_release_date,
            projected_full_term_release_date_min AS sentence_group_projected_full_term_release_date_min,
            projected_full_term_release_date_max AS sentence_group_projected_full_term_release_date_max
            )
        ORDER BY
            sentence_group_id
    ) AS sentence_group_array 
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
    sentence_group_array,
FROM 
(
{aggregate_adjacent_spans(
    table_name = 'aggregated_cte',
    index_columns = ['state_code','person_id','sentence_inferred_group_id'],
    attribute = ['parole_eligibility_date',
                 'projected_parole_release_date',
                 'projected_full_term_release_date_min',
                 'projected_full_term_release_date_max',
                 'sentence_group_array'],
    struct_attribute_subset = 'sentence_group_array',
    end_date_field_name='end_date_exclusive')
}
)
"""


INFERRED_GROUP_AGGREGATED_SENTENCE_GROUP_PROJECTED_DATES_VIEW_BUILDER = (
    SimpleBigQueryViewBuilder(
        dataset_id=SENTENCE_SESSIONS_DATASET,
        view_id=INFERRED_GROUP_AGGREGATED_SENTENCE_GROUP_PROJECTED_DATES_VIEW_ID,
        view_query_template=QUERY_TEMPLATE,
        sentence_sessions_dataset=SENTENCE_SESSIONS_DATASET,
        description=__doc__,
        should_materialize=True,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INFERRED_GROUP_AGGREGATED_SENTENCE_GROUP_PROJECTED_DATES_VIEW_BUILDER.build_and_print()
