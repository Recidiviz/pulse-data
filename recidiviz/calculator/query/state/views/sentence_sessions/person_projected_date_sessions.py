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
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SENTENCE_SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PERSON_PROJECTED_DATE_SESSIONS = "person_projected_date_sessions"

QUERY_TEMPLATE = f"""
WITH all_aggregated_projected_dates AS
/*
By definition, the two views that are being unioned together (inferred_group_aggregated_sentence_projected_dates and 
inferred_group_aggregated_sentence_group_projected_dates) will each have the same date coverage within a 
sentence_inferred_group_id. This is because when constructed, both sentence_group_projected_date_sessions and 
sentence_projected_date_sessions are intersected with sentence_serving_period and have their coverage defined based on 
the time that the component sentences in a group / inferred group are active. Calling 
create_sub_sessions_with_attributes afterward is done to sub-sessionize based on attribute changes within each of those
views, so that we know the set of dates from both sources at each point in time.
*/
(
SELECT 
    state_code,
    person_id,
    sentence_inferred_group_id,
    start_date,
    end_date_exclusive,
    parole_eligibility_date AS group_parole_eligibility_date,
    projected_parole_release_date AS group_projected_parole_release_date,
    projected_full_term_release_date_min AS group_projected_full_term_release_date_min,
    projected_full_term_release_date_max AS group_projected_full_term_release_date_max,
    CAST(NULL AS INT64) AS sentence_id,
    CAST(NULL AS DATE) AS sentence_parole_eligibility_date,
    CAST(NULL AS DATE) AS sentence_projected_parole_release_date,
    CAST(NULL AS DATE) AS sentence_projected_full_term_release_date_min,
    CAST(NULL AS DATE) AS sentence_projected_full_term_release_date_max,
    CAST(NULL AS INT64) AS sentence_length_days_min,
    CAST(NULL AS INT64) AS sentence_length_days_max,
    CAST(NULL AS INT64) AS sentence_good_time_days,
    CAST(NULL AS INT64) AS sentence_earned_time_days,
FROM `{{project_id}}.{{sentence_sessions_dataset}}.inferred_sentence_group_aggregated_sentence_group_projected_dates_materialized`
UNION ALL
SELECT 
    state_code,
    person_id,
    sentence_inferred_group_id,
    start_date,
    end_date_exclusive,
    parole_eligibility_date AS group_parole_eligibility_date,
    projected_parole_release_date AS group_projected_parole_release_date,
    projected_full_term_release_date_min AS group_projected_full_term_release_date_min,
    projected_full_term_release_date_max AS group_projected_full_term_release_date_max,
    sentence_id,
    sentence_parole_eligibility_date,
    sentence_projected_parole_release_date,
    sentence_projected_full_term_release_date_min,
    sentence_projected_full_term_release_date_max,
    sentence_length_days_min,
    sentence_length_days_max,
    sentence_good_time_days,
    sentence_earned_time_days,
FROM `{{project_id}}.{{sentence_sessions_dataset}}.inferred_sentence_group_aggregated_sentence_projected_dates_materialized`,
UNNEST(sentence_array)
)
,
{create_sub_sessions_with_attributes(
    table_name="all_aggregated_projected_dates",
    end_date_field_name="end_date_exclusive",
    index_columns=['state_code','person_id','sentence_inferred_group_id']
)}
,
_pre_aggregated_cte AS
(
SELECT
    state_code,
    person_id,
    sentence_inferred_group_id,
    start_date,
    end_date_exclusive,
    --Takes max in cases where the dates sourced from sentences differ from those sourced from groups
    MAX(group_parole_eligibility_date) OVER w AS group_parole_eligibility_date,
    MAX(group_projected_parole_release_date) OVER w AS group_projected_parole_release_date,
    MAX(group_projected_full_term_release_date_min) OVER w AS group_projected_full_term_release_date_min,
    MAX(group_projected_full_term_release_date_max) OVER w AS group_projected_full_term_release_date_max,
    sentence_id,
    sentence_parole_eligibility_date,
    sentence_projected_parole_release_date,
    sentence_projected_full_term_release_date_min,
    sentence_projected_full_term_release_date_max,
    sentence_length_days_min,
    sentence_length_days_max,
    sentence_good_time_days,
    sentence_earned_time_days,
FROM sub_sessions_with_attributes
--The group projected dates sourced from `inferred_sentence_group_aggregated_sentence_group_projected_dates` will have a null sentence_id value
--However, we have already taken the max dates for each group, so we can now drop that row as it is not needed when we aggregate the sentence dates
--into a struct. It needs to be done as a QUALIFY and not WHERE because of the order of operations (we want to drop after the max group value has been taken)
QUALIFY sentence_id is not null
WINDOW w AS (PARTITION BY state_code, person_id, sentence_inferred_group_id, start_date, end_date_exclusive)
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
    sentence_inferred_group_id,
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
FROM _pre_aggregated_cte
GROUP BY 1,2,3,4,5
)
SELECT 
    * EXCEPT(date_gap_id, session_id)
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
    view_id=PERSON_PROJECTED_DATE_SESSIONS,
    sentence_sessions_dataset=SENTENCE_SESSIONS_DATASET,
    view_query_template=QUERY_TEMPLATE,
    description=__doc__,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PERSON_PROJECTED_DATE_SESSIONS_VIEW_BUILDER.build_and_print()
