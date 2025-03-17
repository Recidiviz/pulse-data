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
View that sessionizes normalized_state.state_sentence_group_length and intersects it with
`sentence_sessions.sentence_serving_period` to create a view with all sentence group length attributes confined to the
time that the sentence group is being served. The view is unique on the combination of state_code, person_id,
sentence_group_id, start_date, and end_date_exclusive.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import MAGIC_START_DATE, list_to_query_string
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
    sessionize_ledger_data,
)
from recidiviz.calculator.query.state.dataset_config import SENTENCE_SESSIONS_DATASET
from recidiviz.calculator.query.state.views.sessions.state_sentence_configurations import (
    STATES_NOT_MIGRATED_TO_SENTENCE_V2_SCHEMA,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SENTENCE_GROUP_PROJECTED_DATE_SESSIONS_VIEW_ID = (
    "sentence_group_projected_date_sessions"
)

_SOURCE_LEDGER_TABLE = "`{project_id}.normalized_state.state_sentence_group_length`"

_INDEX_COLUMNS = [
    "state_code",
    "person_id",
    "sentence_group_id",
]

_UPDATE_COLUMN_NAME = "group_update_datetime"

_ATTRIBUTE_COLUMNS = [
    "parole_eligibility_date_external",
    "projected_parole_release_date_external",
    "projected_full_term_release_date_min_external",
    "projected_full_term_release_date_max_external",
]


QUERY_TEMPLATE = f"""
WITH sessionized_group_length AS (
{sessionize_ledger_data(
    table_name=_SOURCE_LEDGER_TABLE,
    index_columns=_INDEX_COLUMNS,
    update_column_name=_UPDATE_COLUMN_NAME,
    attribute_columns=_ATTRIBUTE_COLUMNS,
)})
,
serving_periods_and_dates AS 
(
SELECT 
    state_code,
    person_id,
    sentence_group_id,
    --If row represents first sentence group length session, leave the start date open so that we hydrate any sentence 
    --serving periods that started prior to the first update with these date values 
    IF(ROW_NUMBER() OVER(PARTITION BY state_code, person_id, sentence_group_id ORDER BY start_date) = 1, '{MAGIC_START_DATE}', start_date) AS start_date,
    end_date_exclusive,
    parole_eligibility_date_external AS parole_eligibility_date,
    projected_parole_release_date_external AS projected_parole_release_date,
    projected_full_term_release_date_min_external AS projected_full_term_release_date_min,
    projected_full_term_release_date_max_external AS projected_full_term_release_date_max,
    CAST(NULL AS BOOLEAN) AS is_serving,
FROM sessionized_group_length
WHERE state_code NOT IN ({{v2_non_migrated_states}})

UNION ALL

SELECT DISTINCT
    state_code,
    person_id,
    sentence_group_id,
    start_date,
    end_date_exclusive,
    CAST(NULL AS DATE) AS parole_eligibility_date,
    CAST(NULL AS DATE) AS projected_parole_release_date,
    CAST(NULL AS DATE) AS projected_full_term_release_date_min,
    CAST(NULL AS DATE) AS projected_full_term_release_date_max,
    TRUE AS is_serving,
FROM `{{project_id}}.{{sentence_sessions_dataset}}.sentence_serving_period_materialized`
--TODO(#38977) Add sentence_group_id to normalized state sentence
JOIN `{{project_id}}.{{sentence_sessions_dataset}}.sentences_and_charges_materialized`
    USING(state_code, person_id, sentence_id)
)
,
{create_sub_sessions_with_attributes(
    table_name="serving_periods_and_dates",
    end_date_field_name="end_date_exclusive",
    index_columns=['state_code','person_id','sentence_group_id']
)}
,
deduped_cte AS
(
SELECT 
    state_code,
    person_id,
    sentence_group_id,
    start_date,
    end_date_exclusive,
    --choose the projected dates from the sessionized_group_length_cte and subset for periods of times where any
    --sentence in the group is being served.
    ANY_VALUE(parole_eligibility_date) AS parole_eligibility_date,
    ANY_VALUE(projected_parole_release_date) AS projected_parole_release_date,
    ANY_VALUE(projected_full_term_release_date_min) AS projected_full_term_release_date_min,
    ANY_VALUE(projected_full_term_release_date_max) AS projected_full_term_release_date_max
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4,5
HAVING ANY_VALUE(is_serving)
)
SELECT
  state_code,
  person_id,
  sentence_group_id,
  start_date,
  end_date_exclusive,
  parole_eligibility_date,
  projected_parole_release_date,
  projected_full_term_release_date_min,
  projected_full_term_release_date_max,
FROM
(
{aggregate_adjacent_spans(
    table_name = 'deduped_cte',
    index_columns = ['state_code','person_id','sentence_group_id'],
    attribute = ['parole_eligibility_date','projected_parole_release_date','projected_full_term_release_date_min','projected_full_term_release_date_max'],
    end_date_field_name='end_date_exclusive')
}
)
"""

SENTENCE_GROUP_PROJECTED_DATE_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SENTENCE_SESSIONS_DATASET,
    view_id=SENTENCE_GROUP_PROJECTED_DATE_SESSIONS_VIEW_ID,
    view_query_template=QUERY_TEMPLATE,
    sentence_sessions_dataset=SENTENCE_SESSIONS_DATASET,
    description=__doc__,
    should_materialize=True,
    v2_non_migrated_states=list_to_query_string(
        string_list=STATES_NOT_MIGRATED_TO_SENTENCE_V2_SCHEMA,
        quoted=True,
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SENTENCE_GROUP_PROJECTED_DATE_SESSIONS_VIEW_BUILDER.build_and_print()
