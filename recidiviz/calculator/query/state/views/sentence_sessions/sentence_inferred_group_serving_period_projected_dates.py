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
View that is unique on person_id, state_code, sentence_inferred_group_id, and start_date. Intersects sentence_serving_periods
and sentence_inferred_group_projected_date_sessions to create a view that represents the projected dates associated with a person
at a given time
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SENTENCE_SESSIONS_DATASET
from recidiviz.calculator.query.state.views.sessions.state_sentence_configurations import (
    STATES_NOT_MIGRATED_TO_SENTENCE_V2_SCHEMA,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SENTENCE_INFERRED_GROUP_SERVING_PERIOD_PROJECTED_DATES_VIEW_ID = (
    "sentence_inferred_group_serving_period_projected_dates"
)

QUERY_TEMPLATE = f"""
WITH serving_periods_and_dates AS
(
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
FROM `{{project_id}}.{{sentence_sessions_dataset}}.sentence_serving_period_materialized`sp
JOIN `{{project_id}}.{{sentence_sessions_dataset}}.sentences_and_charges_materialized`
    USING(person_id, state_code, sentence_id)
--only include migrated states here because we will add in v1 serving periods separately
WHERE state_code NOT IN ({{v2_non_migrated_states}})

UNION ALL

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
FROM `{{project_id}}.{{sentence_sessions_dataset}}.sentence_inferred_group_projected_date_sessions_materialized`
-- this view already only has v2 migrated states

UNION ALL
-- v1 states get unioned in and have the person_id temporarily set as the sentence_inferred_group_id. This is because
-- the inferred_group_id concept doesn't exist for v1 states and because we want this output view to not have overlaps
-- within a person_id
-- TODO(#33402): deprecate `sentences_preprocessed`
SELECT
    state_code,
    person_id,
    person_id AS sentence_inferred_group_id,     
    sentence_id,
    start_date,
    end_date_exclusive,
    TRUE AS is_serving,
    parole_eligibility_date AS parole_eligibility_date,
    CAST(NULL AS DATE) AS projected_parole_release_date,
    projected_completion_date_min AS projected_full_term_release_date_min,
    projected_completion_date_max AS projected_full_term_release_date_max,
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
SELECT
    state_code,
    person_id,
    start_date,
    end_date_exclusive,
    -- now that we have sub-sessionized, set the inferred_group_id as NULL for non-migrated states
    IF(state_code IN ({{v2_non_migrated_states}}), NULL, sentence_inferred_group_id) AS sentence_inferred_group_id,
    ARRAY_AGG(DISTINCT sentence_id IGNORE NULLS ORDER BY sentence_id) AS sentence_id_array,
    --TODO(#33402) Can change MAX to ANY_VALUE when sentences_preprocessed is deprecated
    MAX(parole_eligibility_date) AS parole_eligibility_date,
    MAX(projected_parole_release_date) AS projected_parole_release_date,
    MAX(projected_full_term_release_date_min) AS projected_full_term_release_date_min,
    MAX(projected_full_term_release_date_max) AS projected_full_term_release_date_max,
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4,5
--excludes periods of time where we have projected date data but the person is not serving
HAVING(ANY_VALUE(is_serving))
"""

SENTENCE_INFERRED_GROUP_SERVING_PERIOD_PROJECTED_DATES_VIEW_BUILDER = (
    SimpleBigQueryViewBuilder(
        dataset_id=SENTENCE_SESSIONS_DATASET,
        view_id=SENTENCE_INFERRED_GROUP_SERVING_PERIOD_PROJECTED_DATES_VIEW_ID,
        view_query_template=QUERY_TEMPLATE,
        description=__doc__,
        should_materialize=True,
        sentence_sessions_dataset=SENTENCE_SESSIONS_DATASET,
        v2_non_migrated_states=list_to_query_string(
            string_list=STATES_NOT_MIGRATED_TO_SENTENCE_V2_SCHEMA,
            quoted=True,
        ),
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SENTENCE_INFERRED_GROUP_SERVING_PERIOD_PROJECTED_DATES_VIEW_BUILDER.build_and_print()
