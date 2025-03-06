# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
View that is unique on person_id, state_code, sentence_id. Has the value of the first date at which a sentence is
being served. For v1 states, we take the effective_date, and for v2 states, we take the min start_date from
`sentence_serving_periods`
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.state.dataset_config import SENTENCE_SESSIONS_DATASET
from recidiviz.calculator.query.state.views.sessions.state_sentence_configurations import (
    STATES_NOT_MIGRATED_TO_SENTENCE_V2_SCHEMA,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SENTENCE_SERVING_START_DATE_VIEW_ID = "sentence_serving_start_date"

QUERY_TEMPLATE = """
WITH v2_states AS
(
SELECT 
  state_code,
  person_id,
  sentence_id,
  MIN(start_date) AS effective_date
FROM `{project_id}.{sentence_sessions_dataset}.sentence_serving_period_materialized`
WHERE state_code NOT IN ({v2_non_migrated_states})
GROUP BY 1,2,3
)
,
-- TODO(#33402): deprecate `sentences_preprocessed`
-- TODO(#39119) Test impact of prioritizing effective_date as start date of v1 serving periods
v1_states AS
(
SELECT DISTINCT
    state_code,
    person_id,
    sentence_id,
    effective_date
FROM `{project_id}.sessions.sentences_preprocessed_materialized` 
WHERE state_code IN ({v2_non_migrated_states})
)
SELECT
    *
FROM v2_states

UNION ALL

SELECT 
    *
FROM v1_states
"""

SENTENCE_SERVING_START_DATE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SENTENCE_SESSIONS_DATASET,
    view_id=SENTENCE_SERVING_START_DATE_VIEW_ID,
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
        SENTENCE_SERVING_START_DATE_VIEW_BUILDER.build_and_print()
