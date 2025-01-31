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
View that sessionizes normalized_state.state_sentence_length
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.sessions_query_fragments import sessionize_ledger_data
from recidiviz.calculator.query.state.dataset_config import SENTENCE_SESSIONS_DATASET
from recidiviz.calculator.query.state.views.sessions.state_sentence_configurations import (
    STATES_NOT_MIGRATED_TO_SENTENCE_V2_SCHEMA,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SENTENCE_PROJECTED_DATE_SESSIONS_VIEW_ID = "sentence_projected_date_sessions"

_SOURCE_LEDGER_TABLE = "`{project_id}.normalized_state.state_sentence_length`"

_INDEX_COLUMNS = [
    "state_code",
    "person_id",
    "sentence_id",
]

_UPDATE_COLUMN_NAME = "length_update_datetime"

_ATTRIBUTE_COLUMNS = [
    "sentence_length_days_min",
    "sentence_length_days_max",
    "good_time_days",
    "earned_time_days",
    "parole_eligibility_date_external",
    "projected_parole_release_date_external",
    "projected_completion_date_min_external",
    "projected_completion_date_max_external",
]


QUERY_TEMPLATE = f"""
WITH sessionized_sentence_length AS (
{sessionize_ledger_data(
    table_name=_SOURCE_LEDGER_TABLE,
    index_columns=_INDEX_COLUMNS,
    update_column_name=_UPDATE_COLUMN_NAME,
    attribute_columns=_ATTRIBUTE_COLUMNS,
)})
SELECT 
    state_code,
    person_id,
    start_date,
    end_date_exclusive,
    sentence_id,
    sentence_length_days_min,
    sentence_length_days_max,
    good_time_days,
    earned_time_days,
    parole_eligibility_date_external AS parole_eligibility_date,
    projected_parole_release_date_external AS projected_parole_release_date,
    projected_completion_date_min_external AS projected_full_term_release_date_min,
    projected_completion_date_max_external AS projected_full_term_release_date_max,    
FROM sessionized_sentence_length
WHERE state_code NOT IN ({{v2_non_migrated_states}})
"""

SENTENCE_PROJECTED_DATE_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SENTENCE_SESSIONS_DATASET,
    view_id=SENTENCE_PROJECTED_DATE_SESSIONS_VIEW_ID,
    view_query_template=QUERY_TEMPLATE,
    description=__doc__,
    should_materialize=True,
    v2_non_migrated_states=list_to_query_string(
        string_list=STATES_NOT_MIGRATED_TO_SENTENCE_V2_SCHEMA,
        quoted=True,
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SENTENCE_PROJECTED_DATE_SESSIONS_VIEW_BUILDER.build_and_print()
