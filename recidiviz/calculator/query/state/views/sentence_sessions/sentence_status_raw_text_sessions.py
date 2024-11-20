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
Sentence statuses sessionized across sentence_id and status_raw_text

Includes v1 sentence status sessions translated from sentences_preprocessed TODO(#33402)
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    list_to_query_string,
    nonnull_end_date_clause,
)
from recidiviz.calculator.query.state.dataset_config import SENTENCE_SESSIONS_DATASET
from recidiviz.calculator.query.state.views.sentence_sessions.sentence_sessions_query_utils import (
    convert_normalized_ledger_data_to_sessions_query,
)
from recidiviz.calculator.query.state.views.sessions.state_sentence_configurations import (
    STATES_NOT_MIGRATED_TO_SENTENCE_V2_SCHEMA,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_ID = "sentence_status_raw_text_sessions"

_NORMALIZED_STATE_SOURCE_TABLE = "state_sentence_status_snapshot"

_INDEX_COLUMNS = [
    "state_code",
    "person_id",
    "sentence_id",
]

_UPDATE_COLUMN_NAME = "status_update_datetime"

_ATTRIBUTE_COLUMNS = [
    "status",
    "status_raw_text",
]

_TERMINATING_STATUS_TYPES = list_to_query_string(
    string_list=[
        status.name for status in StateSentenceStatus if status.is_terminating_status
    ],
    quoted=True,
)

_SERVING_STATUS_TYPES = list_to_query_string(
    string_list=[
        status.name
        for status in StateSentenceStatus
        if status.is_considered_serving_status
    ],
    quoted=True,
)

_VIEW_TEMPLATE = f"""
WITH
-- Create the status spans from the
v2_sentence_statuses AS (
{convert_normalized_ledger_data_to_sessions_query(
    table_name=_NORMALIZED_STATE_SOURCE_TABLE,
    index_columns=_INDEX_COLUMNS,
    update_column_name=_UPDATE_COLUMN_NAME,
    attribute_columns=_ATTRIBUTE_COLUMNS,
)})
-- Use the v2 sentence status snapshot data
-- for all fully migrated states
SELECT
    *,
    status IN ({_SERVING_STATUS_TYPES}) AS is_serving_sentence_status
FROM v2_sentence_statuses
WHERE state_code NOT IN ({{v2_non_migrated_states}})

UNION ALL

-- TODO(#33402): deprecate `sentences_preprocessed`
--
-- For V1 states:
-- Create a "SERVING" status session for each sentence from sentence
-- imposed date to the completion date, matching the v2 format:
--   overwrite all terminated sentence status types to "SERVING"
--   set `is_serving_sentence_status = True` for the entire period
--   drop any future dates:
--     drop rows with an imposed date in the future
--     set `end_date_exclusive` to NULL for sentence completion dates in the future
SELECT DISTINCT
    state_code,
    person_id,
    sentence_id,
    date_imposed AS start_date,
    -- Convert future completion dates to NULL
    IF(completion_date > CURRENT_DATE("US/Eastern"), NULL, completion_date) AS end_date_exclusive,
    -- Overwrite all terminated sentence status types to "SERVING" for these inferred serving sessions
    IF(status IN ({_TERMINATING_STATUS_TYPES}), status, "SERVING") AS status,
    IF(status IN ({_TERMINATING_STATUS_TYPES}), status_raw_text, "SERVING_INFERRED") AS status_raw_text,
    -- Mark this entire inferred status session as "serving"
    TRUE AS is_serving_sentence_status,
FROM `{{project_id}}.sessions.sentences_preprocessed_materialized`
WHERE state_code IN ({{v2_non_migrated_states}})
    -- Drop rows with imposed date in the future
    AND date_imposed <= CURRENT_DATE("US/Eastern")
    AND date_imposed != {nonnull_end_date_clause("completion_date")}

UNION ALL

-- Create an open "COMPLETED" (or other terminated status) status span
-- if the sentence imposed date and completion date have both passed
SELECT DISTINCT
    state_code,
    person_id,
    sentence_id,
    completion_date AS start_date,
    CAST(NULL AS DATE) AS end_date_exclusive,
    -- Overwrite the sentence status from the v1 schema with "COMPLETED" if it is not a terminating status
    IF(status IN ({_TERMINATING_STATUS_TYPES}), status, "COMPLETED") AS status,
    IF(status IN ({_TERMINATING_STATUS_TYPES}), status_raw_text, "COMPLETED_INFERRED") AS status_raw_text,
    -- Mark this entired inferred status session as "not serving"
    FALSE AS is_serving_sentence_status,
FROM `{{project_id}}.sessions.sentences_preprocessed_materialized`
WHERE state_code IN ({{v2_non_migrated_states}})
    AND date_imposed <= CURRENT_DATE("US/Eastern")
    AND completion_date < CURRENT_DATE("US/Eastern")
"""


SENTENCE_STATUS_RAW_TEXT_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SENTENCE_SESSIONS_DATASET,
    view_id=_VIEW_ID,
    view_query_template=_VIEW_TEMPLATE,
    description=__doc__,
    should_materialize=True,
    clustering_fields=_INDEX_COLUMNS,
    v2_non_migrated_states=list_to_query_string(
        string_list=STATES_NOT_MIGRATED_TO_SENTENCE_V2_SCHEMA,
        quoted=True,
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SENTENCE_STATUS_RAW_TEXT_SESSIONS_VIEW_BUILDER.build_and_print()
