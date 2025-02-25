# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Spans of time when certain sentence eligibility deadlines (incarceration/supervision completion dates, parole
eligibility date) were active. Spans are on the person/sentence level."""
from typing import Dict

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    list_to_query_string,
    nonnull_end_date_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.state.state_task_deadline import StateTaskType
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SENTENCE_DEADLINE_SPANS_VIEW_NAME = "sentence_deadline_spans"

SENTENCE_DEADLINE_SPANS_VIEW_DESCRIPTION = """Spans of time when certain sentence eligibility deadlines (incarceration/supervision completion dates, parole
eligibility date) were active. Spans are on the person/sentence level."""

SENTENCE_ELIGIBILITY_DATE_TYPE_TO_NAME_MAP: Dict[StateTaskType, str] = {
    StateTaskType.DISCHARGE_FROM_SUPERVISION: "projected_supervision_release_date",
    StateTaskType.DISCHARGE_FROM_INCARCERATION: "projected_incarceration_release_date",
    StateTaskType.TRANSFER_TO_SUPERVISION_FROM_INCARCERATION: "parole_eligibility_date",
}

SENTENCE_DEADLINE_SPANS_QUERY_TEMPLATE = f"""
WITH parsed_eligibility_dates AS (
  -- Extract the sentence indicators from the task metadata for each deadline/eligibility date type
  SELECT
    state_code,
    person_id,
    update_datetime,
    task_type,
    JSON_EXTRACT_SCALAR(task_metadata, "$.sentence_type") AS sentence_type,
    JSON_EXTRACT_SCALAR(task_metadata, "$.external_id") AS external_id,
    eligible_date,
  FROM `{{project_id}}.{{normalized_state_dataset}}.state_task_deadline`
  WHERE task_type IN ({list_to_query_string(
    string_list=[task_type.name for task_type in SENTENCE_ELIGIBILITY_DATE_TYPE_TO_NAME_MAP],
    quoted=True,
  )})
),
ingest_start_date_per_state AS (
  SELECT
    state_code,
    MIN(CAST(update_datetime AS DATE)) AS ingest_start_date
  FROM parsed_eligibility_dates
  GROUP BY 1
),
deadline_spans AS (
  -- Create the start & end date for each sentence and deadline type
  SELECT
    state_code,
    person_id,
    sent.sentences_preprocessed_id,
    sentence_type,
    external_id,
    -- If the deadline span starts on the ingest date use the sentence effective date to start the span instead
    IF(
      CAST(update_datetime AS DATE) = ingest_start_date,
      sent.effective_date,
      CAST(update_datetime AS DATE)
    ) AS start_date,
    -- Use the next record's update date as the span end date or the sentence completion date if null
    COALESCE(
        LEAD(CAST(update_datetime AS DATE)) OVER (
          PARTITION BY state_code, person_id, sentence_type, external_id, task_type
          ORDER BY update_datetime
        ),
        sent.completion_date
    ) AS end_date_exclusive,
    -- Start converting the spans table from 1 span per eligibility date to 1 column per eligibility date
    {list_to_query_string(
        string_list=[f'IF(task_type = "{task_type.name}", eligible_date, NULL) AS {column_name}'
                     for task_type, column_name in SENTENCE_ELIGIBILITY_DATE_TYPE_TO_NAME_MAP.items()]
    )},
  FROM parsed_eligibility_dates
  INNER JOIN `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` sent
    USING (state_code, person_id, sentence_type, external_id)
  INNER JOIN ingest_start_date_per_state
    USING (state_code)
  WHERE
    -- Drop sentences that were completed before the sentence eligibility date was ingested since those eligibility
    -- dates represent the latest/up-to-date values that are already ingested in `sentences_preprocessed`, and any
    -- sentence deadline span that would starts & ends pre-ingest would not necessarily represent the actual eligibility
    -- date that was applicable during that period since we cannot observe sentence dates before ingest started.
    CAST(update_datetime AS DATE) < {nonnull_end_date_clause("sent.completion_date")}
),
{create_sub_sessions_with_attributes(
    table_name="deadline_spans",
    index_columns=["state_code", "person_id", "sentences_preprocessed_id", "sentence_type", "external_id"],
    end_date_field_name="end_date_exclusive",
)},
collapsed_sub_sessions AS (
  SELECT
    state_code,
    person_id,
    sentences_preprocessed_id,
    sentence_type,
    external_id,
    start_date,
    end_date_exclusive,
    -- Collapse overlapping sub sessions by taking the max eligibility date per date type
    {list_to_query_string(
        string_list=[f"MAX({column_name})" + f" AS {column_name}"
                     for column_name in SENTENCE_ELIGIBILITY_DATE_TYPE_TO_NAME_MAP.values()]
    )},
  FROM sub_sessions_with_attributes
  WHERE start_date != {nonnull_end_date_clause("end_date_exclusive")}
  GROUP BY 1, 2, 3, 4, 5, 6, 7
),
aggregated_adjacent_spans AS (
{aggregate_adjacent_spans(
    table_name="collapsed_sub_sessions",
    index_columns=["state_code", "person_id", "sentences_preprocessed_id", "sentence_type", "external_id"],
    attribute=list(SENTENCE_ELIGIBILITY_DATE_TYPE_TO_NAME_MAP.values()),
    end_date_field_name="end_date_exclusive",
)}
)
SELECT
  state_code,
  person_id,
  ROW_NUMBER() OVER (
    PARTITION BY state_code, person_id
    ORDER BY sentences_preprocessed_id, start_date
  ) AS sentence_deadline_id,
  sentences_preprocessed_id,
  sentence_type,
  external_id,
  start_date,
  end_date_exclusive,
  {list_to_query_string(string_list=list(SENTENCE_ELIGIBILITY_DATE_TYPE_TO_NAME_MAP.values()))},
FROM aggregated_adjacent_spans
"""

SENTENCE_DEADLINE_SPANS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=SENTENCE_DEADLINE_SPANS_VIEW_NAME,
    view_query_template=SENTENCE_DEADLINE_SPANS_QUERY_TEMPLATE,
    description=SENTENCE_DEADLINE_SPANS_VIEW_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=True,
    clustering_fields=["state_code", "person_id"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SENTENCE_DEADLINE_SPANS_VIEW_BUILDER.build_and_print()
