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
"""Spans of time with the projected max completion date for clients under supervision or supervision out of state
as indicated by the sentences that were active during that span."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    list_to_query_string,
    nonnull_end_date_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_intersection_spans,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.calculator.query.state.views.sessions.state_sentence_configurations import (
    STATES_WITH_NO_INCARCERATION_SENTENCES_ON_SUPERVISION,
    STATES_WITH_NO_INFERRED_OPEN_SPANS,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "supervision_projected_completion_date_spans"

_VIEW_DESCRIPTION = """
Spans of time with the projected max completion date for clients under supervision or supervision out of state as
indicated by the sentences that were active during that span.
"""

_QUERY_TEMPLATE = f"""
WITH projected_completion_date_spans AS (
-- Use `sentence_spans` to pull the supervision completion dates from `sentences_preprocessed` and
-- `sentence_deadline_spans`, with a higher priority given to the `sentence_deadline_spans` when available within
-- each sentence span
    SELECT
        span.state_code,
        span.person_id,
        span.start_date,
        span.end_date_exclusive,
        MAX(sent.projected_completion_date_max) AS projected_completion_date_max,
        1 AS priority,
    FROM `{{project_id}}.{{sessions_dataset}}.sentence_spans_materialized` span,
    UNNEST (sentences_preprocessed_id_array_actual_completion) AS sentences_preprocessed_id
    INNER JOIN `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` sent
        USING (state_code, person_id, sentences_preprocessed_id)
    WHERE
        -- Exclude incarceration sentences for states that store all supervision sentence data (including parole)
        -- separately in supervision sentences
        (sent.state_code NOT IN ({{excluded_incarceration_states}}) OR sent.sentence_type = "SUPERVISION")
    GROUP BY 1, 2, 3, 4

    UNION ALL

    SELECT
        span.state_code,
        span.person_id,
        span.start_date,
        span.end_date_exclusive,
        MAX(
            -- Exclude incarceration projected release dates for states that store all supervision sentence data
            -- separately in supervision sentences
            IF(
                span.state_code IN ({{excluded_incarceration_states}}),
                sent.projected_supervision_release_date,
                COALESCE(sent.projected_supervision_release_date, sent.projected_incarceration_release_date)
            )
        ) AS projected_completion_date_max,
        0 as priority
    FROM `{{project_id}}.{{sessions_dataset}}.sentence_spans_materialized` span,
    UNNEST (sentence_deadline_id_array) AS sentence_deadline_id
    INNER JOIN `{{project_id}}.{{sessions_dataset}}.sentence_deadline_spans_materialized` sent
        USING (state_code, person_id, sentence_deadline_id)
    GROUP BY 1, 2, 3, 4
),
prioritized_projected_completion_dates AS (
-- Prioritize the projected completion dates from deadline spans over sentences preprocessed when hydrated
    SELECT
      state_code,
      person_id,
      start_date,
      -- Set the latest span end date to NULL in order to cover cases where an open supervision session has no
      -- overlapping sentence span, and the latest (non-overlapping) sentence span will be applied. If the client isn't
      -- currently on supervision then the sentence span will get clipped to the supervision session end date when
      -- taking the intersection with supervision sessions below.
      IF(
        state_code NOT IN ({{no_inferred_open_span_states}})
          AND start_date = MAX(start_date) OVER (PARTITION BY state_code, person_id)
          AND {nonnull_end_date_clause("end_date_exclusive")} <= CURRENT_DATE("US/Eastern"),
        NULL,
        end_date_exclusive
      ) AS end_date_exclusive,
      projected_completion_date_max,
    FROM projected_completion_date_spans
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY state_code, person_id, start_date
      ORDER BY projected_completion_date_max IS NOT NULL DESC, priority
    ) = 1
),
collapsed_adjacent_spans AS (
-- Collapse any adjacent spans with the same `projected_completion_date_max`
{aggregate_adjacent_spans(
    table_name="prioritized_projected_completion_dates",
    index_columns=["state_code", "person_id"],
    attribute=["projected_completion_date_max"],
    end_date_field_name="end_date_exclusive",
)}),
supervision_sessions AS (
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive,
        compartment_level_1
    FROM `{{project_id}}.{{sessions_dataset}}.prioritized_supervision_sessions_materialized`
    WHERE compartment_level_1 IN ("SUPERVISION", "SUPERVISION_OUT_OF_STATE")
),
collapsed_supervision_sessions AS (
SELECT 
    *,
    end_date_exclusive IS NULL AS open_supervision_session,
FROM 
({aggregate_adjacent_spans(
    table_name="supervision_sessions",
    index_columns=["state_code", "person_id"],
    attribute = ["compartment_level_1"],
    end_date_field_name="end_date_exclusive",
)})),
overlapping_supervision_sessions AS (
-- Left join the supervision projected completion date spans onto the supervision sessions so that every supervision
-- and supervision out of state session is maintained, but split if the `projected_completion_date_max` changes. 
-- If no sentence is available the `projected_completion_date_max` will be NULL.
{create_intersection_spans(
    table_1_name="collapsed_supervision_sessions",
    table_2_name="collapsed_adjacent_spans",
    index_columns=["state_code", "person_id"],
    table_1_columns=["open_supervision_session"],
    table_2_columns=["projected_completion_date_max"],
    use_left_join=True,
)})
SELECT
    state_code,
    person_id,
    start_date,
    end_date_exclusive,
    end_date_exclusive AS end_date,
    projected_completion_date_max,
FROM overlapping_supervision_sessions

"""

SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=_VIEW_NAME,
    view_query_template=_QUERY_TEMPLATE,
    description=_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    excluded_incarceration_states=list_to_query_string(
        string_list=STATES_WITH_NO_INCARCERATION_SENTENCES_ON_SUPERVISION,
        quoted=True,
    ),
    no_inferred_open_span_states=list_to_query_string(
        string_list=STATES_WITH_NO_INFERRED_OPEN_SPANS,
        quoted=True,
    ),
    should_materialize=True,
    clustering_fields=["state_code", "person_id"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.build_and_print()
