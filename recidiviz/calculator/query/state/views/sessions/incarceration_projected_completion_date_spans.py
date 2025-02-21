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
"""Spans of time with the projected max completion date for incarcerated clients, as
indicated by the sentences that were active during that span.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_intersection_spans,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "incarceration_projected_completion_date_spans"

_QUERY_TEMPLATE = f"""
WITH projected_completion_date_spans_sentences_preprocessed AS (
    /* Pull info on sentences from `sessions.sentences_preprocessed_materialized`, which
    has information at the person-sentence level. */
    SELECT
        state_code,
        person_id,
        sentences_preprocessed_id,
        span.start_date,
        span.end_date_exclusive,
        sent.projected_completion_date_max,
        sent.parole_eligibility_date,
        /* Because some life sentences can have null projected maximum completion dates,
        we'll need to handle those carefully, since a null date here is more reflective
        of the lack of a completion date rather than a completion date being unknown.
        For now, we'll pull out the `life_sentence` field so that we can hold onto this
        information. */
        sent.life_sentence,
    FROM `{{project_id}}.{{sessions_dataset}}.sentence_spans_materialized` span,
    UNNEST (span.sentences_preprocessed_id_array_actual_completion) AS sentences_preprocessed_id
    INNER JOIN `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` sent
        USING (state_code, person_id, sentences_preprocessed_id)
    WHERE sent.sentence_type='INCARCERATION'
),
projected_completion_date_spans_deadlines AS (
    /* Pull info on sentences from `sessions.sentence_deadline_spans_materialized`,
    which has information at the person-sentence level. We'll prioritize these dates
    when they're available. */
    SELECT
        state_code,
        person_id,
        deadlines.sentences_preprocessed_id,
        span.start_date,
        span.end_date_exclusive,
        deadlines.projected_incarceration_release_date AS projected_completion_date_max,
        deadlines.parole_eligibility_date,
    FROM `{{project_id}}.{{sessions_dataset}}.sentence_spans_materialized` span,
    UNNEST (sentence_deadline_id_array) AS sentence_deadline_id
    INNER JOIN `{{project_id}}.{{sessions_dataset}}.sentence_deadline_spans_materialized` deadlines
        USING (state_code, person_id, sentence_deadline_id)
),
prioritized_projected_completion_date_spans AS (
    SELECT
        state_code,
        person_id,
        sentences_preprocessed_id,
        start_date,
        end_date_exclusive,
        sp.life_sentence,
        /* For each sentence, we prioritize the date from task deadlines when it's
        available. */
        COALESCE(
            d.projected_completion_date_max,
            sp.projected_completion_date_max
        ) AS projected_completion_date_max,
        COALESCE(
            d.parole_eligibility_date,
            sp.parole_eligibility_date
        ) AS parole_eligibility_date,
    FROM projected_completion_date_spans_sentences_preprocessed sp
    FULL OUTER JOIN projected_completion_date_spans_deadlines d
        USING (state_code, person_id, sentences_preprocessed_id, start_date, end_date_exclusive)
),
prioritized_projected_completion_dates AS (
    /* Up until this point, we've kept things at the person-sentence level, identifying
    a projected maximum completion date for each person-sentence. Now, we'll move to the
    person level, finding each person's maximum date across all sentences in a given
    span. */
    SELECT DISTINCT
        state_code,
        person_id,
        start_date,
        -- Set the latest span end date to NULL in order to cover cases where an open supervision session has no
        -- overlapping sentence span, and the latest (non-overlapping) sentence span will be applied. If the client isn't
        -- currently on supervision then the sentence span will get clipped to the supervision session end date when
        -- taking the intersection with supervision sessions below.
        IF(
            start_date = MAX(start_date) OVER (PARTITION BY state_code, person_id)
            AND {nonnull_end_date_clause('end_date_exclusive')}<=CURRENT_DATE('US/Eastern'),
            NULL,
            end_date_exclusive
        ) AS end_date_exclusive,
        FIRST_VALUE(projected_completion_date_max) OVER (projected_completion_date_max_window) AS projected_completion_date_max,
        FIRST_VALUE(parole_eligibility_date) OVER (parole_eligibility_date_window) AS parole_eligibility_date,
    FROM prioritized_projected_completion_date_spans
    WINDOW
        projected_completion_date_max_window AS (
            PARTITION BY state_code, person_id, start_date
            ORDER BY
                /* For any life sentences that have a null
                `projected_completion_date_max` (which, at this point, means it's null
                across both `sentences_preprocessed` and deadlines data), we want to
                hold on to that null value. We therefore prioritize that null value
                above anything else. */
                CASE
                    WHEN (life_sentence AND projected_completion_date_max IS NULL) THEN 1
                    ELSE 2
                    END,
                /* If a person has no life sentences with null dates, then we want to
                take the maximum date across their sentences. */
                projected_completion_date_max DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ),
        parole_eligibility_date_window AS (
            PARTITION BY state_code, person_id, start_date
            ORDER BY
                /* For any life sentences that have a null `parole_eligibility_date`
                (which, at this point, means it's null across both
                `sentences_preprocessed` and deadlines data), we want to hold on to that
                null value. We therefore prioritize that null value above anything
                else. */
                CASE
                    WHEN (life_sentence AND parole_eligibility_date IS NULL) THEN 1
                    ELSE 2
                    END,
                /* If a person has no life sentences with null dates, then we want to
                take the maximum date across their sentences. */
                parole_eligibility_date DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )
),
collapsed_adjacent_spans AS (
    {aggregate_adjacent_spans(
        table_name="prioritized_projected_completion_dates",
        index_columns=["state_code", "person_id"],
        attribute=["projected_completion_date_max", "parole_eligibility_date"],
        end_date_field_name="end_date_exclusive",
    )}
),
incarceration_sessions AS (
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive,
        end_date_exclusive IS NULL AS open_incarceration_session,
    FROM `{{project_id}}.{{sessions_dataset}}.compartment_level_1_super_sessions_materialized`
    WHERE compartment_level_1="INCARCERATION"
),
overlapping_incarceration_sessions AS (
    {create_intersection_spans(
        table_1_name="incarceration_sessions",
        table_2_name="collapsed_adjacent_spans",
        index_columns=["state_code", "person_id"],
        table_1_columns=["open_incarceration_session"],
        table_2_columns=["projected_completion_date_max", "parole_eligibility_date"],
        use_left_join=True,
    )}
)
SELECT
    state_code,
    person_id,
    start_date,
    end_date_exclusive,
    end_date_exclusive AS end_date,
    projected_completion_date_max,
    parole_eligibility_date,
FROM overlapping_incarceration_sessions
"""

INCARCERATION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=_VIEW_NAME,
    view_query_template=_QUERY_TEMPLATE,
    description=__doc__,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=True,
    clustering_fields=["state_code", "person_id"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCARCERATION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.build_and_print()
