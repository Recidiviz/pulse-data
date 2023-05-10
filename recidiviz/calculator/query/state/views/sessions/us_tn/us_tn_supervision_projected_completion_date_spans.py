# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Spans of time with the projected max completion date for clients under supervision
as indicated by the sentences that were active during that span."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "us_tn_supervision_projected_completion_date_spans"

_VIEW_DESCRIPTION = """
Spans of time with the projected max completion date for clients under supervision as
indicated by the sentences that were active during that span.
"""

# TODO(#20650): deprecate this view once `Sentence.ExpirationDate` is not mapped to `sentences_preprocessed.completion_date`
_QUERY_TEMPLATE = f"""
WITH all_states_spans AS (
        SELECT
            span.state_code,
            span.person_id,
            span.start_date,
            span.end_date_exclusive,
            MAX(sent.projected_completion_date_max) AS projected_completion_date_max,
        FROM `{{project_id}}.{{sessions_dataset}}.sentence_spans_materialized` span,
        UNNEST (sentences_preprocessed_id_array) AS sentences_preprocessed_id
        INNER JOIN `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` sent
          USING (state_code, person_id, sentences_preprocessed_id)
        INNER JOIN `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` sess
            ON span.state_code = sess.state_code
            AND span.person_id = sess.person_id
            -- Restrict to spans that overlap with supervision sessions
            AND sess.compartment_level_1 = "SUPERVISION"
            -- Use strictly less than for exclusive end_dates
            AND span.start_date < {nonnull_end_date_clause('sess.end_date_exclusive')}
            AND sess.start_date < {nonnull_end_date_clause('span.end_date_exclusive')}
        WHERE
            span.state_code = 'US_TN'
        GROUP BY 1, 2, 3, 4
    )
    SELECT 
        state_code,
        person_id,
        start_date,
        /* Make the most recent span end_date NULL. end_dates (completion_date in ingested data) in TN are expiration
        dates, rather than when the sentence was actually completed. So we see people on supervision past this end_date,
        who should be surfaced as overdue for discharge. If we don't leave the latest span "open", then we won't surface
        them as eligible in TES since TES will say they were eligible, but that eligibility ended on the span end_date.
        Leaving spans open may "overcorrect" and leave spans open when someone did get discharged from supervision and 
        then come back, so we've added a new `has_active_sentence` criteria to check for overlapping spans during
        supervision sessions.        
        */
        IF(
            MAX({nonnull_end_date_exclusive_clause('end_date_exclusive')}) OVER(PARTITION BY person_id) = {nonnull_end_date_exclusive_clause('end_date_exclusive')},
            NULL, 
            end_date_exclusive
        ) AS end_date_exclusive,
        IF(
            MAX({nonnull_end_date_exclusive_clause('end_date_exclusive')}) OVER(PARTITION BY person_id) = {nonnull_end_date_exclusive_clause('end_date_exclusive')},
            NULL, 
            end_date_exclusive
        ) AS end_date,
        projected_completion_date_max,
    FROM all_states_spans 
"""

US_TN_SUPERVISION_LATEST_PROJECTED_COMPLETION_DATE_VIEW_BUILDER = (
    SimpleBigQueryViewBuilder(
        dataset_id=SESSIONS_DATASET,
        view_id=_VIEW_NAME,
        view_query_template=_QUERY_TEMPLATE,
        description=_VIEW_DESCRIPTION,
        sessions_dataset=SESSIONS_DATASET,
        should_materialize=True,
        clustering_fields=["state_code", "person_id"],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_SUPERVISION_LATEST_PROJECTED_COMPLETION_DATE_VIEW_BUILDER.build_and_print()
