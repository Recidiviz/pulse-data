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
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "supervision_projected_completion_date_spans"

_VIEW_DESCRIPTION = """
Spans of time with the projected max completion date for clients under supervision as
indicated by the sentences that were active during that span.
"""

STATES_WITH_NO_INCARCERATION_SENTENCES_ON_SUPERVISION = [StateCode.US_ND]

_QUERY_TEMPLATE = f"""
    SELECT
        span.state_code,
        span.person_id,
        span.start_date,
        span.end_date_exclusive,
        span.end_date_exclusive AS end_date,
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
        -- Exclude incarceration sentences for states that store all supervision
        -- sentence data (including parole)
        -- separately in supervision sentences
        (sent.state_code NOT IN ("{{excluded_incarceration_states}}", "US_ID") OR sent.sentence_type = "SUPERVISION")
    GROUP BY 1, 2, 3, 4
UNION ALL
    SELECT * 
    FROM `{{project_id}}.{{sessions_dataset}}.us_id_supervision_projected_completion_date_spans_materialized`
"""

SUPERVISION_LATEST_PROJECTED_COMPLETION_DATE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=_VIEW_NAME,
    view_query_template=_QUERY_TEMPLATE,
    description=_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    excluded_incarceration_states='", "'.join(
        [state.name for state in STATES_WITH_NO_INCARCERATION_SENTENCES_ON_SUPERVISION]
    ),
    should_materialize=True,
    clustering_fields=["state_code", "person_id"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_LATEST_PROJECTED_COMPLETION_DATE_VIEW_BUILDER.build_and_print()
