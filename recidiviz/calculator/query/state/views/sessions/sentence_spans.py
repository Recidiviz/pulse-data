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
"""Spans when sentences were being served or pending served and already imposed"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SENTENCE_SPANS_VIEW_NAME = "sentence_spans"

SENTENCE_SPANS_VIEW_DESCRIPTION = """Spans of time when a collection of sentences were being served or pending served and already imposed"""

SENTENCE_SPANS_QUERY_TEMPLATE = f"""
    /*{{description}}*/
    WITH
    /*
    Combine the sentence imposed groups with the individual sentences with columns for
    the session start and end in order to create the spans when each sentence and group
    were being served.
    */
    sentences AS (
        SELECT
            sent.state_code,
            sent.person_id,
            attr.date_imposed AS start_date,
            attr.completion_date AS end_date,
            sent.sentence_imposed_group_id,
            attr.sentences_preprocessed_id,
        FROM `{{project_id}}.{{sessions_dataset}}.sentence_imposed_group_summary_materialized` sent,
        UNNEST(offense_attributes) AS attr
        WHERE attr.date_imposed != {nonnull_end_date_clause('attr.completion_date')}
    ),
    {create_sub_sessions_with_attributes("sentences")}
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        ARRAY_AGG(
            DISTINCT sentence_imposed_group_id IGNORE NULLS ORDER BY sentence_imposed_group_id
        ) AS sentence_imposed_group_id_array,
        ARRAY_AGG(
            DISTINCT sentences_preprocessed_id IGNORE NULLS ORDER BY sentences_preprocessed_id
        ) AS sentences_preprocessed_id_array,
    FROM sub_sessions_with_attributes
    GROUP BY state_code, person_id, start_date, end_date
"""

SENTENCE_SPANS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=SENTENCE_SPANS_VIEW_NAME,
    view_query_template=SENTENCE_SPANS_QUERY_TEMPLATE,
    description=SENTENCE_SPANS_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=True,
    clustering_fields=["state_code", "person_id"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SENTENCE_SPANS_VIEW_BUILDER.build_and_print()
