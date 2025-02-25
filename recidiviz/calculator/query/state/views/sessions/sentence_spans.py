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
        attr.completion_date AS end_date_exclusive,
        sent.sentence_imposed_group_id AS sentence_imposed_group_id_actual_completion,
        attr.sentences_preprocessed_id AS sentences_preprocessed_id_actual_completion,
        CAST(NULL AS INT64) AS sentences_preprocessed_id_projected_completion,
        CAST(NULL AS INT64) AS sentence_deadline_id,
    FROM `{{project_id}}.sessions.sentence_imposed_group_summary_materialized` sent,
    UNNEST(offense_attributes) AS attr
    WHERE attr.date_imposed != {nonnull_end_date_clause('attr.completion_date')}

    UNION ALL

    SELECT
        sent.state_code,
        sent.person_id,
        sent.date_imposed AS start_date,
        sent.projected_completion_date_max AS end_date_exclusive,
        CAST(NULL AS INT64) AS sentence_imposed_group_id_actual_completion,
        CAST(NULL AS INT64) AS sentences_preprocessed_id_actual_completion,
        sent.sentences_preprocessed_id AS sentences_preprocessed_id_projected_completion,
        CAST(NULL AS INT64) AS sentence_deadline_id,
    FROM `{{project_id}}.sessions.sentences_preprocessed_materialized` sent
    WHERE date_imposed != {nonnull_end_date_clause('projected_completion_date_max')}

    UNION ALL

    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive,
        CAST(NULL AS INT64) AS sentence_imposed_group_id_actual_completion,
        CAST(NULL AS INT64) AS sentences_preprocessed_id_actual_completion,
        CAST(NULL AS INT64) AS sentences_preprocessed_id_projected_completion,
        sentence_deadline_id,
    FROM `{{project_id}}.sessions.sentence_deadline_spans_materialized`
),
{create_sub_sessions_with_attributes(
    table_name="sentences",
    end_date_field_name='end_date_exclusive'
)}
SELECT
    state_code,
    person_id,
    start_date,
    end_date_exclusive,
    end_date_exclusive AS end_date,
    ARRAY_AGG(
        DISTINCT sentence_imposed_group_id_actual_completion IGNORE NULLS ORDER BY sentence_imposed_group_id_actual_completion
    ) AS sentence_imposed_group_id_array_actual_completion,
    ARRAY_AGG(
        DISTINCT sentences_preprocessed_id_actual_completion IGNORE NULLS ORDER BY sentences_preprocessed_id_actual_completion
    ) AS sentences_preprocessed_id_array_actual_completion,
    ARRAY_AGG(
        DISTINCT sentences_preprocessed_id_projected_completion IGNORE NULLS ORDER BY sentences_preprocessed_id_projected_completion
    ) AS sentences_preprocessed_id_array_projected_completion,
    ARRAY_AGG(
        DISTINCT sentence_deadline_id IGNORE NULLS ORDER BY sentence_deadline_id
    ) AS sentence_deadline_id_array,
FROM sub_sessions_with_attributes
GROUP BY 1, 2, 3, 4, 5
"""

SENTENCE_SPANS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=SENTENCE_SPANS_VIEW_NAME,
    view_query_template=SENTENCE_SPANS_QUERY_TEMPLATE,
    description=SENTENCE_SPANS_VIEW_DESCRIPTION,
    should_materialize=True,
    clustering_fields=["state_code", "person_id"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SENTENCE_SPANS_VIEW_BUILDER.build_and_print()
