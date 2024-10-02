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
"""View with span of attributes of sentences being served"""

from recidiviz.observations.span_observation_big_query_view_builder import (
    SpanObservationBigQueryViewBuilder,
)
from recidiviz.observations.span_type import SpanType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = "Span of attributes of sentences being served"

_SOURCE_DATA_QUERY_TEMPLATE = """
SELECT
    spans.state_code,
    spans.person_id,
    spans.start_date,
    spans.end_date_exclusive,

    -- Characteristics from sentence spans
    LOGICAL_OR(sentences.is_drug_uniform) AS any_is_drug_uniform,
    LOGICAL_OR(sentences.is_violent_uniform) AS any_is_violent_uniform,
    LOGICAL_OR(sentences.crime_against_uniform = "Person") AS any_is_crime_against_person,
    LOGICAL_OR(sentences.crime_against_uniform = "Property") AS any_is_crime_against_property,
    LOGICAL_OR(sentences.crime_against_uniform = "Society") AS any_is_crime_against_society,
    MIN(sentences.effective_date) AS effective_date,
    MAX(sentences.parole_eligibility_date) AS parole_eligibility_date,
    MAX(sentences.projected_completion_date_max) AS projected_completion_date_max,
    -- Expected release dates from sentence deadline spans
    MAX(task_deadlines.projected_supervision_release_date) AS projected_supervision_release_snapshot_date,
    MAX(task_deadlines.projected_incarceration_release_date) AS projected_incarceration_release_snapshot_date,
    MAX(task_deadlines.parole_eligibility_date) AS parole_eligibility_snapshot_date,

FROM
    `{project_id}.sessions.sentence_spans_materialized` spans,
    UNNEST(sentences_preprocessed_id_array_actual_completion) AS sentences_preprocessed_id,
    UNNEST(sentence_deadline_id_array) AS sentence_deadline_id
LEFT JOIN
    `{project_id}.sessions.sentences_preprocessed_materialized` sentences
USING
    (person_id, state_code, sentences_preprocessed_id)
LEFT JOIN
    `{project_id}.sessions.sentence_deadline_spans_materialized` task_deadlines
USING
    (person_id, state_code, sentence_deadline_id)
GROUP BY 1, 2, 3, 4
"""

VIEW_BUILDER: SpanObservationBigQueryViewBuilder = SpanObservationBigQueryViewBuilder(
    span_type=SpanType.SENTENCE_SPAN,
    description=_VIEW_DESCRIPTION,
    sql_source=_SOURCE_DATA_QUERY_TEMPLATE,
    attribute_cols=[
        "any_is_crime_against_person",
        "any_is_crime_against_property",
        "any_is_crime_against_society",
        "any_is_drug_uniform",
        "any_is_violent_uniform",
        "effective_date",
        "parole_eligibility_date",
        "parole_eligibility_snapshot_date",
        "projected_completion_date_max",
        "projected_incarceration_release_snapshot_date",
        "projected_supervision_release_snapshot_date",
    ],
    span_start_date_col="start_date",
    span_end_date_col="end_date_exclusive",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
