# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Defines a BigQueryViewBuilder that encodes a single classification score
component (i.e. one classification-instrument question) as spans of time.

This is the classification analog of the TES criteria view builder: every score
component view shares the same output contract as a criteria span view, with the
boolean ``meets_criteria`` swapped for an integer ``component_score``. It reuses
the criteria builder's schema-from-``reasons_fields`` generation and the
reason-JSON template wrapper (adjacent-span merge + date casting).
"""
from typing import List, Sequence

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.big_query.big_query_view_column import BigQueryViewColumn, Integer
from recidiviz.calculator.query.state.dataset_config import (
    classification_score_components_state_specific_dataset,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    get_reason_field_by_name,
    get_template_with_reasons_as_json,
    span_with_reasons_schema,
    validate_and_derive_state_specific_view_id,
)

# TODO(OBT-42797): Move this builder next to its collector and views, in
# recidiviz/calculator/query/state/views/classification/. Blocked on the
# task_eligibility utils that import the column-name constants below.

# Name of the integer column each score component view emits, carrying the
# points this question contributes during the span. Analogous to the criteria
# builder's `meets_criteria`.
COMPONENT_SCORE_COLUMN_NAME = "component_score"

# Name under which a second, secondary-form score is carried in `reason_v2` when one
# question scores differently on two forms of the same instrument and only one score can
# occupy `component_score`. Used by US_MI, whose initial and reclassification forms score
# most questions differently.
# TODO(OBT-41792): A score in `reason_v2` is covered by none of the score component span
# validations, and `all_classification_score_components` exposes only `component_score`.
# Give each form its own score component views once MI's per-form point values move to a
# CSV-backed static reference table, then delete this constant.
INITIAL_COMPONENT_SCORE_COLUMN_NAME = "initial_component_score"


def classification_score_component_schema(
    reasons_fields: List[ReasonsField],
) -> Sequence[BigQueryViewColumn]:
    """Returns the schema for a classification score component view. The shared
    span-with-reasons schema, with an integer ``component_score`` value column in
    place of the criteria schema's boolean ``meets_criteria``."""
    return span_with_reasons_schema(
        # NULLABLE so that a component view emitting an unscored span fails a
        # validation instead of failing to materialize. Every emitted span is
        # expected to carry a score: NULLs are flagged by
        # null_component_score_classification_score_component_spans.
        # TODO(OBT-42943): Take a `default_score` on the builder so a question with
        # missing inputs falls back to it here, instead of leaving each scoring view
        # to substitute its own default for a NULL.
        value_column=Integer(
            name=COMPONENT_SCORE_COLUMN_NAME,
            mode="NULLABLE",
            description="The classification points this question contributes during the span",
        ),
        reasons_fields=reasons_fields,
        span_label="score component span",
        state_code_mode="REQUIRED",
        person_id_mode="REQUIRED",
    )


class ClassificationScoreComponentBigQueryViewBuilder(SimpleBigQueryViewBuilder):
    """A builder for a view that defines spans of time during which a single
    classification-instrument question contributes a given number of points
    (``component_score``) to a resident's recommended-custody-level score.

    This is the classification analog of the TES criteria view builder: it reuses the
    criteria schema-from-``reasons_fields`` generation and reason-JSON template wrapper,
    emitting an integer ``component_score`` in place of the boolean ``meets_criteria``.
    Like the criteria builder, it hardcodes its output dataset
    (``classification_score_components_<state>``) and derives its ``view_id`` from the
    state-prefixed ``component_name``, so the state is encoded in exactly one place.
    """

    def __init__(
        self,
        *,
        # State whose classification instrument this question belongs to. Used to
        # scope the emitted spans to that state.
        state_code: StateCode,
        # Upper-case, `US_XX_`-prefixed name uniquely identifying this score component
        # (e.g. `US_IX_SLS_Q1`). The view id is this name with the state prefix
        # stripped, and must match the name of the file the builder is defined in.
        component_name: str,
        # Query producing one row per (person, span) with the columns
        # state_code, person_id, start_date, end_date, component_score, reason,
        # and one column per entry in `reasons_fields`.
        score_component_query_template: str,
        # Human-readable description of what this question scores.
        description: str,
        # Fields packed into the reason_v2 blob, describing why the span scored
        # the way it did.
        reasons_fields: List[ReasonsField],
        **query_format_kwargs: str,
    ) -> None:
        view_id = validate_and_derive_state_specific_view_id(
            state_code=state_code,
            name=component_name,
            name_type_label="Component",
            entity_description="classification score component",
        )
        super().__init__(
            dataset_id=classification_score_components_state_specific_dataset(
                state_code
            ),
            view_id=view_id,
            description=description,
            view_query_template=get_template_with_reasons_as_json(
                query_template=score_component_query_template,
                reasons_fields=reasons_fields,
                state_code=state_code.value,
                value_column_name=COMPONENT_SCORE_COLUMN_NAME,
            ),
            should_materialize=True,
            materialized_address_override=None,
            projects_to_deploy=None,
            clustering_fields=None,
            time_partitioning=None,
            schema=classification_score_component_schema(reasons_fields),
            **query_format_kwargs,
        )
        self.state_code = state_code
        self.component_name = component_name
        self.reasons_fields = reasons_fields

    def get_reason_field_from_name(self, reason_name: str) -> ReasonsField:
        """Returns the reason field object with the corresponding name."""
        return get_reason_field_by_name(
            self.reasons_fields,
            reason_name,
            f"Classification score component {self.component_name}",
        )
