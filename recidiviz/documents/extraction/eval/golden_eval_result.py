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
"""The output of a golden eval run: one scored comparison of an actual field value to
its expected value.
"""
import attr

from recidiviz.common import attr_validators
from recidiviz.common.constants.operations.llm_extraction_job import (
    LLMExtractionJobDocumentResultType,
)
from recidiviz.documents.extraction.models.llm_document_extraction_golden_eval_config import (
    GoldenEvalTestType,
)

ARRAY_ELEMENT_COUNT_PREFIX = "count:"
"""Prefix of the value an array-level score renders — an ARRAY_OF_STRUCT field
has no scalar value of its own, so its row records the number of elements on each
side instead (e.g. `count:2`).
"""

# Separates an ARRAY_OF_STRUCT field's name from a sub-field's name in a scored
# row's field_name (e.g. `assignments.assignment_name`).
_SUB_FIELD_NAME_SEPARATOR = "."


def render_array_element_count(element_count: int) -> str:
    """Returns the value an array-level score renders for |element_count|
    elements.
    """
    return f"{ARRAY_ELEMENT_COUNT_PREFIX}{element_count}"


def array_sub_field_score_name(*, array_field_name: str, sub_field_name: str) -> str:
    """Returns the `field_name` a scored comparison of one ARRAY_OF_STRUCT
    sub-field carries.
    """
    return f"{array_field_name}{_SUB_FIELD_NAME_SEPARATOR}{sub_field_name}"


@attr.define(frozen=True, kw_only=True)
class GoldenEvalFieldScore:
    """One scored (document, field[, element]) comparison — shaped like a row of
    the `document_extraction_golden_eval_results` table.
    """

    golden_document_id: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The test document this comparison scores."""

    test_type: GoldenEvalTestType = attr.ib(
        validator=attr.validators.in_(GoldenEvalTestType)
    )
    """The scored document's test type, denormalized onto every score so accuracy
    can be aggregated per test type without rejoining the documents.
    """

    test_case: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The scored document's scenario category, denormalized for the same reason
    as `test_type`.
    """

    field_name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The output-schema field being scored, or `{array_field}.{sub_field}` for an
    ARRAY_OF_STRUCT sub-field.
    """

    element_index: int | None = attr.ib(
        validator=attr_validators.is_opt_non_negative_int
    )
    """The paired-element index for an ARRAY_OF_STRUCT sub-field score; `None` for
    a flat field or an array-level score.
    """

    expected_value: str | None = attr.ib(validator=attr_validators.is_opt_str)
    """The expected value, stringified; `None` when the field was not expected to
    be present (including every sub-field of an unmatched actual element).
    """

    actual_value: str | None = attr.ib(validator=attr_validators.is_opt_str)
    """The extracted value, stringified; `None` when the extractor did not produce
    it (including every sub-field of an unmatched expected element).
    """

    is_correct: bool = attr.ib(validator=attr_validators.is_bool)
    """Whether the actual value matched the expected one."""


@attr.define(frozen=True, kw_only=True)
class GoldenEvalResult:
    """The structured result of a golden eval run, for callers to report.

    Accuracies are derived from the field scores rather than stored;
    `actual_llm_result_type_by_document_id` is stored because it is NOT derivable
    — from the field scores alone, an all-miss document could be wrong values, a
    failed request, or a validation downgrade.
    """

    field_scores: list[GoldenEvalFieldScore] = attr.ib(
        validator=attr_validators.is_list_of(GoldenEvalFieldScore)
    )
    """Every scored comparison across every evaluated document."""

    actual_llm_result_type_by_document_id: dict[
        str, LLMExtractionJobDocumentResultType
    ] = attr.ib(
        validator=attr_validators.is_dict_of(str, LLMExtractionJobDocumentResultType)
    )
    """Each document's processed classification, so request errors and validation
    downgrades read as such in the summary rather than as generic field misses.
    """

    def __attrs_post_init__(self) -> None:
        scored_document_ids = {score.golden_document_id for score in self.field_scores}
        if unclassified_document_ids := scored_document_ids - set(
            self.actual_llm_result_type_by_document_id
        ):
            raise ValueError(
                f"Golden eval result has scored field(s) for document(s) with no "
                f"processed result type: {sorted(unclassified_document_ids)}."
            )

    @property
    def accuracy_by_test_type(self) -> dict[GoldenEvalTestType, float]:
        """Returns the fraction of correct comparisons per test type, over every
        document of that test type. Each ARRAY_OF_STRUCT sub-field of each element
        counts as one comparison, as does each array-level comparison.
        """
        scores_by_test_type: dict[GoldenEvalTestType, list[GoldenEvalFieldScore]] = {}
        for score in self.field_scores:
            scores_by_test_type.setdefault(score.test_type, []).append(score)
        return {
            test_type: _accuracy(scores)
            for test_type, scores in scores_by_test_type.items()
        }

    @property
    def accuracy_by_field(self) -> dict[str, float]:
        """Returns the fraction of correct comparisons per `field_name`, over
        every document. An ARRAY_OF_STRUCT field's array-level comparisons and
        each of its sub-fields' comparisons are reported separately.
        """
        scores_by_field_name: dict[str, list[GoldenEvalFieldScore]] = {}
        for score in self.field_scores:
            scores_by_field_name.setdefault(score.field_name, []).append(score)
        return {
            field_name: _accuracy(scores)
            for field_name, scores in scores_by_field_name.items()
        }


def _accuracy(scores: list[GoldenEvalFieldScore]) -> float:
    """Returns the fraction of |scores| that were correct."""
    return len([score for score in scores if score.is_correct]) / len(scores)
