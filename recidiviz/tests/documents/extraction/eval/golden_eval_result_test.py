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
"""Tests for GoldenEvalFieldScore and the accuracies derived on GoldenEvalResult."""
from unittest import TestCase

from recidiviz.common.constants.operations.llm_extraction_job import (
    LLMExtractionJobDocumentResultType,
)
from recidiviz.documents.extraction.eval.golden_eval_result import (
    GoldenEvalFieldScore,
    GoldenEvalResult,
)
from recidiviz.documents.extraction.models.llm_document_extraction_golden_eval_config import (
    GoldenEvalTestType,
)


def _score(
    golden_document_id: str,
    test_type: str,
    field_name: str,
    element_index: int | None,
    is_correct: bool,
) -> GoldenEvalFieldScore:
    """Returns a scored comparison with fixed placeholder values. |test_type| is
    a GoldenEvalTestType member name, e.g. "UNIT".
    """
    return GoldenEvalFieldScore(
        golden_document_id=golden_document_id,
        test_type=GoldenEvalTestType[test_type],
        test_case="base_case",
        field_name=field_name,
        element_index=element_index,
        expected_value="active",
        actual_value="active",
        is_correct=is_correct,
    )


class GoldenEvalFieldScoreTest(TestCase):
    """Tests for GoldenEvalFieldScore."""

    def test_negative_element_index_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Field \[element_index\] on \[GoldenEvalFieldScore\] must be a "
            r"non-negative integer\. Found value \[-1\]$",
        ):
            _score("unit_1", "UNIT", "primary_status", -1, True)


class GoldenEvalResultTest(TestCase):
    """Tests the accuracies GoldenEvalResult derives from its field scores."""

    def test_accuracy_by_test_type(self) -> None:
        result = GoldenEvalResult(
            field_scores=[
                _score("unit_1", "UNIT", "primary_status", None, True),
                _score("unit_1", "UNIT", "location", None, False),
                _score("unit_2", "UNIT", "primary_status", None, True),
                _score("unit_2", "UNIT", "location", None, True),
                _score("sample_1", "SAMPLE", "primary_status", None, False),
            ],
            actual_llm_result_type_by_document_id={
                "unit_1": LLMExtractionJobDocumentResultType.SUCCESS,
                "unit_2": LLMExtractionJobDocumentResultType.SUCCESS,
                "sample_1": LLMExtractionJobDocumentResultType.SUCCESS,
            },
        )

        self.assertEqual(
            {GoldenEvalTestType.UNIT: 0.75, GoldenEvalTestType.SAMPLE: 0.0},
            result.accuracy_by_test_type,
        )

    def test_accuracy_by_field(self) -> None:
        result = GoldenEvalResult(
            field_scores=[
                _score("unit_1", "UNIT", "primary_status", None, True),
                _score("unit_1", "UNIT", "assignments", None, False),
                _score("unit_1", "UNIT", "assignments.assignment_name", 0, True),
                _score("unit_1", "UNIT", "assignments.assignment_name", 1, False),
            ],
            actual_llm_result_type_by_document_id={
                "unit_1": LLMExtractionJobDocumentResultType.SUCCESS
            },
        )

        self.assertEqual(
            {
                "primary_status": 1.0,
                "assignments": 0.0,
                "assignments.assignment_name": 0.5,
            },
            result.accuracy_by_field,
        )

    def test_accuracies_with_no_scores(self) -> None:
        result = GoldenEvalResult(
            field_scores=[], actual_llm_result_type_by_document_id={}
        )

        self.assertEqual({}, result.accuracy_by_test_type)
        self.assertEqual({}, result.accuracy_by_field)

    def test_scored_document_missing_result_type_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Golden eval result has scored field\(s\) for document\(s\) with no "
            r"processed result type: \['unit_2'\]\.$",
        ):
            GoldenEvalResult(
                field_scores=[
                    _score("unit_1", "UNIT", "primary_status", None, True),
                    _score("unit_2", "UNIT", "primary_status", None, True),
                ],
                actual_llm_result_type_by_document_id={
                    "unit_1": LLMExtractionJobDocumentResultType.SUCCESS
                },
            )
