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
"""Tests for GoldenEvalDocumentValidator.

Covers the contract GoldenEvalDocumentReader depends on and its own tests cannot
see: which value each issue names, through the `field_path` a ValidationIssue
would carry once these checks are delegated to
LLMExtractionResultValidator. The rendered text of each issue is asserted in
golden_eval_document_reader_test.py.
"""
from typing import Any
from unittest import TestCase

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.eval.golden_eval_document import GoldenEvalDocument
from recidiviz.documents.extraction.eval.golden_eval_document_validator import (
    GoldenEvalDocumentValidator,
)
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    get_first_order_llm_extractor_config,
)
from recidiviz.documents.extraction.models.llm_document_extraction_golden_eval_config import (
    GoldenEvalTestType,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    IS_RELEVANT_FIELD_NAME,
)
from recidiviz.tests.documents import fake_config

_COLLECTION_NAME = "FAKE_EXTRACTOR_COLLECTION"

_DISH_DUTY_ELEMENT = {
    "assignment_name": "Dish duty",
    "assignment_type": "internal",
    "rate_amount": 12.5,
    "rate_period": "hourly",
}


def _document(**overrides: Any) -> GoldenEvalDocument:
    """Returns one document, defaulting to expected values that are legal against
    FAKE_EXTRACTOR_COLLECTION's schema and allowing passed-in overrides.
    """
    expected_values: dict[str, Any] = {
        IS_RELEVANT_FIELD_NAME: True,
        "primary_status": "active",
        "status_note": "Currently active.",
        "location": "Kitchen",
        "assignments": [dict(_DISH_DUTY_ELEMENT)],
    }
    expected_values.update(overrides)
    return GoldenEvalDocument(
        golden_document_id="unit_1",
        test_type=GoldenEvalTestType.UNIT,
        test_case="base_case",
        state_code=StateCode.US_XX,
        document_text="The record is active.",
        expected_values=expected_values,
    )


class GoldenEvalDocumentValidatorTest(TestCase):
    """Tests for GoldenEvalDocumentValidator."""

    def setUp(self) -> None:
        self.validator = GoldenEvalDocumentValidator(
            output_schema=get_first_order_llm_extractor_config(
                StateCode.US_XX, _COLLECTION_NAME, config_module=fake_config
            ).extractor_collection.output_schema
        )

    def _field_paths(self, document: GoldenEvalDocument) -> list[str]:
        """Returns the path of each value the validator flags in |document|."""
        return [issue.field_path for issue in self.validator.issues(document=document)]

    def test_legal_values_yield_no_issues(self) -> None:
        self.assertEqual([], self._field_paths(_document()))

    def test_undeclared_enum_value_names_the_field(self) -> None:
        self.assertEqual(
            ["primary_status"], self._field_paths(_document(primary_status="pending"))
        )

    def test_enum_value_spelled_differently_than_declared_is_flagged(self) -> None:
        # The sheet must spell the value exactly as the field declares it, even
        # though the scorer would compare the two case-insensitively.
        self.assertEqual(
            ["primary_status"], self._field_paths(_document(primary_status="ACTIVE"))
        )

    def test_float_sub_field_accepts_an_integer(self) -> None:
        # Mirrors the pipeline: a FLOAT field validates actual output as JSON
        # Schema `number`, which accepts 12, and the scorer compares 12 == 12.0.
        self.assertEqual(
            [],
            self._field_paths(
                _document(assignments=[{**_DISH_DUTY_ELEMENT, "rate_amount": 12}])
            ),
        )

    def test_float_sub_field_rejects_a_boolean(self) -> None:
        self.assertEqual(
            ["assignments[0].rate_amount"],
            self._field_paths(
                _document(assignments=[{**_DISH_DUTY_ELEMENT, "rate_amount": True}])
            ),
        )

    def test_wrongly_typed_sub_field_names_the_element_and_sub_field(self) -> None:
        self.assertEqual(
            ["assignments[0].assignment_name"],
            self._field_paths(_document(assignments=[{"assignment_name": 12}])),
        )

    def test_unknown_element_key_names_the_element(self) -> None:
        self.assertEqual(
            ["assignments[0]"],
            self._field_paths(
                _document(
                    assignments=[
                        {"assignment_name": "Dish duty", "assignment_nmae": "x"}
                    ]
                )
            ),
        )

    def test_flags_every_illegal_field_at_once(self) -> None:
        self.assertEqual(
            ["primary_status", "assignments[0].assignment_name"],
            self._field_paths(
                _document(
                    primary_status="pending", assignments=[{"assignment_name": 12}]
                )
            ),
        )

    def test_missing_relevance_answer_is_reported_alone(self) -> None:
        # Every other check reads that answer, so nothing else is worth reporting
        # while it is missing — `pending` goes unmentioned.
        self.assertEqual(
            [IS_RELEVANT_FIELD_NAME],
            self._field_paths(
                _document(**{IS_RELEVANT_FIELD_NAME: None, "primary_status": "pending"})
            ),
        )

    def test_required_field_is_not_required_of_an_irrelevant_document(self) -> None:
        self.assertEqual(
            [],
            self._field_paths(
                _document(
                    **{
                        IS_RELEVANT_FIELD_NAME: False,
                        "primary_status": None,
                        "status_note": None,
                        "location": None,
                        "assignments": None,
                    }
                )
            ),
        )

    def test_required_field_is_required_of_a_relevant_document(self) -> None:
        self.assertEqual(
            ["primary_status"], self._field_paths(_document(primary_status=None))
        )
