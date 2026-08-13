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
"""Tests for ConfidenceThresholdAdjustment, exercised against the fake extractor
collection, whose minimum confidence level is `inferred` collection-wide except
on `assignments[].rate_amount`, which overrides it up to `explicit`.

Unlike the checks, an adjustment both reports and rewrites: every case here pins
the findings AND what the adjusted output JSON looks like. The adjustment
rewrites a copy and leaves the output it was handed alone, so every case reads
its assertions off the returned copy. Required fields falling short are
RequiredFieldConfidenceCheck's business, and are asserted here only to show the
adjustment leaves them alone.

Its wiring into the validator is covered in
llm_extraction_result_validator_test.py.
"""

import copy
from typing import Any
from unittest import TestCase

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    get_first_order_llm_extractor_config,
)
from recidiviz.documents.extraction.models.llm_request_output_values import (
    LLMRequestOutputValues,
)
from recidiviz.documents.extraction.validation.confidence_threshold_adjustment import (
    ConfidenceThresholdAdjustment,
)
from recidiviz.documents.extraction.validation.llm_document_validation_result import (
    ValidationCheckType,
    ValidationIssue,
)
from recidiviz.tests.documents import fake_config
from recidiviz.tests.documents.extraction.fake_extractor_result_json import (
    build_inferred_field_result_json,
    build_null_inferred_field_result_json,
    fake_all_fields_result_json,
    fake_irrelevant_result_json,
    fake_minimal_relevant_result_json,
)

_STATE_CODE = StateCode.US_XX
_COLLECTION_NAME = "FAKE_EXTRACTOR_COLLECTION"


class ConfidenceThresholdAdjustmentTest(TestCase):
    """Tests for ConfidenceThresholdAdjustment."""

    def setUp(self) -> None:
        self.output_schema = get_first_order_llm_extractor_config(
            _STATE_CODE, _COLLECTION_NAME, config_module=fake_config
        ).extractor_collection.output_schema

    def _apply(
        self, result_json: dict[str, Any]
    ) -> tuple[list[ValidationIssue], dict[str, Any]]:
        """Returns the findings from adjusting |result_json|, paired with the
        adjusted JSON — a copy, leaving |result_json| itself untouched."""
        adjusted, issues = ConfidenceThresholdAdjustment.apply(
            output=LLMRequestOutputValues(
                output_schema=self.output_schema, output_json=result_json
            )
        )
        return issues, adjusted.output_json

    def _assert_unchanged(self, result_json: dict[str, Any]) -> None:
        """Asserts adjusting |result_json| reports nothing and returns a copy of
        it unchanged."""
        before = copy.deepcopy(result_json)
        issues, adjusted_json = self._apply(result_json)
        self.assertEqual([], issues)
        self.assertEqual(before, adjusted_json)
        self.assertEqual(before, result_json)

    def test_clean_result_untouched(self) -> None:
        self._assert_unchanged(fake_all_fields_result_json())

    def test_minimal_result_untouched(self) -> None:
        self._assert_unchanged(fake_minimal_relevant_result_json())

    def test_irrelevant_result_untouched(self) -> None:
        self._assert_unchanged(fake_irrelevant_result_json())

    def test_optional_field_below_minimum_nulled(self) -> None:
        result_json = fake_minimal_relevant_result_json()
        result_json["result"]["location"] = build_inferred_field_result_json(
            "Kitchen", "speculative", adversarial_interpretation="Could be the Yard."
        )
        issues, adjusted_json = self._apply(result_json)

        self.assertEqual(1, len(issues))
        issue = issues[0]
        self.assertEqual(
            ValidationCheckType.BELOW_MINIMUM_CONFIDENCE_LEVEL, issue.check_type
        )
        self.assertEqual("location", issue.field_name)
        self.assertIn(
            "confidence level [speculative], below its minimum of [inferred]; "
            "its value was withheld",
            issue.detail,
        )
        # The value is withheld, but everything that explains why it was
        # withheld survives into the validated output.
        self.assertEqual(
            {
                "value": None,
                "confidence_level": "speculative",
                "adversarial_interpretation": "Could be the Yard.",
                "citations": [{"text": "citation for Kitchen", "start": 0, "end": 15}],
            },
            adjusted_json["result"]["location"],
        )

    def test_optional_field_exactly_at_minimum_kept(self) -> None:
        # The bar is inclusive, so a field at exactly the minimum survives.
        result_json = fake_minimal_relevant_result_json()
        result_json["result"]["location"] = build_inferred_field_result_json(
            "Kitchen", "inferred"
        )
        self._assert_unchanged(result_json)

    def test_optional_field_above_minimum_kept(self) -> None:
        result_json = fake_minimal_relevant_result_json()
        result_json["result"]["location"] = build_inferred_field_result_json(
            "Kitchen", "verbatim"
        )
        self._assert_unchanged(result_json)

    def test_required_field_below_minimum_left_alone(self) -> None:
        # A required field falling short is a contradiction the validator fails
        # the document over, so it never reaches this adjustment — and if it
        # did, the adjustment would not quietly null it.
        result_json = fake_minimal_relevant_result_json()
        result_json["result"]["primary_status"] = build_inferred_field_result_json(
            "active", "speculative"
        )
        self._assert_unchanged(result_json)

    def test_optional_field_on_null_branch_left_alone(self) -> None:
        # There is no value to withhold on the null branch, whatever confidence
        # the model reported for the absence.
        result_json = fake_minimal_relevant_result_json()
        result_json["result"]["location"] = build_null_inferred_field_result_json(
            confidence_level="speculative"
        )
        self._assert_unchanged(result_json)

    def test_structural_field_left_alone(self) -> None:
        # `status_note` is a bare value with no confidence level to threshold.
        result_json = fake_minimal_relevant_result_json()
        result_json["result"]["status_note"] = "Currently active."
        self._assert_unchanged(result_json)

    def test_per_field_minimum_override_applied(self) -> None:
        # `rate_amount` clears the collection-wide `inferred` bar but not its own
        # `explicit` one — so the override, not the default, is what it is held
        # to.
        result_json = fake_all_fields_result_json()
        result_json["result"]["assignments"][0][
            "rate_amount"
        ] = build_inferred_field_result_json(12.5, "inferred")
        issues, adjusted_json = self._apply(result_json)

        self.assertEqual(1, len(issues))
        issue = issues[0]
        self.assertEqual("assignments[0].rate_amount", issue.field_name)
        self.assertIn(
            "confidence level [inferred], below its minimum of [explicit]",
            issue.detail,
        )
        self.assertIsNone(
            adjusted_json["result"]["assignments"][0]["rate_amount"]["value"]
        )
        # Its sibling sub-fields, which clear their own bars, are untouched.
        self.assertEqual(
            "Dish duty",
            adjusted_json["result"]["assignments"][0]["assignment_name"]["value"],
        )

    def test_only_the_offending_array_element_adjusted(self) -> None:
        result_json = fake_all_fields_result_json()
        result_json["result"]["assignments"][0][
            "assignment_type"
        ] = build_inferred_field_result_json("internal", "speculative")
        issues, adjusted_json = self._apply(result_json)

        self.assertEqual(
            ["assignments[0].assignment_type"], [issue.field_name for issue in issues]
        )
        self.assertIsNone(
            adjusted_json["result"]["assignments"][0]["assignment_type"]["value"]
        )
        # The second element took the null branch for the same sub-field and is
        # untouched.
        self.assertNotIn(
            "value", adjusted_json["result"]["assignments"][1]["assignment_type"]
        )

    def test_every_field_below_its_minimum_adjusted(self) -> None:
        result_json = fake_all_fields_result_json()
        result_json["result"]["location"] = build_inferred_field_result_json(
            "Kitchen", "speculative"
        )
        result_json["result"]["assignments"][0][
            "rate_amount"
        ] = build_inferred_field_result_json(12.5, "inferred")
        issues, adjusted_json = self._apply(result_json)

        self.assertEqual(
            ["location", "assignments[0].rate_amount"],
            [issue.field_name for issue in issues],
        )
        self.assertIsNone(adjusted_json["result"]["location"]["value"])
        self.assertIsNone(
            adjusted_json["result"]["assignments"][0]["rate_amount"]["value"]
        )

    def test_output_it_is_given_left_untouched(self) -> None:
        # The adjustment rewrites a copy, so the raw output the extractor
        # returned survives even a document the adjustment did withhold a value
        # from — which is what lets the validator hand it the raw output
        # directly.
        raw_json = fake_minimal_relevant_result_json()
        raw_json["result"]["location"] = build_inferred_field_result_json(
            "Kitchen", "speculative"
        )
        raw_output = LLMRequestOutputValues(
            output_schema=self.output_schema, output_json=raw_json
        )
        before = copy.deepcopy(raw_json)

        adjusted_output, issues = ConfidenceThresholdAdjustment.apply(output=raw_output)

        self.assertEqual(1, len(issues))
        self.assertEqual(before, raw_output.output_json)
        self.assertIsNot(raw_output.output_json, adjusted_output.output_json)
        self.assertIsNone(adjusted_output.output_json["result"]["location"]["value"])
