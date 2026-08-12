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
"""Tests for RequiredFieldConfidenceCheck, exercised against the fake extractor
collection, whose minimum confidence level is `inferred` collection-wide and
which declares a required field at the top level (`primary_status`) and inside
its array (`assignments[].assignment_name`) alongside optional ones.

Its wiring into the validator is covered in
llm_extraction_result_validator_test.py.
"""

from typing import Any
from unittest import TestCase

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    get_first_order_llm_extractor_config,
)
from recidiviz.documents.extraction.models.llm_request_output_values import (
    LLMRequestOutputValues,
)
from recidiviz.documents.extraction.validation.llm_document_validation_result import (
    ValidationCheckType,
    ValidationIssue,
)
from recidiviz.documents.extraction.validation.required_field_confidence_check import (
    RequiredFieldConfidenceCheck,
)
from recidiviz.tests.documents import fake_config
from recidiviz.tests.documents.extraction.fake_extractor_result_json import (
    build_fake_extractor_assignment_result_json,
    build_inferred_field_result_json,
    build_null_inferred_field_result_json,
    fake_all_fields_result_json,
    fake_irrelevant_result_json,
    fake_minimal_relevant_result_json,
)

_STATE_CODE = StateCode.US_XX
_COLLECTION_NAME = "FAKE_EXTRACTOR_COLLECTION"


class RequiredFieldConfidenceCheckTest(TestCase):
    """Tests for RequiredFieldConfidenceCheck."""

    def setUp(self) -> None:
        self.output_schema = get_first_order_llm_extractor_config(
            _STATE_CODE, _COLLECTION_NAME, config_module=fake_config
        ).extractor_collection.output_schema

    def _issues(self, result_json: dict[str, Any]) -> list[ValidationIssue]:
        return RequiredFieldConfidenceCheck.issues(
            output=LLMRequestOutputValues(
                output_schema=self.output_schema, output_json=result_json
            )
        )

    def _assert_single_issue(
        self,
        result_json: dict[str, Any],
        *,
        expected_field_name: str,
        expected_detail_substring: str,
    ) -> None:
        issues = self._issues(result_json)
        self.assertEqual(1, len(issues))
        issue = issues[0]
        self.assertEqual(
            ValidationCheckType.REQUIRED_FIELD_BELOW_MINIMUM_CONFIDENCE_LEVEL,
            issue.check_type,
        )
        self.assertEqual(expected_field_name, issue.field_name)
        self.assertIn(expected_detail_substring, issue.detail)

    def test_clean_result_has_no_issues(self) -> None:
        self.assertEqual([], self._issues(fake_all_fields_result_json()))

    def test_minimal_result_has_no_issues(self) -> None:
        self.assertEqual([], self._issues(fake_minimal_relevant_result_json()))

    def test_irrelevant_result_has_no_issues(self) -> None:
        # An irrelevant result carries no extracted fields at all, so there is
        # nothing to hold to a confidence bar.
        self.assertEqual([], self._issues(fake_irrelevant_result_json()))

    def test_required_field_below_minimum_flagged(self) -> None:
        result_json = fake_minimal_relevant_result_json()
        result_json["result"]["primary_status"] = build_inferred_field_result_json(
            "active", "speculative"
        )
        self._assert_single_issue(
            result_json,
            expected_field_name="primary_status",
            expected_detail_substring=(
                "extracted with confidence level [speculative], below its "
                "minimum of [inferred]"
            ),
        )

    def test_required_field_exactly_at_minimum_passes(self) -> None:
        # The bar is inclusive: the collection minimum is `inferred`, and a field
        # extracted at exactly `inferred` clears it.
        result_json = fake_minimal_relevant_result_json()
        result_json["result"]["primary_status"] = build_inferred_field_result_json(
            "active", "inferred"
        )
        self.assertEqual([], self._issues(result_json))

    def test_required_field_above_minimum_passes(self) -> None:
        result_json = fake_minimal_relevant_result_json()
        result_json["result"]["primary_status"] = build_inferred_field_result_json(
            "active", "verbatim"
        )
        self.assertEqual([], self._issues(result_json))

    def test_optional_field_below_minimum_not_flagged(self) -> None:
        # `location` is optional, so falling short is a quality shortfall for
        # the confidence-threshold quality filter to withhold, not an
        # extraction error.
        result_json = fake_minimal_relevant_result_json()
        result_json["result"]["location"] = build_inferred_field_result_json(
            "Kitchen", "speculative"
        )
        self.assertEqual([], self._issues(result_json))

    def test_structural_field_never_flagged(self) -> None:
        # `status_note` is STRUCTURAL — a bare value with no confidence level to
        # compare against a minimum.
        result_json = fake_minimal_relevant_result_json()
        result_json["result"]["status_note"] = "Currently active."
        self.assertEqual([], self._issues(result_json))

    def test_required_field_on_null_branch_not_flagged(self) -> None:
        # A null branch's confidence level describes how sure the model is that
        # nothing was there to extract — there is no extracted value to hold to
        # the field's bar.
        result_json = fake_minimal_relevant_result_json()
        result_json["result"]["primary_status"] = build_null_inferred_field_result_json(
            confidence_level="speculative"
        )
        self.assertEqual([], self._issues(result_json))

    def test_required_field_absent_from_json_not_flagged(self) -> None:
        # A field the result omits entirely (as an older extractor version's
        # output would) carries no confidence level to check. Its absence is the
        # structural check's business, not this one's.
        result_json = fake_minimal_relevant_result_json()
        del result_json["result"]["primary_status"]
        self.assertEqual([], self._issues(result_json))

    def test_required_array_sub_field_below_minimum_flagged_with_index(self) -> None:
        result_json = fake_all_fields_result_json()
        result_json["result"]["assignments"][0][
            "assignment_name"
        ] = build_inferred_field_result_json("Dish duty", "speculative")
        self._assert_single_issue(
            result_json,
            expected_field_name="assignments[0].assignment_name",
            expected_detail_substring="Required field [assignments[0].assignment_name]",
        )

    def test_array_elements_checked_independently(self) -> None:
        # Only the second element's required sub-field falls short; the first
        # stays clean and the finding names the offending index.
        result_json = fake_all_fields_result_json()
        result_json["result"]["assignments"] = [
            build_fake_extractor_assignment_result_json(
                "Dish duty", "internal", 12.5, "hourly"
            ),
            build_fake_extractor_assignment_result_json(
                "Laundry", "external", 20.0, "hourly"
            ),
        ]
        result_json["result"]["assignments"][1][
            "assignment_name"
        ] = build_inferred_field_result_json("Laundry", "speculative")
        self._assert_single_issue(
            result_json,
            expected_field_name="assignments[1].assignment_name",
            expected_detail_substring="below its minimum of [inferred]",
        )

    def test_optional_array_sub_field_below_its_override_not_flagged(self) -> None:
        # `rate_amount` overrides the collection minimum up to `explicit` and is
        # emitted below it, but it is optional — the filter withholds it rather
        # than this check failing the document.
        result_json = fake_all_fields_result_json()
        result_json["result"]["assignments"][0][
            "rate_amount"
        ] = build_inferred_field_result_json(12.5, "inferred")
        self.assertEqual([], self._issues(result_json))

    def test_every_shortfall_reported(self) -> None:
        result_json = fake_all_fields_result_json()
        result_json["result"]["primary_status"] = build_inferred_field_result_json(
            "active", "speculative"
        )
        result_json["result"]["assignments"][0][
            "assignment_name"
        ] = build_inferred_field_result_json("Dish duty", "speculative")
        self.assertEqual(
            {"primary_status", "assignments[0].assignment_name"},
            {issue.field_name for issue in self._issues(result_json)},
        )
