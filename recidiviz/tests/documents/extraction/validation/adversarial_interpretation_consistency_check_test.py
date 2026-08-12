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
"""Tests for AdversarialInterpretationConsistencyCheck, exercised against the
fake extractor collection.

The invariant under test is a biconditional the prompt states outright: a field
records an `adversarial_interpretation` if and only if its `confidence_level` is
`speculative`. Every case below is one of the four combinations of those two,
on one branch or the other.

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
from recidiviz.documents.extraction.validation.adversarial_interpretation_consistency_check import (
    AdversarialInterpretationConsistencyCheck,
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
_ADVERSARIAL = "The note may be describing a prior assignment, not a current one."


class AdversarialInterpretationConsistencyCheckTest(TestCase):
    """Tests for AdversarialInterpretationConsistencyCheck."""

    def setUp(self) -> None:
        self.output_schema = get_first_order_llm_extractor_config(
            _STATE_CODE, _COLLECTION_NAME, config_module=fake_config
        ).extractor_collection.output_schema

    def _issues(self, result_json: dict[str, Any]) -> list[ValidationIssue]:
        return AdversarialInterpretationConsistencyCheck.issues(
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
        [issue] = issues
        self.assertEqual(
            (
                ValidationCheckType.ADVERSARIAL_INTERPRETATION_CONFIDENCE_LEVEL_INCONSISTENCY
            ),
            issue.check_type,
        )
        self.assertEqual(expected_field_name, issue.field_name)
        self.assertIn(expected_detail_substring, issue.detail)

    def _result_with_primary_status(
        self, primary_status_json: dict[str, Any]
    ) -> dict[str, Any]:
        """Returns a minimal relevant result whose `primary_status` field is
        |primary_status_json|."""
        result_json = fake_minimal_relevant_result_json()
        result_json["result"]["primary_status"] = primary_status_json
        return result_json

    def test_clean_result_has_no_issues(self) -> None:
        # The shared fixtures carry no adversarial interpretation and no
        # speculative confidence — the common consistent case.
        self.assertEqual([], self._issues(fake_all_fields_result_json()))

    def test_irrelevant_result_has_no_issues(self) -> None:
        self.assertEqual([], self._issues(fake_irrelevant_result_json()))

    def test_speculative_with_adversarial_interpretation_passes(self) -> None:
        self.assertEqual(
            [],
            self._issues(
                self._result_with_primary_status(
                    build_inferred_field_result_json(
                        "active",
                        "speculative",
                        adversarial_interpretation=_ADVERSARIAL,
                    )
                )
            ),
        )

    def test_adversarial_interpretation_above_speculative_flagged(self) -> None:
        self._assert_single_issue(
            self._result_with_primary_status(
                build_inferred_field_result_json(
                    "active", "explicit", adversarial_interpretation=_ADVERSARIAL
                )
            ),
            expected_field_name="primary_status",
            expected_detail_substring=(
                "recorded an adversarial interpretation but has confidence level "
                "[explicit]; an alternative reading requires [speculative]"
            ),
        )

    def test_speculative_without_adversarial_interpretation_flagged(self) -> None:
        self._assert_single_issue(
            self._result_with_primary_status(
                build_inferred_field_result_json("active", "speculative")
            ),
            expected_field_name="primary_status",
            expected_detail_substring=(
                "has confidence level [speculative] but recorded no adversarial "
                "interpretation to justify it"
            ),
        )

    def test_every_non_speculative_level_flagged_with_adversarial(self) -> None:
        # The rule is speculative-or-nothing, not a threshold: no level above
        # speculative tolerates an alternative reading.
        for confidence_level in ("inferred", "explicit", "verbatim"):
            with self.subTest(confidence_level=confidence_level):
                self._assert_single_issue(
                    self._result_with_primary_status(
                        build_inferred_field_result_json(
                            "active",
                            confidence_level,
                            adversarial_interpretation=_ADVERSARIAL,
                        )
                    ),
                    expected_field_name="primary_status",
                    expected_detail_substring=(
                        f"has confidence level [{confidence_level}]"
                    ),
                )

    def test_empty_adversarial_interpretation_counts_as_recorded(self) -> None:
        # The schema distinguishes null from a string; an empty string is a
        # string the model chose to emit, so it is held to the same rule.
        self._assert_single_issue(
            self._result_with_primary_status(
                build_inferred_field_result_json(
                    "active", "explicit", adversarial_interpretation=""
                )
            ),
            expected_field_name="primary_status",
            expected_detail_substring="recorded an adversarial interpretation",
        )

    def test_null_branch_with_adversarial_interpretation_flagged(self) -> None:
        # The null branch carries both keys too: the model can record why a value
        # might have been present when it extracted none.
        result_json = fake_minimal_relevant_result_json()
        result_json["result"]["location"] = build_null_inferred_field_result_json(
            confidence_level="explicit", adversarial_interpretation=_ADVERSARIAL
        )
        self._assert_single_issue(
            result_json,
            expected_field_name="location",
            expected_detail_substring="recorded an adversarial interpretation",
        )

    def test_null_branch_speculative_with_adversarial_passes(self) -> None:
        result_json = fake_minimal_relevant_result_json()
        result_json["result"]["location"] = build_null_inferred_field_result_json(
            confidence_level="speculative", adversarial_interpretation=_ADVERSARIAL
        )
        self.assertEqual([], self._issues(result_json))

    def test_null_branch_speculative_without_adversarial_flagged(self) -> None:
        result_json = fake_minimal_relevant_result_json()
        result_json["result"]["location"] = build_null_inferred_field_result_json(
            confidence_level="speculative"
        )
        self._assert_single_issue(
            result_json,
            expected_field_name="location",
            expected_detail_substring="but recorded no adversarial interpretation",
        )

    def test_structural_field_never_flagged(self) -> None:
        # `status_note` is a bare STRUCTURAL value with neither key.
        result_json = fake_minimal_relevant_result_json()
        result_json["result"]["status_note"] = "Currently active."
        self.assertEqual([], self._issues(result_json))

    def test_array_sub_field_flagged_with_index(self) -> None:
        result_json = fake_all_fields_result_json()
        result_json["result"]["assignments"][1][
            "rate_period"
        ] = build_null_inferred_field_result_json(
            "not_applicable", confidence_level="speculative"
        )
        self._assert_single_issue(
            result_json,
            expected_field_name="assignments[1].rate_period",
            expected_detail_substring="Field [assignments[1].rate_period]",
        )

    def test_every_inconsistent_field_reported(self) -> None:
        result_json = fake_all_fields_result_json()
        result_json["result"]["primary_status"] = build_inferred_field_result_json(
            "active", "speculative"
        )
        result_json["result"]["assignments"][0][
            "assignment_name"
        ] = build_inferred_field_result_json(
            "Dish duty", "verbatim", adversarial_interpretation=_ADVERSARIAL
        )
        self.assertEqual(
            ["primary_status", "assignments[0].assignment_name"],
            [issue.field_name for issue in self._issues(result_json)],
        )
