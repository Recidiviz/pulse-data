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
"""Tests for LLMExtractionResultValidator.

Exercises the validator against the fake extractor collection: structural
conformance against the JSON Schema generated from its output schema (a
conforming result passes, each way a result can violate the schema is flagged,
and a field absent from the JSON is treated as null (forward-compat)), plus the
wiring of the checks layered on top of it — a semantic-consistency,
required-field-confidence, or relevant-but-all-null violation downgrades the
result the same way a structural one does, a structurally-broken result skips
them all, and findings from several checks land in one audit. Each check itself
is tested directly in its own test file.

These all use the relevance-bearing fake collection (a `result`-wrapped result
with `is_relevant`). The validator also handles a relevance-free (ER) collection,
whose result is a flat root object with no `result` wrapper.
TODO(OBT-41146): Cover the relevance-free path once a relevance-free fake
collection YAML fixture exists to load.
"""

import copy
import datetime
from typing import Any
from unittest import TestCase

import pytz

from recidiviz.common.constants.operations.llm_extraction_job import (
    LLMExtractionJobDocumentResultType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.llm_client.types import (
    LLMClientDocumentExtractionResult,
    LLMDocumentExtractionTokenCounts,
)
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    get_first_order_llm_extractor_config,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    IS_RELEVANT_FIELD_NAME,
)
from recidiviz.documents.extraction.models.llm_request_output_values import (
    LLMRequestOutputValues,
)
from recidiviz.documents.extraction.validation.llm_document_validation_result import (
    LLMDocumentValidationResult,
    ValidationCheckType,
)
from recidiviz.documents.extraction.validation.llm_extraction_result_validator import (
    LLMExtractionResultValidator,
)
from recidiviz.tests.documents import fake_config
from recidiviz.tests.documents.extraction.fake_extractor_result_json import (
    build_inferred_field_result_json,
    build_null_inferred_field_result_json,
    fake_all_fields_result_json,
    fake_minimal_relevant_result_json,
    wrap_in_result_key,
)

_STATE_CODE = StateCode.US_XX
_COLLECTION_NAME = "FAKE_EXTRACTOR_COLLECTION"
_DOCUMENT_CONTENTS_ID = "doc1"
_SOURCE_TEXT = "The record is active. Assigned to the kitchen."
_VALIDATION_DATETIME = datetime.datetime(2026, 1, 1, 12, 0, tzinfo=pytz.UTC)


class LLMExtractionResultValidatorTest(TestCase):
    """Tests LLMExtractionResultValidator."""

    def setUp(self) -> None:
        self.config = get_first_order_llm_extractor_config(
            _STATE_CODE, _COLLECTION_NAME, config_module=fake_config
        )
        self.validator = LLMExtractionResultValidator()
        self.version_id = self.config.extractor_collection.validation_config_version_id

    def _validate(self, result_json: dict[str, Any]) -> LLMDocumentValidationResult:
        return self.validator.validate(
            config=self.config,
            raw_result=LLMClientDocumentExtractionResult.from_success(
                document_contents_id=_DOCUMENT_CONTENTS_ID,
                result_json=result_json,
                token_counts=LLMDocumentExtractionTokenCounts.empty(),
            ),
            _source_document_text=_SOURCE_TEXT,
            validation_datetime_utc=_VALIDATION_DATETIME,
        )

    def _passing_result(
        self, validated_output_json: dict[str, Any]
    ) -> LLMDocumentValidationResult:
        return LLMDocumentValidationResult(
            validated_output=LLMRequestOutputValues(
                output_schema=self.config.extractor_collection.output_schema,
                output_json=validated_output_json,
            ),
            audit_issues=[],
            result_type_override=None,
            validation_config_version_id=self.version_id,
            validation_datetime_utc=_VALIDATION_DATETIME,
        )

    def test_minimal_conforming_result_passes(self) -> None:
        result_json = fake_minimal_relevant_result_json()
        self.assertEqual(self._passing_result(result_json), self._validate(result_json))

    def test_all_fields_conforming_result_passes(self) -> None:
        result_json = fake_all_fields_result_json()
        self.assertEqual(self._passing_result(result_json), self._validate(result_json))

    def test_irrelevant_result_passes(self) -> None:
        # The irrelevant branch of the anyOf carries only is_relevant.
        result_json = {"result": {"is_relevant": False}}
        self.assertEqual(self._passing_result(result_json), self._validate(result_json))

    def test_absent_optional_fields_treated_as_null(self) -> None:
        # location and assignments are optional; a result omitting them (as an
        # older schema's output would) still validates — absent is treated as null.
        result_json = fake_minimal_relevant_result_json()
        self.assertNotIn("location", result_json["result"])
        self.assertNotIn("assignments", result_json["result"])
        self.assertEqual(self._passing_result(result_json), self._validate(result_json))

    def test_unknown_field_does_not_fail_validation(self) -> None:
        # A field the current schema does not declare (e.g. one dropped in a newer
        # version) is tolerated — the schema does not forbid extra properties.
        result_json = fake_minimal_relevant_result_json()
        result_json["result"]["a_field_not_in_the_schema"] = "x"
        self.assertEqual(self._passing_result(result_json), self._validate(result_json))

    def test_validate_does_not_mutate_input(self) -> None:
        result_json = fake_all_fields_result_json()
        before = copy.deepcopy(result_json)
        self._validate(result_json)
        self.assertEqual(before, result_json)

    def _assert_single_flagged_field(
        self,
        result_json: dict[str, Any],
        *,
        expected_field_name: str | None,
        expected_detail_substring: str,
    ) -> None:
        """Validates |result_json|, asserts it failed with a transient override
        (nothing usable persisted, document retried), and that its single audit
        issue is the expected SCHEMA_CONFORMANCE finding on |expected_field_name|.
        """
        validation = self._validate(result_json)

        self.assertFalse(validation.passed_validation)
        self.assertIsNone(validation.validated_output)
        self.assertEqual(
            LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT,
            validation.result_type_override,
        )
        self.assertTrue(validation.will_retry)

        self.assertEqual(1, len(validation.audit_issues))
        issue = validation.audit_issues[0]
        self.assertEqual(ValidationCheckType.SCHEMA_CONFORMANCE, issue.check_type)
        self.assertEqual(expected_field_name, issue.field_name)
        self.assertIn(expected_detail_substring, issue.detail)

    def test_bad_enum_value_flagged_on_field(self) -> None:
        # The bad value lives on the field's value branch; the branch is resolved
        # (the object carries a `value` key) and the failure surfaces on the
        # exact offending property.
        result_json = fake_minimal_relevant_result_json()
        result_json["result"]["primary_status"]["value"] = "not_a_status"
        self._assert_single_flagged_field(
            result_json,
            expected_field_name="result.primary_status.value",
            expected_detail_substring="'not_a_status' is not one of",
        )

    def test_empty_citations_flagged_on_field(self) -> None:
        # Empty citations violate the value branch's minItems; the null branch
        # allows them but the object carries a `value` key, so the value branch
        # is the intended one.
        result_json = fake_minimal_relevant_result_json()
        result_json["result"]["primary_status"]["citations"] = []
        self._assert_single_flagged_field(
            result_json,
            expected_field_name="result.primary_status.citations",
            expected_detail_substring="should be non-empty",
        )

    def test_neither_value_nor_null_reason_flagged_on_field(self) -> None:
        # Neither value nor null_reason present: the field's value/null anyOf
        # matches neither branch (this is how the schema enforces value/null_reason
        # mutual exclusivity), and nothing distinguishes which branch was intended,
        # so the failure reports against the first (value) branch.
        result_json = fake_minimal_relevant_result_json()
        del result_json["result"]["primary_status"]["value"]
        self._assert_single_flagged_field(
            result_json,
            expected_field_name="result.primary_status",
            expected_detail_substring="'value' is a required property",
        )

    def test_wrong_scalar_type_flagged_on_field(self) -> None:
        result_json = fake_minimal_relevant_result_json()
        result_json["result"]["status_note"] = 123
        self._assert_single_flagged_field(
            result_json,
            expected_field_name="result.status_note",
            expected_detail_substring="is not of type 'string'",
        )

    def test_missing_required_field_flagged_at_result(self) -> None:
        # A required field missing from the object is reported on the object that
        # requires it — the result wrapper — not on the field itself.
        result_json = fake_minimal_relevant_result_json()
        del result_json["result"]["primary_status"]
        self._assert_single_flagged_field(
            result_json,
            expected_field_name="result",
            expected_detail_substring="'primary_status' is a required property",
        )

    def test_missing_result_key_flagged_at_root(self) -> None:
        self._assert_single_flagged_field(
            {},
            expected_field_name=None,
            expected_detail_substring="'result' is a required property",
        )

    def test_bad_enum_inside_array_element_flagged_with_indexed_path(self) -> None:
        # A failure inside an array element names the element, sub-field, and
        # offending property: assignments[0].assignment_type.value.
        result_json = fake_all_fields_result_json()
        result_json["result"]["assignments"][0]["assignment_type"][
            "value"
        ] = "not_a_type"
        self._assert_single_flagged_field(
            result_json,
            expected_field_name="result.assignments[0].assignment_type.value",
            expected_detail_substring="'not_a_type' is not one of",
        )

    def test_missing_required_field_inside_array_element_flagged_with_index(
        self,
    ) -> None:
        # assignment_name is required within each element; a second element missing
        # it is reported on that element, so field_name carries the index.
        result_json = fake_all_fields_result_json()
        del result_json["result"]["assignments"][1]["assignment_name"]
        self._assert_single_flagged_field(
            result_json,
            expected_field_name="result.assignments[1]",
            expected_detail_substring="'assignment_name' is a required property",
        )

    def test_wrong_type_for_array_field_flagged_on_field(self) -> None:
        result_json = fake_all_fields_result_json()
        result_json["result"]["assignments"] = "not_an_array"
        self._assert_single_flagged_field(
            result_json,
            expected_field_name="result.assignments",
            expected_detail_substring="is not of type 'array'",
        )

    def test_non_boolean_is_relevant_flagged_on_field(self) -> None:
        # A non-boolean is_relevant fails both relevance branches' const
        # discriminators equally; the relevant branch wins on property overlap
        # and its is_relevant failures surface at the exact field.
        result_json = fake_minimal_relevant_result_json()
        result_json["result"]["is_relevant"] = "yes"
        validation = self._validate(result_json)

        self.assertFalse(validation.passed_validation)
        self.assertEqual(
            ["result.is_relevant", "result.is_relevant"],
            [issue.field_name for issue in validation.audit_issues],
        )
        details = [issue.detail for issue in validation.audit_issues]
        self.assertIn("'yes' is not of type 'boolean'", details)
        self.assertIn("True was expected", details)

    def test_multiple_bad_fields_each_flagged(self) -> None:
        result_json = fake_minimal_relevant_result_json()
        result_json["result"]["primary_status"]["value"] = "not_a_status"
        result_json["result"]["status_note"] = 123
        validation = self._validate(result_json)

        self.assertFalse(validation.passed_validation)
        self.assertEqual(
            {"result.primary_status.value", "result.status_note"},
            {issue.field_name for issue in validation.audit_issues},
        )

    def test_semantic_violation_downgrades_result(self) -> None:
        # Structurally conforming, but `location` is set while `primary_status`
        # is `inactive` — forbidden by location's not_applicable_when_value
        # constraint.
        result_json = fake_minimal_relevant_result_json()
        result_json["result"]["primary_status"]["value"] = "inactive"
        result_json["result"]["location"] = build_inferred_field_result_json("Kitchen")
        validation = self._validate(result_json)

        self.assertFalse(validation.passed_validation)
        self.assertIsNone(validation.validated_output)
        self.assertEqual(
            LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT,
            validation.result_type_override,
        )
        [issue] = validation.audit_issues
        self.assertEqual(ValidationCheckType.SEMANTIC_CONSISTENCY, issue.check_type)
        self.assertEqual("location", issue.field_name)

    def test_structurally_broken_result_skips_later_checks(self) -> None:
        # A bare string where an INFERRED field's dict belongs is a structural
        # violation, and one the semantic check's value readers cannot even
        # read — the validator must report only the structural issues rather
        # than crash reading values for the later checks.
        result_json = fake_minimal_relevant_result_json()
        result_json["result"]["location"] = "Kitchen"
        validation = self._validate(result_json)

        self.assertFalse(validation.passed_validation)
        self.assertTrue(validation.audit_issues)
        self.assertEqual(
            {ValidationCheckType.SCHEMA_CONFORMANCE},
            {issue.check_type for issue in validation.audit_issues},
        )

    def test_relevant_but_all_null_downgrades_result(self) -> None:
        # Structurally conforming — primary_status is present, on its null
        # branch — but the model called the document relevant while extracting
        # nothing from it.
        result_json = wrap_in_result_key(
            {
                IS_RELEVANT_FIELD_NAME: True,
                "primary_status": build_null_inferred_field_result_json(),
            }
        )
        validation = self._validate(result_json)

        self.assertFalse(validation.passed_validation)
        self.assertIsNone(validation.validated_output)
        self.assertEqual(
            LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT,
            validation.result_type_override,
        )
        [issue] = validation.audit_issues
        self.assertEqual(ValidationCheckType.RELEVANT_BUT_ALL_NULL, issue.check_type)
        # The finding is about the document as a whole, so it names no field.
        self.assertIsNone(issue.field_name)

    def test_required_field_below_minimum_confidence_downgrades_result(self) -> None:
        # Structurally conforming, but the required `primary_status` was
        # extracted at `speculative`, below the collection's `inferred` minimum.
        result_json = fake_minimal_relevant_result_json()
        result_json["result"]["primary_status"] = build_inferred_field_result_json(
            "active", "speculative"
        )
        validation = self._validate(result_json)

        self.assertFalse(validation.passed_validation)
        self.assertIsNone(validation.validated_output)
        self.assertEqual(
            LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT,
            validation.result_type_override,
        )
        [issue] = validation.audit_issues
        self.assertEqual(
            ValidationCheckType.REQUIRED_FIELD_BELOW_MINIMUM_CONFIDENCE_LEVEL,
            issue.check_type,
        )
        self.assertEqual("primary_status", issue.field_name)

    def test_findings_from_several_checks_land_in_one_audit(self) -> None:
        # Every extraction-error check past structural conformance runs even
        # once one has failed, so the audit row records everything wrong with
        # the document rather than only the first thing found.
        result_json = fake_minimal_relevant_result_json()
        result_json["result"]["primary_status"] = build_inferred_field_result_json(
            "inactive", "speculative"
        )
        result_json["result"]["location"] = build_inferred_field_result_json("Kitchen")
        validation = self._validate(result_json)

        self.assertFalse(validation.passed_validation)
        self.assertEqual(
            {
                # location set while primary_status is inactive.
                ValidationCheckType.SEMANTIC_CONSISTENCY,
                # primary_status is required and speculative.
                ValidationCheckType.REQUIRED_FIELD_BELOW_MINIMUM_CONFIDENCE_LEVEL,
            },
            {issue.check_type for issue in validation.audit_issues},
        )
