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
wiring of the checks layered on top of it — any extraction-error check's
violation downgrades the result the same way a structural one does, a
structurally-broken result skips them all, and findings from several checks land
in one audit. Each check itself is tested directly in its own test file.

The quality adjustments are exercised alongside them: they are applied to the
validated copy of a result that cleared every extraction-error check, and
audited without costing the document a retry.

Every case but the hallucinated-citation one validates its result against a
source document built to ground the result's citations, so that the citation
checks pass and the case under test is the only thing failing.

Most of those use the relevance-bearing fake collection (a `result`-wrapped result
with `is_relevant`). The relevance-free path — a flat root object with no `result`
wrapper — is covered separately against the ER extractor collection synthesized
from the fake collection's `assignment` entity group, whose entity fields cover a
required non-nullable field (`assignment_name`), an optional field the synthesized
schema makes nullable (`assignment_type`), an integer (`entity_id`), and an
integer array (`entry_nums`).
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
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    CITATION_START_FIELD_NAME,
    CITATION_TEXT_FIELD_NAME,
    CITATIONS_FIELD_NAME,
    ENTITIES_FIELD_NAME,
    ENTITY_ID_FIELD_NAME,
    ENTRY_NUMS_FIELD_NAME,
    IS_RELEVANT_FIELD_NAME,
    RESULT_KEY,
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
from recidiviz.tests.documents.extraction.entity_resolution.entity_resolution_test_utils import (
    fake_entity_resolution_extractor_config,
    patch_fake_entity_resolution_model_config_name,
)
from recidiviz.tests.documents.extraction.fake_extractor_result_json import (
    build_fake_entity_resolution_entity_result_json,
    build_fake_entity_resolution_result_content,
    build_inferred_field_result_json,
    build_null_inferred_field_result_json,
    fake_all_fields_result_json,
    fake_minimal_relevant_result_json,
    ground_citations_in_fake_source_text,
    wrap_in_result_key,
)
from recidiviz.utils.types import assert_type

_STATE_CODE = StateCode.US_XX
_COLLECTION_NAME = "FAKE_EXTRACTOR_COLLECTION"
_ASSIGNMENT_GROUP_NAME = "assignment"
_LOCATION_GROUP_NAME = "location"
_DOCUMENT_CONTENTS_ID = "doc1"
_VALIDATION_DATETIME = datetime.datetime(2026, 1, 1, 12, 0, tzinfo=pytz.UTC)
_ADVERSARIAL = "The record might describe a closed matter."
# How far off a citation's reported start offset is put in the tests that
# exercise offset drift — close enough to its quoted text that the grounding
# check still counts it as grounded, so `CitationOffsetDriftAdjustment` is what
# acts on it.
_CITATION_OFFSET_DRIFT_CHARS = 3


class _ValidatorTestBase(TestCase):
    """Harness shared by the relevance-bearing and relevance-free test cases.
    Subclasses set `self.config` to the extractor config under test.
    """

    config: LLMExtractorConfig

    def setUp(self) -> None:
        self.validator = LLMExtractionResultValidator()

    @property
    def version_id(self) -> str:
        return self.config.extractor_collection.validation_config_version_id

    def _validate(
        self,
        result_json: dict[str, Any],
        *,
        source_document_text: str,
        expected_entry_nums: set[int] | None = None,
    ) -> LLMDocumentValidationResult:
        return self.validator.validate(
            config=self.config,
            raw_result=LLMClientDocumentExtractionResult.from_success(
                document_contents_id=_DOCUMENT_CONTENTS_ID,
                result_json=result_json,
                token_counts=LLMDocumentExtractionTokenCounts.empty(),
            ),
            source_document_text=source_document_text,
            expected_entry_nums=expected_entry_nums,
            validation_datetime_utc=_VALIDATION_DATETIME,
        )

    def _validate_grounded(
        self,
        result_json: dict[str, Any],
        *,
        expected_entry_nums: set[int] | None = None,
    ) -> tuple[LLMDocumentValidationResult, dict[str, Any]]:
        """Validates |result_json| against a source document that grounds every
        citation it carries, returning the outcome paired with the grounded
        result JSON that was actually validated.
        """
        grounded = ground_citations_in_fake_source_text(result_json)
        validation = self._validate(
            grounded.result_json,
            source_document_text=grounded.source_document_text,
            expected_entry_nums=expected_entry_nums,
        )
        return validation, grounded.result_json

    def _passing_result(
        self, validated_output_json: dict[str, Any]
    ) -> LLMDocumentValidationResult:
        """Returns an LLMDocumentValidationResult representing a passing result"""
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

    def _assert_single_flagged_field(
        self,
        result_json: dict[str, Any],
        *,
        expected_field_name: str | None,
        expected_detail_substring: str,
        expected_entry_nums: set[int] | None = None,
    ) -> None:
        """Validates |result_json|, asserts it fails the SCHEMA_CONFORMANCE check on
        |expected_field_name|, indicating the extraction should be retried.
        """
        validation, _ = self._validate_grounded(
            result_json, expected_entry_nums=expected_entry_nums
        )

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


class LLMRelevanceBearingExtractionResultValidatorTest(_ValidatorTestBase):
    """Tests LLMExtractionResultValidator against a relevance-bearing collection."""

    def setUp(self) -> None:
        super().setUp()
        self.config = get_first_order_llm_extractor_config(
            _STATE_CODE, _COLLECTION_NAME, config_module=fake_config
        )

    def test_minimal_conforming_result_passes(self) -> None:
        validation, result_json = self._validate_grounded(
            fake_minimal_relevant_result_json()
        )
        self.assertEqual(self._passing_result(result_json), validation)

    def test_expected_entry_nums_rejected_for_first_order(self) -> None:
        # Only an entity-resolution extractor's composite documents have
        # numbered entries, so supplying an entry set for a first-order
        # extractor is a caller bug.
        with self.assertRaisesRegex(
            ValueError,
            r"^Extractor \[US_XX_FAKE_EXTRACTOR_COLLECTION\] is first-order, but "
            r"validation received expected entry nums \[\[1, 2\]\]\.",
        ):
            self._validate(
                fake_minimal_relevant_result_json(),
                source_document_text="Some note text.",
                expected_entry_nums={1, 2},
            )

    def test_all_fields_conforming_result_passes(self) -> None:
        validation, result_json = self._validate_grounded(fake_all_fields_result_json())
        self.assertEqual(self._passing_result(result_json), validation)

    def test_irrelevant_result_passes(self) -> None:
        # The irrelevant branch of the anyOf carries only is_relevant.
        result_json = wrap_in_result_key({IS_RELEVANT_FIELD_NAME: False})
        self.assertEqual(
            self._passing_result(result_json),
            self._validate(result_json, source_document_text="Some note text."),
        )

    def test_absent_optional_fields_treated_as_null(self) -> None:
        # location and assignments are optional; a result omitting them (as an
        # older schema's output would) still validates — absent is treated as null.
        result_json = fake_minimal_relevant_result_json()
        self.assertNotIn("location", result_json["result"])
        self.assertNotIn("assignments", result_json["result"])
        validation, grounded_json = self._validate_grounded(result_json)
        self.assertEqual(self._passing_result(grounded_json), validation)

    def test_unknown_field_does_not_fail_validation(self) -> None:
        # A field the current schema does not declare (e.g. one dropped in a newer
        # version) is tolerated — the schema does not forbid extra properties.
        result_json = fake_minimal_relevant_result_json()
        result_json["result"]["a_field_not_in_the_schema"] = "x"
        validation, grounded_json = self._validate_grounded(result_json)
        self.assertEqual(self._passing_result(grounded_json), validation)

    def test_validate_does_not_mutate_input(self) -> None:
        grounded = ground_citations_in_fake_source_text(fake_all_fields_result_json())
        before = copy.deepcopy(grounded.result_json)
        self._validate(
            grounded.result_json,
            source_document_text=grounded.source_document_text,
        )
        self.assertEqual(before, grounded.result_json)

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
        result_json["result"]["primary_status"][CITATIONS_FIELD_NAME] = []
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
        result_json["result"][IS_RELEVANT_FIELD_NAME] = "yes"
        validation, _ = self._validate_grounded(result_json)

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
        validation, _ = self._validate_grounded(result_json)

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
        validation, _ = self._validate_grounded(result_json)

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
        validation, _ = self._validate_grounded(result_json)

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
        validation, _ = self._validate_grounded(result_json)

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
            "active", "speculative", adversarial_interpretation=_ADVERSARIAL
        )
        validation, _ = self._validate_grounded(result_json)

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

    def test_adversarial_interpretation_inconsistency_downgrades_result(self) -> None:
        # Structurally conforming, but `primary_status` records an alternative
        # reading at a confidence level above `speculative`.
        result_json = fake_minimal_relevant_result_json()
        result_json["result"]["primary_status"] = build_inferred_field_result_json(
            "active", "explicit", adversarial_interpretation=_ADVERSARIAL
        )
        validation, _ = self._validate_grounded(result_json)

        self.assertFalse(validation.passed_validation)
        self.assertIsNone(validation.validated_output)
        self.assertEqual(
            LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT,
            validation.result_type_override,
        )
        [issue] = validation.audit_issues
        self.assertEqual(
            ValidationCheckType.ADVERSARIAL_INTERPRETATION_CONFIDENCE_LEVEL_INCONSISTENCY,
            issue.check_type,
        )
        self.assertEqual("primary_status", issue.field_name)

    def test_hallucinated_citation_downgrades_result(self) -> None:
        # Validated against a document that grounds none of the result's quotes,
        # rather than the grounded pairing every other case here uses.
        validation = self._validate(
            fake_minimal_relevant_result_json(),
            source_document_text="A note quoting none of that.",
        )

        self.assertFalse(validation.passed_validation)
        self.assertIsNone(validation.validated_output)
        self.assertEqual(
            LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT,
            validation.result_type_override,
        )
        [issue] = validation.audit_issues
        self.assertEqual(ValidationCheckType.HALLUCINATED_CITATION, issue.check_type)
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
        validation, _ = self._validate_grounded(result_json)

        self.assertFalse(validation.passed_validation)
        self.assertEqual(
            {
                # location set while primary_status is inactive.
                ValidationCheckType.SEMANTIC_CONSISTENCY,
                # primary_status is required and speculative.
                ValidationCheckType.REQUIRED_FIELD_BELOW_MINIMUM_CONFIDENCE_LEVEL,
                # primary_status is speculative with no adversarial interpretation.
                ValidationCheckType.ADVERSARIAL_INTERPRETATION_CONFIDENCE_LEVEL_INCONSISTENCY,
            },
            {issue.check_type for issue in validation.audit_issues},
        )

    def _assert_kept(
        self, validation: LLMDocumentValidationResult
    ) -> LLMRequestOutputValues:
        """Asserts |validation| kept the document — something usable was
        persisted, the raw SUCCESS classification stands, and no retry was queued
        — and returns the validated copy the adjustments produced."""
        self.assertTrue(validation.passed_validation)
        self.assertIsNone(validation.result_type_override)
        self.assertEqual(
            LLMExtractionJobDocumentResultType.SUCCESS, validation.result_type
        )
        self.assertFalse(validation.will_retry)
        return assert_type(validation.validated_output, LLMRequestOutputValues)

    def test_low_confidence_optional_field_adjusted_without_retry(self) -> None:
        # `location` is optional, so falling below its minimum is a quality
        # shortfall rather than a contradiction: the document is kept, the value
        # is withheld from the validated copy, and nothing is retried.
        result_json = fake_minimal_relevant_result_json()
        result_json["result"]["location"] = build_inferred_field_result_json(
            "Kitchen", "speculative", adversarial_interpretation=_ADVERSARIAL
        )
        validation, grounded_json = self._validate_grounded(result_json)

        validated_output = self._assert_kept(validation)
        [issue] = validation.audit_issues
        self.assertEqual(
            ValidationCheckType.BELOW_MINIMUM_CONFIDENCE_LEVEL, issue.check_type
        )
        self.assertEqual("location", issue.field_name)
        self.assertFalse(issue.will_retry)

        self.assertIsNone(validated_output.output_json["result"]["location"]["value"])
        # The raw result the extractor returned is left exactly as it was.
        self.assertEqual("Kitchen", grounded_json["result"]["location"]["value"])

    def test_extraction_error_suppresses_the_confidence_adjustment(self) -> None:
        # The document has a low-confidence optional field the adjustment would
        # normally withhold, but its required field is speculative — nothing is
        # persisted, so there is no validated copy to adjust and no adjustment
        # finding to audit.
        result_json = fake_minimal_relevant_result_json()
        result_json["result"]["primary_status"] = build_inferred_field_result_json(
            "active", "speculative", adversarial_interpretation=_ADVERSARIAL
        )
        result_json["result"]["location"] = build_inferred_field_result_json(
            "Kitchen", "speculative", adversarial_interpretation=_ADVERSARIAL
        )
        validation, _ = self._validate_grounded(result_json)

        self.assertFalse(validation.passed_validation)
        self.assertIsNone(validation.validated_output)
        self.assertNotIn(
            ValidationCheckType.BELOW_MINIMUM_CONFIDENCE_LEVEL,
            {issue.check_type for issue in validation.audit_issues},
        )

    def test_drifted_citation_corrected_without_retry(self) -> None:
        # The reported offset lands near the quote rather than on it, which is
        # drift the grounding check tolerates and this adjustment corrects.
        # Drift past that tolerance is a hallucinated citation instead, covered
        # by test_hallucinated_citation_downgrades_result.
        grounded = ground_citations_in_fake_source_text(
            fake_minimal_relevant_result_json()
        )
        raw_citation = grounded.result_json["result"]["primary_status"][
            CITATIONS_FIELD_NAME
        ][0]
        correct_start = raw_citation[CITATION_START_FIELD_NAME]
        drifted_start = correct_start - _CITATION_OFFSET_DRIFT_CHARS
        raw_citation[CITATION_START_FIELD_NAME] = drifted_start

        validation = self._validate(
            grounded.result_json,
            source_document_text=grounded.source_document_text,
        )

        validated_output = self._assert_kept(validation)
        [issue] = validation.audit_issues
        self.assertEqual(ValidationCheckType.CITATION_OFFSET_DRIFT, issue.check_type)
        self.assertEqual("primary_status", issue.field_name)
        self.assertFalse(issue.will_retry)

        validated_citation = validated_output.output_json["result"]["primary_status"][
            CITATIONS_FIELD_NAME
        ][0]
        self.assertEqual(correct_start, validated_citation[CITATION_START_FIELD_NAME])
        self.assertEqual(
            validated_citation[CITATION_TEXT_FIELD_NAME],
            grounded.source_document_text[
                validated_citation[CITATION_START_FIELD_NAME] : validated_citation[
                    "end"
                ]
            ],
        )
        # The raw result keeps the offsets the model reported.
        self.assertEqual(drifted_start, raw_citation[CITATION_START_FIELD_NAME])

    def test_extraction_error_suppresses_the_citation_drift_adjustment(self) -> None:
        # The document has a citation the drift adjustment would normally
        # correct, but another of its citations is fabricated — nothing is
        # persisted, so there is no validated copy to adjust and no adjustment
        # finding to audit.
        result_json = fake_minimal_relevant_result_json()
        result_json["result"]["location"] = build_inferred_field_result_json("Kitchen")
        grounded = ground_citations_in_fake_source_text(result_json)
        grounded.result_json["result"]["location"][CITATIONS_FIELD_NAME][0][
            CITATION_START_FIELD_NAME
        ] = _CITATION_OFFSET_DRIFT_CHARS
        grounded.result_json["result"]["primary_status"][CITATIONS_FIELD_NAME][0][
            CITATION_TEXT_FIELD_NAME
        ] = "a quote the document never contained"

        validation = self._validate(
            grounded.result_json,
            # Grounds `location`'s citation but not the fabricated one.
            source_document_text="citation for Kitchen",
        )

        self.assertFalse(validation.passed_validation)
        self.assertIsNone(validation.validated_output)
        self.assertEqual(
            {ValidationCheckType.HALLUCINATED_CITATION},
            {issue.check_type for issue in validation.audit_issues},
        )


class LLMRelevanceFreeExtractionResultValidatorTest(_ValidatorTestBase):
    """Tests LLMExtractionResultValidator against a relevance-free (entity
    resolution) collection, whose result is a flat root object: no `result`
    wrapper and no `is_relevant` field.

    The synthesized ER schema declares a single `entities` ARRAY_OF_STRUCT
    (required, minimum one element) at the root, so every field path a finding
    names is rooted at `entities` rather than at a `result` wrapper. The schema is
    also entirely free of `anyOf` — every field is STRUCTURAL, there is no
    relevance branch, and nullability is expressed as a type union — so each way
    of violating it produces exactly one finding at the offending field.

    Being all-STRUCTURAL, an ER result carries no confidence metadata and no
    citations, so grounding it is a no-op and the INFERRED-field checks and
    quality filters have nothing to act on.
    """

    def setUp(self) -> None:
        super().setUp()
        self.enterContext(patch_fake_entity_resolution_model_config_name())
        self.config = fake_entity_resolution_extractor_config(_ASSIGNMENT_GROUP_NAME)

    @staticmethod
    def _result_json(*entities: dict[str, Any]) -> dict[str, Any]:
        """Returns the flat result JSON holding |entities| — deliberately not run
        through wrap_in_result_key, which is what distinguishes this shape.
        """
        return build_fake_entity_resolution_result_content(list(entities))

    @staticmethod
    def _entity(
        entity_id: int,
        *,
        assignment_name: str = "Kitchen",
        assignment_type: str | None = "internal",
        entry_nums: list[int] | None = None,
    ) -> dict[str, Any]:
        """Returns a JSON object representing an entity"""
        return build_fake_entity_resolution_entity_result_json(
            entity_id,
            entry_nums=entry_nums if entry_nums is not None else [entity_id],
            assignment_name=assignment_name,
            assignment_type=assignment_type,
        )

    def test_conforming_flat_result_passes(self) -> None:
        # The second entity leaves assignment_type null, which the synthesized
        # schema allows because the field is optional in the first-order
        # collection — no mention of that entity filled it in.
        result_json = self._result_json(
            self._entity(1),
            self._entity(2, assignment_name="Laundry", assignment_type=None),
        )
        self.assertNotIn(RESULT_KEY, result_json)
        validation, validated_json = self._validate_grounded(
            result_json, expected_entry_nums={1, 2}
        )
        self.assertEqual(self._passing_result(validated_json), validation)
        # Grounding had nothing to rewrite, so the validated result is the
        # flat result verbatim.
        self.assertEqual(result_json, validated_json)

    def test_wrong_scalar_type_flagged_on_flat_field(self) -> None:
        # The path is rooted at `entities`, with no `result.` prefix — the
        # relevance-bearing counterparts all carry one.
        result_json = self._result_json(self._entity(1))
        result_json[ENTITIES_FIELD_NAME][0][ENTITY_ID_FIELD_NAME] = "1"
        self._assert_single_flagged_field(
            result_json,
            expected_field_name="entities[0].entity_id",
            expected_detail_substring="is not of type 'integer'",
            expected_entry_nums={1},
        )

    def test_bad_enum_inside_entity_flagged_with_indexed_path(self) -> None:
        # Compare test_bad_enum_inside_array_element_flagged_with_indexed_path,
        # which expects `result.assignments[0].assignment_type.value`: this path
        # loses both the `result.` prefix (flat root) and the `.value` suffix (a
        # STRUCTURAL entity field carries a bare value, not the INFERRED wrapper).
        result_json = self._result_json(self._entity(1))
        result_json[ENTITIES_FIELD_NAME][0]["assignment_type"] = "not_a_type"
        self._assert_single_flagged_field(
            result_json,
            expected_field_name="entities[0].assignment_type",
            expected_detail_substring="'not_a_type' is not one of",
            expected_entry_nums={1},
        )

    def test_missing_required_field_flagged_on_entity(self) -> None:
        # assignment_name is required within each entity; a second entity missing
        # it is reported on that entity, so the field name carries the index.
        result_json = self._result_json(self._entity(1), self._entity(2))
        del result_json[ENTITIES_FIELD_NAME][1]["assignment_name"]
        self._assert_single_flagged_field(
            result_json,
            expected_field_name="entities[1]",
            expected_detail_substring="'assignment_name' is a required property",
            expected_entry_nums={1, 2},
        )

    def test_missing_entities_key_flagged_at_root(self) -> None:
        # The counterpart to test_missing_result_key_flagged_at_root: both report
        # against the root object, but here it is the `entities` field that is
        # missing rather than the wrapper. Asserting a *single* issue also pins
        # that the validator returned after structural conformance failed —
        # RelevantButAllNullCheck would otherwise flag this same result, since a
        # relevance-free output reads as relevant by construction and no
        # user-defined field carries a value.
        self._assert_single_flagged_field(
            {},
            expected_field_name=None,
            expected_detail_substring="'entities' is a required property",
            expected_entry_nums={1},
        )

    def test_empty_entities_flagged_as_schema_conformance(self) -> None:
        # `entities` requires at least one element, so an empty array is a
        # structural failure and not a relevant-but-all-null one. Two independent
        # things keep that check quiet: the validator returns as soon as
        # structural conformance fails, and an empty array counts as a present
        # value anyway.
        result_json = self._result_json()
        self._assert_single_flagged_field(
            result_json,
            expected_field_name="entities",
            expected_detail_substring="should be non-empty",
            expected_entry_nums={1},
        )
        validation, _ = self._validate_grounded(result_json, expected_entry_nums={1})
        self.assertNotIn(
            ValidationCheckType.RELEVANT_BUT_ALL_NULL,
            [issue.check_type for issue in validation.audit_issues],
        )

    def test_empty_entry_nums_flagged_on_flat_field(self) -> None:
        # entry_nums is an integer array that must name at least one entry: an
        # entity clustered from no entries describes nothing.
        result_json = self._result_json(self._entity(1, entry_nums=[]))
        self.assertEqual([], result_json[ENTITIES_FIELD_NAME][0][ENTRY_NUMS_FIELD_NAME])
        self._assert_single_flagged_field(
            result_json,
            expected_field_name="entities[0].entry_nums",
            expected_detail_substring="should be non-empty",
            expected_entry_nums={1},
        )

    def test_validate_does_not_mutate_input(self) -> None:
        result_json = self._result_json(self._entity(1), self._entity(2))
        before = copy.deepcopy(result_json)
        self._validate_grounded(result_json, expected_entry_nums={1, 2})
        self.assertEqual(before, result_json)

    def test_entry_partition_violation_downgrades_result(self) -> None:
        # Structurally conforming, but the composite document has three entries
        # and the entities only account for two of them.
        result_json = self._result_json(
            self._entity(1), self._entity(2, assignment_name="Laundry")
        )
        validation, _ = self._validate_grounded(
            result_json, expected_entry_nums={1, 2, 3}
        )

        self.assertFalse(validation.passed_validation)
        self.assertIsNone(validation.validated_output)
        self.assertEqual(
            LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT,
            validation.result_type_override,
        )
        self.assertTrue(validation.will_retry)
        [issue] = validation.audit_issues
        self.assertEqual(
            ValidationCheckType.ENTRY_PARTITION_VIOLATION, issue.check_type
        )
        self.assertEqual(ENTITIES_FIELD_NAME, issue.field_name)

    def test_duplicate_entity_downgrades_result(self) -> None:
        # Structurally conforming and an exact entry partition, but the two
        # entities carry identical canonical entity-field values — the model
        # split one real-world assignment across two elements.
        result_json = self._result_json(
            self._entity(1, entry_nums=[1]),
            self._entity(2, entry_nums=[2]),
        )
        validation, _ = self._validate_grounded(result_json, expected_entry_nums={1, 2})

        self.assertFalse(validation.passed_validation)
        self.assertIsNone(validation.validated_output)
        self.assertEqual(
            LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT,
            validation.result_type_override,
        )
        self.assertTrue(validation.will_retry)
        [issue] = validation.audit_issues
        self.assertEqual(ValidationCheckType.DUPLICATE_ENTITY, issue.check_type)
        self.assertEqual(ENTITIES_FIELD_NAME, issue.field_name)

    def test_missing_expected_entry_nums_rejected(self) -> None:
        # An ER extractor's partition check is meaningless without the composite
        # document's entry set, and every composite document has at least one
        # entry — so both an omitted and an empty entry set are caller bugs, not
        # model errors.
        result_json = self._result_json(self._entity(1))
        empty_cases: tuple[set[int] | None, ...] = (None, set())
        for empty_entry_nums in empty_cases:
            with self.subTest(expected_entry_nums=empty_entry_nums):
                with self.assertRaisesRegex(
                    ValueError,
                    r"^Extractor \[US_XX_FAKE_EXTRACTOR_COLLECTION_ASSIGNMENT"
                    r"_ENTITY_RESOLUTION\] is an entity-resolution extractor, but "
                    r"validation received expected entry nums \[(None|set\(\))\]\.",
                ):
                    self._validate(
                        result_json,
                        source_document_text="[Entry 1] text",
                        expected_entry_nums=empty_entry_nums,
                    )


class LLMAllNullEntityValidatorWiringTest(_ValidatorTestBase):
    """Wires AllNullEntityCheck through the validator against the ER collection
    synthesized from the fake collection's `location` entity group. Its single
    entity field is optional, so an all-null entity is structurally conformant —
    under the `assignment` group the required `assignment_name` would fail
    structural conformance first, and the check would never be reached.
    """

    def setUp(self) -> None:
        super().setUp()
        self.enterContext(patch_fake_entity_resolution_model_config_name())
        self.config = fake_entity_resolution_extractor_config(_LOCATION_GROUP_NAME)

    def test_all_null_entity_downgrades_result(self) -> None:
        result_json = build_fake_entity_resolution_result_content(
            [
                build_fake_entity_resolution_entity_result_json(
                    1, entry_nums=[1], location=None
                )
            ]
        )
        validation, _ = self._validate_grounded(result_json, expected_entry_nums={1})

        self.assertFalse(validation.passed_validation)
        self.assertIsNone(validation.validated_output)
        self.assertEqual(
            LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT,
            validation.result_type_override,
        )
        self.assertTrue(validation.will_retry)
        [issue] = validation.audit_issues
        self.assertEqual(ValidationCheckType.ALL_NULL_ENTITY, issue.check_type)
        self.assertEqual("entities[0]", issue.field_name)
