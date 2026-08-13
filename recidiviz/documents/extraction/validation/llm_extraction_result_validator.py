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
"""Validator that decides whether one raw LLM extraction result is usable,
returning an `LLMDocumentValidationResult`.

It runs the checks in three stages, because each stage depends on the one before
it holding:

1. **Structural conformance** against the JSON Schema generated from the
   extractor's output schema. Nothing downstream can read a result the schema
   rejects, so a structural failure short-circuits the rest.
2. **The remaining extraction-error checks.** Each reads values through the
   output schema, which is only safe once the result is structurally conformant.
   Any finding here means the model violated its instructions and we should retry the
   document with another LLM call.
3. **The quality adjustments**, each returning an adjusted copy of the output it
   is given, chained to produce the validated output and leaving the raw output
   the extractor returned untouched. Findings here do not trigger a retry. For an
   entity-resolution extractor, the ER-only checks run in this stage too,
   validating the clustering result against the composite document's complete
   entry set.
"""

import datetime

from recidiviz.common.constants.operations.llm_extraction_job import (
    LLMExtractionJobDocumentResultType,
)
from recidiviz.documents.extraction.llm_client.types import (
    LLMClientDocumentExtractionResult,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.documents.extraction.models.llm_request_output_values import (
    LLMRequestOutputValues,
)
from recidiviz.documents.extraction.validation.adversarial_interpretation_consistency_check import (
    AdversarialInterpretationConsistencyCheck,
)
from recidiviz.documents.extraction.validation.all_null_entity_check import (
    AllNullEntityCheck,
)
from recidiviz.documents.extraction.validation.citation_grounding_check import (
    CitationGroundingCheck,
)
from recidiviz.documents.extraction.validation.citation_matching import (
    SourceDocumentText,
)
from recidiviz.documents.extraction.validation.citation_offset_drift_adjustment import (
    CitationOffsetDriftAdjustment,
)
from recidiviz.documents.extraction.validation.confidence_threshold_adjustment import (
    ConfidenceThresholdAdjustment,
)
from recidiviz.documents.extraction.validation.duplicate_entity_check import (
    DuplicateEntityCheck,
)
from recidiviz.documents.extraction.validation.entry_partition_check import (
    EntryPartitionCheck,
)
from recidiviz.documents.extraction.validation.llm_document_validation_result import (
    LLMDocumentValidationResult,
    ValidationIssue,
)
from recidiviz.documents.extraction.validation.relevant_but_all_null_check import (
    RelevantButAllNullCheck,
)
from recidiviz.documents.extraction.validation.required_field_confidence_check import (
    RequiredFieldConfidenceCheck,
)
from recidiviz.documents.extraction.validation.semantic_consistency_check import (
    SemanticConsistencyCheck,
)
from recidiviz.documents.extraction.validation.structural_conformance_check import (
    StructuralConformanceCheck,
)
from recidiviz.utils.types import assert_type


class LLMExtractionResultValidator:
    """Validator that decides whether one raw LLM extraction result is usable,
    returning an `LLMDocumentValidationResult`.
    """

    def validate(
        self,
        *,
        config: LLMExtractorConfig,
        raw_result: LLMClientDocumentExtractionResult,
        source_document_text: str,
        # The complete entry set E = {1..N} of the composite document the result
        # was extracted from, read from the entry→source map table. Required for
        # an entity-resolution extractor (the entry-partition check validates the
        # result against it); must be None for a first-order extractor, whose
        # documents have no numbered entries.
        expected_entry_nums: set[int] | None,
        validation_datetime_utc: datetime.datetime,
    ) -> LLMDocumentValidationResult:
        """Returns the validation outcome for |raw_result| against |config|,
        matching its citations against |source_document_text| — the text of the
        document the result was extracted from, exactly as the model saw it.
        """
        self._validate_expected_entry_nums_pairing(
            config=config, expected_entry_nums=expected_entry_nums
        )
        output_schema = config.extractor_collection.output_schema
        raw_output = LLMRequestOutputValues(
            output_schema=output_schema,
            output_json=assert_type(raw_result.result_json, dict),
        )

        document_text = SourceDocumentText(text=source_document_text)

        extraction_error_issues = self._extraction_error_issues(
            raw_output=raw_output,
            document_text=document_text,
            expected_entry_nums=expected_entry_nums,
        )
        if extraction_error_issues:
            return self._result(
                config=config,
                validation_datetime_utc=validation_datetime_utc,
                validated_output=None,
                audit_issues=extraction_error_issues,
                result_type_override=(
                    LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT
                ),
            )
        adjustment_issues: list[ValidationIssue] = []
        validated_output, new_issues = ConfidenceThresholdAdjustment.apply(
            output=raw_output
        )
        adjustment_issues.extend(new_issues)

        validated_output, citation_drift_issues = CitationOffsetDriftAdjustment.apply(
            output=validated_output, source_document_text=document_text
        )
        adjustment_issues.extend(citation_drift_issues)

        return self._result(
            config=config,
            validation_datetime_utc=validation_datetime_utc,
            validated_output=validated_output,
            audit_issues=adjustment_issues,
            result_type_override=None,
        )

    @staticmethod
    def _validate_expected_entry_nums_pairing(
        *, config: LLMExtractorConfig, expected_entry_nums: set[int] | None
    ) -> None:
        """Validates that |expected_entry_nums| is supplied exactly when |config|
        is an entity-resolution extractor: a composite document always has at
        least one numbered entry, and a first-order document has none.
        """
        if config.entity_group is None:
            if expected_entry_nums is not None:
                raise ValueError(
                    f"Extractor [{config.extractor_id}] is first-order, but "
                    f"validation received expected entry nums "
                    f"[{sorted(expected_entry_nums)}]. Only an entity-resolution "
                    f"extractor's composite documents have numbered entries."
                )
            return
        if not expected_entry_nums:
            raise ValueError(
                f"Extractor [{config.extractor_id}] is an entity-resolution "
                f"extractor, but validation received expected entry nums "
                f"[{expected_entry_nums}]. Every composite document has at least "
                f"one numbered entry; read the entry set from the entry→source "
                f"map table."
            )

    def _extraction_error_issues(
        self,
        *,
        raw_output: LLMRequestOutputValues,
        document_text: SourceDocumentText,
        expected_entry_nums: set[int] | None,
    ) -> list[ValidationIssue]:
        """Returns every extraction-error finding against |raw_output|, or an
        empty list when it clears them all.

        Structural conformance runs alone first: every other check reads values
        through the output schema, which a result the schema rejects cannot be
        read by at all. The entity-resolution-only checks run for exactly the
        results that have |expected_entry_nums| — the pairing check in `validate`
        guarantees those are the entity-resolution extractors' results.
        """
        structural_issues = StructuralConformanceCheck.issues(output=raw_output)
        if structural_issues:
            return structural_issues
        issues = [
            *SemanticConsistencyCheck.issues(output=raw_output),
            *RequiredFieldConfidenceCheck.issues(output=raw_output),
            *RelevantButAllNullCheck.issues(output=raw_output),
            *AdversarialInterpretationConsistencyCheck.issues(output=raw_output),
            *CitationGroundingCheck.issues(
                output=raw_output, source_document_text=document_text
            ),
        ]
        if expected_entry_nums is not None:
            issues.extend(
                self._entity_resolution_error_issues(
                    raw_output=raw_output, expected_entry_nums=expected_entry_nums
                )
            )
        return issues

    @staticmethod
    def _entity_resolution_error_issues(
        *,
        raw_output: LLMRequestOutputValues,
        expected_entry_nums: set[int],
    ) -> list[ValidationIssue]:
        """Returns every finding from the entity-resolution-only extraction-error
        checks — the checks over the resolved entities that only make sense for a
        clustering result, validating it against the composite document's
        complete entry set.
        """
        return [
            *EntryPartitionCheck.issues(
                output=raw_output, expected_entry_nums=expected_entry_nums
            ),
            *DuplicateEntityCheck.issues(output=raw_output),
            *AllNullEntityCheck.issues(output=raw_output),
        ]

    @staticmethod
    def _result(
        *,
        config: LLMExtractorConfig,
        validation_datetime_utc: datetime.datetime,
        validated_output: LLMRequestOutputValues | None,
        audit_issues: list[ValidationIssue],
        result_type_override: LLMExtractionJobDocumentResultType | None,
    ) -> LLMDocumentValidationResult:
        """Returns the validation result stamped with the config version and time
        validation ran."""
        return LLMDocumentValidationResult(
            validation_config_version_id=(
                config.extractor_collection.validation_config_version_id
            ),
            validation_datetime_utc=validation_datetime_utc,
            validated_output=validated_output,
            audit_issues=audit_issues,
            result_type_override=result_type_override,
        )
