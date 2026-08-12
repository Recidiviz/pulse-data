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
        # Unused by the structural check, but in the signature now because the
        # later semantic citation-grounding and offset-drift checks need it.
        # TODO(OBT-32169): Remove the leading underscore once a check uses it.
        _source_document_text: str,
        validation_datetime_utc: datetime.datetime,
    ) -> LLMDocumentValidationResult:
        """Returns the validation outcome for |raw_result| against |config|."""
        output_schema = config.extractor_collection.output_schema
        raw_output = LLMRequestOutputValues(
            output_schema=output_schema,
            output_json=assert_type(raw_result.result_json, dict),
        )

        issues = self._extraction_error_issues(raw_output=raw_output)

        validated_output: LLMRequestOutputValues | None
        result_type_override: LLMExtractionJobDocumentResultType | None
        if issues:
            validated_output = None
            result_type_override = (
                LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT
            )
        else:
            validated_output = raw_output
            result_type_override = None

        return LLMDocumentValidationResult(
            validation_config_version_id=config.extractor_collection.validation_config_version_id,
            validation_datetime_utc=validation_datetime_utc,
            validated_output=validated_output,
            audit_issues=issues,
            result_type_override=result_type_override,
        )

    @staticmethod
    def _extraction_error_issues(
        *, raw_output: LLMRequestOutputValues
    ) -> list[ValidationIssue]:
        """Returns every extraction-error finding against |raw_output|, or an
        empty list when it clears them all.

        Structural conformance runs alone first: every other check reads values
        through the output schema, which a result the schema rejects cannot be
        read by at all.
        """
        structural_issues = StructuralConformanceCheck.issues(output=raw_output)
        if structural_issues:
            return structural_issues
        return [
            *SemanticConsistencyCheck.issues(output=raw_output),
            *RequiredFieldConfidenceCheck.issues(output=raw_output),
            *RelevantButAllNullCheck.issues(output=raw_output),
        ]
