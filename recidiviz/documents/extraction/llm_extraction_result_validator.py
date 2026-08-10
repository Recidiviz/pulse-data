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

Today it runs a single check — structural conformance against the JSON Schema
generated from the extractor's output schema. That one check covers structure,
required fields, enum membership, the `value` / `null_reason` split (via the
per-field `anyOf` branches: a field must supply a complete value branch or a
complete null branch), `is_relevant` branch selection, and citation `minItems: 1`,
so none of those need bespoke checks. The semantic-consistency, citation-grounding,
and quality-filter checks are layered on when full validation lands.
"""

import datetime

from jsonschema.exceptions import ValidationError

from recidiviz.common.constants.operations.llm_extraction_job import (
    LLMExtractionJobDocumentResultType,
)
from recidiviz.documents.extraction.llm_client.types import (
    LLMClientDocumentExtractionResult,
)
from recidiviz.documents.extraction.llm_document_validation_result import (
    LLMDocumentValidationResult,
    ValidationCheckType,
    ValidationIssue,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.documents.extraction.models.llm_json_schema_generator import (
    LLMJsonSchemaGenerator,
)
from recidiviz.documents.extraction.models.llm_request_output_values import (
    LLMRequestOutputValues,
)
from recidiviz.utils.types import assert_type
from recidiviz.utils.validate_json_schema import iter_leaf_validation_errors

# The json_path of a jsonschema error is rooted at this JSONPath prefix.
_JSON_PATH_ROOT = "$"


class StructuralConformanceCheck:
    """The structural conformance check: validates a raw result against the JSON
    Schema generated from the extractor's output schema, one `ValidationIssue` per
    way it fails to conform.
    """

    @classmethod
    def issues(cls, *, output: LLMRequestOutputValues) -> list[ValidationIssue]:
        """Returns one `ValidationIssue` per way |result|'s raw JSON fails to
        conform to the JSON Schema generated from its output schema, or an empty
        list when it conforms.

        The generated schema unions branches with `anyOf` in two places — the
        top-level irrelevant/relevant split of a relevance-bearing schema, and
        each INFERRED field's value/null split — and a raw `jsonschema` error on
        an unmatched union is one opaque failure on the whole union, losing the
        offending field. `iter_leaf_validation_errors` resolves each union error
        to the branch the result plausibly intended and surfaces that branch's
        failures at their exact fields, with no knowledge of the schema's
        structure needed here.
        """
        output_json = assert_type(output.output_json, dict)
        json_schema = LLMJsonSchemaGenerator.generate(output.output_schema)
        return [
            ValidationIssue(
                check_type=ValidationCheckType.SCHEMA_CONFORMANCE,
                field_name=cls._field_name_for_error(error),
                detail=error.message,
            )
            for error in iter_leaf_validation_errors(output_json, json_schema)
        ]

    @staticmethod
    def _field_name_for_error(error: ValidationError) -> str | None:
        """Returns the dotted/bracketed name of the field |error| occurred on —
        e.g. `result.assignments[0].assignment_name` — or None for a failure on
        the root object as a whole. This is the error's JSONPath with the `$`
        root prefix stripped: a relevance-bearing schema's paths start at its
        `result` wrapper key; a relevance-free schema's result has no such
        wrapper, so its paths are rooted at the extracted fields directly.
        """
        if (json_path := error.json_path) == _JSON_PATH_ROOT:
            return None
        return json_path.removeprefix(f"{_JSON_PATH_ROOT}.")


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

        structural_violations = StructuralConformanceCheck.issues(output=raw_output)

        if structural_violations:
            validated_content = LLMRequestOutputValues(
                output_schema=output_schema, output_json=None
            )
            result_type_override = (
                LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT
            )
        else:
            validated_content = raw_output
            result_type_override = None

        return LLMDocumentValidationResult(
            validation_config_version_id=config.extractor_collection.validation_config_version_id,
            validation_datetime_utc=validation_datetime_utc,
            validated_content=validated_content,
            audit_issues=structural_violations,
            result_type_override=result_type_override,
        )
