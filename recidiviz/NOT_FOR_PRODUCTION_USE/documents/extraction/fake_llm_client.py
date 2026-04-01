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
"""Fake LLM client for testing document extraction."""
import uuid
from typing import Any

from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.extraction_output_schema import (
    ExtractionFieldType,
    ExtractionInferredField,
    ExtractionOutputSchema,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.llm_client import (
    LLMClient,
    LLMExtractionBatchResult,
    LLMExtractionInProgressBatchStatus,
    LLMExtractionRequest,
    LLMExtractionResult,
    LLMExtractionStatus,
    LLMResultReader,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.extraction_exclusion_type import (
    ExtractionExclusionType,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.extraction_job_metadata import (
    ExtractionJobMetadata,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.extraction_job_submitted_document_metadata import (
    ExtractionJobSubmittedDocumentMetadata,
)


class FakeLLMClient(LLMClient, LLMResultReader):
    """A fake LLM client that generates deterministic results for testing.

    Generates fake results on-the-fly based on submitted_documents when
    get_results() is called. Stores output schema per job_id to enable
    fake result generation.

    Implements both LLMClient and LLMResultReader for convenience.
    """

    def __init__(self) -> None:
        # Store output schema per job_id (needed for fake generation)
        self._output_schemas: dict[str, ExtractionOutputSchema] = {}

    def submit_batch(self, requests: list[LLMExtractionRequest]) -> str:
        """Submits requests and stores schema for later fake result generation.

        Returns the job_id for tracking.
        """
        job_id = str(uuid.uuid4())
        if requests:
            self._output_schemas[job_id] = requests[0].output_schema
        return job_id

    def get_results(
        self,
        extraction_job: ExtractionJobMetadata,
        submitted_documents: list[ExtractionJobSubmittedDocumentMetadata],
    ) -> LLMExtractionBatchResult | LLMExtractionInProgressBatchStatus:
        """Generates fake results for each submitted document (always returns completed).

        The first document always produces a failure result to exercise error
        handling paths in tests.
        """
        job_id = extraction_job.job_id
        schema = self._output_schemas.get(job_id)

        results: list[LLMExtractionResult] = []
        for idx, doc in enumerate(submitted_documents):
            if idx == 0:
                results.append(
                    LLMExtractionResult(
                        document_id=doc.document_id,
                        status=LLMExtractionStatus.PERMANENT_FAILURE,
                        extracted_data=None,
                        error_message="Fake extraction failure for testing",
                        error_type=ExtractionExclusionType.LLM_UNKNOWN_ERROR,
                    )
                )
            else:
                results.append(self._generate_fake_result(doc.document_id, schema))

        return LLMExtractionBatchResult(
            job_id=job_id,
            results=results,
            batch_error_type=None,
            batch_error_message=None,
        )

    def _generate_fake_result(
        self, document_id: str, schema: ExtractionOutputSchema | None
    ) -> LLMExtractionResult:
        """Generates a fake extraction result for testing purposes.

        Inferred fields are nested objects with value, null_reason, confidence_score, and citations.
        ARRAY_OF_STRUCT fields produce an array with one struct item whose sub-fields
        each have the standard {value, null_reason, confidence_score, citations} wrapper.
        """
        if schema is None:
            return LLMExtractionResult(
                document_id=document_id,
                status=LLMExtractionStatus.SUCCESS,
                extracted_data={},
                error_message=None,
                error_type=None,
            )

        result: dict[str, Any] = {}

        # Use document_id length as a deterministic seed for generating values
        seed = len(document_id)

        # Fill in inferred fields with nested structure
        for field in schema.inferred_fields:
            if field.field_type == ExtractionFieldType.ARRAY_OF_STRUCT:
                assert field.struct_fields is not None
                struct_item: dict[str, Any] = {}
                for sf in field.struct_fields:
                    struct_item[sf.name] = self._generate_fake_field_value(sf, seed)
                result[field.name] = [struct_item]
            else:
                result[field.name] = self._generate_fake_field_value(field, seed)

        return LLMExtractionResult(
            document_id=document_id,
            status=LLMExtractionStatus.SUCCESS,
            extracted_data=result,
            error_message=None,
            error_type=None,
        )

    @staticmethod
    def _generate_fake_field_value(
        field: ExtractionInferredField, seed: int
    ) -> dict[str, Any]:
        """Generates a fake {value, null_reason, confidence_score, citations} object."""
        if field.field_type == ExtractionFieldType.BOOLEAN:
            value: Any = seed % 2 == 0
        elif field.field_type == ExtractionFieldType.ENUM:
            assert field.enum_values is not None
            idx = seed % len(field.enum_values)
            value = field.enum_values[idx]
        elif field.field_type == ExtractionFieldType.STRING:
            if field.required:
                value = f"fake_{field.name}"
            else:
                value = None
        elif field.field_type == ExtractionFieldType.INTEGER:
            value = str(seed % 100)
        elif field.field_type == ExtractionFieldType.FLOAT:
            value = str(round(seed / 10.0, 2))
        else:
            value = None

        if value is not None:
            null_reason = None
            citations: list[dict[str, Any]] | None = [
                {
                    "text": f"fake citation for {field.name}",
                    "start": seed % 10,
                    "end": (seed % 10) + 20,
                }
            ]
        else:
            null_reason_options = ["not_applicable", "no_info_found"]
            null_reason = null_reason_options[seed % len(null_reason_options)]
            citations = None

        return {
            "value": value,
            "null_reason": null_reason,
            "confidence_score": round(0.5 + (seed % 50) / 100.0, 2),
            "citations": citations,
        }
