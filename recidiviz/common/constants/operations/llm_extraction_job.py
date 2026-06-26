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
"""Constants related to the LLM extraction job operations tables."""
from enum import unique

import recidiviz.common.constants.operations.enum_canonical_strings as operations_enum_strings
from recidiviz.common.constants.operations.operations_enum import OperationsEnum


@unique
class LLMExtractionJobResultType(OperationsEnum):
    """The terminal result of an LLM extraction job."""

    SUCCESS = operations_enum_strings.llm_extraction_job_result_type_success
    PARTIAL_FAILURE = (
        operations_enum_strings.llm_extraction_job_result_type_partial_failure
    )
    FAILURE = operations_enum_strings.llm_extraction_job_result_type_failure

    @classmethod
    def get_enum_description(cls) -> str:
        return "The terminal result of an LLM extraction job."

    @classmethod
    def get_value_descriptions(cls) -> dict["OperationsEnum", str]:
        return _LLM_EXTRACTION_JOB_RESULT_TYPE_VALUE_DESCRIPTIONS


_LLM_EXTRACTION_JOB_RESULT_TYPE_VALUE_DESCRIPTIONS: dict[OperationsEnum, str] = {
    LLMExtractionJobResultType.SUCCESS: (
        "All documents in the job completed extraction successfully."
    ),
    LLMExtractionJobResultType.PARTIAL_FAILURE: (
        "The job completed, but at least one document failed at the document level "
        "while others succeeded."
    ),
    LLMExtractionJobResultType.FAILURE: (
        "The job failed at the job level — the job itself errored before any document "
        "could be successfully processed."
    ),
}


@unique
class LLMExtractionJobDocumentResultType(OperationsEnum):
    """The result of LLM extraction for a single document within a job."""

    SUCCESS = operations_enum_strings.llm_extraction_job_document_result_type_success
    JOB_LEVEL_FAILURE = (
        operations_enum_strings.llm_extraction_job_document_result_type_job_level_failure
    )
    DOCUMENT_LEVEL_FAILURE_TRANSIENT = (
        operations_enum_strings.llm_extraction_job_document_result_type_document_level_failure_transient
    )
    DOCUMENT_LEVEL_FAILURE_PERMANENT = (
        operations_enum_strings.llm_extraction_job_document_result_type_document_level_failure_permanent
    )
    DOCUMENT_LEVEL_FAILURE_RETRIES_EXHAUSTED = (
        operations_enum_strings.llm_extraction_job_document_result_type_document_level_failure_retries_exhausted
    )

    @classmethod
    def get_enum_description(cls) -> str:
        return "The result of LLM extraction for a single document within a job."

    @classmethod
    def get_value_descriptions(cls) -> dict["OperationsEnum", str]:
        return _LLM_EXTRACTION_JOB_DOCUMENT_RESULT_TYPE_VALUE_DESCRIPTIONS


_LLM_EXTRACTION_JOB_DOCUMENT_RESULT_TYPE_VALUE_DESCRIPTIONS: dict[
    OperationsEnum, str
] = {
    LLMExtractionJobDocumentResultType.SUCCESS: (
        "Extraction completed successfully for this document and a valid result was "
        "written."
    ),
    LLMExtractionJobDocumentResultType.JOB_LEVEL_FAILURE: (
        "The document inherited a job-level failure — the enclosing job errored "
        "before this document could be processed."
    ),
    LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT: (
        "Extraction failed for this document with a transient error (e.g. provider "
        "rate-limiting or a recoverable network issue) and will be retried in a "
        "future job."
    ),
    LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_PERMANENT: (
        "Extraction failed for this document with a permanent error (e.g. malformed "
        "input, schema-level validation failure) and will not be retried."
    ),
    LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_RETRIES_EXHAUSTED: (
        "Extraction failed for this document after the retry budget was exhausted "
        "and will not be retried."
    ),
}
