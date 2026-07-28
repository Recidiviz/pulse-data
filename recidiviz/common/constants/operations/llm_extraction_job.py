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

    @classmethod
    def is_success_result_type(
        cls, result_type: "LLMExtractionJobDocumentResultType"
    ) -> bool:
        """Returns whether |result_type| represents a successful extraction.

        Enumerates every member so adding a new one without classifying it here
        fails loudly.
        """
        if result_type is cls.SUCCESS:
            return True
        if result_type is cls.JOB_LEVEL_FAILURE:
            return False
        if result_type is cls.DOCUMENT_LEVEL_FAILURE_TRANSIENT:
            return False
        if result_type is cls.DOCUMENT_LEVEL_FAILURE_PERMANENT:
            return False
        if result_type is cls.DOCUMENT_LEVEL_FAILURE_RETRIES_EXHAUSTED:
            return False
        raise ValueError(f"Unexpected result_type: [{result_type}]")

    @classmethod
    def is_terminal_result_type(
        cls, result_type: "LLMExtractionJobDocumentResultType"
    ) -> bool:
        """Returns whether |result_type| permanently removes a document from job
        selection for an extractor version — a document with a terminal result
        is "done" and is not re-selected.

        Enumerates every member so adding a new one without classifying it here
        fails loudly.
        """
        if result_type is cls.SUCCESS:
            return True
        if result_type is cls.DOCUMENT_LEVEL_FAILURE_PERMANENT:
            return True
        if result_type is cls.DOCUMENT_LEVEL_FAILURE_RETRIES_EXHAUSTED:
            return True
        if result_type is cls.JOB_LEVEL_FAILURE:
            return False
        if result_type is cls.DOCUMENT_LEVEL_FAILURE_TRANSIENT:
            return False
        raise ValueError(f"Unexpected result_type: [{result_type}]")


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


@unique
class LLMDocumentExtractionErrorType(OperationsEnum):
    """The per-document error category persisted with a job-document result.

    Holds a superset of the LLMRequestErrorType values (the errors surfaced by
    the LLM client while executing a single request); enum values for errors
    encountered later, during validation, are added when validation lands.
    """

    LLM_REQUEST_MALFORMED_RESPONSE = (
        operations_enum_strings.llm_document_extraction_error_type_llm_request_malformed_response
    )
    LLM_REQUEST_EMPTY_RESPONSE = (
        operations_enum_strings.llm_document_extraction_error_type_llm_request_empty_response
    )
    LLM_REQUEST_CONTENT_FILTERED = (
        operations_enum_strings.llm_document_extraction_error_type_llm_request_content_filtered
    )
    LLM_REQUEST_TIMEOUT = (
        operations_enum_strings.llm_document_extraction_error_type_llm_request_timeout
    )
    LLM_REQUEST_RATE_LIMITED = (
        operations_enum_strings.llm_document_extraction_error_type_llm_request_rate_limited
    )
    LLM_REQUEST_SERVER_ERROR = (
        operations_enum_strings.llm_document_extraction_error_type_llm_request_server_error
    )
    LLM_REQUEST_UNKNOWN_ERROR = (
        operations_enum_strings.llm_document_extraction_error_type_llm_request_unknown_error
    )

    @classmethod
    def get_enum_description(cls) -> str:
        return "The per-document error category persisted with a job-document result."

    @classmethod
    def get_value_descriptions(cls) -> dict["OperationsEnum", str]:
        return {
            LLMDocumentExtractionErrorType.LLM_REQUEST_MALFORMED_RESPONSE: (
                "The LLM returned a response that could not be parsed into the expected "
                "structured output."
            ),
            LLMDocumentExtractionErrorType.LLM_REQUEST_EMPTY_RESPONSE: (
                "The LLM returned no content."
            ),
            LLMDocumentExtractionErrorType.LLM_REQUEST_CONTENT_FILTERED: (
                "The LLM blocked the request or response with a safety filter."
            ),
            LLMDocumentExtractionErrorType.LLM_REQUEST_TIMEOUT: (
                "The LLM request timed out before a response was received."
            ),
            LLMDocumentExtractionErrorType.LLM_REQUEST_RATE_LIMITED: (
                "The LLM request was rejected because the provider rate limit was exceeded."
            ),
            LLMDocumentExtractionErrorType.LLM_REQUEST_SERVER_ERROR: (
                "The LLM request failed with a server-side (5xx) error from the provider."
            ),
            LLMDocumentExtractionErrorType.LLM_REQUEST_UNKNOWN_ERROR: (
                "The LLM request failed for a reason that did not fall into any other "
                "category."
            ),
        }
