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
"""Processes one raw LLM extraction result into an `LLMJobDocumentExtractionResult`:
classify the LLM call, for a SUCCESS run the validator and fold its result in, and
escalate a transient failure to retries-exhausted once the retry budget is spent.
"""

import datetime

from recidiviz.common.constants.operations.llm_extraction_job import (
    LLMDocumentExtractionErrorType,
    LLMExtractionJobDocumentResultType,
)
from recidiviz.documents.extraction.llm_client.types import (
    LLMClientDocumentExtractionResult,
    LLMRequestErrorType,
)
from recidiviz.documents.extraction.llm_extraction_job_manager import (
    LLMJobDocumentExtractionResult,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.documents.extraction.validation.llm_extraction_result_validator import (
    LLMExtractionResultValidator,
)


def result_type_for_request_error(
    error_type: LLMRequestErrorType,
) -> LLMExtractionJobDocumentResultType:
    """Returns the document-level classification for an LLM-request-phase error.
    Transient errors (provider rate-limiting, timeouts, and 5xx server errors)
    leave the document eligible for a future job; every other error is permanent.

    Enumerates every member so adding a new LLMRequestErrorType without classifying
    it here fails loudly.
    """
    if error_type is LLMRequestErrorType.MALFORMED_RESPONSE:
        return LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_PERMANENT
    if error_type is LLMRequestErrorType.EMPTY_RESPONSE:
        return LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_PERMANENT
    if error_type is LLMRequestErrorType.CONTENT_FILTERED:
        return LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_PERMANENT
    if error_type is LLMRequestErrorType.TIMEOUT:
        return LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT
    if error_type is LLMRequestErrorType.RATE_LIMITED:
        return LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT
    if error_type is LLMRequestErrorType.SERVER_ERROR:
        return LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT
    if error_type is LLMRequestErrorType.UNKNOWN_ERROR:
        return LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_PERMANENT
    raise ValueError(f"Unexpected error_type: [{error_type}]")


def error_type_for_request_error(
    error_type: LLMRequestErrorType,
) -> LLMDocumentExtractionErrorType:
    """Returns the persisted per-document error category for an LLM-request-phase
    error.

    Enumerates every member so adding a new LLMRequestErrorType without mapping it
    here fails loudly.
    """
    if error_type is LLMRequestErrorType.MALFORMED_RESPONSE:
        return LLMDocumentExtractionErrorType.LLM_REQUEST_MALFORMED_RESPONSE
    if error_type is LLMRequestErrorType.EMPTY_RESPONSE:
        return LLMDocumentExtractionErrorType.LLM_REQUEST_EMPTY_RESPONSE
    if error_type is LLMRequestErrorType.CONTENT_FILTERED:
        return LLMDocumentExtractionErrorType.LLM_REQUEST_CONTENT_FILTERED
    if error_type is LLMRequestErrorType.TIMEOUT:
        return LLMDocumentExtractionErrorType.LLM_REQUEST_TIMEOUT
    if error_type is LLMRequestErrorType.RATE_LIMITED:
        return LLMDocumentExtractionErrorType.LLM_REQUEST_RATE_LIMITED
    if error_type is LLMRequestErrorType.SERVER_ERROR:
        return LLMDocumentExtractionErrorType.LLM_REQUEST_SERVER_ERROR
    if error_type is LLMRequestErrorType.UNKNOWN_ERROR:
        return LLMDocumentExtractionErrorType.LLM_REQUEST_UNKNOWN_ERROR
    raise ValueError(f"Unexpected error_type: [{error_type}]")


class LLMExtractionResultProcessor:
    """Processes one raw result into an `LLMJobDocumentExtractionResult`: classify
    the LLM call, for a SUCCESS run the validator and fold its result in, and
    escalate a transient failure to retries-exhausted once the retry budget is
    spent.
    """

    def __init__(self, *, validator: LLMExtractionResultValidator) -> None:
        # Runs the structural/quality checks over a raw SUCCESS result.
        self._validator = validator

    def validate_and_classify(
        self,
        *,
        config: LLMExtractorConfig,
        raw_result: LLMClientDocumentExtractionResult,
        # Stamped onto the result: the pipeline's real job id, or golden eval's
        # synthetic per-run id.
        job_id: str,
        source_document_text: str,
        # How many times this document has already failed transiently across prior
        # completed jobs (0 on its first attempt). Determines whether a transient
        # failure here escalates to retries-exhausted.
        prior_transient_failure_count: int,
    ) -> LLMJobDocumentExtractionResult:
        """Returns the full processed outcome for one raw extraction result:
        the raw call classified into a per-document result type, the validation
        outcome folded in for a SUCCESS, and a transient failure escalated to
        retries-exhausted once the retry budget is spent.
        """
        # One timestamp for both the result and its validation, so Postgres and BQ
        # share it.
        now = datetime.datetime.now(tz=datetime.UTC)

        if raw_result.error_type is not None:
            return LLMJobDocumentExtractionResult.for_failed_request(
                job_id=job_id,
                raw_result=raw_result,
                result_datetime_utc=now,
                result_type=self._effective_result_type(
                    classified_result_type=result_type_for_request_error(
                        raw_result.error_type
                    ),
                    config=config,
                    prior_transient_failure_count=prior_transient_failure_count,
                ),
                error_type=error_type_for_request_error(raw_result.error_type),
            )

        validation_results = self._validator.validate(
            config=config,
            raw_result=raw_result,
            source_document_text=source_document_text,
            validation_datetime_utc=now,
        )

        if not validation_results.passed_validation:
            return LLMJobDocumentExtractionResult.for_failed_validation(
                job_id=job_id,
                raw_result=raw_result,
                result_datetime_utc=now,
                result_type=self._effective_result_type(
                    classified_result_type=validation_results.result_type,
                    config=config,
                    prior_transient_failure_count=prior_transient_failure_count,
                ),
                validation_results=validation_results,
            )

        return LLMJobDocumentExtractionResult.for_success(
            job_id=job_id,
            raw_result=raw_result,
            result_datetime_utc=now,
            validation_results=validation_results,
        )

    @staticmethod
    def _effective_result_type(
        *,
        classified_result_type: LLMExtractionJobDocumentResultType,
        config: LLMExtractorConfig,
        prior_transient_failure_count: int,
    ) -> LLMExtractionJobDocumentResultType:
        """Returns the classification a document lands on once the retry budget is
        applied: a transient failure whose document has already exhausted its
        transient-retry budget escalates to DOCUMENT_LEVEL_FAILURE_RETRIES_EXHAUSTED
        so job selection treats it as terminal instead of retrying it forever; every
        other classification is returned unchanged.
        """
        if (
            classified_result_type
            is LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT
            and prior_transient_failure_count >= config.max_transient_retry_count
        ):
            return (
                LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_RETRIES_EXHAUSTED
            )
        return classified_result_type
