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
"""Tests for LLMExtractionResultProcessor."""

import datetime
from typing import Any
from unittest import TestCase

import pytz
from freezegun import freeze_time

from recidiviz.common.constants.operations.llm_extraction_job import (
    LLMDocumentExtractionErrorType,
    LLMExtractionJobDocumentResultType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.llm_client.types import (
    LLMClientDocumentExtractionResult,
    LLMDocumentExtractionTokenCounts,
    LLMRequestErrorType,
)
from recidiviz.documents.extraction.llm_extraction_result_processor import (
    LLMExtractionResultProcessor,
    error_type_for_request_error,
    result_type_for_request_error,
)
from recidiviz.documents.extraction.llm_extraction_result_validator import (
    LLMExtractionResultValidator,
)
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    get_first_order_llm_extractor_config,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    RESULT_KEY,
)
from recidiviz.tests.documents import fake_config
from recidiviz.tests.documents.extraction.fake_extractor_result_json import (
    fake_minimal_relevant_result_json,
)

_STATE_CODE = StateCode.US_XX
_COLLECTION_NAME = "FAKE_EXTRACTOR_COLLECTION"
_DOCUMENT_CONTENTS_ID = "doc1"
_JOB_ID = "job1"
_SOURCE_TEXT = "The record is active. Assigned to the kitchen."
_NOW = datetime.datetime(2026, 1, 1, 12, 0, tzinfo=pytz.UTC)
_TOKEN_COUNTS = LLMDocumentExtractionTokenCounts(
    input_token_count=100,
    output_token_count=20,
    cached_input_token_count=10,
    thinking_token_count=0,
)


@freeze_time(_NOW)
class LLMExtractionResultProcessorTest(TestCase):
    """Tests the classify-and-validate composition of LLMExtractionResultProcessor
    against the real validator and the fake extractor collection."""

    def setUp(self) -> None:
        self.config = get_first_order_llm_extractor_config(
            _STATE_CODE, _COLLECTION_NAME, config_module=fake_config
        )
        self.processor = LLMExtractionResultProcessor(
            validator=LLMExtractionResultValidator()
        )

    def build_fake_collection_relevant_result_json(self) -> dict[str, Any]:
        """Returns a relevant result conforming to FAKE_EXTRACTOR_COLLECTION's output
        schema."""
        return fake_minimal_relevant_result_json()

    def test_conforming_relevant_result_is_success(self) -> None:
        result_json = self.build_fake_collection_relevant_result_json()
        raw_result = LLMClientDocumentExtractionResult.from_success(
            document_contents_id=_DOCUMENT_CONTENTS_ID,
            result_json=result_json,
            token_counts=_TOKEN_COUNTS,
        )
        result = self.processor.validate_and_classify(
            config=self.config,
            raw_result=raw_result,
            job_id=_JOB_ID,
            source_document_text=_SOURCE_TEXT,
            prior_transient_failure_count=0,
        )

        self.assertEqual(LLMExtractionJobDocumentResultType.SUCCESS, result.result_type)
        self.assertTrue(result.is_relevant)
        self.assertIsNone(result.error_type)
        self.assertIsNone(result.error_message)
        assert result.validation_results is not None
        self.assertTrue(result.is_validated_result)
        validated_output = result.validation_results.validated_output
        assert validated_output is not None
        self.assertEqual(result_json, validated_output.output_json)
        self.assertEqual(_JOB_ID, result.job_id)
        self.assertEqual(_DOCUMENT_CONTENTS_ID, result.document_contents_id)
        self.assertEqual(_NOW, result.result_datetime_utc)
        self.assertEqual(_NOW, result.validation_results.validation_datetime_utc)

    def test_structurally_invalid_result_downgraded_to_transient(self) -> None:
        # A schema violation (a required field removed) is an extraction-error
        # check failure: the raw SUCCESS is downgraded to a transient failure with
        # no usable content and no persisted error category (validation errors have
        # no LLMDocumentExtractionErrorType value yet — the audit issues carry the
        # detail), so relevance is unknown.
        invalid_json = self.build_fake_collection_relevant_result_json()
        del invalid_json[RESULT_KEY]["primary_status"]

        raw_result = LLMClientDocumentExtractionResult.from_success(
            document_contents_id=_DOCUMENT_CONTENTS_ID,
            result_json=invalid_json,
            token_counts=_TOKEN_COUNTS,
        )
        result = self.processor.validate_and_classify(
            config=self.config,
            raw_result=raw_result,
            job_id=_JOB_ID,
            source_document_text=_SOURCE_TEXT,
            prior_transient_failure_count=0,
        )

        self.assertEqual(
            LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT,
            result.result_type,
        )
        self.assertIsNone(result.is_relevant)
        self.assertFalse(result.is_validated_result)
        self.assertIsNone(result.error_type)
        self.assertIsNone(result.error_message)
        # validation_results is still set (the raw call produced JSON), carrying
        # the override and the audit issues.
        assert result.validation_results is not None
        self.assertIsNone(result.validation_results.validated_output)
        self.assertTrue(result.validation_results.audit_issues)

    def test_every_request_error_type_is_classified_and_categorized(self) -> None:
        # Every LLMRequestErrorType must map to a result type and error category
        # without falling through to the exhaustiveness guard, so adding a new
        # member without classifying it fails here.
        for request_error_type in LLMRequestErrorType:
            with self.subTest(request_error_type=request_error_type):
                self.assertIsInstance(
                    result_type_for_request_error(request_error_type),
                    LLMExtractionJobDocumentResultType,
                )
                self.assertIsInstance(
                    error_type_for_request_error(request_error_type),
                    LLMDocumentExtractionErrorType,
                )

    def test_transient_failures_escalate_once_retry_budget_is_spent(self) -> None:
        # A transient failure stays TRANSIENT until the document's prior-transient
        # count reaches the config's retry budget, then escalates to
        # RETRIES_EXHAUSTED so job selection stops retrying it. Both transient
        # sources escalate: a transient request error (carrying its error category
        # and message) and a validation downgrade (carrying neither).
        budget = self.config.max_transient_retry_count
        self.assertGreater(budget, 0)

        transient_request = LLMClientDocumentExtractionResult.from_error(
            document_contents_id=_DOCUMENT_CONTENTS_ID,
            error_type=LLMRequestErrorType.TIMEOUT,
            error_message="failed: timeout",
            token_counts=_TOKEN_COUNTS,
        )
        downgraded_json = self.build_fake_collection_relevant_result_json()
        del downgraded_json[RESULT_KEY]["primary_status"]
        validation_downgrade = LLMClientDocumentExtractionResult.from_success(
            document_contents_id=_DOCUMENT_CONTENTS_ID,
            result_json=downgraded_json,
            token_counts=_TOKEN_COUNTS,
        )

        transient = LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT
        exhausted = (
            LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_RETRIES_EXHAUSTED
        )
        for raw_result in (transient_request, validation_downgrade):
            for prior_count, expected_result_type in (
                (budget - 1, transient),
                (budget, exhausted),
                (budget + 1, exhausted),
            ):
                with self.subTest(
                    error_type=raw_result.error_type, prior_count=prior_count
                ):
                    result = self.processor.validate_and_classify(
                        config=self.config,
                        raw_result=raw_result,
                        job_id=_JOB_ID,
                        source_document_text=_SOURCE_TEXT,
                        prior_transient_failure_count=prior_count,
                    )
                    self.assertEqual(expected_result_type, result.result_type)
                    # Escalation only re-stamps result_type; the request error's
                    # category/message and the downgrade's null error carry over.
                    if raw_result is transient_request:
                        self.assertEqual(
                            LLMDocumentExtractionErrorType.LLM_REQUEST_TIMEOUT,
                            result.error_type,
                        )
                        self.assertEqual("failed: timeout", result.error_message)
                    else:
                        self.assertIsNone(result.error_type)
                        self.assertIsNone(result.error_message)
                    self.assertIsNone(result.is_relevant)

    def test_permanent_and_success_results_never_escalate(self) -> None:
        # Only transient failures escalate: a permanent request error and a
        # successful result are unaffected even past the retry budget.
        beyond_budget = self.config.max_transient_retry_count + 1

        permanent = LLMClientDocumentExtractionResult.from_error(
            document_contents_id=_DOCUMENT_CONTENTS_ID,
            error_type=LLMRequestErrorType.CONTENT_FILTERED,
            error_message="failed: content filtered",
            token_counts=_TOKEN_COUNTS,
        )
        permanent_result = self.processor.validate_and_classify(
            config=self.config,
            raw_result=permanent,
            job_id=_JOB_ID,
            source_document_text=_SOURCE_TEXT,
            prior_transient_failure_count=beyond_budget,
        )
        self.assertEqual(
            LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_PERMANENT,
            permanent_result.result_type,
        )

        success = LLMClientDocumentExtractionResult.from_success(
            document_contents_id=_DOCUMENT_CONTENTS_ID,
            result_json=self.build_fake_collection_relevant_result_json(),
            token_counts=_TOKEN_COUNTS,
        )
        success_result = self.processor.validate_and_classify(
            config=self.config,
            raw_result=success,
            job_id=_JOB_ID,
            source_document_text=_SOURCE_TEXT,
            prior_transient_failure_count=beyond_budget,
        )
        self.assertEqual(
            LLMExtractionJobDocumentResultType.SUCCESS, success_result.result_type
        )

    def test_request_failures_classified_and_categorized(self) -> None:
        # A raw call that failed at the request phase is routed to the failed-request
        # outcome (never validated). The processor owns the classification: every
        # LLMRequestErrorType maps to the right per-document result type and persisted
        # error category, with the error message and token usage carried through.
        transient = LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT
        permanent = LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_PERMANENT
        expected = {
            LLMRequestErrorType.MALFORMED_RESPONSE: (
                permanent,
                LLMDocumentExtractionErrorType.LLM_REQUEST_MALFORMED_RESPONSE,
            ),
            LLMRequestErrorType.EMPTY_RESPONSE: (
                permanent,
                LLMDocumentExtractionErrorType.LLM_REQUEST_EMPTY_RESPONSE,
            ),
            LLMRequestErrorType.CONTENT_FILTERED: (
                permanent,
                LLMDocumentExtractionErrorType.LLM_REQUEST_CONTENT_FILTERED,
            ),
            LLMRequestErrorType.TIMEOUT: (
                transient,
                LLMDocumentExtractionErrorType.LLM_REQUEST_TIMEOUT,
            ),
            LLMRequestErrorType.RATE_LIMITED: (
                transient,
                LLMDocumentExtractionErrorType.LLM_REQUEST_RATE_LIMITED,
            ),
            LLMRequestErrorType.SERVER_ERROR: (
                transient,
                LLMDocumentExtractionErrorType.LLM_REQUEST_SERVER_ERROR,
            ),
            LLMRequestErrorType.UNKNOWN_ERROR: (
                permanent,
                LLMDocumentExtractionErrorType.LLM_REQUEST_UNKNOWN_ERROR,
            ),
        }
        # Guards against a new LLMRequestErrorType being added without classifying it.
        self.assertEqual(set(LLMRequestErrorType), set(expected))

        for request_error_type, (
            expected_result_type,
            expected_error_type,
        ) in expected.items():
            with self.subTest(request_error_type=request_error_type):
                error_message = f"failed: {request_error_type.value}"
                raw_result = LLMClientDocumentExtractionResult.from_error(
                    document_contents_id=_DOCUMENT_CONTENTS_ID,
                    error_type=request_error_type,
                    error_message=error_message,
                    token_counts=_TOKEN_COUNTS,
                )
                result = self.processor.validate_and_classify(
                    config=self.config,
                    raw_result=raw_result,
                    job_id=_JOB_ID,
                    source_document_text=_SOURCE_TEXT,
                    prior_transient_failure_count=0,
                )

                self.assertEqual(expected_result_type, result.result_type)
                self.assertEqual(expected_error_type, result.error_type)
                self.assertEqual(error_message, result.error_message)
                self.assertIsNone(result.is_relevant)
                self.assertIsNone(result.validation_results)
                self.assertEqual(_NOW, result.result_datetime_utc)
