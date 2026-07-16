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
"""Tests for VertexAISyncLLMClient."""

from typing import Any
from unittest import TestCase
from unittest.mock import MagicMock, patch

import httpx
from google.genai import errors, types  # pylint: disable=no-name-in-module

from recidiviz.documents.extraction.llm_client.types import (
    LLMDocumentExtractionRequest,
    LLMRequestErrorType,
)
from recidiviz.documents.extraction.llm_client.vertex_ai_sync_llm_client import (
    JSON_RESPONSE_MIME_TYPE,
    VertexAISyncLLMClient,
)
from recidiviz.documents.extraction.models.llm_model_registry import (
    load_llm_model_registry,
)
from recidiviz.utils.types import assert_type

_RESPONSE_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"result": {"type": "object"}},
    "required": ["result"],
}

_DEFAULT_REQUEST_PARAMETERS: dict[str, Any] = {
    "temperature": 0.0,
    "top_p": 1.0,
    "max_output_tokens": 8192,
    "thinking_budget_tokens": 0,
    "labels": {
        "state_code": "us_xx",
        "environment": "staging",
        "requester": "test_user",
    },
}


def _request(
    *,
    document_contents_id: str = "doc_1",
    system_prompt: str = "You extract employment info.",
    document_text: str = "Client started a new job at Walmart.",
    request_parameters: dict[str, Any] | None = None,
) -> LLMDocumentExtractionRequest:
    return LLMDocumentExtractionRequest(
        document_contents_id=document_contents_id,
        system_prompt=system_prompt,
        document_text=document_text,
        response_json_schema=_RESPONSE_JSON_SCHEMA,
        request_parameters=(
            _DEFAULT_REQUEST_PARAMETERS
            if request_parameters is None
            else request_parameters
        ),
    )


def _response(
    *,
    text: str | None = '{"result": {"is_relevant": true}}',
    finish_reason: types.FinishReason | None = types.FinishReason.STOP,
    prompt_tokens: int = 100,
    candidates_tokens: int = 20,
    cached_tokens: int = 80,
    thoughts_tokens: int = 5,
    with_candidate: bool = True,
) -> types.GenerateContentResponse:
    candidates = None
    if with_candidate:
        parts = [types.Part(text=text)] if text is not None else []
        candidates = [
            types.Candidate(
                content=types.Content(parts=parts, role="model"),
                finish_reason=finish_reason,
            )
        ]
    return types.GenerateContentResponse(
        candidates=candidates,
        usage_metadata=types.GenerateContentResponseUsageMetadata(
            prompt_token_count=prompt_tokens,
            candidates_token_count=candidates_tokens,
            cached_content_token_count=cached_tokens,
            thoughts_token_count=thoughts_tokens,
        ),
    )


class VertexAISyncLLMClientTest(TestCase):
    """Tests for VertexAISyncLLMClient."""

    def setUp(self) -> None:
        self.model_config = load_llm_model_registry().get_model_config(
            "GEMINI_2_5_FLASH_NO_THINKING"
        )

        self.genai_client_patcher = patch(
            "recidiviz.documents.extraction.llm_client."
            "vertex_ai_sync_llm_client.genai.Client"
        )
        self.mock_genai_client_cls = self.genai_client_patcher.start()
        self.mock_genai_client = self.mock_genai_client_cls.return_value
        self.mock_genai_client.caches.create.return_value = types.CachedContent(
            name="cachedContents/abc123"
        )
        self.mock_generate_content = self.mock_genai_client.models.generate_content
        self.project_id_patcher = patch(
            "recidiviz.documents.extraction.llm_client."
            "vertex_ai_sync_llm_client.metadata.project_id",
            return_value="recidiviz-testing",
        )
        self.project_id_patcher.start()

    def tearDown(self) -> None:
        self.genai_client_patcher.stop()
        self.project_id_patcher.stop()

    def _execute(self, request: LLMDocumentExtractionRequest | None = None) -> Any:
        client = VertexAISyncLLMClient(model_config=self.model_config)
        return client.execute_document_extraction_request(
            request=request if request is not None else _request()
        )

    def _last_generate_config(self) -> types.GenerateContentConfig:
        return self.mock_generate_content.call_args.kwargs["config"]

    def test_success_maps_content_and_tokens(self) -> None:
        self.mock_generate_content.return_value = _response()

        result = self._execute()

        self.assertEqual("doc_1", result.document_contents_id)
        self.assertEqual({"result": {"is_relevant": True}}, result.result_json)
        self.assertEqual(100, result.token_counts.input_token_count)
        self.assertEqual(20, result.token_counts.output_token_count)
        self.assertEqual(80, result.token_counts.cached_input_token_count)
        self.assertEqual(5, result.token_counts.thinking_token_count)
        self.assertIsNone(result.error_type)
        self.assertIsNone(result.error_message)

    def test_generation_config_maps_parameters_and_schema(self) -> None:
        self.mock_generate_content.return_value = _response()

        self._execute()

        self.mock_generate_content.assert_called_once()
        self.assertEqual(
            self.model_config.model,
            self.mock_generate_content.call_args.kwargs["model"],
        )
        config = self._last_generate_config()
        self.assertEqual(0.0, config.temperature)
        self.assertEqual(1.0, config.top_p)
        self.assertEqual(8192, config.max_output_tokens)
        self.assertEqual(JSON_RESPONSE_MIME_TYPE, config.response_mime_type)
        self.assertEqual(_RESPONSE_JSON_SCHEMA, config.response_json_schema)
        # response_schema (the coercing field) must never be set.
        self.assertIsNone(config.response_schema)

    def test_thinking_budget_zero_disables_thinking(self) -> None:
        self.mock_generate_content.return_value = _response()

        self._execute()

        thinking_config = assert_type(
            self._last_generate_config().thinking_config, types.ThinkingConfig
        )
        self.assertEqual(0, thinking_config.thinking_budget)

    def test_thinking_budget_nonzero_is_passed_through(self) -> None:
        self.mock_generate_content.return_value = _response()

        self._execute(
            _request(
                request_parameters={
                    **_DEFAULT_REQUEST_PARAMETERS,
                    "thinking_budget_tokens": 2048,
                }
            )
        )

        thinking_config = assert_type(
            self._last_generate_config().thinking_config, types.ThinkingConfig
        )
        self.assertEqual(2048, thinking_config.thinking_budget)

    def test_omitted_thinking_budget_leaves_thinking_config_unset(self) -> None:
        self.mock_generate_content.return_value = _response()
        parameters = {
            k: v
            for k, v in _DEFAULT_REQUEST_PARAMETERS.items()
            if k != "thinking_budget_tokens"
        }

        self._execute(_request(request_parameters=parameters))

        self.assertIsNone(self._last_generate_config().thinking_config)

    def test_billing_labels_passed_through_verbatim(self) -> None:
        self.mock_generate_content.return_value = _response()

        self._execute()

        self.assertEqual(
            {
                "state_code": "us_xx",
                "environment": "staging",
                "requester": "test_user",
            },
            self._last_generate_config().labels,
        )

    def test_missing_required_parameter_raises(self) -> None:
        # temperature/top_p/max_output_tokens/labels are required; a request
        # missing any of them fails loudly with a KeyError rather than sending a
        # silent None to the SDK.
        self.mock_generate_content.return_value = _response()
        for missing_key in ("temperature", "top_p", "max_output_tokens", "labels"):
            with self.subTest(missing_key=missing_key):
                parameters = {
                    k: v
                    for k, v in _DEFAULT_REQUEST_PARAMETERS.items()
                    if k != missing_key
                }
                with self.assertRaises(KeyError):
                    self._execute(_request(request_parameters=parameters))

    def test_system_prompt_cached_and_reused(self) -> None:
        self.mock_generate_content.return_value = _response()
        client = VertexAISyncLLMClient(model_config=self.model_config)

        client.execute_document_extraction_request(
            request=_request(document_contents_id="doc_1")
        )
        client.execute_document_extraction_request(
            request=_request(document_contents_id="doc_2")
        )

        # One cache for the shared system prompt, reused on the second call.
        self.mock_genai_client.caches.create.assert_called_once()
        self.assertEqual(
            "You extract employment info.",
            self.mock_genai_client.caches.create.call_args.kwargs[
                "config"
            ].system_instruction,
        )
        config = self._last_generate_config()
        self.assertEqual("cachedContents/abc123", config.cached_content)
        # The prompt lives in the cache, so it must not also be set on the call.
        self.assertIsNone(config.system_instruction)

    def test_uncacheable_small_prompt_falls_back_to_inline(self) -> None:
        # A prompt below Vertex's minimum cacheable size makes the create call
        # 400; the client passes it inline instead of failing, and does not retry
        # caching it.
        self.mock_genai_client.caches.create.side_effect = errors.ClientError(
            400,
            {
                "error": {
                    "code": 400,
                    "message": (
                        "The cached content is of 94 tokens. The minimum token "
                        "count to start caching is 1024."
                    ),
                    "status": "INVALID_ARGUMENT",
                }
            },
        )
        self.mock_generate_content.return_value = _response()
        client = VertexAISyncLLMClient(model_config=self.model_config)

        result = client.execute_document_extraction_request(
            request=_request(document_contents_id="doc_1")
        )
        client.execute_document_extraction_request(
            request=_request(document_contents_id="doc_2")
        )

        self.assertIsNone(result.error_type)
        # Caching attempted once, memoized as uncacheable, not retried.
        self.mock_genai_client.caches.create.assert_called_once()
        config = self._last_generate_config()
        self.assertIsNone(config.cached_content)
        self.assertEqual("You extract employment info.", config.system_instruction)

    def test_expired_cache_recreated_and_call_retried(self) -> None:
        # The memoized cache was torn down server-side after its TTL; the first
        # call fails with the "invalid resource state" error, and the client
        # recreates the cache and retries the same document once, succeeding.
        self.mock_genai_client.caches.create.side_effect = [
            types.CachedContent(name="cachedContents/first"),
            types.CachedContent(name="cachedContents/second"),
        ]
        self.mock_generate_content.side_effect = [
            errors.ClientError(
                400,
                {
                    "error": {
                        "code": 400,
                        "message": "Invalid resource state for cache content 123.",
                        "status": "INVALID_ARGUMENT",
                    }
                },
            ),
            _response(),
        ]

        result = self._execute()

        self.assertIsNone(result.error_type)
        self.assertEqual({"result": {"is_relevant": True}}, result.result_json)
        self.assertEqual(2, self.mock_genai_client.caches.create.call_count)
        self.assertEqual(2, self.mock_generate_content.call_count)
        # The retry referenced the freshly-created cache, not the dead one.
        self.assertEqual(
            "cachedContents/second", self._last_generate_config().cached_content
        )

    def test_stale_cache_error_on_retry_becomes_error_result(self) -> None:
        # The client retries at most once: a second stale-cache failure is not
        # retried again and surfaces as an error result.
        stale_error = errors.ClientError(
            400,
            {
                "error": {
                    "code": 400,
                    "message": "Invalid resource state for cache content 123.",
                    "status": "INVALID_ARGUMENT",
                }
            },
        )
        self.mock_generate_content.side_effect = [stale_error, stale_error]

        result = self._execute()

        self.assertIsNone(result.result_json)
        self.assertEqual(LLMRequestErrorType.UNKNOWN_ERROR, result.error_type)
        self.assertIn("Invalid resource state", str(result.error_message))
        self.assertEqual(2, self.mock_generate_content.call_count)

    def test_other_cache_error_propagates_as_error_result(self) -> None:
        self.mock_genai_client.caches.create.side_effect = errors.ClientError(
            403, {"error": {"code": 403, "message": "Permission denied."}}
        )

        result = self._execute()

        self.assertIsNone(result.result_json)
        self.assertEqual(LLMRequestErrorType.UNKNOWN_ERROR, result.error_type)
        self.assertIn("Permission denied", str(result.error_message))

    def test_content_filtered_response_is_classified(self) -> None:
        # Every content/safety-filter finish reason surfaces as a candidate with
        # no text and classifies as CONTENT_FILTERED.
        for finish_reason in (
            types.FinishReason.SAFETY,
            types.FinishReason.RECITATION,
            types.FinishReason.BLOCKLIST,
            types.FinishReason.PROHIBITED_CONTENT,
            types.FinishReason.SPII,
        ):
            with self.subTest(finish_reason=finish_reason):
                self.mock_generate_content.return_value = _response(
                    text=None, finish_reason=finish_reason
                )

                result = self._execute()

                self.assertIsNone(result.result_json)
                self.assertEqual(
                    LLMRequestErrorType.CONTENT_FILTERED, result.error_type
                )
                self.assertIn("no content", str(result.error_message))

    def test_empty_response_is_classified(self) -> None:
        # No text with a non-safety finish reason is an empty response. A
        # truncated/empty response still bills the tokens the model consumed, so
        # the counts must be reported, not zeroed.
        self.mock_generate_content.return_value = _response(
            text=None, finish_reason=types.FinishReason.MAX_TOKENS
        )

        result = self._execute()

        self.assertIsNone(result.result_json)
        self.assertEqual(LLMRequestErrorType.EMPTY_RESPONSE, result.error_type)
        self.assertEqual(100, result.token_counts.input_token_count)
        self.assertEqual(20, result.token_counts.output_token_count)

    def test_malformed_json_is_an_error(self) -> None:
        self.mock_generate_content.return_value = _response(text="not json {")

        result = self._execute()

        self.assertIsNone(result.result_json)
        self.assertEqual(LLMRequestErrorType.MALFORMED_RESPONSE, result.error_type)
        self.assertIn("Failed to parse JSON", str(result.error_message))
        # A response that ran to completion bills tokens even if it won't parse.
        self.assertEqual(100, result.token_counts.input_token_count)

    def test_non_object_json_is_an_error(self) -> None:
        self.mock_generate_content.return_value = _response(text="[1, 2, 3]")

        result = self._execute()

        self.assertIsNone(result.result_json)
        self.assertEqual(LLMRequestErrorType.MALFORMED_RESPONSE, result.error_type)
        self.assertIn("Expected a JSON object", str(result.error_message))

    def test_rate_limit_exception_is_classified(self) -> None:
        self.mock_generate_content.side_effect = errors.ClientError(
            429, {"error": {"code": 429, "message": "Resource exhausted."}}
        )

        result = self._execute()

        self.assertIsNone(result.result_json)
        self.assertEqual(LLMRequestErrorType.RATE_LIMITED, result.error_type)
        self.assertEqual(0, result.token_counts.input_token_count)

    def test_exceptions_are_classified(self) -> None:
        # A client-side transport timeout (httpx.TimeoutException), a plain
        # TimeoutError, and a 504 gateway-timeout ServerError all classify as
        # TIMEOUT; other 5xx server errors classify as the retryable SERVER_ERROR,
        # distinct from the UNKNOWN_ERROR catch-all.
        for error, expected_type in (
            (httpx.ReadTimeout("read timed out"), LLMRequestErrorType.TIMEOUT),
            (TimeoutError("deadline exceeded"), LLMRequestErrorType.TIMEOUT),
            (
                errors.ServerError(
                    504, {"error": {"code": 504, "message": "Deadline exceeded."}}
                ),
                LLMRequestErrorType.TIMEOUT,
            ),
            (
                errors.ServerError(
                    503, {"error": {"code": 503, "message": "Service unavailable."}}
                ),
                LLMRequestErrorType.SERVER_ERROR,
            ),
            (
                errors.ServerError(
                    500, {"error": {"code": 500, "message": "Internal error."}}
                ),
                LLMRequestErrorType.SERVER_ERROR,
            ),
        ):
            with self.subTest(error=error):
                self.mock_generate_content.side_effect = error

                result = self._execute()

                self.assertIsNone(result.result_json)
                self.assertEqual(expected_type, result.error_type)

    def test_response_with_no_candidates_is_empty(self) -> None:
        # A response with no candidates at all has no finish reason and classifies
        # as an empty response.
        self.mock_generate_content.return_value = _response(with_candidate=False)

        result = self._execute()

        self.assertIsNone(result.result_json)
        self.assertEqual(LLMRequestErrorType.EMPTY_RESPONSE, result.error_type)

    def test_multiple_candidates_violate_invariant(self) -> None:
        # We make single-candidate requests, so more than one candidate is an
        # unexpected shape we surface loudly rather than silently reading the first.
        response = types.GenerateContentResponse(
            candidates=[
                types.Candidate(
                    content=types.Content(parts=[], role="model"),
                    finish_reason=types.FinishReason.STOP,
                ),
                types.Candidate(
                    content=types.Content(parts=[], role="model"),
                    finish_reason=types.FinishReason.STOP,
                ),
            ],
            usage_metadata=types.GenerateContentResponseUsageMetadata(
                prompt_token_count=100
            ),
        )
        self.mock_generate_content.return_value = response

        with self.assertRaisesRegex(ValueError, r"expected at most one"):
            self._execute()

    def test_unknown_exception_becomes_error_result(self) -> None:
        self.mock_generate_content.side_effect = RuntimeError("boom")

        result = self._execute()

        self.assertIsNone(result.result_json)
        self.assertEqual(LLMRequestErrorType.UNKNOWN_ERROR, result.error_type)
        self.assertIn("boom", str(result.error_message))

    def test_non_vertex_model_config_rejected(self) -> None:
        with patch.object(type(self.model_config), "api_provider", new=MagicMock()):
            with self.assertRaisesRegex(
                ValueError, r"^VertexAISyncLLMClient cannot serve model config"
            ):
                VertexAISyncLLMClient(model_config=self.model_config)
