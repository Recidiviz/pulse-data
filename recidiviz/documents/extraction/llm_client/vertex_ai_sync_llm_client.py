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
"""A thin synchronous Vertex AI document-extraction client built on the
google-genai SDK."""

import json
import logging

import httpx
from google import genai
from google.genai import errors, types

from recidiviz.documents.extraction.llm_client.sync_llm_client import SyncLLMClient
from recidiviz.documents.extraction.llm_client.types import (
    BILLING_LABELS_EXTRACTION_REQUEST_PARAMETER_NAME,
    MAX_OUTPUT_TOKENS_EXTRACTION_REQUEST_PARAMETER_NAME,
    TEMPERATURE_EXTRACTION_REQUEST_PARAMETER_NAME,
    THINKING_BUDGET_TOKENS_EXTRACTION_REQUEST_PARAMETER_NAME,
    THINKING_LEVEL_EXTRACTION_REQUEST_PARAMETER_NAME,
    TOP_P_EXTRACTION_REQUEST_PARAMETER_NAME,
    LLMClientDocumentExtractionResult,
    LLMDocumentExtractionRequest,
    LLMDocumentExtractionTokenCounts,
    LLMRequestErrorType,
)
from recidiviz.documents.extraction.llm_client.vertex_context_cache import (
    VertexContextCacheManager,
)
from recidiviz.documents.extraction.models.llm_model_registry import (
    LLMAPIProvider,
    LLMModelConfig,
)
from recidiviz.utils import metadata

# Vertex AI region requests are routed to.
DEFAULT_VERTEX_AI_LOCATION = "us-central1"

# JSON mime type for schema-constrained structured output.
JSON_RESPONSE_MIME_TYPE = "application/json"

# HTTP status Vertex returns when a request is rate limited.
_RATE_LIMITED_CODE = 429

# HTTP status Vertex returns when the request timed out at the gateway. Every
# other 5xx server error (500, 502, 503, ...) is a transient SERVER_ERROR.
_GATEWAY_TIMEOUT_CODE = 504

# Vertex FinishReason values that indicate the response was blocked by a
# content/safety filter, as opposed to merely coming back empty.
_CONTENT_FILTER_FINISH_REASONS = frozenset(
    {
        types.FinishReason.SAFETY,
        types.FinishReason.RECITATION,
        types.FinishReason.BLOCKLIST,
        types.FinishReason.PROHIBITED_CONTENT,
        types.FinishReason.SPII,
    }
)


def _error_type_for_exception(error: Exception) -> LLMRequestErrorType:
    """Classifies an SDK exception into a request error type."""
    if isinstance(error, errors.ClientError) and error.code == _RATE_LIMITED_CODE:
        return LLMRequestErrorType.RATE_LIMITED
    # Two distinct timeout surfaces: the client giving up locally before any
    # response arrives (an httpx transport timeout — not the builtin
    # TimeoutError, which httpx does not subclass), and the server itself timing
    # out and returning a 504. Both are transient, but they are different
    # exception types with no common base, so each needs its own clause.
    if isinstance(error, (TimeoutError, httpx.TimeoutException)):
        return LLMRequestErrorType.TIMEOUT
    if isinstance(error, errors.ServerError):
        if error.code == _GATEWAY_TIMEOUT_CODE:
            return LLMRequestErrorType.TIMEOUT
        return LLMRequestErrorType.SERVER_ERROR
    return LLMRequestErrorType.UNKNOWN_ERROR


def _error_type_for_empty_response(
    finish_reason: types.FinishReason | None,
) -> LLMRequestErrorType:
    """Classifies a response we could not derive text from, using its
    FinishReason. Content-filtered responses are distinguished from otherwise
    empty responses; a missing reason and unrecognized values from a newer API
    are both treated as empty.
    """
    if finish_reason in _CONTENT_FILTER_FINISH_REASONS:
        return LLMRequestErrorType.CONTENT_FILTERED
    return LLMRequestErrorType.EMPTY_RESPONSE


class VertexAISyncLLMClient(SyncLLMClient):
    """Vertex-AI-specific `SyncLLMClient`, a thin wrapper over the google-genai
    SDK. Builds one `GenerateContent` call per document from the request and
    translates the raw SDK response (or any failure) into a provider-agnostic
    `LLMClientDocumentExtractionResult`.
    """

    def __init__(
        self,
        *,
        model_config: LLMModelConfig,
        location: str = DEFAULT_VERTEX_AI_LOCATION,
    ) -> None:
        if model_config.api_provider is not LLMAPIProvider.VERTEX_AI:
            raise ValueError(
                f"VertexAISyncLLMClient cannot serve model config "
                f"[{model_config.name}] with api_provider "
                f"[{model_config.api_provider}]. "
                f"Use api_provider={LLMAPIProvider.VERTEX_AI.name} for this client."
            )
        super().__init__(model_config=model_config)
        self._client = genai.Client(
            vertexai=True,
            project=metadata.project_id(),
            location=location,
        )
        self._cache = VertexContextCacheManager(
            client=self._client,
            model=self.model_config.model,
        )

    def execute_document_extraction_request(
        self, *, request: LLMDocumentExtractionRequest
    ) -> LLMClientDocumentExtractionResult:
        return self._execute(request=request, retry_on_stale_cache=True)

    def _execute(
        self, *, request: LLMDocumentExtractionRequest, retry_on_stale_cache: bool
    ) -> LLMClientDocumentExtractionResult:
        """Executes a single document extraction request against Vertex AI, returning
        LLMClientDocumentExtractionResult on both success and failure."""
        # The two provider calls are each wrapped; the pure config assembly between
        # them is not, nor is translating the response into a result (a bug in our
        # own parsing must not be misclassified as a provider failure). Only
        # generate_content can surface a stale-cache error — the cache is torn down
        # server-side and detected when a call references it — so only that except
        # carries the invalidate-and-retry branch.
        try:
            cached_content = self._cache.cached_content_name(
                system_prompt=request.system_prompt
            )
        except Exception as e:
            return self._handle_provider_error(request=request, error=e)

        config = self._build_generate_content_config(
            request=request, cached_content=cached_content
        )

        try:
            # The document text is the user turn; the instructions live in the
            # config's system_instruction (or the context cache when one exists).
            response = self._client.models.generate_content(
                model=self.model_config.model,
                contents=request.document_text,
                config=config,
            )
        except Exception as e:
            is_stale_cache_error = isinstance(
                e, errors.ClientError
            ) and self._cache.is_stale_cache_error(e)
            if is_stale_cache_error and retry_on_stale_cache:
                # The context cache we memoized for this system prompt was torn
                # down server-side (its TTL elapsed) between calls. Drop the stale
                # name so the next call recreates it, and retry this one once — its
                # recreated config will not reference the dead cache.
                logging.info(
                    "Context cache for model config [%s] was evicted; invalidating "
                    "local cached content and retrying document [%s].",
                    self.model_config.name,
                    request.document_contents_id,
                )
                self._cache.invalidate(system_prompt=request.system_prompt)
                return self._execute(request=request, retry_on_stale_cache=False)
            return self._handle_provider_error(request=request, error=e)

        return self._build_result(request=request, response=response)

    def _handle_provider_error(
        self, *, request: LLMDocumentExtractionRequest, error: Exception
    ) -> LLMClientDocumentExtractionResult:
        """Logs a failed provider call and returns it as a classified error result."""
        logging.warning(
            "Vertex AI extraction call failed for document [%s]: %s",
            request.document_contents_id,
            error,
        )
        return LLMClientDocumentExtractionResult.from_error(
            document_contents_id=request.document_contents_id,
            error_type=_error_type_for_exception(error),
            error_message=str(error),
        )

    def _build_generate_content_config(
        self, *, request: LLMDocumentExtractionRequest, cached_content: str | None
    ) -> types.GenerateContentConfig:
        """Builds the per-call config from the request's generation parameters,
        structured output via response_json_schema, billing labels, and the
        resolved |cached_content| name (None when the prompt is passed inline).
        Pure assembly over already-validated inputs — makes no provider calls.
        """
        parameters = request.request_parameters

        thinking_config: types.ThinkingConfig | None = None
        if THINKING_BUDGET_TOKENS_EXTRACTION_REQUEST_PARAMETER_NAME in parameters:
            thinking_config = types.ThinkingConfig(
                thinking_budget=parameters[
                    THINKING_BUDGET_TOKENS_EXTRACTION_REQUEST_PARAMETER_NAME
                ]
            )
        if THINKING_LEVEL_EXTRACTION_REQUEST_PARAMETER_NAME in parameters:
            thinking_config = types.ThinkingConfig(
                thinking_level=types.ThinkingLevel(
                    parameters[THINKING_LEVEL_EXTRACTION_REQUEST_PARAMETER_NAME]
                )
            )

        return types.GenerateContentConfig(
            # The system prompt lives in the context cache when one exists; Vertex
            # rejects setting both system_instruction and cached_content on the
            # same call, so we set the prompt inline only when it isn't cached.
            system_instruction=None if cached_content else request.system_prompt,
            cached_content=cached_content,
            temperature=parameters[TEMPERATURE_EXTRACTION_REQUEST_PARAMETER_NAME],
            top_p=parameters[TOP_P_EXTRACTION_REQUEST_PARAMETER_NAME],
            max_output_tokens=parameters[
                MAX_OUTPUT_TOKENS_EXTRACTION_REQUEST_PARAMETER_NAME
            ],
            thinking_config=thinking_config,
            # We declare no tools, so the SDK's automatic-function-calling loop
            # has nothing to call and exits after a single generate_content. But
            # leaving it on is not free: it logs "AFC is enabled with max remote
            # calls: N" at INFO on every call — one line per document — and
            # deep-copies the whole config, response schema included, before each
            # dispatch. Turning it off skips both.
            automatic_function_calling=types.AutomaticFunctionCallingConfig(
                disable=True
            ),
            response_mime_type=JSON_RESPONSE_MIME_TYPE,
            # The raw-JSON-Schema field — NOT response_schema, which coerces into
            # the google-genai Schema type (an OpenAPI-3.0 subset that cannot
            # represent our generated schema).
            response_json_schema=request.response_json_schema,
            labels=parameters[BILLING_LABELS_EXTRACTION_REQUEST_PARAMETER_NAME],
        )

    def _build_result(
        self,
        *,
        request: LLMDocumentExtractionRequest,
        response: types.GenerateContentResponse,
    ) -> LLMClientDocumentExtractionResult:
        """Translates a raw SDK response into an `LLMClientDocumentExtractionResult`,
        distinguishing a content-filtered/empty response and unparseable JSON
        from a successful extraction.
        """
        token_counts = (
            LLMDocumentExtractionTokenCounts.from_google_genai_usage_metadata(
                usage=response.usage_metadata
            )
            if response.usage_metadata is not None
            else LLMDocumentExtractionTokenCounts.empty()
        )

        # We derive the extraction from response.text — the SDK property that
        # concatenates the text parts of the single response candidate. We make a
        # single-candidate request (one document, no candidate_count), so we
        # expect at most one candidate and read its finish_reason to explain an
        # empty response, keeping the error tied to the field we actually consume.
        candidates = response.candidates or []
        if len(candidates) > 1:
            raise ValueError(
                f"Vertex AI returned [{len(candidates)}] candidates for document "
                f"[{request.document_contents_id}]; expected at most one."
            )
        finish_reason = candidates[0].finish_reason if candidates else None

        response_text = response.text
        if not response_text:
            return LLMClientDocumentExtractionResult.from_error(
                document_contents_id=request.document_contents_id,
                error_type=_error_type_for_empty_response(finish_reason),
                error_message=(
                    f"Vertex AI returned no content (finish_reason="
                    f"[{finish_reason}])."
                ),
                token_counts=token_counts,
            )

        try:
            result_json = json.loads(response_text)
        except json.JSONDecodeError as e:
            return LLMClientDocumentExtractionResult.from_error(
                document_contents_id=request.document_contents_id,
                error_type=LLMRequestErrorType.MALFORMED_RESPONSE,
                error_message=f"Failed to parse JSON response: {e}.",
                token_counts=token_counts,
            )

        if not isinstance(result_json, dict):
            return LLMClientDocumentExtractionResult.from_error(
                document_contents_id=request.document_contents_id,
                error_type=LLMRequestErrorType.MALFORMED_RESPONSE,
                error_message=(
                    f"Expected a JSON object response, got "
                    f"[{type(result_json).__name__}]."
                ),
                token_counts=token_counts,
            )

        return LLMClientDocumentExtractionResult.from_success(
            document_contents_id=request.document_contents_id,
            result_json=result_json,
            token_counts=token_counts,
        )
