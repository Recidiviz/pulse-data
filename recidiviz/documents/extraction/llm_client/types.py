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
"""The provider-agnostic contracts spanning the LLM lane of the document
extraction pipeline: an `LLMDocumentExtractionRequest` to extract structured
information from one document, and the `LLMClientDocumentExtractionResult` it
produces. Downstream processing consumes the result type, never a provider's raw
SDK response, so a future batch path can produce the same type from echoed
JSONL.
"""

from enum import StrEnum
from typing import Any

import attr
from google.genai import types

from recidiviz.common import attr_validators

TEMPERATURE_EXTRACTION_REQUEST_PARAMETER_NAME = "temperature"
"""Name of the tunable parameter that controls the randomness of the model's
output. Lower values make responses more deterministic.
"""

TOP_P_EXTRACTION_REQUEST_PARAMETER_NAME = "top_p"
"""Name of the tunable parameter that caps nucleus sampling: the model only
considers tokens whose cumulative probability is within this threshold.
"""

MAX_OUTPUT_TOKENS_EXTRACTION_REQUEST_PARAMETER_NAME = "max_output_tokens"
"""Name of the tunable parameter that caps the number of tokens the model may
generate in its response.
"""

BILLING_LABELS_EXTRACTION_REQUEST_PARAMETER_NAME = "labels"
"""Name of the parameter carrying the billing labels attached to each request
for cost attribution.
"""

THINKING_BUDGET_TOKENS_EXTRACTION_REQUEST_PARAMETER_NAME = "thinking_budget_tokens"
"""Name of the tunable parameter that caps a thinking model's internal
reasoning tokens. A value of 0 disables thinking; omitting it uses
model-managed dynamic thinking.
"""


class LLMRequestErrorType(StrEnum):
    """An error encountered while executing a single LLM extraction request (as
    opposed to an error found later, e.g. during validation).
    """

    MALFORMED_RESPONSE = "MALFORMED_RESPONSE"
    EMPTY_RESPONSE = "EMPTY_RESPONSE"
    CONTENT_FILTERED = "CONTENT_FILTERED"
    TIMEOUT = "TIMEOUT"
    RATE_LIMITED = "RATE_LIMITED"
    UNKNOWN_ERROR = "UNKNOWN_ERROR"


@attr.define(frozen=True, kw_only=True)
class LLMDocumentExtractionRequest:
    """A provider-agnostic request to extract structured information from a single
    document's text.
    """

    document_contents_id: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Content-addressed identifier of the document being extracted from."""

    system_prompt: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The fully-rendered system prompt (instructions + output format)."""

    document_text: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The text of the document to extract structured information from."""

    response_json_schema: dict[str, Any] = attr.ib(validator=attr_validators.is_dict)
    """The standard-JSON-Schema description of the structured output, passed to
    the provider's raw-JSON-Schema field (NOT the OpenAPI-3.0-subset field). This
    is the shape the A3 schema generator emits.
    """

    request_parameters: dict[str, Any] = attr.ib(
        validator=attr_validators.is_dict_where_each(
            key_validator=attr_validators.is_str,
            value_validator=[],
        )
    )
    """The generation parameters for the call — temperature, thinking budget,
    caching, and billing labels — keyed by parameter name. Populated upstream from
    the resolved model config; the client translates the keys it knows into the
    provider's request.
    """


@attr.define(frozen=True, kw_only=True)
class LLMDocumentExtractionTokenCounts:
    """The token counts for a single document extraction request (the inputs to
    cost observability — dollar costs are derived from these downstream).
    """

    input_token_count: int = attr.ib(validator=attr_validators.is_non_negative_int)
    """Total prompt (input) tokens billed, including any cached tokens."""

    output_token_count: int = attr.ib(validator=attr_validators.is_non_negative_int)
    """Tokens in the generated response (excluding thinking tokens)."""

    cached_input_token_count: int = attr.ib(
        validator=attr_validators.is_non_negative_int
    )
    """The portion of `input_token_count` served from the context cache."""

    thinking_token_count: int = attr.ib(validator=attr_validators.is_non_negative_int)
    """Tokens spent on the model's internal reasoning, when applicable."""

    @classmethod
    def empty(cls) -> "LLMDocumentExtractionTokenCounts":
        """Returns an all-zero token count, for a call that never billed (e.g. a
        failure) or a response without usage metadata.
        """
        return cls(
            input_token_count=0,
            output_token_count=0,
            cached_input_token_count=0,
            thinking_token_count=0,
        )

    @classmethod
    def from_google_genai_usage_metadata(
        cls, *, usage: types.GenerateContentResponseUsageMetadata
    ) -> "LLMDocumentExtractionTokenCounts":
        """Builds token counts from a google-genai `GenerateContentResponseUsageMetadata` object."""
        return cls(
            input_token_count=usage.prompt_token_count or 0,
            output_token_count=usage.candidates_token_count or 0,
            cached_input_token_count=usage.cached_content_token_count or 0,
            thinking_token_count=usage.thoughts_token_count or 0,
        )


@attr.define(frozen=True, kw_only=True)
class LLMClientDocumentExtractionResult:
    """The provider-agnostic result of a single document extraction request. A
    failed call still produces one of these (with `result_json=None` and
    `error_type`/`error_message` set) rather than raising — classifying the
    failure into a transient vs. permanent job outcome is the next layer's job.
    """

    document_contents_id: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Identifier of the document this result is for, echoed from the request."""

    result_json: dict[str, Any] | None = attr.ib(validator=attr_validators.is_opt_dict)
    """The parsed JSON content the model returned, or None if the call failed."""

    token_counts: LLMDocumentExtractionTokenCounts = attr.ib(
        validator=attr.validators.instance_of(LLMDocumentExtractionTokenCounts)
    )
    """The token counts billed for the call (zero on a failure that never billed)."""

    error_type: LLMRequestErrorType | None = attr.ib(
        validator=attr_validators.is_opt(LLMRequestErrorType)
    )
    """The category of failure, or None on success."""

    error_message: str | None = attr.ib(validator=attr_validators.is_opt_str)
    """A human-readable description of the failure, or None on success."""

    def __attrs_post_init__(self) -> None:
        if (self.result_json is None) == (self.error_type is None):
            raise ValueError(
                f"Exactly one of result_json and error_type must be set for "
                f"document [{self.document_contents_id}]: got "
                f"result_json=[{self.result_json!r}], error_type=[{self.error_type!r}]."
            )
        if (self.error_type is None) != (self.error_message is None):
            raise ValueError(
                f"error_type and error_message must both be set or both be None "
                f"for document [{self.document_contents_id}]: got "
                f"error_type=[{self.error_type!r}], "
                f"error_message=[{self.error_message!r}]."
            )

    @classmethod
    def from_error(
        cls,
        *,
        document_contents_id: str,
        error_type: LLMRequestErrorType,
        error_message: str,
        token_counts: LLMDocumentExtractionTokenCounts | None = None,
    ) -> "LLMClientDocumentExtractionResult":
        """Constructs a failed result from the given error type and message."""
        return cls(
            document_contents_id=document_contents_id,
            result_json=None,
            token_counts=token_counts or LLMDocumentExtractionTokenCounts.empty(),
            error_type=error_type,
            error_message=error_message,
        )

    @classmethod
    def from_success(
        cls,
        *,
        document_contents_id: str,
        result_json: dict[str, Any],
        token_counts: LLMDocumentExtractionTokenCounts,
    ) -> "LLMClientDocumentExtractionResult":
        """Constructs a successful result from the given parsed JSON and token counts."""
        return cls(
            document_contents_id=document_contents_id,
            result_json=result_json,
            token_counts=token_counts,
            error_type=None,
            error_message=None,
        )
