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
"""Provider delegates for LLM batch clients.

Each delegate encapsulates provider-specific logic: environment variable setup,
response format parsing, file ID processing, and the custom_llm_provider string
used in LiteLLM API calls.
"""
import abc
import urllib.parse
from typing import Any

import attr


class LLMProviderDelegate(abc.ABC):
    """Abstract delegate that encapsulates LLM provider-specific behavior."""

    @property
    @abc.abstractmethod
    def custom_llm_provider(self) -> str:
        """Provider string for LiteLLM API calls (e.g., "vertex_ai")."""

    @abc.abstractmethod
    def get_env_vars(self) -> dict[str, str]:
        """Returns env vars to set before LiteLLM calls."""

    @abc.abstractmethod
    def process_output_file_id(self, output_file_id: str) -> str:
        """Transform file ID before passing to litellm.file_content()."""

    @abc.abstractmethod
    def decode_file_content(self, content: Any) -> str:
        """Decode the response from litellm.file_content() to a string."""

    @abc.abstractmethod
    def extract_text_from_batch_result_line(self, entry: dict[str, Any]) -> str | None:
        """Extract LLM response text from a single JSONL result entry.

        Returns None if no text could be extracted.
        """

    @abc.abstractmethod
    def extract_custom_id_from_batch_result_line(
        self, entry: dict[str, Any]
    ) -> str | None:
        """Extract custom_id from a result entry, if the provider returns it.

        Returns None if the provider doesn't include custom_id in results
        (e.g., Vertex AI), meaning positional correlation should be used.
        """


@attr.define
class VertexAIProviderDelegate(LLMProviderDelegate):
    """Delegate for Vertex AI batch processing via LiteLLM."""

    gcs_bucket: str
    project: str
    location: str = "us-central1"

    @property
    def custom_llm_provider(self) -> str:
        return "vertex_ai"

    def get_env_vars(self) -> dict[str, str]:
        return {
            "GCS_BUCKET_NAME": self.gcs_bucket,
            "VERTEXAI_PROJECT": self.project,
            "VERTEXAI_LOCATION": self.location,
        }

    def process_output_file_id(self, output_file_id: str) -> str:
        return urllib.parse.quote(output_file_id, safe="")

    def decode_file_content(self, content: Any) -> str:
        if hasattr(content, "content"):
            return content.content.decode("utf-8")
        if isinstance(content, bytes):
            return content.decode("utf-8")
        return str(content)

    def extract_text_from_batch_result_line(self, entry: dict[str, Any]) -> str | None:
        response_obj = entry.get("response", {})
        candidates = response_obj.get("candidates", [])
        if not candidates:
            return None
        content_parts = candidates[0].get("content", {}).get("parts", [])
        if not content_parts:
            return None
        return content_parts[0].get("text")

    def extract_custom_id_from_batch_result_line(
        self, entry: dict[str, Any]
    ) -> str | None:
        return None


def get_llm_provider_delegate(llm_provider: str, **kwargs: Any) -> LLMProviderDelegate:
    """Factory function that maps an llm_provider string to a delegate.

    Args:
        llm_provider: Provider string from extractor config (e.g., "vertex_ai").
        **kwargs: Provider-specific arguments passed to the delegate constructor.

    Returns:
        An LLMProviderDelegate instance for the given provider.

    Raises:
        ValueError: If the provider is not supported.
    """
    if llm_provider == "vertex_ai":
        return VertexAIProviderDelegate(**kwargs)
    raise ValueError(
        f"Unsupported llm_provider: {llm_provider!r}. "
        f"Supported providers: vertex_ai"
    )
