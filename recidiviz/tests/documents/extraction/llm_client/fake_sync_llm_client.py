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
"""A fake `SyncLLMClient` that returns canned `LLMClientDocumentExtractionResult`s,
for testing the layers that consume the client without making real provider
calls.
"""

from collections.abc import Callable

from recidiviz.documents.extraction.llm_client.sync_llm_client import SyncLLMClient
from recidiviz.documents.extraction.llm_client.types import (
    LLMClientDocumentExtractionResult,
    LLMDocumentExtractionRequest,
)
from recidiviz.documents.extraction.models.llm_model_registry import LLMModelConfig


class FakeSyncLLMClient(SyncLLMClient):
    """A `SyncLLMClient` that defers to a caller-supplied function to produce the
    result for each request. Records every request it is handed so tests can
    assert on what was sent.
    """

    def __init__(
        self,
        *,
        model_config: LLMModelConfig,
        result_fn: Callable[
            [LLMDocumentExtractionRequest], LLMClientDocumentExtractionResult
        ],
    ) -> None:
        super().__init__(model_config=model_config)
        # Function that produces the canned result for a given request.
        self._result_fn = result_fn
        # Every request passed to execute_document_extraction_request(), in call order.
        self.requests: list[LLMDocumentExtractionRequest] = []

    def execute_document_extraction_request(
        self, *, request: LLMDocumentExtractionRequest
    ) -> LLMClientDocumentExtractionResult:
        self.requests.append(request)
        return self._result_fn(request)
