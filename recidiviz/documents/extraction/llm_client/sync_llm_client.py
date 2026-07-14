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
"""Abstract base class for a client that makes a single synchronous document
extraction request to a given provider.
"""

import abc

from recidiviz.documents.extraction.llm_client.types import (
    LLMClientDocumentExtractionResult,
    LLMDocumentExtractionRequest,
)
from recidiviz.documents.extraction.models.llm_model_registry import LLMModelConfig


class SyncLLMClient(abc.ABC):
    """Abstract base class for a client that does synchronous document extraction
    requests against the provider declared by a resolved `LLMModelConfig`.
    """

    def __init__(self, *, model_config: LLMModelConfig) -> None:
        self.model_config = model_config

    @abc.abstractmethod
    def execute_document_extraction_request(
        self, *, request: LLMDocumentExtractionRequest
    ) -> LLMClientDocumentExtractionResult:
        """Extracts structured data from a single document. Returns an
        `LLMClientDocumentExtractionResult` on both success and failure
        """
