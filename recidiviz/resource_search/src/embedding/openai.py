# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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

"""
OpenAI embedding model configuration and utilities.

This module provides the configuration and utilities for the OpenAI embedding model, including the embedding model instance, batch size, and dimension settings. It also includes functions to generate text embeddings for individual strings and batches of strings.
"""

from llama_index.embeddings.openai import OpenAIEmbedding  # type: ignore

from recidiviz.resource_search.src.constants import (
    EMBED_BATCH_SIZE,
    EMBEDDING_DIMENSION,
    EMBEDDING_MODEL,
)
from recidiviz.resource_search.src.settings import Settings


class OpenAIEmbedModel:
    def __init__(self, settings: Settings):
        self.embed_model = OpenAIEmbedding(
            model=EMBEDDING_MODEL,
            dimensions=EMBEDDING_DIMENSION,
            embed_batch_size=EMBED_BATCH_SIZE,
            api_key=settings.openai_api_key,
        )

    async def make_text_embeddings(self, text: str) -> list[float]:
        """Make embeddings for a string"""
        return await self.embed_model.aget_text_embedding(text)

    async def make_text_embeddings_batch(self, texts: list[str]) -> list[list[float]]:
        """Make embeddings for a list of strings"""
        return await self.embed_model.aget_text_embedding_batch(texts)
