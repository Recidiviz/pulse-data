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
Helper functions for generating and handling text embeddings for resource search.

This module provides utilities to construct embedding text from resource and query components, generate text embeddings for individual and batches of resource candidates, and compute cosine distances for embedding similarity. It integrates with embedding models (such as OpenAI) and supports the embedding workflow for resource search and ranking.
"""

import logging
from typing import List

from recidiviz.persistence.database.schema.resource_search import schema
from recidiviz.resource_search.src.embedding.openai import OpenAIEmbedModel
from recidiviz.resource_search.src.settings import Settings
from recidiviz.resource_search.src.typez.crud.resources import (
    ResourceCandidate,
    ResourceCandidateWithURI,
)
from recidiviz.resource_search.src.typez.embedding import EmbeddingParts
from recidiviz.resource_search.src.typez.handlers.resource_store import (
    ResourceSearchBodyParams,
)


def cosine_distance_comparator(embeddings: list[float]) -> str:
    return schema.Resource.embedding.cosine_distance(embeddings)


def _make_embedding_text(parts: EmbeddingParts) -> str:
    """Build embedding text string from parts"""
    text: List[str] = []

    # Query
    if parts.query:
        text.append("query: " + parts.query)
    else:
        text.append("query: none")

    # Resource
    text.append("resource:")
    if parts.name:
        text.append(parts.name)
    if parts.description:
        text.append(parts.description)
    if parts.tags:
        text.extend(parts.tags)
    if parts.price_level:
        text.append("price: " + parts.price_level)

    embedding_text = " ".join(text).lower()
    return embedding_text


def make_candidate_embedding_text(query: str, resource: ResourceCandidate) -> str:
    """Make text embedding for a resource candidate"""
    return _make_embedding_text(
        EmbeddingParts(
            query=query,
            name=resource.name,
            description=resource.description,
            tags=resource.tags,
            price_level=resource.price_level,
        )
    )


async def make_embedding_text_batch(
    settings: Settings, query: str, resource_candidates: list[ResourceCandidateWithURI]
) -> list[list[float]]:
    """Make text embedding for a list of candidates"""
    texts = []  # For generating batch of embeddings

    for candidate in resource_candidates:
        # Generate embedding text
        embedding_text = make_candidate_embedding_text(query, candidate)
        candidate.embedding_text = embedding_text
        texts.append(embedding_text)

    logging.debug("Generating embeddings for %s candidates", len(texts))
    open_ai_embed_model = OpenAIEmbedModel(settings=settings)
    embeddings = await open_ai_embed_model.make_text_embeddings_batch(texts)
    return embeddings


def make_query_embedding_text(query: ResourceSearchBodyParams) -> str:
    """Make text embedding for a query"""
    return _make_embedding_text(
        EmbeddingParts(
            query=query.textSearch,
        )
    )
