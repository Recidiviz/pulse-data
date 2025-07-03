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
Unit tests for embedding logic in the resource search system.
"""

from unittest import TestCase

from recidiviz.resource_search.src.embedding.helpers import (
    _make_embedding_text,
    make_candidate_embedding_text,
)
from recidiviz.resource_search.src.typez.crud.resources import ResourceCandidate
from recidiviz.resource_search.src.typez.embedding import EmbeddingParts


class TestResourceSearchEmbedding(TestCase):
    """Implements tests for the embedding logic for Resource Search."""

    def test_embedding_all_fields(self) -> None:
        parts = EmbeddingParts(
            query="affordable housing",
            name="Affordable Housing Complex",
            description="Low-income apartment units",
            tags=["housing", "affordable", "apartments"],
            price_level="moderate",
        )

        result = _make_embedding_text(parts)
        expected = (
            "query: affordable housing resource: "
            "affordable housing complex low-income apartment units housing affordable apartments "
            "price: moderate"
        )
        assert result == expected

    def test_candidate_text(self) -> None:
        candidate = ResourceCandidate(
            name="City Housing Authority",
            description="Public housing assistance",
        )

        result = make_candidate_embedding_text("rental assistance", candidate)
        expected = (
            "query: rental assistance resource: "
            "city housing authority public housing assistance"
        )
        assert result == expected

    def test_handle_none(self) -> None:
        parts = EmbeddingParts(query=None, name=None, description=None, tags=None)

        result = _make_embedding_text(parts)
        expected = "query: none resource:"
        assert result == expected
        assert result == expected.lower()
