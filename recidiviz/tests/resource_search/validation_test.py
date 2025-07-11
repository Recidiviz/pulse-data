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

"""Tests for the validation module

This module contains unit tests for LLM-based validation and ranking
functionality in the resource search system. It tests data mapping,
LLM prompt processing, and validation result handling.
"""

import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from recidiviz.resource_search.src.models.resource_enums import ResourceCategory
from recidiviz.resource_search.src.modules.llm.validate import (
    map_llm_output,
    map_to_llm_input,
    ranking_prompt,
    run_validation,
    validation_prompt,
)
from recidiviz.resource_search.src.typez.crud.resources import ResourceCandidateWithURI
from recidiviz.resource_search.src.typez.modules.llm.validate import (
    LlmInput,
    MergedOutput,
    RankingOutput,
    ValidationOutput,
)


class TestResourceSearchValidation(unittest.IsolatedAsyncioTestCase):
    """
    Tests LLM-based validation and ranking functionality.

    This test class covers data mapping between resource candidates and LLM
    input/output formats, validation and ranking prompt processing, and
    the integration of LLM responses with resource search results.
    """

    def test_map_to_llm_input(self) -> None:
        """Test mapping ResourceCandidate to LlmInput"""
        resource = ResourceCandidateWithURI(
            uri="resource://resource%20with%20no%20address",
            name="Test Resource",
            tags=["tag1", "tag2"],
            rating=4.5,
            ratingCount=100,
            reviews=["Good", "Great"],
            operationalStatus="OPERATIONAL",
        )

        result = map_to_llm_input(resource)

        self.assertIsInstance(result, LlmInput)
        self.assertEqual(result.id, resource.uri)
        self.assertEqual(result.name, resource.name)
        self.assertEqual(result.tags, resource.tags)
        self.assertEqual(result.rating, resource.rating)
        self.assertEqual(result.ratingCount, resource.ratingCount)
        self.assertEqual(result.reviews, resource.reviews)
        self.assertEqual(result.operationalStatus, resource.operationalStatus)

    def test_map_llm_output(self) -> None:
        """Test mapping LLM output back to ResourceCandidates"""
        resources = [
            ResourceCandidateWithURI(
                uri="resource://resource%20with%20no%20address",
                name="Test 1",
                llm_rank=None,
                llm_valid=None,
            ),
            ResourceCandidateWithURI(
                uri="resource://789%20elm%20st%20miami%20fl%2073301",
                name="Test 2",
                llm_rank=None,
                llm_valid=None,
            ),
        ]

        llm_output = {
            "resource://resource%20with%20no%20address": MergedOutput(
                rank=5, valid=True
            ),
            "resource://789%20elm%20st%20miami%20fl%2073301": MergedOutput(
                rank=3, valid=False
            ),
        }

        result = map_llm_output(llm_output, resources)

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].llm_rank, 5)
        self.assertTrue(result[0].llm_valid)
        self.assertEqual(result[1].llm_rank, 3)
        self.assertFalse(result[1].llm_valid)

    async def test_validation_prompt(self) -> None:
        """Test validation prompt with LLM"""
        mock_response = MagicMock()
        mock_response.text = '[{"id": "test.com", "valid": true}]'

        with patch(
            "recidiviz.resource_search.src.modules.llm.validate.llm"
        ) as mock_llm:
            mock_llm.acomplete = AsyncMock(return_value=mock_response)

            input_data = [LlmInput(id="test.com", name="Test Resource")]

            result = (
                await validation_prompt(input_data, ResourceCategory.BASIC_NEEDS) or []
            )

            self.assertIsNotNone(result)
            self.assertEqual(len(result), 1)
            self.assertIsInstance(result[0], ValidationOutput)
            self.assertEqual(result[0].id, "test.com")
            self.assertTrue(result[0].valid)

    async def test_ranking_prompt(self) -> None:
        """Test ranking prompt with LLM"""
        mock_response = MagicMock()
        mock_response.text = '[{"id": "test.com", "rank": 5}]'

        with patch(
            "recidiviz.resource_search.src.modules.llm.validate.llm"
        ) as mock_llm:
            mock_llm.acomplete = AsyncMock(return_value=mock_response)

            input_data = [LlmInput(id="test.com", name="Test Resource")]

            result = (
                await ranking_prompt(input_data, ResourceCategory.BASIC_NEEDS) or []
            )

            self.assertIsNotNone(result)
            self.assertEqual(len(result), 1)
            self.assertIsInstance(result[0], RankingOutput)
            self.assertEqual(result[0].id, "test.com")
            self.assertEqual(result[0].rank, 5)

    async def test_run_validation(self) -> None:
        """Test full validation run"""
        mock_ranking = AsyncMock(
            return_value=[
                RankingOutput(id="resource://resource%20with%20no%20address", rank=5)
            ]
        )
        mock_validation = AsyncMock(
            return_value=[
                ValidationOutput(
                    id="resource://resource%20with%20no%20address", valid=True
                )
            ]
        )

        with patch(
            "recidiviz.resource_search.src.modules.llm.validate.ranking_prompt",
            mock_ranking,
        ):
            with patch(
                "recidiviz.resource_search.src.modules.llm.validate.validation_prompt",
                mock_validation,
            ):
                resources = [
                    ResourceCandidateWithURI(
                        uri="resource://resource%20with%20no%20address",
                        name="Test Resource",
                    )
                ]

                # Test with ranking and validation enabled
                result = await run_validation(resources, ResourceCategory.BASIC_NEEDS)

                self.assertEqual(len(result), 1)
                self.assertEqual(result[0].llm_rank, 5)
                self.assertTrue(result[0].llm_valid)
