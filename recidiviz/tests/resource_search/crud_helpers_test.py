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
"""Tests for CRUD helpers and resource operations"""

import unittest

from recidiviz.resource_search.src.crud.helpers import (
    make_resource_create,
    make_resource_uri,
)
from recidiviz.resource_search.src.geo import coordinates_to_location_wkb
from recidiviz.resource_search.src.models.resource import ResourceCreate
from recidiviz.resource_search.src.models.resource_enums import (
    ResourceCategory,
    ResourceOrigin,
)
from recidiviz.resource_search.src.typez.crud.resources import (
    ResourceCandidate,
    ResourceCandidateWithURICoord,
)


class TestResourceSearchCrudHelpers(unittest.IsolatedAsyncioTestCase):
    """Test cases for CRUD helper functions related to resource creation."""

    async def test_make_resource_basic(self) -> None:
        """Test resource creation with required fields"""
        coordinates = {"lat": 10.0, "lon": 100.0}
        candidate = ResourceCandidateWithURICoord(
            uri="resource://another%20resource/456%20broadway%20francisco%20ca%2090210",
            name="Test Resource",
            lat=coordinates["lat"],
            lon=coordinates["lon"],
            category=ResourceCategory.BASIC_NEEDS,
            origin=ResourceOrigin.GOOGLE_PLACES,
            rawData={"source": "test"},
            price_level="moderate",
            operationalStatus="OPERATIONAL",
        )

        result = make_resource_create(candidate)

        expected = ResourceCreate(
            uri=candidate.uri,
            name=candidate.name,
            category=candidate.category,
            location=coordinates_to_location_wkb(
                coordinates["lat"], coordinates["lon"]
            ),
            origin=ResourceOrigin.GOOGLE_PLACES,
            embedding=None,
            extra_data={"priceLevel": "moderate", "operationalStatus": "OPERATIONAL"},
        )
        self.assertEqual(result, expected)

    async def test_make_with_embedding(self) -> None:
        """Test resource creation with embedding"""
        coordinates = {"lat": 10.0, "lon": 100.0}
        candidate = ResourceCandidateWithURICoord(
            uri="resource://another%20resource/456%20broadway%20francisco%20ca%2090210",
            name="Test Resource",
            lat=coordinates["lat"],
            lon=coordinates["lon"],
            category=ResourceCategory.BASIC_NEEDS,
            origin=ResourceOrigin.GOOGLE_PLACES,
            rawData={"source": "test"},
        )

        embedding = [0.1, 0.2, 0.3]
        result = make_resource_create(candidate, embedding)

        expected = ResourceCreate(
            uri=candidate.uri,
            name=candidate.name,
            category=candidate.category,
            origin=ResourceOrigin.GOOGLE_PLACES,
            location=coordinates_to_location_wkb(
                coordinates["lat"], coordinates["lon"]
            ),
            embedding=embedding,
            extra_data={},
        )

        self.assertEqual(result, expected)


class TestResourceUriHelper(unittest.TestCase):
    """Test cases for the make_resource_uri helper function."""

    def test_make_resource_uri(self) -> None:
        """Test resource URL creation for various candidate inputs."""
        test_cases = [
            (
                ResourceCandidate(
                    name="Test Resource",
                    address="123 Main St, Brooklyn, NY 10001",
                ),
                "resource://test%20resource/123%20main%20st%20brooklyn%20ny%2010001",
                None,
            ),
            (
                ResourceCandidate(
                    name="Another Resource",
                    street="456 Broadway",
                    city="San Francisco",
                    state="CA",
                    zip="90210",
                ),
                "resource://another%20resource/456%20broadway%20francisco%20ca%2090210",
                None,
            ),
            (
                ResourceCandidate(
                    street="789 Elm St", city="Miami", state="FL", zip="73301"
                ),
                "resource://789%20elm%20st%20miami%20fl%2073301",
                None,
            ),
            (
                ResourceCandidate(name="Resource With No Address"),
                "resource://resource%20with%20no%20address",
                None,
            ),
            (
                ResourceCandidate(
                    name="Special &*() Resource",
                    street="123 Main St",
                    city="Brooklyn",
                    state="NY",
                    zip="02134",
                ),
                "resource://special%20%26%2A%28%29%20resource/123%20main%20st%20brooklyn%20ny%2002134",
                None,
            ),
            (
                ResourceCandidate(name="", street=None, city=None, address=""),
                "i will fail",  # Adjusted expectation with hyphens
                ValueError,
            ),
        ]
        for candidate, expected_url, expect_exception in test_cases:
            if expect_exception:
                with self.assertRaises(expect_exception):
                    make_resource_uri(candidate)
            else:
                result = make_resource_uri(candidate)
                self.assertEqual(result, expected_url)


if __name__ == "__main__":
    unittest.main()
