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
"""Tests for organization_type.py."""
from unittest import TestCase

from recidiviz.documents.extraction.models.reference_data.organization_type import (
    OrganizationType,
)


class OrganizationTypeTest(TestCase):
    """Tests for the OrganizationType enum."""

    def test_description_defined_for_every_value(self) -> None:
        # Every member must resolve to a meaningful description; a new member with
        # no `description` branch would hit the `raise` and fail here.
        for organization_type in OrganizationType:
            description = organization_type.description
            self.assertIsInstance(description, str)
            self.assertTrue(description.strip())

    def test_descriptions_are_unique(self) -> None:
        descriptions = [
            organization_type.description for organization_type in OrganizationType
        ]
        self.assertEqual(len(descriptions), len(set(descriptions)))
