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
"""Tests for known_organization_reference_data_entry.py."""
from unittest import TestCase

from recidiviz.documents.extraction.models.reference_data.known_organization_reference_data_entry import (
    KnownOrganizationReferenceDataEntry,
)
from recidiviz.documents.extraction.models.reference_data.organization_type import (
    OrganizationType,
)
from recidiviz.utils.yaml_dict import YAMLDict


class KnownOrganizationReferenceDataEntryTest(TestCase):
    """Tests for KnownOrganizationReferenceDataEntry."""

    def test_from_yaml_dict_with_aliases(self) -> None:
        self.assertEqual(
            KnownOrganizationReferenceDataEntry(
                name="The Green Dragon Inn",
                organization_type=OrganizationType.EMPLOYER,
                aliases=["Green Dragon", "GD Inn"],
            ),
            KnownOrganizationReferenceDataEntry.from_yaml_dict(
                YAMLDict(
                    {
                        "name": "The Green Dragon Inn",
                        "organization_type": "employer",
                        "aliases": ["Green Dragon", "GD Inn"],
                    }
                )
            ),
        )

    def test_from_yaml_dict_without_aliases(self) -> None:
        # Omitted aliases resolve to an empty list.
        self.assertEqual(
            KnownOrganizationReferenceDataEntry(
                name="The Forsaken Inn",
                organization_type=OrganizationType.HOTEL_MOTEL,
                aliases=[],
            ),
            KnownOrganizationReferenceDataEntry.from_yaml_dict(
                YAMLDict(
                    {"name": "The Forsaken Inn", "organization_type": "hotel_motel"}
                )
            ),
        )

    def test_dedup_key(self) -> None:
        entry = KnownOrganizationReferenceDataEntry(
            name="The Prancing Pony",
            organization_type=OrganizationType.EMPLOYER,
            aliases=[],
        )
        self.assertEqual("The Prancing Pony", entry.dedup_key)

    def test_parse_unknown_organization_type_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "'not_a_type' is not a valid OrganizationType"
        ):
            KnownOrganizationReferenceDataEntry.from_yaml_dict(
                YAMLDict({"name": "X", "organization_type": "not_a_type"})
            )

    def test_parse_empty_alias_raises(self) -> None:
        with self.assertRaisesRegex(ValueError, "String value should not be empty."):
            KnownOrganizationReferenceDataEntry.from_yaml_dict(
                YAMLDict(
                    {
                        "name": "X",
                        "organization_type": "employer",
                        "aliases": ["ok", ""],
                    }
                )
            )

    def test_parse_empty_name_raises(self) -> None:
        with self.assertRaisesRegex(ValueError, "String value should not be empty."):
            KnownOrganizationReferenceDataEntry.from_yaml_dict(
                YAMLDict({"name": "", "organization_type": "employer"})
            )

    def test_parse_unexpected_key_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Found unexpected config values for known organization \[X\]:",
        ):
            KnownOrganizationReferenceDataEntry.from_yaml_dict(
                YAMLDict({"name": "X", "organization_type": "employer", "extra": "y"})
            )
