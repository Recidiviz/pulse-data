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
"""Tests for recidiviz/utils/user_hash.py."""
from unittest import TestCase

from recidiviz.utils.user_hash import (
    generate_user_hash,
    normalize_email,
    normalized_email_hash,
)


class TestNormalizeEmail(TestCase):
    """Tests for normalize_email."""

    def test_strips_surrounding_whitespace_and_casefolds(self) -> None:
        self.assertEqual("frodo@fake.com", normalize_email("  Frodo@Fake.COM "))

    def test_already_normalized_address_is_unchanged(self) -> None:
        self.assertEqual("frodo@fake.com", normalize_email("frodo@fake.com"))


class TestNormalizedEmailHash(TestCase):
    """Tests for normalized_email_hash."""

    def test_addresses_differing_only_in_case_or_whitespace_hash_the_same(
        self,
    ) -> None:
        self.assertEqual(
            normalized_email_hash("frodo@fake.com"),
            normalized_email_hash("  FRODO@Fake.com "),
        )

    def test_matches_generate_user_hash_of_the_normalized_address(self) -> None:
        self.assertEqual(
            generate_user_hash("frodo@fake.com"),
            normalized_email_hash("Frodo@Fake.com"),
        )

    def test_different_addresses_hash_differently(self) -> None:
        self.assertNotEqual(
            normalized_email_hash("frodo@fake.com"),
            normalized_email_hash("sam@fake.com"),
        )
