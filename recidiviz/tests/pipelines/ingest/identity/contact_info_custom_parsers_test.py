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
"""Tests for the contact-info custom parsers."""
import unittest

from recidiviz.pipelines.ingest.identity.contact_info_custom_parsers import (
    normalize_phone_or_null,
    validate_email_or_null,
)


class TestNormalizePhoneOrNull(unittest.TestCase):
    """Tests normalize_phone_or_null."""

    def test_formatted_number_normalizes_to_bare_digits(self) -> None:
        self.assertEqual("7015550123", normalize_phone_or_null("(701) 555-0123"))

    def test_leading_country_code_stripped(self) -> None:
        # With and without the country code, the same number normalizes to the
        # same ten digits.
        self.assertEqual("7015550123", normalize_phone_or_null("+1 (701) 555-0123"))
        self.assertEqual("7015550123", normalize_phone_or_null("1-701-555-0123"))
        self.assertEqual("7015550123", normalize_phone_or_null("17015550123"))

    def test_eleven_digits_without_leading_one_is_null(self) -> None:
        self.assertIsNone(normalize_phone_or_null("27015550123"))

    def test_too_few_digits_is_null(self) -> None:
        self.assertIsNone(normalize_phone_or_null("999"))

    def test_empty_is_null(self) -> None:
        self.assertIsNone(normalize_phone_or_null(""))


class TestValidateEmailOrNull(unittest.TestCase):
    """Tests validate_email_or_null."""

    def test_valid_email_passes_through_unchanged(self) -> None:
        self.assertEqual(
            "ANN.SMITH@EXAMPLE.COM", validate_email_or_null("ANN.SMITH@EXAMPLE.COM")
        )

    def test_invalid_email_is_null(self) -> None:
        self.assertIsNone(validate_email_or_null("not-an-email"))

    def test_suspicious_username_is_null(self) -> None:
        self.assertIsNone(validate_email_or_null("none@example.com"))

    def test_empty_is_null(self) -> None:
        self.assertIsNone(validate_email_or_null(""))
