# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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

"""Unit tests for ncic.py"""

import unittest

from attr.exceptions import FrozenInstanceError

from recidiviz.common import ncic
from recidiviz.common.ncic import NcicCode


class NcicTest(unittest.TestCase):
    """Unit tests for NCIC functionality."""

    def test_get_all_codes(self) -> None:
        codes = ncic.get_all_codes()
        # Ensure that we have the expected number of codes
        self.assertEqual(len(codes), 458)
        # Ensure that all codes are unique
        self.assertEqual(len(codes), len(set(codes)))

    def test_get_all_codes_not_modifiable(self) -> None:
        codes = ncic.get_all_codes()

        with self.assertRaises(FrozenInstanceError):
            codes[0].description = "Bogus description change"  # type: ignore[misc]

    def test_get(self) -> None:
        code = ncic.get("2312")
        self.assertEqual(
            code,
            NcicCode(
                ncic_code="2312",
                category_code="23",
                category="LARCENY",
                description="LARC - FROM INTERSTATE SHIPMENT",
                is_drug=False,
                is_violent=False,
            ),
        )

        last_code = ncic.get("7399")
        self.assertEqual(
            last_code,
            NcicCode(
                ncic_code="7399",
                category_code="73",
                category="GENERAL CRIMES",
                description="PUBLIC ORDER CRIMES - (SPECIFY)",
                is_drug=False,
                is_violent=False,
            ),
        )

        first_code = ncic.get("0999")
        self.assertEqual(
            first_code,
            NcicCode(
                ncic_code="0999",
                category_code="09",
                category="HOMICIDE",
                description="HOMICIDE",
                is_drug=False,
                is_violent=True,
            ),
        )

    def test_get_not_found(self) -> None:
        self.assertIsNone(ncic.get("9999"))

    def test_get_category_code(self) -> None:
        description = ncic.get_category_code("1313")
        self.assertEqual(description, "13")

    def test_get_category_code_not_found(self) -> None:
        self.assertIsNone(ncic.get_category_code("9999"))

    def test_get_category(self) -> None:
        description = ncic.get_category("1313")
        self.assertEqual(description, "ASSAULT")

    def test_get_category_not_found(self) -> None:
        self.assertIsNone(ncic.get_category("9999"))

    def test_get_description(self) -> None:
        description = ncic.get_description("1313")
        self.assertEqual(description, "SIMPLE ASSLT")

    def test_get_description_not_found(self) -> None:
        self.assertIsNone(ncic.get_description("9999"))

    def test_get_is_drug(self) -> None:
        self.assertTrue(ncic.get_is_drug("3501"))
        self.assertFalse(ncic.get_is_drug("2007"))
        self.assertIsNotNone(ncic.get_is_drug("2007"))

    def test_get_is_drug_not_found(self) -> None:
        self.assertIsNone(ncic.get_is_drug("9999"))

    def test_get_is_violent(self) -> None:
        self.assertTrue(ncic.get_is_violent("1301"))
        self.assertFalse(ncic.get_is_violent("2007"))
        self.assertIsNotNone(ncic.get_is_violent("2007"))

    def test_get_is_violent_not_found(self) -> None:
        self.assertIsNone(ncic.get_is_violent("9999"))
