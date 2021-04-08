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
        self.assertEqual(len(codes), 371)

    def test_get_all_codes_not_modifiable(self) -> None:
        codes = ncic.get_all_codes()

        with self.assertRaises(FrozenInstanceError):
            codes[0].description = "Bogus description change"  # type: ignore[misc]

    def test_get(self) -> None:
        code = ncic.get("2312")
        self.assertEqual(
            code,
            NcicCode(
                ncic_code="2312", description="THEFT OF CABLE TV", is_violent=False
            ),
        )

        last_code = ncic.get("7399")
        self.assertEqual(
            last_code,
            NcicCode(
                ncic_code="7399",
                description="PUBLIC ORDER CRIMES - (FREE TEXT)",
                is_violent=False,
            ),
        )

        first_code = ncic.get("0900")
        self.assertEqual(
            first_code,
            NcicCode(
                ncic_code="0900", description="HOMICIDE - (FREE TEXT)", is_violent=True
            ),
        )

    def test_get_not_found(self) -> None:
        self.assertIsNone(ncic.get("9999"))

    def test_get_description(self) -> None:
        description = ncic.get_description("1313")
        self.assertEqual(description, "SIMPLE ASSAULT")

    def test_get_description_not_found(self) -> None:
        self.assertIsNone(ncic.get_description("9999"))

    def test_get_is_violent(self) -> None:
        self.assertTrue(ncic.get_is_violent("1317"))
        self.assertFalse(ncic.get_is_violent("2007"))
        self.assertIsNotNone(ncic.get_is_violent("2007"))

    def test_get_is_violent_not_found(self) -> None:
        self.assertIsNone(ncic.get_is_violent("9999"))
