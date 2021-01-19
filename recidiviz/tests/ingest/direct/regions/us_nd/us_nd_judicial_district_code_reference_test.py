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
"""Tests for us_nd_judicial_code_reference_table.py."""
import unittest

from recidiviz.ingest.direct.regions.us_nd.\
    us_nd_judicial_district_code_reference import \
    normalized_judicial_district_code


class TestUsNdJudicialDistrictCodeReferenceTable(unittest.TestCase):
    """Tests for us_nd_judicial_code_reference_table.py."""

    def test_normalized_judicial_district_code(self) -> None:
        self.assertEqual(normalized_judicial_district_code('FD'),
                         'FEDERAL')
        self.assertEqual(normalized_judicial_district_code('NC'),
                         'NORTH_CENTRAL')
        self.assertEqual(normalized_judicial_district_code('OS'),
                         'OUT_OF_STATE')
        self.assertEqual(normalized_judicial_district_code('OOS'),
                         'OUT_OF_STATE')

    def test_normalized_judicial_district_code_unmapped(self) -> None:
        self.assertEqual(normalized_judicial_district_code('ABC'), 'ABC')

    def test_normalized_judicial_district_code_empty(self) -> None:
        self.assertIsNone(normalized_judicial_district_code(''))
        self.assertIsNone(normalized_judicial_district_code(None))
