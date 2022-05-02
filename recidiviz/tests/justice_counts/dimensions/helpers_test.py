# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Tests for justice counts dimension helpers"""

from unittest import TestCase

from recidiviz.justice_counts.dimensions.helpers import DimensionParser
from recidiviz.justice_counts.dimensions.person import Gender


class DimensionParserTest(TestCase):
    """Tests for justice counts dimension helpers"""

    def test_parse_dimension_name(self) -> None:
        parser = DimensionParser()
        self.assertEqual(parser.get_dimension_for_name("GENDER"), Gender)

    def test_no_dimension(self) -> None:
        parser = DimensionParser()
        with self.assertRaisesRegex(KeyError, "No dimension"):
            parser.get_dimension_for_name("NO_DIMENSION_WITH_NAME")
