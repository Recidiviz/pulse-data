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

"""Unit tests for the North Dakota county code reference cache."""
import unittest

from recidiviz.ingest.direct.regions.us_nd.us_nd_county_code_reference import \
    COUNTY_CODES, normalized_county_code


class TestUsNdCountyCodeReferenceTable(unittest.TestCase):
    """Unit tests for the North Dakota county code reference cache."""

    def test_normalized_county_code(self):
        assert normalized_county_code('009', COUNTY_CODES) == 'US_ND_CASS'
        assert normalized_county_code('9', COUNTY_CODES) == 'US_ND_CASS'
        assert normalized_county_code('CA', COUNTY_CODES) == 'US_ND_CASS'

    def test_normalized_county_code_states(self):
        assert normalized_county_code('OS', COUNTY_CODES) == 'OUT_OF_STATE'

        assert normalized_county_code('MINN', COUNTY_CODES) == 'US_MN'
        assert normalized_county_code('124', COUNTY_CODES) == 'US_MN'

        assert normalized_county_code('WY', COUNTY_CODES) == 'US_WY'
        assert normalized_county_code('153', COUNTY_CODES) == 'US_WY'

    def test_normalized_county_code_rare_codes(self):
        assert normalized_county_code('154', COUNTY_CODES) == 'INTERNATIONAL'
        assert normalized_county_code('155', COUNTY_CODES) == 'ERROR'
        assert normalized_county_code('156', COUNTY_CODES) == 'PAROLE'

    def test_normalized_county_code_no_input(self):
        assert normalized_county_code('', COUNTY_CODES) is None
        assert normalized_county_code(None, COUNTY_CODES) is None

    def test_normalized_county_code_input_not_found(self):
        assert normalized_county_code('199', COUNTY_CODES) == '199'
