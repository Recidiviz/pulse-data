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

from recidiviz.ingest.direct.regions.us_nd.county_code_reference import \
    normalized_county_code


def test_normalized_county_code():
    assert normalized_county_code('009') == 'US_ND_CASS'
    assert normalized_county_code('9') == 'US_ND_CASS'
    assert normalized_county_code('CA') == 'US_ND_CASS'


def test_normalized_county_code_states():
    assert normalized_county_code('OS') == 'OUT_OF_STATE'

    assert normalized_county_code('MINN') == 'US_MN'
    assert normalized_county_code('124') == 'US_MN'

    assert normalized_county_code('WY') == 'US_WY'
    assert normalized_county_code('153') == 'US_WY'


def test_normalized_county_code_rare_codes():
    assert normalized_county_code('154') == 'INTERNATIONAL'
    assert normalized_county_code('155') == 'ERROR'
    assert normalized_county_code('156') == 'PAROLE'


def test_normalized_county_code_no_input():
    assert normalized_county_code(None) is None


def test_normalized_county_code_input_not_found():
    assert normalized_county_code('199') is None
