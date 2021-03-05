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
"""Tests for fips.py"""
from recidiviz.common import fips


def test_getStateAndCountyForFips_removes_city_suffix_properly():
    [state1, county1] = fips.get_state_and_county_for_fips("32510")
    [state2, county2] = fips.get_state_and_county_for_fips("51510")
    assert county1 == "carson_city".upper()
    assert state1 == "US_NV"
    assert county2 == "alexandria".upper()
    assert state2 == "US_VA"
