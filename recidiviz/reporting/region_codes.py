# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Provides a mapping from region code shorthand to region or district name."""

REGION_CODES = {
    'US_ID_D3': 'DISTRICT OFFICE 3, CALDWELL',
    'US_ID_D5': 'DISTRICT OFFICE 5, TWIN FALLS',
    'US_ID_D7': 'DISTRICT OFFICE 7, IDAHO FALLS',
}


class InvalidRegionCodeException(Exception):
    """Raised when a region code is provided that doesn't map to a known one."""
