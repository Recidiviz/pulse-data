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
"""A static reference cache for converting raw incoming codes into normalized
judicial district codes."""

import logging
from typing import Optional

_JUDICIAL_DISTRICT_CODE_MAPPINGS = {
    # ND Jurisdictions
    "SE": "SOUTHEAST",
    "EAST": "EAST_CENTRAL",
    "SC": "SOUTH_CENTRAL",
    "NW": "NORTHWEST",
    "NE": "NORTHEAST",
    "SW": "SOUTHWEST",
    "NEC": "NORTHEAST_CENTRAL",
    "NC": "NORTH_CENTRAL",
    # Non-ND Jurisidctions
    "OOS": "OUT_OF_STATE",
    "OS": "OUT_OF_STATE",
    "FD": "FEDERAL",
}


def normalized_judicial_district_code(
    judicial_district_code: Optional[str],
) -> Optional[str]:
    if not judicial_district_code:
        return None

    if judicial_district_code not in _JUDICIAL_DISTRICT_CODE_MAPPINGS:
        logging.warning(
            "Found new judicial district code not in reference cache: [%s]",
            judicial_district_code,
        )
        return judicial_district_code

    return _JUDICIAL_DISTRICT_CODE_MAPPINGS[judicial_district_code]
