# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
from typing import Any, List, Optional, SupportsInt, Union

class State:
    abbr: str
    ap_abbr: Optional[str]
    capital: Optional[str]
    capital_tz: Optional[str]
    fips: Union[str, SupportsInt]
    is_territory: bool
    is_obsolete: bool
    is_contiguous: bool
    is_continental: bool
    name: str
    name_metaphone: str
    statehood_year: Optional[int]
    time_zones: List[str]
    def __init__(self, **kwargs: Any):
        for k, v in kwargs.items():
            setattr(self, k, v)

def lookup(
    val: Union[int, str], field: Optional[str] = None, use_cache: bool = True
) -> State: ...

AK: State
CA: State
FL: State
GA: State
IL: State
IN: State
KY: State
NY: State
PA: State
TN: State
TX: State
