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
"""Constants for working with states."""

import enum
import re
from typing import Dict

import us

from recidiviz.utils import environment

STATE_CODE_PATTERN = re.compile(r"US_[A-Z]{2}")
TEST_STATE_CODE = "US_XX"

TEST_STATE_INFO = {
    TEST_STATE_CODE: us.states.State(
        **{
            "fips": "XX",
            "name": "Test State",
            "abbr": "XX",
            "is_territory": False,
            "is_obsolete": False,
            "is_contiguous": False,
            "is_continental": True,
            "statehood_year": 9999,
            "capital": "Test",
            "capital_tz": "America/Test",
            "ap_abbr": "Test",
            "time_zones": ["America/Test", "America/Test"],
            "name_metaphone": "TEST",
        }
    )
}


class StateCode(enum.Enum):
    """Code for every state in the US"""

    US_AK = "US_AK"
    US_AL = "US_AL"
    US_AR = "US_AR"
    US_AZ = "US_AZ"
    US_CA = "US_CA"
    US_CO = "US_CO"
    US_CT = "US_CT"
    US_DE = "US_DE"
    US_FL = "US_FL"
    US_GA = "US_GA"
    US_HI = "US_HI"
    US_IA = "US_IA"
    US_ID = "US_ID"
    US_IL = "US_IL"
    US_IN = "US_IN"
    US_KS = "US_KS"
    US_KY = "US_KY"
    US_LA = "US_LA"
    US_MA = "US_MA"
    US_MD = "US_MD"
    US_ME = "US_ME"
    US_MI = "US_MI"
    US_MN = "US_MN"
    US_MO = "US_MO"
    US_MS = "US_MS"
    US_MT = "US_MT"
    US_NC = "US_NC"
    US_ND = "US_ND"
    US_NE = "US_NE"
    US_NH = "US_NH"
    US_NJ = "US_NJ"
    US_NM = "US_NM"
    US_NV = "US_NV"
    US_NY = "US_NY"
    US_OH = "US_OH"
    US_OK = "US_OK"
    US_OR = "US_OR"
    US_PA = "US_PA"
    US_RI = "US_RI"
    US_SC = "US_SC"
    US_SD = "US_SD"
    US_TN = "US_TN"
    US_TX = "US_TX"
    US_UT = "US_UT"
    US_VA = "US_VA"
    US_VT = "US_VT"
    US_WA = "US_WA"
    US_WI = "US_WI"
    US_WV = "US_WV"
    US_WY = "US_WY"

    if environment.in_test():
        US_XX = TEST_STATE_CODE

    def get_state(self) -> us.states.State:
        return self._state_info_map()[self.value]

    @classmethod
    def _state_info_map(cls) -> Dict[str, us.states.State]:
        info_map = {}
        for e in cls:
            state_abbrev = e.value[len("US_") :]
            if hasattr(us.states, state_abbrev):
                info_map[e.value] = getattr(us.states, state_abbrev)
            elif e.value in TEST_STATE_INFO:
                info_map[e.value] = TEST_STATE_INFO[e.value]
            else:
                raise ValueError(
                    f"Unexpected state code [{e.value}] has no state info."
                )

        return info_map

    @classmethod
    def is_valid(cls, state_code: str) -> bool:
        return cls.is_state_code(state_code)

    @classmethod
    def is_state_code(cls, state_code: str) -> bool:
        try:
            StateCode(state_code.upper())
            return True
        except ValueError as _:
            return False
