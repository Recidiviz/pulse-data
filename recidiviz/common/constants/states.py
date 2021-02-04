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

import us

STATE_CODE_PATTERN = re.compile(r'US_[A-Z]{2}')


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

    def get_state(self) -> us.states.State:
        # pylint: disable=unsubscriptable-object
        return getattr(us.states, self.value[len('US_'):])

    @classmethod
    def is_valid(cls, state_code: str) -> bool:
        # TODO(#5508): Have this actually check it's one of the valid states once we have a way to handle tests with
        #  US_XX etc in them. For now, this just checks that it is upper case and matches the correct pattern.
        return bool(re.match(STATE_CODE_PATTERN, state_code))

    @classmethod
    def is_state_code(cls, state_code: str) -> bool:
        try:
            StateCode(state_code.upper())
            return True
        except ValueError as _:
            return False
