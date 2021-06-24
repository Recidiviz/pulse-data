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
import typing
from abc import abstractmethod
from typing import Any, Dict, Optional

import us

from recidiviz.utils import environment

STATE_CODE_PATTERN = re.compile(r"US_[A-Z]{2}")


class _SharedStateCode(enum.Enum):
    """Mixin for StateCode functionality so that the real and fake classes behave the same."""

    def get_state(self) -> us.states.State:
        return self._state_info_map()[self.value]

    @classmethod
    @abstractmethod
    def _inner_get_state(cls, state_code: str) -> Optional[us.states.State]:
        pass

    @classmethod
    def _state_info_map(cls) -> Dict[str, us.states.State]:
        info_map = {}
        for e in cls:
            if state_info := cls._inner_get_state(e.value):
                info_map[e.value] = state_info
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
            cls(state_code.upper())
            return True
        except ValueError as _:
            return False


class _RealStateCode(_SharedStateCode):
    """Code for every state in the US"""

    US_AK = "US_AK"
    US_AL = "US_AL"
    US_AR = "US_AR"
    US_AZ = "US_AZ"
    US_CA = "US_CA"
    US_CO = "US_CO"
    US_CT = "US_CT"
    US_DC = (
        "US_DC"  # Counties in DC have a fips "state" code, which is why we need it here
    )
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

    @classmethod
    def _inner_get_state(cls, state_code: str) -> Optional[us.states.State]:
        state_abbrev = state_code[len("US_") :]
        return getattr(us.states, state_abbrev, None)


TEST_STATE_CODE = "US_XX"
TEST_STATE_CODE_DOCS = "US_WW"

"""
    US_XX serves as a generic placeholder state for any state that is to be ingested and
    follows the current format for all newly ingested states.

    US_WW is used within the certain unit tests testing documentation generation in order to test
    an edge case with how raw file directories are set up.

    NOTE: Consider using US_XX for tests unless you need to test a specific edge case that is different
    from all other cases so far for ingest. If the latter, a corresponding folder in
    recidiviz/tests/ingest/direct/fake_regions/ should be added.
"""
TEST_STATE_INFO = {
    TEST_STATE_CODE: us.states.State(
        **{
            "fips": "99",
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
    ),
    TEST_STATE_CODE_DOCS: us.states.State(
        **{
            "fips": "88",
            "name": "Test State",
            "abbr": "WW",
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
    ),
}


class _FakeStateCode(_SharedStateCode):
    """Code for every state in the US, plus codes to only be used in tests"""

    # Real codes
    US_AK = "US_AK"
    US_AL = "US_AL"
    US_AR = "US_AR"
    US_AZ = "US_AZ"
    US_CA = "US_CA"
    US_CO = "US_CO"
    US_CT = "US_CT"
    US_DC = (
        "US_DC"  # Counties in DC have a fips "state" code, which is why we need it here
    )
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

    # Test codes
    US_WW = TEST_STATE_CODE_DOCS
    US_XX = TEST_STATE_CODE

    @classmethod
    def _inner_get_state(cls, state_code: str) -> Optional[us.states.State]:
        # pylint: disable=protected-access
        return _RealStateCode._inner_get_state(state_code) or TEST_STATE_INFO.get(
            state_code
        )


# This is used as a fallback for module lookup. Any attributes that actually exist will
# not be handled by this method, but it allows us to dynamically add the 'StateCode'
# attribute. If we are type checking this will not be used since StateCode is defined
# below.
def __getattr__(name: str) -> Any:
    if name == "StateCode":
        if environment.in_test():
            return _FakeStateCode
        return _RealStateCode
    raise AttributeError(f"states module has no attribute '{name}")


if typing.TYPE_CHECKING:
    # If we are type checking, give it a class with all of the codes.
    # TODO(python/mypy#7568): We have to define an actual class here instead of aliasing
    # _FakeStateCode because mypy doesn't understand type aliases for enums. Once that
    # issue is fixed, we can just do `StateCode = _FakeStateCode` here since it already
    # includes all possible state codes.
    class StateCode(_SharedStateCode):
        """All possible state codes, to be used for type checking."""

        # Real codes
        US_AK = "US_AK"
        US_AL = "US_AL"
        US_AR = "US_AR"
        US_AZ = "US_AZ"
        US_CA = "US_CA"
        US_CO = "US_CO"
        US_CT = "US_CT"
        US_DC = "US_DC"  # Counties in DC have a fips "state" code, which is why we need it here
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

        # Test codes
        US_WW = TEST_STATE_CODE_DOCS
        US_XX = TEST_STATE_CODE

        @classmethod
        def _inner_get_state(cls, state_code: str) -> Optional[us.states.State]:
            return None
