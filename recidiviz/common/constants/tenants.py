# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Constants for working with tenants in the Recidiviz data platform.

A Tenant identifies a segment of data that can be independently namespaced /
isolated from other data. In practice, this usually identifies the jurisdiction
or organization we got the data from.

Every StateCode value has a corresponding Tenant value (e.g.
`Tenant.US_PA.value == StateCode.US_PA.value == "US_PA"`). Tenant additionally
contains non-state values like RECIDIVIZ for entities the Identity Service
tracks that are not tied to a state partner.

This module mirrors recidiviz/common/constants/states.py: a `_SharedTenant`
base, separate `_RealTenant` and `_FakeTenant` subclasses, environment
dispatch via `__getattr__`, and a TYPE_CHECKING-only `Tenant` stub.
"""

import enum
import typing
from typing import Any

from recidiviz.common.constants.states import StateCode
from recidiviz.utils import environment

TenantT = typing.TypeVar("TenantT", bound="_SharedTenant")


class _SharedTenant(enum.Enum):
    """Mixin for Tenant functionality so the real and fake classes behave the
    same. Parallel to `_SharedStateCode` in states.py."""

    def to_state_code(self) -> StateCode:
        """Returns the StateCode this Tenant corresponds to.

        Raises ValueError for non-state Tenants (e.g. RECIDIVIZ).
        """
        return StateCode(self.value)

    @classmethod
    def from_state_code(cls: typing.Type[TenantT], state_code: StateCode) -> TenantT:
        """Returns the Tenant corresponding to the given StateCode."""
        return cls(state_code.value)


class _RealTenant(_SharedTenant):
    """Tenant values available in production: every _RealStateCode value plus
    non-state tenants like RECIDIVIZ."""

    # Real codes
    US_AK = "US_AK"
    US_AL = "US_AL"
    US_AR = "US_AR"
    US_AS = "US_AS"
    US_AZ = "US_AZ"
    US_CA = "US_CA"
    US_CO = "US_CO"
    US_CT = "US_CT"
    US_DC = "US_DC"
    US_DE = "US_DE"
    US_FL = "US_FL"
    US_GA = "US_GA"
    US_GU = "US_GU"
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
    US_MP = "US_MP"
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
    US_PR = "US_PR"
    US_RI = "US_RI"
    US_SC = "US_SC"
    US_SD = "US_SD"
    US_TN = "US_TN"
    US_TX = "US_TX"
    US_UM = "US_UM"
    US_UT = "US_UT"
    US_VA = "US_VA"
    US_VI = "US_VI"
    US_VT = "US_VT"
    US_WA = "US_WA"
    US_WI = "US_WI"
    US_WV = "US_WV"
    US_WY = "US_WY"

    # Playground code
    US_OZ = "US_OZ"

    # Alternate Ingest
    # TODO(#10703): Remove this Tenant after merging US_IX into US_ID
    US_IX = "US_IX"

    # Demo states
    US_DEMO = "US_DEMO"

    # Non-state tenants
    RECIDIVIZ = "RECIDIVIZ"


class _FakeTenant(_SharedTenant):
    """Tenant values available in tests: every _RealTenant value plus the test
    codes (US_DD, US_LL, US_WW, US_XX, US_YY) defined in
    _FakeStateCode."""

    # Real codes
    US_AK = "US_AK"
    US_AL = "US_AL"
    US_AR = "US_AR"
    US_AS = "US_AS"
    US_AZ = "US_AZ"
    US_CA = "US_CA"
    US_CO = "US_CO"
    US_CT = "US_CT"
    US_DC = "US_DC"
    US_DE = "US_DE"
    US_FL = "US_FL"
    US_GA = "US_GA"
    US_GU = "US_GU"
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
    US_MP = "US_MP"
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
    US_PR = "US_PR"
    US_RI = "US_RI"
    US_SC = "US_SC"
    US_SD = "US_SD"
    US_TN = "US_TN"
    US_TX = "US_TX"
    US_UM = "US_UM"
    US_UT = "US_UT"
    US_VA = "US_VA"
    US_VI = "US_VI"
    US_VT = "US_VT"
    US_WA = "US_WA"
    US_WI = "US_WI"
    US_WV = "US_WV"
    US_WY = "US_WY"

    # Playground code
    US_OZ = "US_OZ"

    # Alternate Ingest
    # TODO(#10703): Remove this Tenant after merging US_IX into US_ID
    US_IX = "US_IX"

    # Demo states
    US_DEMO = "US_DEMO"

    # Test codes
    US_DD = "US_DD"
    US_LL = "US_LL"
    US_WW = "US_WW"
    US_XX = "US_XX"
    US_YY = "US_YY"

    # Non-state tenants
    RECIDIVIZ = "RECIDIVIZ"


# This is used as a fallback for module lookup. Any attributes that actually
# exist will not be handled by this method, but it allows us to dynamically
# add the Tenant attribute. If we are type checking this will not be used
# since Tenant is defined below.
def __getattr__(name: str) -> Any:
    if name == "Tenant":
        if environment.in_test():
            return _FakeTenant
        return _RealTenant
    raise AttributeError(f"tenants module has no attribute '{name}'")


if typing.TYPE_CHECKING:
    # For type checking, `Tenant` is the full set of possible values, i.e.
    # `_FakeTenant` (the superset that also includes the test-only codes). We
    # import the class under the `Tenant` name rather than assigning
    # `Tenant = _FakeTenant`, because a plain assignment alias makes mypy treat
    # `Tenant` as EITHER a type or a value, but not both:
    #   - `Tenant = _FakeTenant` (or `Tenant: TypeAlias = _FakeTenant`) makes
    #     it a type, so `Tenant["US_AK"]` is parsed as a generic type
    #     application rather than `Enum.__getitem__`.
    #   - `Tenant: Type[_FakeTenant] = _FakeTenant` makes it a value, so it is
    #     no longer usable as a type annotation.
    # Importing the class under an alias name re-binds it fully, so `Tenant`
    # works as a subscriptable/callable/iterable value AND as a type
    # annotation, without re-listing every member. This is a
    # TYPE_CHECKING-only self-import: it never executes at runtime (where
    # `__getattr__` above resolves `Tenant`), so there is no circular import.
    # See https://github.com/python/mypy/issues/7568.
    # pylint: disable=import-self
    from recidiviz.common.constants.tenants import _FakeTenant as Tenant
