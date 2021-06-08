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
"""Contains factory class for creating StateMatchingDelegate objects"""

from recidiviz.persistence.entity_matching.state.base_state_matching_delegate import (
    BaseStateMatchingDelegate,
)
from recidiviz.persistence.entity_matching.state.us_id.us_id_matching_delegate import (
    UsIdMatchingDelegate,
)
from recidiviz.persistence.entity_matching.state.us_mo.us_mo_matching_delegate import (
    UsMoMatchingDelegate,
)
from recidiviz.persistence.entity_matching.state.us_nd.us_nd_matching_delegate import (
    UsNdMatchingDelegate,
)
from recidiviz.persistence.entity_matching.state.us_pa.us_pa_matching_delegate import (
    UsPaMatchingDelegate,
)
from recidiviz.persistence.entity_matching.state.us_tn.us_tn_matching_delegate import (
    UsTnMatchingDelegate,
)


class StateMatchingDelegateFactory:
    @classmethod
    def build(cls, *, region_code) -> BaseStateMatchingDelegate:
        if region_code.upper() == "US_ID":
            return UsIdMatchingDelegate()
        if region_code.upper() == "US_MO":
            return UsMoMatchingDelegate()
        if region_code.upper() == "US_ND":
            return UsNdMatchingDelegate()
        if region_code.upper() == "US_PA":
            return UsPaMatchingDelegate()
        if region_code.upper() == "US_TN":
            return UsTnMatchingDelegate()
        raise ValueError(f"Unexpected region_code provided: {region_code}.")
