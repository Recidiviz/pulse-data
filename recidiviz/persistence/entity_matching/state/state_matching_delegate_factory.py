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

from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.persistence.entity_matching.state.base_state_matching_delegate import (
    BaseStateMatchingDelegate,
)
from recidiviz.persistence.entity_matching.state.us_co.us_co_matching_delegate import (
    UsCoMatchingDelegate,
)
from recidiviz.persistence.entity_matching.state.us_id.us_id_matching_delegate import (
    UsIdMatchingDelegate,
)
from recidiviz.persistence.entity_matching.state.us_me.us_me_matching_delegate import (
    UsMeMatchingDelegate,
)
from recidiviz.persistence.entity_matching.state.us_mi.us_mi_matching_delegate import (
    UsMiMatchingDelegate,
)
from recidiviz.persistence.entity_matching.state.us_mo.us_mo_matching_delegate import (
    UsMoMatchingDelegate,
)
from recidiviz.persistence.entity_matching.state.us_nd.us_nd_matching_delegate import (
    UsNdMatchingDelegate,
)
from recidiviz.persistence.entity_matching.state.us_oz.us_oz_matching_delegate import (
    UsOzMatchingDelegate,
)
from recidiviz.persistence.entity_matching.state.us_pa.us_pa_matching_delegate import (
    UsPaMatchingDelegate,
)
from recidiviz.persistence.entity_matching.state.us_tn.us_tn_matching_delegate import (
    UsTnMatchingDelegate,
)


class StateMatchingDelegateFactory:
    @staticmethod
    def build(
        *, region_code: str, ingest_metadata: IngestMetadata
    ) -> BaseStateMatchingDelegate:
        if region_code.upper() == "US_ID":
            return UsIdMatchingDelegate(ingest_metadata)
        if region_code.upper() == "US_MO":
            return UsMoMatchingDelegate(ingest_metadata)
        if region_code.upper() == "US_ND":
            return UsNdMatchingDelegate(ingest_metadata)
        if region_code.upper() == "US_PA":
            return UsPaMatchingDelegate(ingest_metadata)
        if region_code.upper() == "US_TN":
            return UsTnMatchingDelegate(ingest_metadata)
        if region_code.upper() == "US_ME":
            return UsMeMatchingDelegate(ingest_metadata)
        if region_code.upper() == "US_MI":
            return UsMiMatchingDelegate(ingest_metadata)
        if region_code.upper() == "US_CO":
            return UsCoMatchingDelegate(ingest_metadata)
        if region_code.upper() == "US_OZ":
            return UsOzMatchingDelegate(ingest_metadata)
        raise ValueError(f"Unexpected region_code provided: {region_code}.")
