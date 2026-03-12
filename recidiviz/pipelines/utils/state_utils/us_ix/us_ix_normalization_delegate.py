# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Contains US_IX implementation of the StateSpecificNormalizationDelegate."""
from recidiviz.common.constants.state.external_id_types import (
    US_IX_CIS_EMPL_CD,
    US_IX_EMPLOYEE,
    US_IX_STAFF_ID,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import StateStaffExternalId
from recidiviz.pipelines.ingest.state.normalization.normalize_external_ids_helpers import (
    select_alphabetically_highest_external_id,
    select_alphabetically_lowest_external_id,
)
from recidiviz.pipelines.ingest.state.normalization.state_specific_normalization_delegate import (
    StateSpecificNormalizationDelegate,
)


class UsIxNormalizationDelegate(StateSpecificNormalizationDelegate):
    """US_IX implementation of the StateSpecificNormalizationDelegate."""

    def select_display_id_for_staff_external_ids_of_type(
        self,
        state_code: StateCode,
        staff_id: int,
        id_type: str,
        staff_external_ids_of_type: list[StateStaffExternalId],
    ) -> StateStaffExternalId:
        if id_type in (US_IX_CIS_EMPL_CD, US_IX_EMPLOYEE, US_IX_STAFF_ID):
            return select_alphabetically_highest_external_id(staff_external_ids_of_type)

        raise ValueError(
            f"Unexpected id type {id_type} with multiple ids per staff member "
            f"and no is_current_display_id_for_type set at ingest time: "
            f"{staff_external_ids_of_type}"
        )

    def select_stable_id_for_staff_external_ids_of_type(
        self,
        state_code: StateCode,
        staff_id: int,
        id_type: str,
        staff_external_ids_of_type: list[StateStaffExternalId],
    ) -> StateStaffExternalId:
        if id_type in (US_IX_CIS_EMPL_CD, US_IX_EMPLOYEE, US_IX_STAFF_ID):
            return select_alphabetically_lowest_external_id(staff_external_ids_of_type)

        raise ValueError(
            f"Unexpected id type {id_type} with multiple ids per staff member "
            f"and no is_stable_id_for_type set at ingest time: "
            f"{staff_external_ids_of_type}"
        )
