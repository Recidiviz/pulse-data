# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Contains the logic for a StaffRolePeriodNormalizationManager that manages the normalization of
StateStaffRolePeriod entities in the calculation pipelines."""
from copy import deepcopy
from typing import List, Optional, Tuple, Type

from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.normalized_entities_utils import (
    AdditionalAttributesMap,
)
from recidiviz.persistence.entity.state.entities import StateStaffRolePeriod
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateStaffRolePeriod,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.entity_normalization_manager import (
    EntityNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalized_entity_conversion_utils import (
    convert_entity_trees_to_normalized_versions,
)
from recidiviz.pipelines.utils.state_utils.state_specific_delegate import (
    StateSpecificDelegate,
)


class StateSpecificStaffRolePeriodNormalizationDelegate(StateSpecificDelegate):
    """Interface for state-specific decisions involved in normalization of staff entities for
    calculations."""

    def normalize_role_periods(
        self, role_periods: List[StateStaffRolePeriod]
    ) -> List[StateStaffRolePeriod]:
        """Performs normalization on staff role periods, currently empty, and returns the list
        of normalized StateStaffRolePeriod."""
        return role_periods


class StaffRolePeriodNormalizationManager(EntityNormalizationManager):
    """Interface for generalized normalization of StateStaffRolePeriod for use in calculations."""

    def __init__(
        self,
        staff_role_periods: List[StateStaffRolePeriod],
        delegate: StateSpecificStaffRolePeriodNormalizationDelegate,
    ) -> None:
        self._staff_role_periods = deepcopy(staff_role_periods)
        self.delegate = delegate
        self._normalized_staff_role_periods_and_additional_attributes: Optional[
            Tuple[List[StateStaffRolePeriod], AdditionalAttributesMap]
        ] = None

    def get_normalized_role_periods(
        self,
    ) -> list[NormalizedStateStaffRolePeriod]:
        (
            processed_staff_role_periods,
            additional_staff_role_period_attributes,
        ) = self.normalized_staff_role_periods_and_additional_attributes()
        return convert_entity_trees_to_normalized_versions(
            processed_staff_role_periods,
            NormalizedStateStaffRolePeriod,
            additional_staff_role_period_attributes,
        )

    def normalized_staff_role_periods_and_additional_attributes(
        self,
    ) -> Tuple[List[StateStaffRolePeriod], AdditionalAttributesMap]:
        """Performs normalization on state staff, currently empty, and returns the list
        of normalized StateStaff."""
        if not self._normalized_staff_role_periods_and_additional_attributes:
            staff_role_periods_for_normalization = deepcopy(self._staff_role_periods)
            normalized_staff_role_periods = self.delegate.normalize_role_periods(
                staff_role_periods_for_normalization
            )
            self._normalized_staff_role_periods_and_additional_attributes = (
                normalized_staff_role_periods,
                self.additional_attributes_map_for_normalized_staff_role_periods(
                    normalized_staff_role_periods
                ),
            )
        return self._normalized_staff_role_periods_and_additional_attributes

    @staticmethod
    def normalized_entity_classes() -> List[Type[Entity]]:
        return [StateStaffRolePeriod]

    def additional_attributes_map_for_normalized_staff_role_periods(
        self, staff_role_periods_for_normalization: List[StateStaffRolePeriod]
    ) -> AdditionalAttributesMap:
        """Returns the attributes that should be set on the normalized version of each of
        the StateStaffRolePeriod for each of the attributes that are unique to the
        NormalizedStateStaffRolePeriod."""
        additional_attributes: AdditionalAttributesMap = {
            StateStaffRolePeriod.__name__: {}
        }
        for staff_role_period in staff_role_periods_for_normalization:
            if not staff_role_period.staff_role_period_id:
                raise ValueError(
                    f"StaffRolePeriod {staff_role_period} has no staff_role_period_id."
                )
            additional_attributes[StateStaffRolePeriod.__name__][
                staff_role_period.staff_role_period_id
            ] = {}
        return additional_attributes
