# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Contains the logic for a SupervisionContactNormalizationManager that manages the
normalization of StateSupervisionContact entities in the calculation
pipelines."""
from collections import defaultdict
from copy import deepcopy
from typing import List, Optional, Tuple, Type

from recidiviz.calculator.pipeline.normalization.utils.normalization_managers.entity_normalization_manager import (
    EntityNormalizationManager,
)
from recidiviz.calculator.pipeline.normalization.utils.normalized_entities_utils import (
    AdditionalAttributesMap,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state.entities import StateSupervisionContact


class SupervisionContactNormalizationManager(EntityNormalizationManager):
    """Interface for generalized normalization of StateSupervisionContacts for use in calculations."""

    def __init__(
        self,
        supervision_contacts: List[StateSupervisionContact],
    ) -> None:
        self._supervision_contacts = deepcopy(supervision_contacts)
        self._normalized_supervision_contacts_and_additional_attributes: Optional[
            Tuple[List[StateSupervisionContact], AdditionalAttributesMap]
        ] = None

    def normalized_supervision_contacts_and_additional_attributes(
        self,
    ) -> Tuple[List[StateSupervisionContact], AdditionalAttributesMap]:
        """Performs normalization on supervision contacts, currently empty,
        and returns the list of normalized StateSupervisionContacts."""
        if not self._normalized_supervision_contacts_and_additional_attributes:
            # TODO(#19965): currently nothing happens here, will eventually hydrate new foreign key fields here
            contacts_for_normalization = deepcopy(self._supervision_contacts)

            self._normalized_supervision_contacts_and_additional_attributes = (
                contacts_for_normalization,
                defaultdict(dict),
            )

        return self._normalized_supervision_contacts_and_additional_attributes

    @staticmethod
    def normalized_entity_classes() -> List[Type[Entity]]:
        return [StateSupervisionContact]
