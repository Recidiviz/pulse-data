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
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple, Type

from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.normalized_entities_utils import (
    AdditionalAttributesMap,
    get_shared_additional_attributes_map_for_entities,
    merge_additional_attributes_maps,
)
from recidiviz.persistence.entity.state.entities import StateSupervisionContact
from recidiviz.pipelines.normalization.utils.normalization_managers.entity_normalization_manager import (
    EntityNormalizationManager,
)


class SupervisionContactNormalizationManager(EntityNormalizationManager):
    """Interface for generalized normalization of StateSupervisionContacts for use in calculations."""

    def __init__(
        self,
        supervision_contacts: List[StateSupervisionContact],
        staff_external_id_to_staff_id: Dict[Tuple[str, str], int],
    ) -> None:
        self._supervision_contacts = deepcopy(supervision_contacts)
        self._normalized_supervision_contacts_and_additional_attributes: Optional[
            Tuple[List[StateSupervisionContact], AdditionalAttributesMap]
        ] = None
        self.staff_external_id_to_staff_id = staff_external_id_to_staff_id

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
                self.additional_attributes_map_for_normalized_scs(
                    contacts_for_normalization
                ),
            )

        return self._normalized_supervision_contacts_and_additional_attributes

    @staticmethod
    def normalized_entity_classes() -> List[Type[Entity]]:
        return [StateSupervisionContact]

    def additional_attributes_map_for_normalized_scs(
        self,
        supervision_contacts: List[StateSupervisionContact],
    ) -> AdditionalAttributesMap:
        """Get additional attributes for each StateSupervisionContact."""

        shared_additional_attributes_map = (
            get_shared_additional_attributes_map_for_entities(
                entities=supervision_contacts
            )
        )
        supervision_contacts_additional_attributes_map: Dict[
            str, Dict[int, Dict[str, Any]]
        ] = {StateSupervisionContact.__name__: {}}

        for supervision_contact in supervision_contacts:
            if not supervision_contact.supervision_contact_id:
                raise ValueError(
                    "Expected non-null supervision_contact_id values"
                    f"at this point. Found {supervision_contact}."
                )

            contacting_staff_id = None
            if supervision_contact.contacting_staff_external_id:
                if not supervision_contact.contacting_staff_external_id_type:
                    raise ValueError(
                        f"Found no contacting_staff_external_id_type for contacting_staff_external_id "
                        f"{supervision_contact.contacting_staff_external_id} on person "
                        f"{supervision_contact.person}"
                    )
                contacting_staff_id = self.staff_external_id_to_staff_id[
                    (
                        supervision_contact.contacting_staff_external_id,
                        supervision_contact.contacting_staff_external_id_type,
                    )
                ]
            supervision_contacts_additional_attributes_map[
                StateSupervisionContact.__name__
            ][supervision_contact.supervision_contact_id] = {
                "contacting_staff_id": contacting_staff_id
            }
        return merge_additional_attributes_maps(
            [
                shared_additional_attributes_map,
                supervision_contacts_additional_attributes_map,
            ]
        )
