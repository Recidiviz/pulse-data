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
from datetime import datetime, time
from typing import Any, Dict, List, Optional, Tuple, Type

from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.normalized_entities_utils import (
    AdditionalAttributesMap,
    get_shared_additional_attributes_map_for_entities,
    merge_additional_attributes_maps,
)
from recidiviz.persistence.entity.state.entities import StateSupervisionContact
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateSupervisionContact,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.entity_normalization_manager import (
    EntityNormalizationManager,
)
from recidiviz.pipelines.ingest.state.normalization.normalized_entity_conversion_utils import (
    convert_entity_trees_to_normalized_versions,
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

    def get_normalized_supervision_contacts(
        self,
    ) -> list[NormalizedStateSupervisionContact]:
        (
            processed_contacts,
            additional_attributes,
        ) = self.normalized_supervision_contacts_and_additional_attributes()
        return convert_entity_trees_to_normalized_versions(
            processed_contacts, NormalizedStateSupervisionContact, additional_attributes
        )

    def normalized_supervision_contacts_and_additional_attributes(
        self,
    ) -> Tuple[List[StateSupervisionContact], AdditionalAttributesMap]:
        """Performs normalization on supervision contacts, currently empty,
        and returns the list of normalized StateSupervisionContacts."""
        if not self._normalized_supervision_contacts_and_additional_attributes:
            # TODO(#19965): currently nothing happens here, will eventually hydrate new foreign key fields here
            contacts_for_normalization = deepcopy(self._supervision_contacts)

            contacts_for_normalization = self.normalize_date_attributes(
                contacts_for_normalization
            )

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

    def normalize_date_attributes(
        self,
        supervision_contacts: List[StateSupervisionContact],
    ) -> List[StateSupervisionContact]:
        """
        For the given list of StateSupervisionContact, updates each contact so that:
            * if either contact_date or contact_datetime is set, then both are set

        Returns the same list of StateSupervisionContact, each with modified dates.
        """

        for supervision_contact in supervision_contacts:
            if not supervision_contact.supervision_contact_id:
                raise ValueError(
                    "Expected non-null supervision_contact_id values "
                    f"at this point. Found {supervision_contact}."
                )
            contact_date = supervision_contact.contact_date
            contact_datetime = supervision_contact.contact_datetime

            # Handle contact date/datetime
            if contact_date and not contact_datetime:
                # Convert date to datetime at midnight
                supervision_contact.contact_datetime = datetime.combine(
                    contact_date, time.min
                )
            elif contact_datetime and not contact_date:
                # Convert datetime to date (drop time)
                supervision_contact.contact_date = contact_datetime.date()
            elif (
                contact_date
                and contact_datetime
                and contact_date != contact_datetime.date()
            ):
                raise ValueError(
                    f"For supervision_contact_id {supervision_contact.supervision_contact_id}, "
                    f"found mismatched contact_date {contact_date} and contact_datetime {contact_datetime}."
                )

        return supervision_contacts

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
