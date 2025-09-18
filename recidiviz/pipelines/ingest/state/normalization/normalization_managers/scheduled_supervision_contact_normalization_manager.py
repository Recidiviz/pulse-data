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
"""Contains the logic for a ScheduledSupervisionContactNormalizationManager that manages the
normalization of StateScheduledSupervisionContact entities in the calculation
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
from recidiviz.persistence.entity.state.entities import StateScheduledSupervisionContact
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateScheduledSupervisionContact,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.entity_normalization_manager import (
    EntityNormalizationManager,
)
from recidiviz.pipelines.ingest.state.normalization.normalized_entity_conversion_utils import (
    convert_entity_trees_to_normalized_versions,
)


class ScheduledSupervisionContactNormalizationManager(EntityNormalizationManager):
    """Interface for generalized normalization of StateScheduledSupervisionContacts for use in calculations."""

    def __init__(
        self,
        scheduled_supervision_contacts: List[StateScheduledSupervisionContact],
        staff_external_id_to_staff_id: Dict[Tuple[str, str], int],
    ) -> None:
        self._scheduled_supervision_contacts = deepcopy(scheduled_supervision_contacts)
        self._normalized_scheduled_supervision_contacts_and_additional_attributes: Optional[
            Tuple[List[StateScheduledSupervisionContact], AdditionalAttributesMap]
        ] = None
        self.staff_external_id_to_staff_id = staff_external_id_to_staff_id

    def get_normalized_scheduled_supervision_contacts(
        self,
    ) -> list[NormalizedStateScheduledSupervisionContact]:
        (
            processed_contacts,
            additional_attributes,
        ) = self.normalized_scheduled_supervision_contacts_and_additional_attributes()
        return convert_entity_trees_to_normalized_versions(
            processed_contacts,
            NormalizedStateScheduledSupervisionContact,
            additional_attributes,
        )

    def normalized_scheduled_supervision_contacts_and_additional_attributes(
        self,
    ) -> Tuple[List[StateScheduledSupervisionContact], AdditionalAttributesMap]:
        """Performs normalization on supervision contacts, currently empty,
        and returns the list of normalized StateScheduledSupervisionContacts."""
        if (
            not self._normalized_scheduled_supervision_contacts_and_additional_attributes
        ):
            # TODO(#19965): currently nothing happens here, will eventually hydrate new foreign key fields here
            contacts_for_normalization = deepcopy(self._scheduled_supervision_contacts)

            contacts_for_normalization = self.normalize_date_attributes(
                contacts_for_normalization
            )

            self._normalized_scheduled_supervision_contacts_and_additional_attributes = (
                contacts_for_normalization,
                self.additional_attributes_map_for_normalized_scs(
                    contacts_for_normalization
                ),
            )

        return self._normalized_scheduled_supervision_contacts_and_additional_attributes

    @staticmethod
    def normalized_entity_classes() -> List[Type[Entity]]:
        return [StateScheduledSupervisionContact]

    def normalize_date_attributes(
        self,
        scheduled_supervision_contacts: List[StateScheduledSupervisionContact],
    ) -> List[StateScheduledSupervisionContact]:
        """
        For the given list of StateScheduledSupervisionContact, updates each contact so that:
            * If either scheduled_contact_date or scheduled_contact_datetime is set, then both are set

        Returns the same list of StateScheduledSupervisionContact, each with modified dates.
        """

        for scheduled_supervision_contact in scheduled_supervision_contacts:
            if not scheduled_supervision_contact.scheduled_supervision_contact_id:
                raise ValueError(
                    "Expected non-null scheduled_supervision_contact_id values "
                    f"at this point. Found {scheduled_supervision_contact}."
                )

            scheduled_date = scheduled_supervision_contact.scheduled_contact_date
            scheduled_datetime = (
                scheduled_supervision_contact.scheduled_contact_datetime
            )

            # Handle scheduled contact date/datetime
            if scheduled_date and not scheduled_datetime:
                scheduled_supervision_contact.scheduled_contact_datetime = (
                    datetime.combine(scheduled_date, time.min)
                )
            elif scheduled_datetime and not scheduled_date:
                scheduled_supervision_contact.scheduled_contact_date = (
                    scheduled_datetime.date()
                )
            elif (
                scheduled_date
                and scheduled_datetime
                and scheduled_date != scheduled_datetime.date()
            ):
                raise ValueError(
                    f"For scheduled_supervision_contact_id {scheduled_supervision_contact.scheduled_supervision_contact_id}, "
                    f"found mismatched scheduled_contact_date {scheduled_date} and scheduled_contact_datetime {scheduled_datetime}."
                )

        return scheduled_supervision_contacts

    def additional_attributes_map_for_normalized_scs(
        self,
        scheduled_supervision_contacts: List[StateScheduledSupervisionContact],
    ) -> AdditionalAttributesMap:
        """Get additional attributes for each StateScheduledSupervisionContact."""

        shared_additional_attributes_map = (
            get_shared_additional_attributes_map_for_entities(
                entities=scheduled_supervision_contacts
            )
        )
        scheduled_supervision_contacts_additional_attributes_map: Dict[
            str, Dict[int, Dict[str, Any]]
        ] = {StateScheduledSupervisionContact.__name__: {}}

        for supervision_contact in scheduled_supervision_contacts:
            if not supervision_contact.scheduled_supervision_contact_id:
                raise ValueError(
                    "Expected non-null scheduled_supervision_contact_id values"
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
            scheduled_supervision_contacts_additional_attributes_map[
                StateScheduledSupervisionContact.__name__
            ][supervision_contact.scheduled_supervision_contact_id] = {
                "contacting_staff_id": contacting_staff_id
            }
        return merge_additional_attributes_maps(
            [
                shared_additional_attributes_map,
                scheduled_supervision_contacts_additional_attributes_map,
            ]
        )
