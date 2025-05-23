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
"""Interface for state-specific decisions about running normalization logic in ingest.
"""
import abc

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state.entities import StatePersonExternalId
from recidiviz.persistence.entity.state.normalized_state_entity import (
    NormalizedStateEntity,
)
from recidiviz.pipelines.ingest.state.normalization.normalize_external_ids_helpers import (
    select_single_external_id_with_is_current_display_id,
)
from recidiviz.pipelines.ingest.state.validator import (
    person_external_id_types_with_allowed_multiples_per_person,
)
from recidiviz.pipelines.utils.state_utils.state_specific_delegate import (
    StateSpecificDelegate,
)


class StateSpecificNormalizationDelegate(abc.ABC, StateSpecificDelegate):
    """Interface for state-specific decisions about running normalization logic in
    ingest.
    """

    def select_display_id_for_person_external_ids_of_type(
        self,
        state_code: StateCode,
        person_id: int,
        id_type: str,
        # pylint: disable=unused-argument
        person_external_ids_of_type: list[StatePersonExternalId],
    ) -> StatePersonExternalId:
        """Given a list of external_ids of the given |id_type|, returns the one that
        should be used as the "display" external id in products that display person
        external_ids of this type.

        States with id types that have multiple of that type per person will need to
        provide a state-specific implementation of this delegate method.
        """
        if id_type not in person_external_id_types_with_allowed_multiples_per_person(
            state_code
        ):
            raise ValueError(
                f"Person external_id type [{id_type}] should never have multiple ids "
                f"of a given type, but found multiple for person [{person_id}]. We "
                f"should have never called "
                f"select_display_id_for_person_external_ids_of_type(). If you expect "
                f"that a single person can have multiple {id_type} ids, update the "
                f"exemptions list in "
                f"person_external_id_types_with_allowed_multiples_per_person()."
            )
        has_any_is_display_id_flags_set = any(
            pei.is_current_display_id_for_type is not None
            for pei in person_external_ids_of_type
        )

        if has_any_is_display_id_flags_set:
            return select_single_external_id_with_is_current_display_id(
                person_external_ids_of_type
            )

        raise NotImplementedError(
            f"No implementation of "
            f"select_display_id_for_person_external_ids_of_type(), but called for "
            f"ids of type [{id_type}] in state [{state_code.value}]. For id types that "
            f"allow multiple per person, this function must be implemented in the "
            f"appropriate state-specific subclass of "
            f"StateSpecificNormalizationDelegate OR is_current_display_id_for_type "
            f"must be set to True at ingest time for exactly one external id of this "
            f"type per person."
        )

    def extra_entities_generated_via_normalization(
        self,
        normalization_input_types: set[type[Entity]],  # pylint: disable=unused-argument
    ) -> set[type[NormalizedStateEntity]]:
        """Returns a list of Normalized* entity types that are *only* produced by
        the normalization step of ingest pipelines and are not produced directly by
        any ingest views / mappings.

        Args:
            normalization_input_types: The entity types that are produced by the
              ingest mappings step and are inputs to the normalization step.
        """
        return set()
