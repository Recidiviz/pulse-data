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
"""Utility classes for validating state entities and entity trees."""

from recidiviz.persistence.database_invariant_validator.state.state_person_invariant_validators import (
    state_allows_multiple_ids_same_type as state_allows_multiple_ids_same_type_for_state_person,
)
from recidiviz.persistence.database_invariant_validator.state.state_staff_invariant_validators import (
    state_allows_multiple_ids_same_type as state_allows_multiple_ids_same_type_for_state_staff,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.persistence_utils import RootEntityT


def validate_root_entity(root_entity: RootEntityT) -> RootEntityT:
    """The assumed input is a root entity with hydrated children entities attached to it.
    This function checks if the root entity does not violate any entity tree specific
    constraints. If the root entity constraints are not met, an exception should be thrown.

    TODO(#21564): Check that the root entities do not violate any entity tree constraints.
    This function should replicate the database_invariant_validator checks.
    """
    if len(root_entity.external_ids) == 0:
        raise ValueError(
            f"Found [{type(root_entity).__name__}] with id [{root_entity.get_id()}] missing an "
            f"external_id: {root_entity}"
        )

    if isinstance(root_entity, state_entities.StatePerson):
        allows_multiple_ids_same_type = (
            state_allows_multiple_ids_same_type_for_state_person(root_entity.state_code)
        )
    elif isinstance(root_entity, state_entities.StateStaff):
        allows_multiple_ids_same_type = (
            state_allows_multiple_ids_same_type_for_state_staff(root_entity.state_code)
        )
    else:
        raise ValueError(f"Unexpected root entity type: {type(root_entity).__name__}")

    if not allows_multiple_ids_same_type:
        external_id_types = set()
        for external_id in root_entity.external_ids:
            if external_id.id_type in external_id_types:
                raise ValueError(
                    f"Duplicate external id types for [{type(root_entity).__name__}] with id "
                    f"[{root_entity.get_id()}]: {external_id.id_type}"
                )

            external_id_types.add(external_id.id_type)

    return root_entity


def validate_entity(entity: Entity) -> Entity:
    """The assumed input is a child entity of a root entity. This function checks if the
    entity does not violate any entity-specific constraints. If the constraints are not
    met, an exception should be thrown.

    TODO(#21564): Check that the entity does not violate any entity-level constraints.
    This function should replicate the CheckConstraints in the entity schema."""
    return entity
