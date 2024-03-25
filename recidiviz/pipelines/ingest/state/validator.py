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

from collections import defaultdict
from typing import Any, Dict, List, Tuple, Type

from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    get_all_entities_from_tree,
)
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.persistence_utils import RootEntityT
from recidiviz.pipelines.ingest.state.constants import EntityKey, Error
from recidiviz.utils.types import assert_type


def state_allows_multiple_ids_same_type_for_state_person(state_code: str) -> bool:
    if state_code.upper() in (
        "US_ND",
        "US_PA",
        "US_MI",
        "US_OR",
    ):  # TODO(#18005): Edit to allow multiple id for OR id_number but not Record_key
        return True

    # By default, states don't allow multiple different ids of the same type
    return False


def state_allows_multiple_ids_same_type_for_state_staff(state_code: str) -> bool:
    if state_code.upper() in ("US_MI", "US_IX"):
        return True

    # By default, states don't allow multiple different ids of the same type
    return False


def validate_root_entity(
    root_entity: RootEntityT, field_index: CoreEntityFieldIndex
) -> List[Error]:
    """The assumed input is a root entity with hydrated children entities attached to it.
    This function checks if the root entity does not violate any entity tree specific
    constraints. If the root entity constraints are not met, an exception should be thrown.
    """
    error_messages: List[Error] = []

    if len(root_entity.external_ids) == 0:
        error_messages.append(
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
        error_messages.append(
            f"Unexpected root entity type: {type(root_entity).__name__}"
        )

    if not allows_multiple_ids_same_type:
        external_id_types = set()
        for external_id in root_entity.external_ids:
            if external_id.id_type in external_id_types:
                error_messages.append(
                    f"Duplicate external id types for [{type(root_entity).__name__}] with id "
                    f"[{root_entity.get_id()}]: {external_id.id_type}"
                )

            external_id_types.add(external_id.id_type)

    child_entities = get_all_entities_from_tree(root_entity, field_index=field_index)

    entities_by_cls: Dict[Type[Entity], List[Entity]] = defaultdict(list)
    for child in child_entities:
        entities_by_cls[type(child)].append(child)

    for entity_cls, entities_of_type in entities_by_cls.items():
        for constraint in entity_cls.entity_tree_unique_constraints():
            seen_tuples: Dict[Tuple[Any, ...], List[Entity]] = defaultdict(list)

            for entity in entities_of_type:
                value_tup = tuple(getattr(entity, field) for field in constraint.fields)
                seen_tuples[value_tup].append(entity)

            for value_tup, entities in seen_tuples.items():
                if len(entities) == 1:
                    continue
                tuple_str = ", ".join(
                    f"{field}={value_tup[i]}"
                    for i, field in enumerate(constraint.fields)
                )
                error_msg = (
                    f"Found [{len(entities)}] {entity_cls.get_entity_name()} entities "
                    f"with ({tuple_str})"
                )

                entities_to_show = min(3, len(entities))
                entities_str = "\n  * ".join(
                    e.limited_pii_repr() for e in entities[:entities_to_show]
                )
                error_msg += (
                    f". First {entities_to_show} entities found:\n  * {entities_str}"
                )
                error_messages.append(error_msg)

    return error_messages


def get_entity_key(entity: Entity) -> EntityKey:
    return (
        assert_type(entity.get_id(), int),
        assert_type(entity.get_entity_name(), str),
    )
