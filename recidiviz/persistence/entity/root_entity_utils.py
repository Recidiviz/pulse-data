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
"""Utils for managing root entities."""
from functools import cache
from types import ModuleType
from typing import Dict, Set, Type

from more_itertools import one

from recidiviz.persistence.entity.base_entity import Entity, RootEntity
from recidiviz.persistence.entity.entity_utils import (
    get_all_entity_classes_in_module,
    module_for_module_name,
)
from recidiviz.utils.types import assert_subclass


def get_root_entity_id(entity: Entity) -> int:
    """Returns the id of the root entity the entity is associated with."""

    entity_cls = type(entity)
    if issubclass(entity_cls, RootEntity):
        return entity.get_id()

    root_entity_cls = get_root_entity_class_for_entity(entity_cls)
    return entity.get_field(root_entity_cls.back_edge_field_name()).get_id()


def get_root_entity_classes_in_module(
    entities_module: ModuleType,
) -> Set[Type[RootEntity]]:
    return {
        e
        for e in get_all_entity_classes_in_module(entities_module)
        if issubclass(e, RootEntity)
    }


def get_root_entity_class_for_entity(entity_cls: Type[Entity]) -> Type[RootEntity]:
    """Returns the RootEntity class the provided |entity_cls| is associated with.

    Attempts to do this directly first, via a back-edge field named
    `root.back_edge_field_name()` or the root's primary-key id column. Some
    entity trees, though, connect a child to its root only through an intermediate
    (non-root) entity. For example, identity fragment demographic entities
    back-edge to `IdentityAttributes`, which in turn back-edges to the
    `IdentityFragment` root. In this case, it falls back to walking up through
    intermediate parents until it reaches the root.
    """
    if direct_roots := _root_entity_classes_via_back_edge_or_id(entity_cls):
        return one(direct_roots)

    if intermediate_roots := _root_entity_classes_via_intermediate_parents(entity_cls):
        return one(intermediate_roots)

    raise ValueError(
        f"Expected non root entity [{entity_cls}] to have a field associated with "
        f"a root entity type but found none."
    )


def _root_entity_classes_via_back_edge_or_id(
    entity_cls: Type[Entity],
) -> list[Type[RootEntity]]:
    """Returns the root entity classes |entity_cls| references directly, via a
    back-edge field or the root's primary-key id column."""
    found_root_entities = []
    for root_entity_cls in get_root_entity_classes_in_module(
        module_for_module_name(entity_cls.__module__)
    ):
        fields = [
            root_entity_cls.back_edge_field_name(),
            assert_subclass(root_entity_cls, Entity).get_class_id_name(),
        ]

        for field in fields:
            if entity_cls.has_field(field):
                found_root_entities.append(root_entity_cls)

    return found_root_entities


def _root_entity_classes_via_intermediate_parents(
    entity_cls: Type[Entity],
) -> set[Type[RootEntity]]:
    """Returns the distinct root entity classes reachable from |entity_cls| by
    walking back edges up through intermediate (non-root) parent entities.
    """
    found_root_entities = set()
    for parent_cls in get_all_entity_classes_in_module(
        module_for_module_name(entity_cls.__module__)
    ):
        # We identify parents via back_edge_field_name() here rather than the
        # module context's field index (EntityFieldType.BACK_EDGE) to avoid
        # a circular import.
        if parent_cls is entity_cls or not hasattr(parent_cls, "back_edge_field_name"):
            continue
        if entity_cls.has_field(parent_cls.back_edge_field_name()):
            found_root_entities.add(get_root_entity_class_for_entity(parent_cls))
    return found_root_entities


@cache
def get_entity_class_to_root_entity_class(
    entities_module: ModuleType,
) -> Dict[type[Entity], type[Entity]]:
    """Returns a dictionary that maps from entity class to the corresponding root entity
    class.
    """
    result = {}
    for entity_cls in get_all_entity_classes_in_module(entities_module):
        root_entity_class = get_root_entity_class_for_entity(entity_cls)
        if not issubclass(root_entity_class, Entity):
            raise ValueError(
                f"Expected root_entity_class to be an Entity subclass, "
                f"found: {root_entity_class}"
            )
        result[entity_cls] = assert_subclass(root_entity_class, Entity)
    return result
