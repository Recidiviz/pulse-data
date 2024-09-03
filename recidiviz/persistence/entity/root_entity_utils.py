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

from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.state import schema as state_schema
from recidiviz.persistence.database.schema_entity_converter.schema_to_entity_class_mapper import (
    SchemaToEntityClassMapper,
)
from recidiviz.persistence.entity.base_entity import Entity, RootEntity
from recidiviz.persistence.entity.entity_utils import (
    get_all_entity_classes_in_module,
    module_for_module_name,
)
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.utils.types import assert_subclass


def is_root_entity(entity_cls: Type[Entity]) -> bool:
    """Returns whether the provided class is a root entity type."""
    if issubclass(entity_cls, DatabaseEntity):
        class_mapper = SchemaToEntityClassMapper.get(
            schema_module=state_schema, entities_module=state_entities
        )
        return issubclass(
            class_mapper.entity_cls_for_schema_cls(entity_cls), RootEntity
        )
    return issubclass(entity_cls, RootEntity)


def get_root_entity_id(entity: Entity) -> int:
    """Returns the id of the root entity the entity is associated with."""

    entity_cls = type(entity)
    if is_root_entity(entity_cls):
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
    """Returns the RootEntity class the provided |entity_cls| is associated with."""

    found_root_entities = []
    for root_entity_cls in get_root_entity_classes_in_module(
        module_for_module_name(entity_cls.__module__)
    ):
        associated_fields = [
            root_entity_cls.back_edge_field_name(),
            assert_subclass(root_entity_cls, Entity).get_class_id_name(),
        ]

        for field in associated_fields:
            if entity_cls.has_field(field):
                found_root_entities.append(root_entity_cls)

    if not found_root_entities:
        raise ValueError(
            f"Expected non root entity {entity_cls} to have a field associated with a root "
            f"entity type but found none."
        )
    return one(found_root_entities)


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
