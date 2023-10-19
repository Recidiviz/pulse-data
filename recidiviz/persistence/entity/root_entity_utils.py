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
from types import ModuleType
from typing import Dict, Type, cast

from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.state import schema as state_schema
from recidiviz.persistence.database.schema_entity_converter.schema_to_entity_class_mapper import (
    SchemaToEntityClassMapper,
)
from recidiviz.persistence.entity.base_entity import Entity, RootEntity
from recidiviz.persistence.entity.core_entity import CoreEntity
from recidiviz.persistence.entity.entity_utils import get_all_entity_classes_in_module
from recidiviz.persistence.entity.state import entities as state_entities


def is_root_entity(entity_cls: Type[CoreEntity]) -> bool:
    """Returns whether the provided class is a root entity type."""
    if issubclass(entity_cls, DatabaseEntity):
        class_mapper = SchemaToEntityClassMapper.get(
            schema_module=state_schema, entities_module=state_entities
        )
        return issubclass(
            class_mapper.entity_cls_for_schema_cls(entity_cls), RootEntity
        )
    return issubclass(entity_cls, RootEntity)


def get_root_entity_id(entity: CoreEntity) -> int:
    """Returns the id of the root entity the entity is associated with."""

    entity_cls = type(entity)
    if is_root_entity(entity_cls):
        return entity.get_id()

    root_entity_cls = get_root_entity_class_for_entity(entity_cls)
    return entity.get_field(root_entity_cls.back_edge_field_name()).get_id()


def get_root_entity_class_for_entity(entity_cls: Type[CoreEntity]) -> Type[RootEntity]:
    """Returns the RootEntity class the provided |entity_cls| is associated with."""

    for root_entity_cls in RootEntity.__subclasses__():
        associated_fields = [
            root_entity_cls.back_edge_field_name(),
            cast(CoreEntity, root_entity_cls).get_class_id_name(),
        ]

        for field in associated_fields:
            if entity_cls.has_field(field):
                return root_entity_cls

    raise ValueError(
        f"Expected non root entity {entity_cls} to have a field associated with a root "
        f"entity type but found none."
    )


def get_entity_class_name_to_root_entity_class_name(
    entities_module: ModuleType,
) -> Dict[str, str]:
    """Returns a dictionary that maps from entity name (e.g. 'state_assessment')
    to the corresponding root entity name (e.g. 'state_person').
    """
    result = {}
    for entity_cls in get_all_entity_classes_in_module(entities_module):
        root_entity_class = get_root_entity_class_for_entity(entity_cls)
        if not issubclass(root_entity_class, Entity):
            raise ValueError(
                f"Expected root_entity_class to be an Entity subclass, "
                f"found: {root_entity_class}"
            )
        result[entity_cls.get_entity_name()] = root_entity_class.get_entity_name()
    return result
