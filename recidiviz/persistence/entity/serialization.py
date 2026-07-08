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
"""Utilities for serializing entities into JSON-serializable dictionaries."""
import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Sequence

from recidiviz.common.attr_mixins import (
    BuildableAttrFieldType,
    attribute_field_type_reference_for_class,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entities_module_context import EntitiesModuleContext
from recidiviz.persistence.entity.entity_field_index import EntityFieldType
from recidiviz.persistence.entity.entity_utils import (
    get_entity_class_in_module_with_name,
    sort_based_on_flat_fields,
)
from recidiviz.persistence.entity.root_entity_utils import (
    entity_class_for_foreign_key_column,
    get_root_entity_id,
)
from recidiviz.utils.types import assert_type


def extract_flat_fields(entity: Entity) -> Dict[str, Any]:
    """Extract all flat field values (non-reference fields) from an entity as
    raw Python values. Does not apply JSON serialization."""
    class_reference = attribute_field_type_reference_for_class(type(entity))
    return {
        field_name: getattr(entity, field_name)
        for field_name in class_reference.fields
        if not class_reference.get_field_info(field_name).referenced_cls_name
    }


def serialize_entity_into_json(
    entity: Entity, entities_module_context: EntitiesModuleContext
) -> Dict[str, Any]:
    """Generate a JSON-serializeable dictionary that represents the table row values
    for this entity.

    Which relationship fields produce an id column, and whose id that column
    holds, is determined by `entity_class_for_foreign_key_column`, the same
    resolver used to generate the BQ schemas for these tables (see
    `entities_bq_schema`). When a field's column resolves past an intermediate
    entity with no id of its own (e.g. `IdentityAttributes`) to the root, the
    column holds the root entity's id, found by walking the referenced entity's
    back edges up to the root.
    """
    entity_field_dict = extract_flat_fields(entity)

    entity_cls = entity.__class__
    class_reference = attribute_field_type_reference_for_class(entity_cls)
    for field_name in class_reference.fields:
        field_info = class_reference.get_field_info(field_name)
        if not field_info.referenced_cls_name:
            # This is a flat field
            continue

        foreign_key_entity_cls = entity_class_for_foreign_key_column(
            entities_module_context=entities_module_context,
            entity_cls=entity_cls,
            field_name=field_name,
        )
        if foreign_key_entity_cls is None:
            continue

        referenced_entity_cls = get_entity_class_in_module_with_name(
            entities_module_context.entities_module(), field_info.referenced_cls_name
        )
        id_field = foreign_key_entity_cls.get_class_id_name()
        id_value = None
        if referenced_entity := getattr(entity, field_name):
            referenced = assert_type(referenced_entity, Entity)
            id_value = (
                referenced.get_id()
                if foreign_key_entity_cls is referenced_entity_cls
                else get_root_entity_id(referenced)
            )
        entity_field_dict[id_field] = id_value

    return json_serializable_dict(entity_field_dict)


def serialize_entity_tree_into_json(
    entity: Entity,
    entities_module_context: EntitiesModuleContext,
) -> Dict[str, Any]:
    """Recursively serialize an entity and its forward-edge children into a
    JSON-serializable dict suitable for deterministic hashing.
    """
    result: Dict[str, Any] = json_serializable_dict(extract_flat_fields(entity))

    field_index = entities_module_context.field_index()
    entity_cls = type(entity)
    class_ref = attribute_field_type_reference_for_class(entity_cls)

    for field_name in field_index.get_all_entity_fields(
        entity_cls, EntityFieldType.FORWARD_EDGE
    ):
        field_info = class_ref.get_field_info(field_name)
        if field_info.field_type == BuildableAttrFieldType.COLLECTION:
            children = entity.get_field_as_list(field_name)
            sort_based_on_flat_fields(children, entities_module_context)
            result[field_name] = [
                serialize_entity_tree_into_json(child, entities_module_context)
                for child in children
            ]
        else:
            child = entity.get_field(field_name)
            result[field_name] = (
                serialize_entity_tree_into_json(child, entities_module_context)
                if child is not None
                else None
            )

    return result


def serialize_entity_trees_into_json(
    entities: Sequence[Entity],
    entities_module_context: EntitiesModuleContext,
) -> List[Dict[str, Any]]:
    """Sort a list of entities by flat fields and serialize each into a
    JSON-serializable dict via serialize_entity_tree_into_json."""
    sorted_entities = list(entities)
    sort_based_on_flat_fields(sorted_entities, entities_module_context)
    return [
        serialize_entity_tree_into_json(entity, entities_module_context)
        for entity in sorted_entities
    ]


def json_serializable_dict(
    element: Dict[str, Any],
    list_serializer: Optional[Callable[[str, List[Any]], str]] = None,
) -> Dict[str, Any]:
    """Converts a dictionary into a format that is JSON serializable.

    For values that are of type Enum, converts to their raw values. For values
    that are dates, converts to a string representation.

    If any of the fields are list types, must provide a |list_serializer| which will
    handle serializing list values to a serializable string value.
    """
    serializable_dict: Dict[str, Any] = {}

    for key, v in element.items():
        if isinstance(v, Enum):
            serializable_dict[key] = v.value
        elif isinstance(v, (datetime.date, datetime.datetime)):
            # By using isoformat, we are guaranteed a string in the form YYYY-MM-DD,
            # padded with leading zeros if necessary. For datetime values, the format
            # will be YYYY-MM-DDTHH:MM:SS, with an optional milliseconds component if
            # relevant.
            serializable_dict[key] = v.isoformat()
        elif isinstance(v, list):
            if not list_serializer:
                raise ValueError(
                    "Must provide list_serializer if there are list "
                    f"values in dict. Found list in key: [{key}]."
                )

            serializable_dict[key] = list_serializer(key, v)
        else:
            serializable_dict[key] = v
    return serializable_dict
