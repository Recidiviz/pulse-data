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
from typing import Any, Callable, Dict, List, Optional

from recidiviz.common.attr_mixins import attr_field_referenced_cls_name_for_field_name
from recidiviz.persistence.entity.base_entity import CoreEntity
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    EntityFieldType,
    get_entity_class_in_module_with_name,
    get_many_to_many_relationships,
    is_one_to_one_relationship,
)
from recidiviz.persistence.entity.state import entities as state_entities


def serialize_entity_into_json(
    entity: CoreEntity,
    field_index: CoreEntityFieldIndex,
    back_edge_values: Optional[Dict[str, int]] = None,
) -> Dict[str, Any]:
    """Generate a JSON string of an entity's serialized flat field and backedge values.

    If |back_edge_values| is given, this means that the entity itself may not have its backedges
    populated, but that we have values for its backedges to be filled in at a later time.
    This is only used for serialization of new entities that are generated at normalization
    time.
    """
    flat_fields = field_index.get_all_core_entity_fields(
        entity.__class__, EntityFieldType.FLAT_FIELD
    )
    back_edges = field_index.get_all_core_entity_fields(
        entity.__class__, EntityFieldType.BACK_EDGE
    )
    for back_edge in back_edges:
        if is_one_to_one_relationship(entity.__class__, back_edge):
            raise ValueError(
                f"Unexpected one-to-one relationship here: {entity.__class__} {back_edge}"
            )

    many_to_many_relationships = get_many_to_many_relationships(
        entity.__class__, field_index
    )

    entity_field_dict: Dict[str, Any] = {
        **{field_name: getattr(entity, field_name) for field_name in flat_fields}
    }

    for field_name in back_edges:
        if field_name in many_to_many_relationships:
            continue
        id_field = get_entity_class_in_module_with_name(
            entities_module=state_entities,
            class_name=attr_field_referenced_cls_name_for_field_name(
                entity.__class__, field_name
            ),
        ).get_class_id_name()

        if getattr(entity, field_name):
            entity_field_dict[id_field] = getattr(entity, field_name).get_id()
        elif back_edge_values and field_name in back_edge_values:
            entity_field_dict[id_field] = back_edge_values[field_name]
        else:
            entity_field_dict[id_field] = None

    return json_serializable_dict(entity_field_dict)


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
