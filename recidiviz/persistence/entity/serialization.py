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
from types import ModuleType
from typing import Any, Callable, Dict, List, Optional, Type

from recidiviz.common.attr_mixins import attribute_field_type_reference_for_class
from recidiviz.persistence.entity.base_entity import Entity, RootEntity
from recidiviz.persistence.entity.entity_utils import (
    entities_have_direct_relationship,
    get_entity_class_in_module_with_name,
    is_many_to_many_relationship,
    is_many_to_one_relationship,
    is_one_to_many_relationship,
)
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state.normalized_entities import (
    state_base_entity_class_for_entity_class,
)
from recidiviz.utils.types import assert_type


def _related_entity_id_field_lives_on_entity(
    entity_cls: Type[Entity], referenced_entity_cls: Type[Entity]
) -> bool:
    """For a class |entity_cls| and class |referenced_entity_cls| that is referenced by
    one of the fields on |entity_cls|, returns True if the relationship between
    these two entities is encoded as an id field on the table for |entity_cls|.

    For example, for entity_cls=StatePerson, referenced_entity_cls=StateAssessment,
    returns False because the relationship between StatePerson and StateAssessment is
    one-to-many, meaning there is no assessment_id field stored on the state_person
    table. For entity_cls=StateAssessment, referenced_entity_cls=StatePerson, this would
    return True because there is a person_id field on the state_assessment table.
    """
    # TODO(#30075): Once we move to v2 normalization, we should be able to just use
    #  entity_cls and referenced_entity_cls directly to determine relationship type.
    entity_cls_for_comparison = state_base_entity_class_for_entity_class(entity_cls)
    referenced_entity_cls_for_comparison = state_base_entity_class_for_entity_class(
        referenced_entity_cls
    )
    if entities_have_direct_relationship(
        entity_cls_for_comparison, referenced_entity_cls_for_comparison
    ):
        if is_many_to_many_relationship(
            entity_cls_for_comparison, referenced_entity_cls_for_comparison
        ) or is_one_to_many_relationship(
            entity_cls_for_comparison, referenced_entity_cls_for_comparison
        ):
            return False

        if is_many_to_one_relationship(
            entity_cls_for_comparison, referenced_entity_cls_for_comparison
        ):
            # For many-to-one relationships, we expect the id field of the related
            # entity to be stored on the table for this entity.
            return True
        raise ValueError(
            f"Found unexpected relationship type between "
            f"[{entity_cls.__name__}] and [{referenced_entity_cls.__name__}]."
        )
    if issubclass(referenced_entity_cls_for_comparison, RootEntity):
        # For indirect relationships to the root entity, we expect the root entity
        # id to be set on this entity.
        return True

    raise ValueError(
        f"Found relationship between [{entity_cls.__name__}] and "
        f"[{referenced_entity_cls.__name__}] which is neither a direct "
        f"relationship or root entity reference."
    )


def serialize_entity_into_json(
    entity: Entity,
    # TODO(#30075): Make this a required field and set to normalized_entities where
    #  appropriate.
    entities_module: ModuleType = state_entities,
) -> Dict[str, Any]:
    """Generate a JSON dictionary that represents the table row values for this entity."""
    entity_field_dict: Dict[str, Any] = {}

    entity_cls = entity.__class__
    for field_name, field_info in attribute_field_type_reference_for_class(
        entity_cls
    ).items():
        if not field_info.referenced_cls_name:
            # This is a flat field
            entity_field_dict[field_name] = getattr(entity, field_name)
            continue

        referenced_entity_cls = get_entity_class_in_module_with_name(
            entities_module, field_info.referenced_cls_name
        )

        if not _related_entity_id_field_lives_on_entity(
            entity_cls, referenced_entity_cls
        ):
            continue

        id_field = referenced_entity_cls.get_class_id_name()
        id_value = None
        if referenced_entity := getattr(entity, field_name):
            id_value = assert_type(referenced_entity, Entity).get_id()
        entity_field_dict[id_field] = id_value

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
