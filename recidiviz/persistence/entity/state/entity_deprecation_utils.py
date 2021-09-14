# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Utils for deprecating fields on state Entity classes, or Entity classes entirely."""
from typing import List

from recidiviz.common.attr_mixins import (
    BuildableAttrFieldType,
    attr_field_type_for_field_name,
)
from recidiviz.persistence.entity.base_entity import Entity, EnumEntity


def _get_state_code_from_entity(entity: Entity) -> str:
    """Returns the value of the state_code attribute on the entity.

    Raises an error if the provided |entity| doesn't have a state_code attribute.
    """
    if not (state_code := getattr(entity, "state_code")):
        raise ValueError(
            "This entity deprecation function should only be called from "
            "__attrs_post_init__ functions on state entities in "
            "state/entities.py"
        )

    return state_code


def validate_deprecated_entity_field_for_states(
    entity: Entity,
    field_name: str,
    deprecated_state_codes: List[str],
) -> None:
    """Validates that an entity field or relationship is not in use for the state_code
    on the given |entity| if it has been fully deprecated for that state.

    Raises a ValueError if the field/relationship is deprecated for the state,
    and the value of the deprecated field is not None.

    If there is a corresponding _raw_text field associated with the field, validates
    that it is deprecated as well.

    This should only be called from __attrs_post_init__ functions.
    """
    state_code = _get_state_code_from_entity(entity)

    if state_code not in deprecated_state_codes:
        return

    attr_field_type = attr_field_type_for_field_name(type(entity), field_name)

    deprecated_field_names = [field_name]

    if attr_field_type == BuildableAttrFieldType.ENUM:
        raw_text_field_name = field_name + EnumEntity.RAW_TEXT_FIELD_SUFFIX
        if not hasattr(entity, raw_text_field_name):
            raise ValueError(
                f"Entity class {entity} missing raw text field "
                f"corresponding to enum field {field_name}."
            )

        # Validate that the associated raw text field for this entity is also
        # deprecated
        deprecated_field_names.append(raw_text_field_name)

    field_type_description = (
        "relationship"
        if attr_field_type == BuildableAttrFieldType.FORWARD_REF
        else "field"
    )

    for deprecated_field in deprecated_field_names:
        field_value = getattr(entity, deprecated_field)

        invalid_set_field = (
            field_value is not None
            if attr_field_type != BuildableAttrFieldType.LIST
            else field_value != []
        )

        if invalid_set_field:
            raise ValueError(
                f"The [{deprecated_field}] {field_type_description} is deprecated for "
                f"state_code: [{state_code}]. This {field_type_description} should not "
                "be populated."
            )


def validate_deprecated_entity_for_states(
    entity: Entity,
    deprecated_state_codes: List[str],
) -> None:
    """Validates that an entity is not in use for the state_code on the given
    |entity| if it has been fully deprecated for that state.

    Raises a ValueError if the entity is deprecated for the state.

    This should only be called from __attrs_post_init__ functions.
    """
    state_code = _get_state_code_from_entity(entity)

    if state_code in deprecated_state_codes:
        raise ValueError(
            f"The {type(entity).__name__} entity is deprecated for "
            f"state_code: [{state_code}]. This entity should not be instantiated."
        )
