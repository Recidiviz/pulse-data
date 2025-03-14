# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Utils for the conversion to/from NormalizedStateEntity objects."""
from collections import defaultdict, deque
from typing import Dict, List, Optional, Sequence, Set, Tuple, Type

import attr

from recidiviz.common.attr_mixins import (
    BuildableAttrFieldType,
    attr_field_name_storing_referenced_cls_name,
    attr_field_referenced_cls_name_for_field_name,
    attr_field_type_for_field_name,
)
from recidiviz.persistence.entity.base_entity import Entity, RootEntity
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_entity,
)
from recidiviz.persistence.entity.entity_field_index import EntityFieldType
from recidiviz.persistence.entity.entity_utils import (
    get_entity_class_in_module_with_name,
    update_reverse_references_on_related_entities,
)
from recidiviz.persistence.entity.normalized_entities_utils import (
    AdditionalAttributesMap,
    NormalizedStateEntityT,
    get_base_entity_class_for_normalized_entity,
    update_forward_references_on_updated_entity,
)
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.persistence.entity.state.normalized_state_entity import (
    NormalizedStateEntity,
)
from recidiviz.utils.types import assert_subclass

NormalizedEntityKey = Tuple[Type[NormalizedStateEntity], int]

# Cached _unique_fields_reference value
_unique_fields_reference: Optional[Dict[Type[NormalizedStateEntity], Set[str]]] = None


def convert_entity_trees_to_normalized_versions(
    root_entities: Sequence[Entity],
    normalized_entity_class: Type[NormalizedStateEntityT],
    additional_attributes_map: AdditionalAttributesMap,
) -> List[NormalizedStateEntityT]:
    """Converts each of the |root_entities| and all related entities in
    the tree hanging off of each entity to the Normalized versions of the entity.

    The |additional_attributes_map| is a dictionary in the following format:

        additional_attributes_map = {
            StateIncarcerationPeriod.__name__: {
                111: {"purpose_for_incarceration_subtype": "XYZ"},
                222: {"purpose_for_incarceration_subtype": "AAA"},
            }
        }

    It stores attributes that are unique to the Normalized version of an entity and the
    values that should be set on these fields for certain entities. In the above
    example, the StateIncarcerationPeriod with the incarceration_period_id of 111
    will have a `purpose_for_incarceration_subtype` value of 'XYZ' on its Normalized
    version.

    Returns a list of normalized root entities, with hydrated relationships to the
    normalized versions of their whole entity trees.
    """
    converted_entities: List[NormalizedStateEntityT] = []

    if not root_entities:
        return converted_entities

    normalized_entities_by_id: Dict[
        NormalizedEntityKey, NormalizedStateEntity
    ] = defaultdict()

    forward_relationships_by_id_and_field: Dict[
        NormalizedEntityKey,
        Dict[Tuple[str, bool], List[NormalizedEntityKey]],
    ] = defaultdict(lambda: defaultdict(list))

    reverse_relationships_by_id_and_field: Dict[
        NormalizedEntityKey,
        Dict[
            Tuple[str, BuildableAttrFieldType],
            List[NormalizedEntityKey],
        ],
    ] = defaultdict(lambda: defaultdict(list))

    stack = deque(
        [(normalized_entity_class, root_entity) for root_entity in root_entities]
    )

    # Discover all of the relationships and normalized entities
    while len(stack) > 0:
        (normalized_base_entity_class, base_entity) = stack.popleft()
        entities_module_context = entities_module_context_for_entity(base_entity)
        state_field_index = entities_module_context.field_index()

        normalized_base_entity_class_name = normalized_base_entity_class.__name__

        # Verify the base entity and normalized entity are compatible
        base_entity_cls = type(base_entity)
        base_entity_cls_name = base_entity_cls.__name__
        if normalized_base_entity_class.base_class_name() != base_entity_cls_name:
            raise ValueError(
                f"Trying to convert entities of type {base_entity_cls} to "
                f"the type {normalized_base_entity_class}, which is not the "
                "normalized version of the class."
            )

        base_entity_key: Tuple[Type[NormalizedStateEntityT], int] = (
            normalized_base_entity_class,
            base_entity.get_id(),
        )
        normalized_base_entity_builder = normalized_base_entity_class.builder()
        normalized_base_entity_attributes = base_entity.__dict__
        additional_args_for_base_entity = additional_attributes_map.get(
            base_entity_cls_name, {}
        ).get(base_entity.get_id())

        if additional_args_for_base_entity:
            normalized_base_entity_attributes.update(additional_args_for_base_entity)

        # Set all flat fields and unique fields to normalized class first on the builder
        flat_fields = state_field_index.get_all_entity_fields(
            type(base_entity), EntityFieldType.FLAT_FIELD
        )
        unique_fields = fields_unique_to_normalized_class(normalized_base_entity_class)
        fields_to_set_or_traverse = flat_fields | unique_fields

        for field, field_value in normalized_base_entity_attributes.items():
            if field not in fields_to_set_or_traverse:
                continue

            setattr(normalized_base_entity_builder, field, field_value)

        # Build the normalized entity
        normalized_entities_by_id[
            base_entity_key
        ] = normalized_base_entity_builder.build()

        # Find all of the relationships
        forward_fields = state_field_index.get_all_entity_fields(
            type(base_entity), EntityFieldType.FORWARD_EDGE
        )
        for field in forward_fields:
            field_value = normalized_base_entity_attributes[field]
            if not field_value:
                continue

            related_entity_cls_name = attr_field_referenced_cls_name_for_field_name(
                normalized_base_entity_class, field
            )
            if not related_entity_cls_name:
                continue

            referenced_normalized_class: Type[NormalizedStateEntity] = assert_subclass(
                get_entity_class_in_module_with_name(
                    normalized_entities, related_entity_cls_name
                ),
                NormalizedStateEntity,
            )

            if issubclass(referenced_normalized_class, RootEntity):
                continue

            field_is_list = isinstance(field_value, list)
            related_entities: Sequence[Entity] = (
                field_value if field_is_list else [field_value]
            )

            reverse_relationship_field = attr_field_name_storing_referenced_cls_name(
                base_cls=referenced_normalized_class,
                referenced_cls_name=normalized_base_entity_class_name,
            )

            reverse_relationship_field_type = (
                attr_field_type_for_field_name(
                    referenced_normalized_class, reverse_relationship_field
                )
                if reverse_relationship_field
                else None
            )

            for related_entity in related_entities:
                related_entity_key = (
                    referenced_normalized_class,
                    related_entity.get_id(),
                )
                if related_entity_key not in normalized_entities_by_id:
                    stack.append(
                        (
                            referenced_normalized_class,
                            related_entity,
                        )  # type: ignore
                    )

                forward_relationships_by_id_and_field[base_entity_key][
                    field, field_is_list
                ].append(related_entity_key)
                if reverse_relationship_field and reverse_relationship_field_type:
                    reverse_relationships_by_id_and_field[related_entity_key][
                        (
                            reverse_relationship_field,
                            reverse_relationship_field_type,
                        )
                    ].append(base_entity_key)

    # At this point all of the entities are normalized, but need to be connected to their
    # appropriate relationships
    for key, normalized_entity in normalized_entities_by_id.items():
        for (
            forward_field_key,
            forward_entity_keys,
        ) in forward_relationships_by_id_and_field[key].items():
            field, field_is_list = forward_field_key
            forward_entities: List[NormalizedStateEntity] = [
                normalized_entities_by_id[related_entity_key]
                for related_entity_key in forward_entity_keys
            ]
            update_forward_references_on_updated_entity(
                updated_entity=normalized_entity,
                new_related_entities=forward_entities,
                forward_relationship_field=field,
                forward_relationship_field_type=(
                    BuildableAttrFieldType.LIST
                    if field_is_list
                    else BuildableAttrFieldType.FORWARD_REF
                ),
            )

        for (
            reverse_field_key,
            reverse_entity_keys,
        ) in reverse_relationships_by_id_and_field[key].items():
            field, field_type = reverse_field_key
            for reverse_entity_key in reverse_entity_keys:
                update_reverse_references_on_related_entities(
                    updated_entity=normalized_entities_by_id[reverse_entity_key],
                    new_related_entities=[normalized_entity],
                    reverse_relationship_field=field,
                    reverse_relationship_field_type=field_type,
                )

        if isinstance(normalized_entity, normalized_entity_class):
            converted_entities.append(normalized_entity)
    return converted_entities


def fields_unique_to_normalized_class(
    normalized_entity_cls: Type[NormalizedStateEntity],
) -> Set[str]:
    """Returns the names of the fields that are unique to the NormalizedStateEntity
    and are not on the base state Entity class.

    Adds the unique fields set to the cached _unique_fields_reference if this class's
    unique fields have not yet been calculated.
    """
    unique_fields_reference = _get_unique_fields_reference()
    unique_fields = unique_fields_reference.get(normalized_entity_cls)

    if unique_fields:
        return unique_fields

    unique_fields = _get_fields_unique_to_normalized_class(normalized_entity_cls)

    # Add the unique fields for this class to the cached reference
    unique_fields_reference[normalized_entity_cls] = unique_fields
    return unique_fields


def _get_unique_fields_reference() -> Dict[Type[NormalizedStateEntity], Set[str]]:
    """Returns the cached _unique_fields_reference object, if it exists. If the
    _unique_fields_reference is None, instantiates it as an empty dict."""
    global _unique_fields_reference
    if not _unique_fields_reference:
        _unique_fields_reference = {}
    return _unique_fields_reference


def _get_fields_unique_to_normalized_class(
    entity_cls: Type[NormalizedStateEntity],
) -> Set[str]:
    """Helper function for attribute_field_type_reference_for_class to map attributes
    to their BuildableAttrFieldType for a class if the attributes of the class aren't
    yet in the cached _class_structure_reference.
    """
    normalized_class_fields_dict = attr.fields_dict(entity_cls)  # type: ignore[arg-type]
    base_class_fields_dict = attr.fields_dict(
        get_base_entity_class_for_normalized_entity(entity_cls)
    )
    return set(normalized_class_fields_dict.keys()).difference(
        set(base_class_fields_dict.keys())
    )
