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
"""Utils for working with NormalizedStateEntity objects."""
from copy import copy
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Type, TypeVar

import attr
from google.cloud import bigquery

from recidiviz.big_query.big_query_utils import (
    schema_field_for_attribute,
    schema_for_sqlalchemy_table,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.normalized_entities import (
    NormalizedStateEntity,
    NormalizedStateIncarcerationPeriod,
    NormalizedStateProgramAssignment,
    NormalizedStateSupervisionCaseTypeEntry,
    NormalizedStateSupervisionPeriod,
    NormalizedStateSupervisionViolatedConditionEntry,
    NormalizedStateSupervisionViolation,
    NormalizedStateSupervisionViolationResponse,
    NormalizedStateSupervisionViolationResponseDecisionEntry,
    NormalizedStateSupervisionViolationTypeEntry,
    SequencedEntityMixin,
    get_entity_class_names_excluded_from_normalization,
)
from recidiviz.common.attr_mixins import (
    BuildableAttr,
    BuildableAttrFieldType,
    attr_field_attribute_for_field_name,
    attr_field_name_storing_referenced_cls_name,
    attr_field_referenced_cls_name_for_field_name,
    attr_field_type_for_field_name,
)
from recidiviz.common.attr_utils import is_flat_field
from recidiviz.persistence.database import schema_utils
from recidiviz.persistence.database.schema_utils import (
    get_state_table_classes,
    get_table_class_by_name,
)

# All entity classes that have Normalized versions
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_deserialize import EntityT
from recidiviz.persistence.entity.entity_utils import (
    update_reverse_references_on_related_entities,
)

NORMALIZED_ENTITY_CLASSES: List[Type[NormalizedStateEntity]] = [
    NormalizedStateIncarcerationPeriod,
    NormalizedStateProgramAssignment,
    NormalizedStateSupervisionPeriod,
    NormalizedStateSupervisionCaseTypeEntry,
    NormalizedStateSupervisionViolationResponse,
    NormalizedStateSupervisionViolationResponseDecisionEntry,
    NormalizedStateSupervisionViolation,
    NormalizedStateSupervisionViolationTypeEntry,
    NormalizedStateSupervisionViolatedConditionEntry,
]

NormalizedStateEntityT = TypeVar("NormalizedStateEntityT", bound=NormalizedStateEntity)

# Cached _unique_fields_reference value
_unique_fields_reference: Optional[Dict[Type[NormalizedStateEntity], Set[str]]] = None


def _get_unique_fields_reference() -> Dict[Type[NormalizedStateEntity], Set[str]]:
    """Returns the cached _unique_fields_reference object, if it exists. If the
    _unique_fields_reference is None, instantiates it as an empty dict."""
    global _unique_fields_reference
    if not _unique_fields_reference:
        _unique_fields_reference = {}
    return _unique_fields_reference


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


def _get_fields_unique_to_normalized_class(
    entity_cls: Type[NormalizedStateEntity],
) -> Set[str]:
    """Helper function for _attribute_field_type_reference_for_class to map attributes
    to their BuildableAttrFieldType for a class if the attributes of the class aren't
    yet in the cached _class_structure_reference.
    """
    normalized_class_fields_dict = attr.fields_dict(entity_cls)
    base_class: Type[BuildableAttr] = entity_cls.__base__
    base_class_fields_dict = attr.fields_dict(base_class)

    return set(normalized_class_fields_dict.keys()).difference(
        set(base_class_fields_dict.keys())
    )


def _normalized_entity_class_with_base_class_name(
    base_class_name: str,
) -> Type[NormalizedStateEntity]:
    """Returns the NormalizedStateEntity class that has a base class with the name
    matching the provided |base_class_name|."""
    for normalized_entity_cls in NORMALIZED_ENTITY_CLASSES:
        if normalized_entity_cls.base_class_name() == base_class_name:
            return normalized_entity_cls

    raise ValueError(
        "No NormalizedStateEntity class corresponding with a base class name of: "
        f"{base_class_name}."
    )


def bq_schema_for_normalized_state_entity(
    entity_cls: Type[NormalizedStateEntity],
) -> List[bigquery.SchemaField]:
    """Returns the necessary BigQuery schema for the NormalizedStateEntity, which is
    a list of SchemaField objects containing the column name and value type for
    each attribute on the NormalizedStateEntity."""
    unique_fields_on_normalized_class = fields_unique_to_normalized_class(entity_cls)

    schema_fields_for_additional_fields: List[bigquery.SchemaField] = []

    for field in unique_fields_on_normalized_class:
        attribute = attr_field_attribute_for_field_name(entity_cls, field)

        if not is_flat_field(attribute):
            raise ValueError(
                "Only flat fields are supported as additional fields on "
                f"NormalizedStateEntities. Found: {attribute} in field "
                f"{field}."
            )

        schema_fields_for_additional_fields.append(
            schema_field_for_attribute(field_name=field, attribute=attribute)
        )

    base_class: Type[BuildableAttr] = entity_cls.__base__
    base_schema_class = schema_utils.get_state_database_entity_with_name(
        base_class.__name__
    )

    return (
        schema_for_sqlalchemy_table(
            get_table_class_by_name(
                base_schema_class.__tablename__, list(get_state_table_classes())
            )
        )
        + schema_fields_for_additional_fields
    )


def _convert_entity_tree_to_normalized_version(
    base_entity: EntityT,
    normalized_base_entity_class: Type[NormalizedStateEntityT],
    additional_attributes_map: Dict[str, Dict[int, Dict[str, Any]]],
    traversed_class_names: Optional[List[str]] = None,
) -> NormalizedStateEntityT:
    """Recursively converts the |base_entity| and its entity tree to their Normalized
    counterparts.

    Recursively converts the subtree of each entity that is related to the base
    entity class into their Normalized versions.

    If an entity class that is related to the provided |base_entity| has already been
    traversed in the normalization process of normalizing this entity tree (and is in
    the |traversed_class_names| list), then that edge is not explored.

    For all related classes that are normalized, hydrates the reverse relationship
    edge (if a reverse relationship edge exists) to point to the normalized version
    of the |base_entity|.

    Returns the normalized version of the |base_entity|.
    """
    traversed_class_names = copy(traversed_class_names) if traversed_class_names else []
    base_entity_cls = type(base_entity)
    base_entity_cls_name = base_entity_cls.__name__
    traversed_class_names.append(base_entity_cls_name)

    if normalized_base_entity_class.__base__ != base_entity_cls:
        raise ValueError(
            f"Trying to convert entities of type {base_entity_cls} to "
            f"the type {normalized_base_entity_class}, which is not the "
            "normalized version of the class."
        )

    normalized_base_entity_builder = normalized_base_entity_class.builder()
    normalized_base_entity_attributes = base_entity.__dict__
    additional_args_for_base_entity = additional_attributes_map.get(
        base_entity_cls_name, {}
    ).get(base_entity.get_id())

    if additional_args_for_base_entity:
        normalized_base_entity_attributes.update(additional_args_for_base_entity)

    reverse_fields_to_set: List[
        Tuple[NormalizedStateEntity, str, BuildableAttrFieldType]
    ] = []

    for field, value in normalized_base_entity_attributes.items():
        if field not in fields_unique_to_normalized_class(normalized_base_entity_class):
            if related_entity_cls_name := attr_field_referenced_cls_name_for_field_name(
                base_entity_cls, field
            ):
                if (
                    related_entity_cls_name in traversed_class_names
                    or related_entity_cls_name
                    in get_entity_class_names_excluded_from_normalization()
                ):
                    # If we've already normalized this class, or if this class is one
                    # that's excluded from normalization entirely, then continue
                    continue

                related_entities: Sequence[EntityT]
                if isinstance(value, list):
                    related_entities = value
                    field_is_list = True
                else:
                    related_entities = [value]
                    field_is_list = False

                # Convert the related entities to their Normalized versions,
                # and collect the reverse relationship fields that need to be set to
                # point back to the normalized base entity
                referenced_normalized_class = (
                    _normalized_entity_class_with_base_class_name(
                        related_entity_cls_name
                    )
                )

                reverse_relationship_field = (
                    attr_field_name_storing_referenced_cls_name(
                        base_cls=referenced_normalized_class,
                        referenced_cls_name=base_entity_cls_name,
                    )
                )

                reverse_relationship_field_type: Optional[BuildableAttrFieldType] = None
                if reverse_relationship_field:
                    reverse_relationship_field_type = attr_field_type_for_field_name(
                        referenced_normalized_class, reverse_relationship_field
                    )

                normalized_related_entities: List[NormalizedStateEntity] = []
                for related_entity in related_entities:
                    normalized_related_entity = (
                        _convert_entity_tree_to_normalized_version(
                            base_entity=related_entity,
                            normalized_base_entity_class=referenced_normalized_class,
                            additional_attributes_map=additional_attributes_map,
                            traversed_class_names=traversed_class_names,
                        )
                    )

                    normalized_related_entities.append(normalized_related_entity)

                    if reverse_relationship_field and reverse_relationship_field_type:
                        # The relationship is bidirectional. Store the attributes of
                        # the reverse edge to be set later.
                        reverse_fields_to_set.append(
                            (
                                normalized_related_entity,
                                reverse_relationship_field,
                                reverse_relationship_field_type,
                            )
                        )

                value = (
                    normalized_related_entities
                    if field_is_list
                    else normalized_related_entities[0]
                )

        setattr(normalized_base_entity_builder, field, value)

    normalized_entity = normalized_base_entity_builder.build()

    for related_normalized_entity, reverse_field, field_type in reverse_fields_to_set:
        if field_type == BuildableAttrFieldType.LIST:
            raise ValueError(
                "Recursive normalization conversion cannot support "
                "reverse fields that store lists. Found entity of type "
                f"{related_normalized_entity.base_class_name()} with the field "
                f"[{reverse_field}] that stores a list of type "
                f"{base_entity_cls_name}. Try normalizing "
                f"this entity tree with the "
                f"{related_normalized_entity.base_class_name()} class as the "
                f"root instead."
            )

        if not isinstance(related_normalized_entity, Entity):
            # To satisfy mypy
            raise ValueError(
                "All NormalizedStateEntity instances should also be "
                "instances of Entity."
            )

        update_reverse_references_on_related_entities(
            updated_entity=normalized_entity,
            new_related_entities=[related_normalized_entity],
            reverse_relationship_field=reverse_field,
            reverse_relationship_field_type=field_type,
        )

    return normalized_entity


def convert_entity_trees_to_normalized_versions(
    root_entities: Sequence[Entity],
    normalized_entity_class: Type[NormalizedStateEntityT],
    additional_attributes_map: Optional[Dict[str, Dict[int, Dict[str, Any]]]] = None,
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

    Is stores attributes that are unique to the Normalized version of an entity and the
    values that should be set on these fields for certain entities. In the above
    example, the StateIncarcerationPeriod with the incarceration_period_id of 111
    will have a `purpose_for_incarceration_subtype` value of 'XYZ' on its Normalized
    version.

    If the |normalized_entity_class| has a `sequence_num` attribute,
    this function determines the sequence numbers to be set on each of the root
    entities, and updates the |additional_attributes_map| accordingly.

    Returns a list of normalized root entities, with hydrated relationships to the
    normalized versions of their whole entity trees.
    """
    normalized_entities: List[NormalizedStateEntityT] = []

    if not root_entities:
        return normalized_entities

    additional_attributes_map = additional_attributes_map or {}
    entity_base_class = type(root_entities[0])

    if issubclass(normalized_entity_class, SequencedEntityMixin):
        if entity_base_class.__name__ not in additional_attributes_map:
            additional_attributes_map[entity_base_class.__name__] = {
                entity.get_id(): {
                    "sequence_num": index,
                }
                for index, entity in enumerate(root_entities)
            }
        else:
            attributes_for_class = additional_attributes_map[entity_base_class.__name__]
            for index, entity in enumerate(root_entities):
                if entity_info := attributes_for_class.get(entity.get_id()):
                    entity_info["sequence_num"] = index
                else:
                    attributes_for_class[entity.get_id()] = {
                        "sequence_num": index,
                    }

    for entity in root_entities:
        normalized_entities.append(
            _convert_entity_tree_to_normalized_version(
                base_entity=entity,
                normalized_base_entity_class=normalized_entity_class,
                additional_attributes_map=additional_attributes_map,
            )
        )

    return normalized_entities
