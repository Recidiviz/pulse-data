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
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Type

import attr
from google.cloud import bigquery

from recidiviz.big_query.big_query_utils import (
    schema_field_for_attribute,
    schema_for_sqlalchemy_table,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.normalized_entities import (
    NormalizedStateEntity,
    get_entity_class_names_excluded_from_normalization,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.normalized_entities_utils import (
    AdditionalAttributesMap,
    NormalizedStateEntityT,
    normalized_entity_class_with_base_class_name,
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
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema_entity_converter.schema_entity_converter import (
    convert_entities_to_schema,
)
from recidiviz.persistence.database.schema_utils import (
    get_state_table_classes,
    get_table_class_by_name,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_deserialize import EntityT
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    EntityFieldType,
    update_reverse_references_on_related_entities,
)

# Cached _unique_fields_reference value
_unique_fields_reference: Optional[Dict[Type[NormalizedStateEntity], Set[str]]] = None


def _convert_entity_tree_to_normalized_version(
    base_entity: EntityT,
    normalized_base_entity_class: Type[NormalizedStateEntityT],
    additional_attributes_map: AdditionalAttributesMap,
    field_index: CoreEntityFieldIndex,
) -> NormalizedStateEntityT:
    """Recursively converts the |base_entity| and its entity tree to their Normalized
    counterparts.

    Recursively converts the subtree of each entity that is related to the base
    entity class into their Normalized versions.

    If an entity class that is related to the provided |base_entity| is not a forward
    edge relationship, then that edge is not explored.

    For all related classes that are normalized, hydrates the reverse relationship
    edge (if a reverse relationship edge exists) to point to the normalized version
    of the |base_entity|.

    Returns the normalized version of the |base_entity|.
    """
    base_entity_cls = type(base_entity)
    base_entity_cls_name = base_entity_cls.__name__

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

    forward_relationship_fields = field_index.get_all_core_entity_fields(
        base_entity, EntityFieldType.FORWARD_EDGE
    )
    flat_fields = field_index.get_all_core_entity_fields(
        base_entity, EntityFieldType.FLAT_FIELD
    )
    unique_fields = fields_unique_to_normalized_class(normalized_base_entity_class)

    fields_to_set_or_traverse = (
        forward_relationship_fields | flat_fields | unique_fields
    )

    for field, field_value in normalized_base_entity_attributes.items():
        if field not in fields_to_set_or_traverse:
            # Don't set this value on the builder
            continue

        related_entity_cls_name = attr_field_referenced_cls_name_for_field_name(
            normalized_base_entity_class, field
        )

        if (
            related_entity_cls_name
            in get_entity_class_names_excluded_from_normalization()
        ):
            continue

        if field_value and related_entity_cls_name:
            related_entities: Sequence[EntityT]
            if isinstance(field_value, list):
                related_entities = field_value
                field_is_list = True
            else:
                related_entities = [field_value]
                field_is_list = False

            # Convert the related entities to their Normalized versions,
            # and collect the reverse relationship fields that need to be set to
            # point back to the normalized base entity
            referenced_normalized_class = normalized_entity_class_with_base_class_name(
                related_entity_cls_name
            )

            reverse_relationship_field = attr_field_name_storing_referenced_cls_name(
                base_cls=referenced_normalized_class,
                referenced_cls_name=base_entity_cls_name,
            )

            reverse_relationship_field_type: Optional[BuildableAttrFieldType] = None
            if reverse_relationship_field:
                reverse_relationship_field_type = attr_field_type_for_field_name(
                    referenced_normalized_class, reverse_relationship_field
                )

            normalized_related_entities: List[NormalizedStateEntity] = []
            for related_entity in related_entities:
                normalized_related_entity = _convert_entity_tree_to_normalized_version(
                    base_entity=related_entity,
                    normalized_base_entity_class=referenced_normalized_class,
                    additional_attributes_map=additional_attributes_map,
                    field_index=field_index,
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

            field_value = (
                normalized_related_entities
                if field_is_list
                else normalized_related_entities[0]
            )

        setattr(normalized_base_entity_builder, field, field_value)

    normalized_entity = normalized_base_entity_builder.build()

    for related_normalized_entity, reverse_field, field_type in reverse_fields_to_set:
        if field_type == BuildableAttrFieldType.LIST:
            # TODO(#11734): Implement support for normalizing entities that have
            #  many:many relationships
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
    additional_attributes_map: AdditionalAttributesMap,
    field_index: CoreEntityFieldIndex,
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

    if not additional_attributes_map:
        raise ValueError("Found empty additional_attributes_map.")

    for entity in root_entities:
        converted_entities.append(
            _convert_entity_tree_to_normalized_version(
                base_entity=entity,
                normalized_base_entity_class=normalized_entity_class,
                additional_attributes_map=additional_attributes_map,
                field_index=field_index,
            )
        )

    return converted_entities


def convert_entities_to_normalized_dicts(
    person_id: int,
    entities: Sequence[Entity],
    additional_attributes_map: AdditionalAttributesMap,
    field_index: CoreEntityFieldIndex,
) -> List[Tuple[str, Dict[str, Any]]]:
    """First, converts each entity tree in |entities| to a tree of connected schema
    objects. Then, converts the schema versions of the entities into dictionary
    representations containing all values required in the table that stores the
    normalized version of the entity.

    Returns a list of tuples, where each tuple stores the name of the entity (e.g.
    'StateIncarcerationPeriod') and the dictionary representation of the entity.
    """
    tagged_entity_dicts: List[Tuple[str, Dict[str, Any]]] = []

    if not entities:
        return tagged_entity_dicts

    for entity in entities:
        # Convert the entity tree to the schema version
        schema_entity_tree = convert_entities_to_schema(
            [entity], populate_back_edges=True
        )

        tagged_entity_dicts.extend(
            _tagged_entity_dicts_for_schema_entities(
                person_id=person_id,
                schema_entity=schema_entity_tree[0],
                additional_attributes_map=additional_attributes_map,
                field_index=field_index,
            )
        )

    return tagged_entity_dicts


def _tagged_entity_dicts_for_schema_entities(
    person_id: int,
    schema_entity: DatabaseEntity,
    additional_attributes_map: AdditionalAttributesMap,
    field_index: CoreEntityFieldIndex,
) -> List[Tuple[str, Dict[str, Any]]]:
    """Converts the |schema_entity| and it's entire entity tree into a list of
    dictionary representations of each entity in the tree.

    Returns a list of tuples, where each tuple stores the name of the entity (e.g.
    'StateIncarcerationPeriod') and the dictionary representation of the entity
    containing all values required in the table that stores the normalized version of
    the entity.

    Recursively collects the list of tagged entity dictionaries for the entity
    sub-trees of all related entities that have not yet been converted to tagged
    entity dictionaries.
    """
    tagged_entity_dicts: List[Tuple[str, Dict[str, Any]]] = []
    base_entity_cls_name = schema_entity.__class__.__name__

    normalized_base_entity_class = normalized_entity_class_with_base_class_name(
        base_entity_cls_name
    )

    forward_relationship_fields = field_index.get_all_core_entity_fields(
        schema_entity, EntityFieldType.FORWARD_EDGE
    )

    # Recursively collect the tagged entity dicts for all related classes that
    # haven't been converted yet
    for field in forward_relationship_fields:
        related_schema_entities: Sequence[
            DatabaseEntity
        ] = schema_entity.get_field_as_list(field)

        for related_schema_entity in related_schema_entities:
            tagged_entity_dicts.extend(
                _tagged_entity_dicts_for_schema_entities(
                    person_id=person_id,
                    schema_entity=related_schema_entity,
                    additional_attributes_map=additional_attributes_map,
                    field_index=field_index,
                )
            )

    additional_args_for_base_entity = (
        additional_attributes_map[base_entity_cls_name].get(schema_entity.get_id())
        or {}
    )

    # Build the entity table dict for the base schema entity
    entity_table_dict: Dict[str, Any] = {
        **{
            col: getattr(schema_entity, col)
            for col in column_names_on_bq_schema_for_normalized_state_entity(
                normalized_base_entity_class
            )
            if col
            not in fields_unique_to_normalized_class(normalized_base_entity_class)
        },
        # The person_id field is not hydrated on any entities we read from BQ for
        # performance reasons. We need to make sure when we write back to BQ, we
        # re-hydrate this field which would otherwise be null.
        **{"person_id": person_id},
        **additional_args_for_base_entity,
    }

    tagged_entity_dicts.append((base_entity_cls_name, entity_table_dict))

    return tagged_entity_dicts


def column_names_on_bq_schema_for_normalized_state_entity(
    entity_cls: Type[NormalizedStateEntity],
) -> List[str]:
    """Returns the names of the columns in the table representation of the
    |entity_cls|."""
    return [
        schema_field.name
        for schema_field in bq_schema_for_normalized_state_entity(entity_cls)
    ]


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
