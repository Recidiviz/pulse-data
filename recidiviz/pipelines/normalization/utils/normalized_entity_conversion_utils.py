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
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Type

import attr
import cachetools
from google.cloud import bigquery

from recidiviz.big_query.big_query_utils import (
    schema_field_for_attribute,
    schema_for_sqlalchemy_table,
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
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.schema_utils import (
    get_state_database_association_with_names,
    get_table_class_by_name,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    EntityFieldType,
    update_reverse_references_on_related_entities,
)
from recidiviz.persistence.entity.normalized_entities_utils import (
    AdditionalAttributesMap,
    NormalizedStateEntityT,
    normalized_entity_class_with_base_class_name,
    update_forward_references_on_updated_entity,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    get_entity_class_names_excluded_from_normalization,
)
from recidiviz.persistence.entity.state.normalized_state_entity import (
    NormalizedStateEntity,
)

NormalizedEntityKey = Tuple[Type[NormalizedStateEntity], int]

# Cached _unique_fields_reference value
_unique_fields_reference: Optional[Dict[Type[NormalizedStateEntity], Set[str]]] = None


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

        # Verify the base entity and normalized entity are compatible
        base_entity_cls = type(base_entity)
        base_entity_cls_name = base_entity_cls.__name__
        if normalized_base_entity_class.__base__ != base_entity_cls:
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
        flat_fields = field_index.get_all_core_entity_fields(
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
        forward_fields = field_index.get_all_core_entity_fields(
            type(base_entity), EntityFieldType.FORWARD_EDGE
        )
        for field in forward_fields:
            related_entity_cls_name = attr_field_referenced_cls_name_for_field_name(
                normalized_base_entity_class, field
            )

            if (
                related_entity_cls_name
                in get_entity_class_names_excluded_from_normalization()
            ):
                continue

            field_value = normalized_base_entity_attributes[field]
            if field_value and related_entity_cls_name:
                field_is_list = isinstance(field_value, list)
                related_entities: Sequence[Entity] = (
                    field_value if field_is_list else [field_value]
                )

                referenced_normalized_class: Type[
                    NormalizedStateEntity
                ] = normalized_entity_class_with_base_class_name(
                    related_entity_cls_name
                )

                reverse_relationship_field = (
                    attr_field_name_storing_referenced_cls_name(
                        base_cls=referenced_normalized_class,
                        referenced_cls_name=base_entity_cls_name,
                    )
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


def convert_entities_to_normalized_dicts(
    root_entity_id: int,
    root_entity_id_name: str,
    state_code: str,
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

    # First check if the entity has a many-to-many relationship with its children
    # instead of a 1-to-many relationship or 1-to-1 relationship
    many_to_many_relationships: Set[str] = set()
    for entity in entities:
        entity_cls = type(entity)
        entity_cls_name = entity_cls.__name__

        forward_fields = field_index.get_all_core_entity_fields(
            type(entity), EntityFieldType.FORWARD_EDGE
        )
        for field in forward_fields:
            related_entity_cls_name = attr_field_referenced_cls_name_for_field_name(
                type(entity), field
            )
            if (
                related_entity_cls_name
                in get_entity_class_names_excluded_from_normalization()
            ):
                continue

            field_value = entity.get_field(field)
            if field_value is not None and related_entity_cls_name:
                field_is_list = isinstance(field_value, list)
                referenced_normalized_class = (
                    normalized_entity_class_with_base_class_name(
                        related_entity_cls_name
                    )
                )
                reverse_relationship_field = (
                    attr_field_name_storing_referenced_cls_name(
                        base_cls=referenced_normalized_class,
                        referenced_cls_name=entity_cls_name,
                    )
                )
                reverse_relationship_field_type = (
                    attr_field_type_for_field_name(
                        referenced_normalized_class, reverse_relationship_field
                    )
                    if reverse_relationship_field
                    else None
                )
                if (
                    field_is_list
                    and reverse_relationship_field_type is not None
                    and reverse_relationship_field_type == BuildableAttrFieldType.LIST
                ):
                    many_to_many_relationships.add(field)

    stack = deque(
        convert_entities_to_schema(
            entities,
            populate_back_edges=True,
        )
    )

    while len(stack) > 0:  # pylint: disable=too-many-nested-blocks
        schema_entity = stack.popleft()
        base_entity_cls = schema_entity.__class__
        base_entity_cls_name = base_entity_cls.__name__

        normalized_base_entity_class = normalized_entity_class_with_base_class_name(
            base_entity_cls_name
        )

        forward_relationship_fields = field_index.get_all_core_entity_fields(
            type(schema_entity), EntityFieldType.FORWARD_EDGE
        )

        # Collect the tagged entity dicts for all related classes that
        # haven't been converted yet
        for field in forward_relationship_fields:
            related_schema_entities: Sequence[
                DatabaseEntity
            ] = schema_entity.get_field_as_list(field)

            for related_schema_entity in related_schema_entities:
                if field in many_to_many_relationships:
                    related_schema_entity_cls = type(related_schema_entity)
                    related_schema_entity_cls_name = related_schema_entity_cls.__name__
                    association_table_dict: Dict[str, Any] = {}
                    association_table_schema = (
                        bq_schema_for_normalized_state_association_table(
                            related_schema_entity_cls_name, base_entity_cls_name
                        )
                    )
                    for schema_field in association_table_schema:
                        if (
                            schema_field.name
                            == related_schema_entity_cls.get_class_id_name()
                        ):
                            association_table_dict[
                                schema_field.name
                            ] = related_schema_entity.get_id()
                        elif schema_field.name == base_entity_cls.get_class_id_name():
                            association_table_dict[
                                schema_field.name
                            ] = schema_entity.get_id()
                        else:
                            association_table_dict[schema_field.name] = state_code
                    entry = (
                        f"{related_schema_entity_cls_name}_{base_entity_cls_name}",
                        association_table_dict,
                    )
                    if entry not in tagged_entity_dicts:
                        tagged_entity_dicts.append(entry)
                stack.append(related_schema_entity)

        additional_args_for_base_entity = (
            additional_attributes_map.get(base_entity_cls_name, {}).get(
                schema_entity.get_id()
            )
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
            **{root_entity_id_name: root_entity_id},
            **additional_args_for_base_entity,
        }
        entity_table_entry = (base_entity_cls_name, entity_table_dict)
        if entity_table_entry not in tagged_entity_dicts:
            tagged_entity_dicts.append(entity_table_entry)

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


# type checking for functools.cache didn't like using a type as the key, but it works
# for cachetools.
@cachetools.cached(cache={})
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

    base_class: Type[BuildableAttr] = entity_cls.get_base_entity_class()
    base_schema_class = schema_utils.get_state_database_entity_with_name(
        base_class.__name__
    )

    return (
        schema_for_sqlalchemy_table(
            get_table_class_by_name(base_schema_class.__tablename__, SchemaType.STATE)
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
    """Helper function for attribute_field_type_reference_for_class to map attributes
    to their BuildableAttrFieldType for a class if the attributes of the class aren't
    yet in the cached _class_structure_reference.
    """
    normalized_class_fields_dict = attr.fields_dict(entity_cls)  # type: ignore[arg-type]
    base_class_fields_dict = attr.fields_dict(entity_cls.get_base_entity_class())  # type: ignore[arg-type]
    return set(normalized_class_fields_dict.keys()).difference(
        set(base_class_fields_dict.keys())
    )


def bq_schema_for_normalized_state_association_table(
    child_cls_name: str, parent_cls_name: str
) -> List[bigquery.SchemaField]:
    association_table = get_state_database_association_with_names(
        child_cls_name, parent_cls_name
    )

    return schema_for_sqlalchemy_table(association_table, add_state_code_field=True)
