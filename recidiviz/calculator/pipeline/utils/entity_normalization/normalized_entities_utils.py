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
"""Utils for working with NormalizedStateEntity objects.

TODO(#10729): Move this file to recidiviz/calculator/pipeline/normalization once all
  functions are only used by the normalization pipeline.
"""
from collections import defaultdict
from copy import copy
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Type, TypeVar

import attr
from google.cloud import bigquery
from more_itertools import one

from recidiviz.big_query.big_query_utils import (
    MAX_BQ_INT,
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
from recidiviz.common.constants.states import MAX_FIPS_CODE
from recidiviz.persistence.database import schema_utils
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema_entity_converter.schema_entity_converter import (
    convert_entities_to_schema,
)
from recidiviz.persistence.database.schema_utils import (
    get_state_table_classes,
    get_table_class_by_name,
)

# All entity classes that have Normalized versions
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_deserialize import EntityT
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    EntityFieldType,
    update_reverse_references_on_related_entities,
)
from recidiviz.utils import environment

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
AdditionalAttributesMap = Dict[
    str,  # Entity class name, e.g. "StateSupervisionPeriod"
    Dict[
        int,  # Value of entity primary key (e.g. state_supervision_id)
        Dict[
            str,  # Field name for additional attribute
            Any,  # Field value for additional attribute
        ],
    ],
]

# Maximum length that the shortened id() value of an entity can be that is added to
# the end of the person_id to create a unique id for the entity
MAX_LEN_SHORTENED_ENTITY_OBJECT_ID = 5

# Cached _unique_fields_reference value
_unique_fields_reference: Optional[Dict[Type[NormalizedStateEntity], Set[str]]] = None

# Cached _entity_id_index value
_entity_id_index: Optional[Dict[int, Dict[str, Set[int]]]] = None


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


def normalized_entity_class_with_base_class_name(
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


# TODO(#10730): Move this to normalized_entities_utils_test.py once it is only being
#  used in tests
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


def column_names_on_bq_schema_for_normalized_state_entity(
    entity_cls: Type[NormalizedStateEntity],
) -> List[str]:
    """Returns the names of the columns in the table representation of the
    |entity_cls|."""
    return [
        schema_field.name
        for schema_field in bq_schema_for_normalized_state_entity(entity_cls)
    ]


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


def merge_additional_attributes_maps(
    additional_attributes_maps: List[AdditionalAttributesMap],
) -> AdditionalAttributesMap:
    """Merges the contents of multiple AdditionalAttributesMaps into a single
    AdditionalAttributesMap.

    Example:
    attributes_map_1 = {
        StateIncarcerationPeriod.__name__: {
            111: {"sequence_num": 1},
            222: {"sequence_num": 2},
        }
    }

    attributes_map_2 = {
        StateIncarcerationPeriod.__name__: {
            111: {"purpose_for_incarceration_subtype": "XYZ"},
            222: {"purpose_for_incarceration_subtype": "AAA"},
        }
    }

    merge_additional_attributes_maps(attributes_map_1, attributes_map2) =>

        {
            StateIncarcerationPeriod.__name__: {
                111: {"purpose_for_incarceration_subtype": "XYZ", "sequence_num": 1},
                222: {"purpose_for_incarceration_subtype": "AAA", "sequence_num": 2},
            }
        }
    """

    base_attributes_map: AdditionalAttributesMap = defaultdict(
        lambda: defaultdict(dict)
    )

    for additional_attributes_map in additional_attributes_maps:
        for entity_name, entity_attributes in additional_attributes_map.items():
            entity_fields_dict = base_attributes_map[entity_name]

            for entity_id, attributes in entity_attributes.items():
                entity_fields_dict[entity_id] = {
                    **entity_fields_dict[entity_id],
                    **attributes,
                }

    return base_attributes_map


def get_shared_additional_attributes_map_for_entities(
    entities: Sequence[Entity],
) -> AdditionalAttributesMap:
    """Returns an AdditionalAttributesMap storing attributes that are relevant to the
    provided entities and are shared amongst more than one Normalized entity class
    (e.g. sequence number for all NormalizedStateEntity entities that store the
    SequencedEntityMixin)."""
    additional_attributes_map: AdditionalAttributesMap = defaultdict(
        lambda: defaultdict(dict)
    )

    if not entities:
        return additional_attributes_map

    entity_base_class = one(set(type(e) for e in entities))
    entity_name = entity_base_class.__name__
    normalized_entity_class = normalized_entity_class_with_base_class_name(entity_name)

    # Add empty map by default for entity types with no additional attributes
    additional_attributes_map[entity_name] = {}

    # Add relevant sequence_num values if relevant
    if issubclass(normalized_entity_class, SequencedEntityMixin):
        attributes_for_class = additional_attributes_map[entity_name]
        for index, entity in enumerate(entities):
            attributes_for_class[entity.get_id()] = {
                "sequence_num": index,
            }

    return additional_attributes_map


@environment.test_only
def clear_entity_id_index_cache() -> None:
    global _entity_id_index
    _entity_id_index = None


def _get_entity_id_index() -> Dict[int, Dict[str, Set[int]]]:
    """Returns the cached _entity_id_index object, if it exists. If the
    _entity_id_index is None, instantiates it as an empty dict."""
    global _entity_id_index
    if not _entity_id_index:
        _entity_id_index = defaultdict(lambda: defaultdict(set))
    return _entity_id_index


def _get_entity_id_index_for_person_id_entity(
    person_id: int, entity_name: str
) -> Set[int]:
    """Returns the set of id values associated with entities with the given
    |entity_name| for the given |person_id|."""
    entity_id_index = _get_entity_id_index()
    entity_ids_for_person = entity_id_index.get(person_id)

    if not entity_ids_for_person:
        return set()

    return entity_ids_for_person.get(entity_name) or set()


def _add_entity_id_to_cache(person_id: int, entity_name: str, entity_id: int) -> None:
    """Adds the |entity_id| value to the set storing the ids of the entities with the
    |entity_name| for the given |person_id| in the _entity_id_index."""
    entity_id_index = _get_entity_id_index()
    entity_id_index[person_id][entity_name].add(entity_id)


def _fixed_length_object_id_for_entity(entity: Entity) -> int:
    """Returns a shortened version of the id of the |entity| with a maximum of
    MAX_LEN_SHORTENED_ENTITY_OBJECT_ID digits."""
    # Get last 5 digits (or full string if less than 5 digits)
    string_entity_object_id = str(id(entity))[-MAX_LEN_SHORTENED_ENTITY_OBJECT_ID:]
    # Convert back to int
    entity_object_id = int(string_entity_object_id)

    return entity_object_id


def _unique_object_id_for_entity(person_id: int, entity: Entity) -> int:
    """Returns an object id value that is globally unique for all entities of this
    type for this person_id."""
    if person_id - (10**12 * MAX_FIPS_CODE) >= 10**12:
        raise ValueError(
            "Our database person_id values have gotten too large and are clobbering "
            "the FIPS code values in the id mask used to write to BigQuery. Must fix "
            "person_id values before running more normalization pipelines."
        )

    # Get a shortened version of the id() of the entity
    entity_object_id = _fixed_length_object_id_for_entity(entity=entity)

    existing_entity_object_ids = _get_entity_id_index_for_person_id_entity(
        person_id=person_id, entity_name=entity.__class__.__name__
    )

    while entity_object_id in existing_entity_object_ids:
        # Increment until we've found a unique entity ID for this entity/person
        entity_object_id += 1
        # Make sure id stays under the max digit length
        entity_object_id = entity_object_id % (10**MAX_LEN_SHORTENED_ENTITY_OBJECT_ID)

    # Add the ID to the cache of entity IDs for this person
    _add_entity_id_to_cache(
        person_id=person_id,
        entity_name=entity.__class__.__name__,
        entity_id=entity_object_id,
    )

    return entity_object_id


def update_normalized_entity_with_globally_unique_id(
    person_id: int, entity: Entity
) -> None:
    """Returns an ID value that will be unique across all entities of the
    entity type normalized for a state in a given pipeline.

    Takes the last 5 digits of the python id() value of the |entity| and prepends
    it with the |person_id| value.

    For example, say the person_id value is 290000700089 and the python id() of the
    |entity| is 123456789. This function will return 29000070008956789. The components
    of this value ({290000700089}{56789}) correspond to:
        {person_id}{last 5 digits of entity id()}.

    If the last 5 digits of the entity object id been used in the id value of another
    normalized entity of this type for this |person_id|, then the value is
    incremented by +1 until a unique value is found.

    This ensures that the normalized version of the |entity| has a unique primary key ID
    in the table that will store the normalized versions of all of the entities of the
    given entity type for all states.
    """
    # Get a shortened version of the python id that is globally unique for all
    # entities of this type for this person_id
    entity_object_id = _unique_object_id_for_entity(
        person_id=person_id,
        entity=entity,
    )

    # Add person_id to the front of the id
    new_entity_id = int(f"{person_id}{entity_object_id}")

    if new_entity_id > MAX_BQ_INT:
        raise ValueError(
            f"Entity id value [{new_entity_id}] is larger than the "
            f"maximum allowed integer value [{MAX_BQ_INT}] in BigQuery. "
            "Must update unique entity id generation logic accordingly."
        )

    # Set new id on the entity
    setattr(
        entity,
        entity.get_class_id_name(),
        new_entity_id,
    )


def copy_entities_and_add_unique_ids(
    person_id: int, entities: List[EntityT]
) -> List[EntityT]:
    """Creates new copies of each of the provided |entities| and adds unique id
    values to the new copies."""
    new_entities: List[EntityT] = []
    for entity in entities:
        entity_copy = copy(entity)
        update_normalized_entity_with_globally_unique_id(
            person_id=person_id, entity=entity_copy
        )

        new_entities.append(entity_copy)

    return new_entities
