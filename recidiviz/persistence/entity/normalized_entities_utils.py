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
"""
import json
from collections import defaultdict
from copy import copy
from typing import Any, Dict, List, Sequence, Type, TypeVar

from more_itertools import one

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import MAX_BQ_INT
from recidiviz.common.attr_mixins import BuildableAttrFieldType
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET

# All entity classes that have Normalized versions
from recidiviz.persistence.entity.base_entity import CoreEntity, Entity, EntityT
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_entity,
)
from recidiviz.persistence.entity.entity_utils import (
    get_all_entity_classes_in_module,
    get_entity_class_in_module_with_name,
)
from recidiviz.persistence.entity.generate_primary_key import generate_primary_key
from recidiviz.persistence.entity.serialization import serialize_entity_into_json
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.persistence.entity.state.normalized_state_entity import (
    NormalizedStateEntity,
)
from recidiviz.persistence.entity.state.state_entity_mixins import (
    SequencedEntityMixin,
    SequencedEntityMixinT,
)
from recidiviz.utils.types import assert_subclass

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


def queryable_address_for_normalized_entity(
    normalized_entity_cls: Type[NormalizedStateEntity],
) -> BigQueryAddress:
    """Returns the BigQueryAddress for the state agnostic table containing all entities of this type"""
    return BigQueryAddress(
        dataset_id=NORMALIZED_STATE_DATASET,
        table_id=assert_subclass(normalized_entity_cls, Entity).get_table_id(),
    )


def normalized_entity_class_with_base_class_name(
    base_class_name: str,
) -> Type[NormalizedStateEntity]:
    """Returns the NormalizedStateEntity class that has a base class with the name
    matching the provided |base_class_name|.
    """

    normalized_entity_classes = get_all_entity_classes_in_module(normalized_entities)

    for normalized_entity_class in normalized_entity_classes:
        if not issubclass(normalized_entity_class, NormalizedStateEntity):
            raise ValueError(
                f"Found normalized entity class which is not a NormalizedStateEntity: "
                f"{normalized_entity_class} "
            )
        if normalized_entity_class.base_class_name() == base_class_name:
            return normalized_entity_class

    raise ValueError(
        "No NormalizedStateEntity class corresponding with a base class name of: "
        f"{base_class_name}."
    )


def get_base_entity_class_for_normalized_entity(
    entity_cls: Type[NormalizedStateEntity],
) -> Type[Entity]:
    """For the given NormalizeStateEntity, returns the base Entity class for this class.
    For example: for NormalizedStatePerson, returns StatePerson.
    """
    base_class_name = entity_cls.base_class_name()
    return get_entity_class_in_module_with_name(state_entities, base_class_name)


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


def _unique_object_id_for_entity(
    person_id: int, entity: Entity, state_code: StateCode
) -> int:
    """Returns an object id value that is globally unique for all entities of this
    type for this person_id.
    """
    entities_module_context = entities_module_context_for_entity(entity)
    entity_json = serialize_entity_into_json(
        entity, entities_module=entities_module_context.entities_module()
    )
    extra_json_fields = {}
    # At this point in normalization we may not have set the backedges to person
    # on this entity - if we have not, add a field with person_id so the generated
    # primary key is unique across all people.
    if entity_json["person_id"] is None:
        extra_json_fields = {"person_id": person_id}
    entity_json_for_primary_key = {**entity_json, **extra_json_fields}
    entity_object_id = generate_primary_key(
        json.dumps(entity_json_for_primary_key, sort_keys=True),
        state_code,
    )

    return entity_object_id


def update_normalized_entity_with_globally_unique_id(
    person_id: int, entity: Entity, state_code: StateCode
) -> None:
    """Updates the entity's primary key field with a primary key values that will be
    unique across all entities of this type, across all states.
    """
    # Get a shortened version of the python id that is globally unique for all
    # entities of this type for this person_id
    new_entity_id = _unique_object_id_for_entity(
        person_id=person_id, entity=entity, state_code=state_code
    )

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
    person_id: int, entities: List[EntityT], state_code: StateCode
) -> List[EntityT]:
    """Creates new copies of each of the provided |entities| and adds unique id
    values to the new copies."""
    new_entities: List[EntityT] = []
    for entity in entities:
        entity_copy = copy(entity)
        update_normalized_entity_with_globally_unique_id(
            person_id=person_id, entity=entity_copy, state_code=state_code
        )

        new_entities.append(entity_copy)

    return new_entities


def sort_normalized_entities_by_sequence_num(
    sequenced_entities: List[SequencedEntityMixinT],
) -> List[SequencedEntityMixinT]:
    """Returns the list of |sequenced_entities| sorted by sequence_num."""

    def get_sequence_num(entity: SequencedEntityMixinT) -> int:
        if entity.sequence_num is not None:
            return entity.sequence_num
        if not isinstance(entity, CoreEntity):
            raise ValueError(
                f"Class {entity.__class__} is not a CoreEntity! Cannot show limited_pii_repr."
            )
        raise ValueError(
            f"Class {entity.__class__} must have a sequence_num. Info: {entity.limited_pii_repr()}"
        )

    return sorted(sequenced_entities, key=get_sequence_num)


def update_forward_references_on_updated_entity(
    updated_entity: NormalizedStateEntityT,
    new_related_entities: List[NormalizedStateEntityT],
    forward_relationship_field: str,
    forward_relationship_field_type: BuildableAttrFieldType,
) -> None:
    """For the |updated_entity|, updates the value stored in the |forward_relationship_field|
    to point to the |new_related_entities| or the first item of |new_related_entities| if
    the field type is not a list."""
    if forward_relationship_field_type == BuildableAttrFieldType.FORWARD_REF:
        if len(new_related_entities) != 1:
            raise ValueError(
                f"Unexpected number entities to be attached to forward ref: [{new_related_entities}]"
            )
        setattr(updated_entity, forward_relationship_field, new_related_entities[0])
        return

    if forward_relationship_field_type != BuildableAttrFieldType.LIST:
        raise ValueError(
            f"Unexpected forward_relationship_field_type: [{forward_relationship_field_type}]"
        )

    setattr(updated_entity, forward_relationship_field, new_related_entities)
