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

TODO(#10731): Move this file to recidiviz/calculator/pipeline/normalization once all
  functions are only used by the normalization pipeline.
"""
from collections import defaultdict
from copy import copy
from typing import Any, Dict, List, Optional, Sequence, Set, Type, TypeVar, Union

from more_itertools import one

from recidiviz.big_query.big_query_utils import MAX_BQ_INT
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
)
from recidiviz.common.constants.states import MAX_FIPS_CODE

# All entity classes that have Normalized versions
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_deserialize import EntityT
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
SequencedEntityMixinT = TypeVar("SequencedEntityMixinT", bound=SequencedEntityMixin)
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

# Cached _entity_id_index value
_entity_id_index: Optional[Dict[int, Dict[str, Set[int]]]] = None


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


def normalized_entity_class_exists_for_base_class_with_name(
    base_class_name: str,
) -> bool:
    """Returns whether there is a Normalized version of the state entity class that has
    the |base_class_name| name.

    Example behavior:
        "StateIncarcerationPeriod" -> True (since NormalizedStateIncarcerationPeriod)
        "StateCharge" -> False (since there is no NormalizedStateCharge)
    """
    try:
        _ = normalized_entity_class_with_base_class_name(base_class_name)
        return True
    except ValueError:
        return False


def state_base_entity_class_for_entity_class(
    entity_class: Union[Type[Entity], Type[NormalizedStateEntity]]
) -> Type[Entity]:
    """Returns the state Entity type of the provided |entity_class|."""
    if issubclass(entity_class, NormalizedStateEntity):
        if not issubclass(entity_class, Entity):
            raise ValueError(
                f"Found entity_class [{entity_class}] that is not a subclass of "
                f"Entity."
            )
        return one(b for b in entity_class.__bases__ if issubclass(b, Entity))

    if issubclass(entity_class, Entity):
        return entity_class

    raise ValueError(f"Unexpected entity_class [{entity_class}]")


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


def sort_normalized_entities_by_sequence_num(
    sequenced_entities: List[SequencedEntityMixinT],
) -> List[SequencedEntityMixinT]:
    """Returns the list of |sequenced_entities| sorted by sequence_num."""
    sorted_entities = sorted(sequenced_entities, key=lambda key: key.sequence_num)
    return sorted_entities
