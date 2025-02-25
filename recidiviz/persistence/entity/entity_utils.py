# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Utils for working with Entity classes or various |entities| modules."""

import inspect
import logging
from collections import defaultdict
from enum import Enum, auto
from types import ModuleType
from typing import Dict, List, Set, Type, Sequence, Optional
from functools import lru_cache

import attr

from recidiviz.common.attr_utils import get_non_flat_property_class_name
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_court_case import StateCourtType
from recidiviz.common.constants.state.state_incarceration import \
    StateIncarcerationType
from recidiviz.ingest.aggregate.aggregate_ingest_utils import pairwise
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity.base_entity import Entity, ExternalIdEntity
from recidiviz.persistence.entity.core_entity import CoreEntity
from recidiviz.persistence.entity.county import entities as county_entities
from recidiviz.persistence.entity.state import entities as state_entities, \
    entities
from recidiviz.persistence.errors import PersistenceError, EntityMatchingError

_STATE_CLASS_HIERARCHY = [
    state_entities.StatePerson.__name__,
    state_entities.StatePersonExternalId.__name__,
    state_entities.StatePersonAlias.__name__,
    state_entities.StatePersonRace.__name__,
    state_entities.StatePersonEthnicity.__name__,
    state_entities.StateSentenceGroup.__name__,
    state_entities.StateFine.__name__,
    state_entities.StateIncarcerationSentence.__name__,
    state_entities.StateSupervisionSentence.__name__,
    state_entities.StateCharge.__name__,
    state_entities.StateBond.__name__,
    state_entities.StateCourtCase.__name__,
    state_entities.StateIncarcerationPeriod.__name__,
    state_entities.StateIncarcerationIncident.__name__,
    state_entities.StateIncarcerationIncidentOutcome.__name__,
    state_entities.StateParoleDecision.__name__,
    state_entities.StateSupervisionPeriod.__name__,
    state_entities.StateSupervisionViolation.__name__,
    state_entities.StateSupervisionViolatedConditionEntry.__name__,
    state_entities.StateSupervisionViolationTypeEntry.__name__,
    state_entities.StateSupervisionViolationResponse.__name__,
    state_entities.StateSupervisionViolationResponseDecisionEntry.__name__,
    state_entities.StateAssessment.__name__,
    state_entities.StateProgramAssignment.__name__,
    state_entities.StateAgent.__name__,
]

_COUNTY_CLASS_HIERARCHY = [
    county_entities.Person.__name__,
    county_entities.Booking.__name__,
    county_entities.Arrest.__name__,
    county_entities.Hold.__name__,
    county_entities.Charge.__name__,
    county_entities.Bond.__name__,
    county_entities.Sentence.__name__,
]


class SchemaEdgeDirectionChecker:
    """A utility class to determine whether relationships between two objects
    are forward or back edges"""

    def __init__(self, class_hierarchy, module):
        self._class_hierarchy_map: Dict[str, int] = \
            _build_class_hierarchy_map(class_hierarchy, module)

    @classmethod
    def state_direction_checker(cls):
        return cls(_STATE_CLASS_HIERARCHY, state_entities)

    @classmethod
    def county_direction_checker(cls):
        return cls(_COUNTY_CLASS_HIERARCHY, county_entities)

    def is_back_edge(self, from_obj, to_field_name) -> bool:
        """Given an object and a field name on that object, returns whether
        traversing from the obj to an object in that field would be traveling
        along a 'back edge' in the object graph. A back edge is an edge that
        might introduce a cycle in the graph.
        Without back edges, the object graph should have no cycles.

        Args:
            from_obj: An object that is the origin of this edge
            to_field_name: A string field name for the field on from_obj
                containing the destination object of this edge
        Returns:
            True if a graph edge travelling from from_src_obj to an object in
                to_field_name is a back edge, i.e. it travels in a direction
                opposite to the class hierarchy.
        """
        from_class_name = from_obj.__class__.__name__

        if isinstance(from_obj, DatabaseEntity):
            to_class_name = \
                from_obj.get_relationship_property_class_name(to_field_name)
        elif isinstance(from_obj, Entity):
            to_class_name = get_non_flat_property_class_name(from_obj,
                                                             to_field_name)
        else:
            raise ValueError(f'Unexpected type [{type(from_obj)}]')

        if to_class_name is None:
            return False

        if from_class_name not in self._class_hierarchy_map:
            raise PersistenceError(
                f"Unable to convert: [{from_class_name}] not in the class "
                f"hierarchy map")

        if to_class_name not in self._class_hierarchy_map:
            raise PersistenceError(
                f"Unable to convert: [{to_class_name}] not in the class "
                f"hierarchy map")

        return self._class_hierarchy_map[from_class_name] >= \
            self._class_hierarchy_map[to_class_name]

    def assert_sorted(self, entity_types: List[Type[DatabaseEntity]]):
        """Throws if the input |entity_types| list is not in descending order
        based on class hierarchy.
        """
        for type_1, type_2 in pairwise(entity_types):
            type_1_name = type_1.__name__
            type_2_name = type_2.__name__

            if not self._class_hierarchy_map[type_1_name] \
                    < self._class_hierarchy_map[type_2_name]:
                raise ValueError(f'Unexpected ordering, found {type_1_name} '
                                 f'before {type_2_name}')


def _build_class_hierarchy_map(class_hierarchy: List[str],
                               entities_module: ModuleType) -> Dict[str, int]:
    """Returns a map of class names with their associated rank in the schema
    graph ordering.

    Args:
        class_hierarchy: A list of class names, ordered by rank in the
            schema graph ordering.
    Returns:
        A map of class names with their associated rank in the schema graph
        ordering. Lower number means closer to the root of the graph.
    """
    _check_class_hierarchy_includes_all_expected_classes(class_hierarchy,
                                                         entities_module)

    return {class_name: i for i, class_name in enumerate(class_hierarchy)}


def _check_class_hierarchy_includes_all_expected_classes(
        class_hierarchy: List[str], entities_module: ModuleType) -> None:
    expected_class_names = \
        get_all_entity_class_names_in_module(entities_module)

    given_minus_expected = \
        set(class_hierarchy).difference(expected_class_names)
    expected_minus_given = expected_class_names.difference(class_hierarchy)

    if given_minus_expected or expected_minus_given:
        msg = ""
        if given_minus_expected:
            msg += f"Found unexpected class in class hierarchy: " \
                f"[{list(given_minus_expected)[0]}]. "
        if expected_minus_given:
            msg += f"Missing expected class in class hierarchy: " \
                f"[{list(expected_minus_given)[0]}]. "

        raise PersistenceError(msg)


def get_all_entity_classes_in_module(
        entities_module: ModuleType) -> Set[Type[Entity]]:
    """Returns a set of all subclasses of Entity/ExternalIdEntity that are
    defined in the given module."""
    expected_classes: Set[Type[Entity]] = set()
    for attribute_name in dir(entities_module):
        attribute = getattr(entities_module, attribute_name)
        if inspect.isclass(attribute):
            if attribute is not Entity and \
                    attribute is not ExternalIdEntity and \
                    issubclass(attribute, Entity):
                expected_classes.add(attribute)

    return expected_classes


@lru_cache(maxsize=None)
def get_entity_class_in_module_with_name(
        entities_module: ModuleType, class_name: str) -> Type[Entity]:
    entity_classes = get_all_entity_classes_in_module(entities_module)

    for entity_class in entity_classes:
        if entity_class.__name__ == class_name:
            return entity_class

    raise LookupError(f"Entity class {class_name} does not exist in"
                      f"{entities_module}.")


def get_all_entity_class_names_in_module(
        entities_module: ModuleType) -> Set[str]:
    """Returns a set of all names of subclasses of Entity/ExternalIdEntity that
     are defined in the given module."""
    return {cls_.__name__
            for cls_ in get_all_entity_classes_in_module(entities_module)}


class EntityFieldType(Enum):
    FLAT_FIELD = auto()
    FOREIGN_KEYS = auto()
    FORWARD_EDGE = auto()
    BACK_EDGE = auto()
    ALL = auto()


# TODO(2383): Move all functions that take in a CoreEntity onto the CoreEntity
#  object itself, once we figure out the circular dependency with the
#  SchemaEdgeDirectionChecker.
def get_set_entity_field_names(
        entity: CoreEntity,
        entity_field_type: EntityFieldType) -> Set[str]:
    result = set()
    for field_name in get_all_core_entity_field_names(entity,
                                                      entity_field_type):
        v = entity.get_field(field_name)
        if isinstance(v, list):
            if v:
                result.add(field_name)
        elif v is not None:
            result.add(field_name)
    return result


def get_all_core_entity_field_names(
        entity: CoreEntity,
        entity_field_type: EntityFieldType) -> Set[str]:
    """Returns a set of field_names that correspond to any set fields on the
    provided |entity| that match the provided |entity_field_type|.
    """
    if entity.get_entity_name().startswith('state_'):
        direction_checker = SchemaEdgeDirectionChecker.state_direction_checker()
    else:
        direction_checker = \
            SchemaEdgeDirectionChecker.county_direction_checker()

    if isinstance(entity, DatabaseEntity):
        return _get_all_database_entity_field_names(entity,
                                                    entity_field_type,
                                                    direction_checker)
    if isinstance(entity, Entity):
        return _get_all_entity_field_names(entity,
                                           entity_field_type,
                                           direction_checker)

    raise ValueError(f"Invalid entity type [{type(entity)}]")


def _get_all_database_entity_field_names(entity: DatabaseEntity,
                                         entity_field_type: EntityFieldType,
                                         direction_checker):
    """Returns a set of field_names that correspond to any set fields on the
    provided DatabaseEntity |entity| that match the provided
    |entity_field_type|.
    """
    back_edges = set()
    forward_edges = set()
    flat_fields = set()
    foreign_keys = set()

    for relationship_field_name in entity.get_relationship_property_names():
        if direction_checker.is_back_edge(entity, relationship_field_name):
            back_edges.add(relationship_field_name)
        else:
            forward_edges.add(relationship_field_name)

    for foreign_key_name in entity.get_foreign_key_names():
        foreign_keys.add(foreign_key_name)

    for column_field_name in entity.get_column_property_names():
        if column_field_name not in foreign_keys:
            flat_fields.add(column_field_name)

    if entity_field_type is EntityFieldType.FLAT_FIELD:
        return flat_fields
    if entity_field_type is EntityFieldType.FOREIGN_KEYS:
        return foreign_keys
    if entity_field_type is EntityFieldType.FORWARD_EDGE:
        return forward_edges
    if entity_field_type is EntityFieldType.BACK_EDGE:
        return back_edges
    if entity_field_type is EntityFieldType.ALL:
        return flat_fields | foreign_keys | forward_edges | back_edges
    raise EntityMatchingError(
        f"Unrecognized EntityFieldType {entity_field_type}",
        'entity_field_type')


def _get_all_entity_field_names(entity: Entity,
                                entity_field_type: EntityFieldType,
                                direction_checker):
    """Returns a set of field_names that correspond to any set fields on the
    provided Entity |entity| that match the provided |entity_field_type|.
    """
    back_edges = set()
    forward_edges = set()
    flat_fields = set()
    for field, _ in attr.fields_dict(entity.__class__).items():
        v = getattr(entity, field)

        if v is None:
            continue

        # TODO(1908): Update traversal logic if relationship fields can be
        # different types aside from Entity and List
        if issubclass(type(v), Entity):
            is_back_edge = direction_checker.is_back_edge(entity, field)
            if is_back_edge:
                back_edges.add(field)
            else:
                forward_edges.add(field)
        elif isinstance(v, list):
            # Disregard empty lists
            if not v:
                continue
            is_back_edge = direction_checker.is_back_edge(entity, field)
            if is_back_edge:
                back_edges.add(field)
            else:
                forward_edges.add(field)
        else:
            flat_fields.add(field)

    if entity_field_type is EntityFieldType.FLAT_FIELD:
        return flat_fields
    if entity_field_type is EntityFieldType.FOREIGN_KEYS:
        return set()  # Entity objects never have foreign keys
    if entity_field_type is EntityFieldType.FORWARD_EDGE:
        return forward_edges
    if entity_field_type is EntityFieldType.BACK_EDGE:
        return back_edges
    if entity_field_type is EntityFieldType.ALL:
        return flat_fields | forward_edges | back_edges
    raise EntityMatchingError(
        f"Unrecognized EntityFieldType {entity_field_type}",
        'entity_field_type')


def is_placeholder(entity: CoreEntity) -> bool:
    """Determines if the provided entity is a placeholder. Conceptually, a
    placeholder is an object that we have no information about, but have
    inferred its existence based on other objects we do have information about.
    Generally, an entity is a placeholder if all of the optional flat fields are
    empty or set to a default value.
    """

    # Although these are not flat fields, they represent characteristics of a
    # person. If present, we do have information about the provided person, and
    # therefore it is not a placeholder.
    if isinstance(entity, (schema.StatePerson, entities.StatePerson)):
        if any([entity.external_ids, entity.races, entity.aliases,
                entity.ethnicities]):
            return False

    set_flat_fields = get_set_entity_field_names(
        entity, EntityFieldType.FLAT_FIELD)

    primary_key_name = entity.get_primary_key_column_name()
    if primary_key_name in set_flat_fields:
        set_flat_fields.remove(primary_key_name)

    # TODO(2244): Change this to a general approach so we don't need to check
    # explicit columns
    if 'state_code' in set_flat_fields:
        set_flat_fields.remove('state_code')

    if 'status' in set_flat_fields:
        if entity.has_default_status():
            set_flat_fields.remove('status')

    if 'incarceration_type' in set_flat_fields:
        if entity.has_default_enum(
                'incarceration_type', StateIncarcerationType.STATE_PRISON):
            set_flat_fields.remove('incarceration_type')

    if 'court_type' in set_flat_fields:
        if entity.has_default_enum('court_type',
                                   StateCourtType.PRESENT_WITHOUT_INFO):
            set_flat_fields.remove('court_type')

    if 'agent_type' in set_flat_fields:
        if entity.has_default_enum('agent_type',
                                   StateAgentType.PRESENT_WITHOUT_INFO):
            set_flat_fields.remove('agent_type')

    return not bool(set_flat_fields)


def _print_indented(s: str, indent: int):
    print(f'{" " * indent}{s}')


def _obj_id_str(entity: CoreEntity):
    return f'{entity.get_entity_name()} ({id(entity)})'


def print_entity_tree(entity: CoreEntity, indent: int = 0):
    """Recursively prints out all objects in the tree below the given entity."""
    _print_indented(_obj_id_str(entity), indent)

    indent = indent + 2
    for field in get_all_core_entity_field_names(entity,
                                                 EntityFieldType.FLAT_FIELD):
        val = entity.get_field(field)
        _print_indented(f'{field}: {str(val)}', indent)

    for child_field in \
            get_all_core_entity_field_names(entity,
                                            EntityFieldType.FORWARD_EDGE):
        child = entity.get_field(child_field)

        if child is not None:
            if isinstance(child, list):
                if not child:
                    _print_indented(f'{child_field}: []', indent)
                else:
                    _print_indented(f'{child_field}: [', indent)
                    for c in child:
                        print_entity_tree(c, indent + 2)
                    _print_indented(f']', indent)

            else:
                _print_indented(f'{child_field}:', indent)
                print_entity_tree(child, indent + 2)
        else:
            _print_indented(f'{child_field}: None', indent)

    for child_field in \
            get_all_core_entity_field_names(entity, EntityFieldType.BACK_EDGE):
        child = entity.get_field(child_field)
        if child:
            if isinstance(child, list):
                first_child = next(iter(child))
                unique = {id(child) for c in child}
                len_str = f'{len(child)}' \
                    if len(unique) == len(child) \
                    else f'{len(child)} - ONLY {len(unique)} UNIQUE!'
                _print_indented(
                    f'{child_field} ({len_str}): '
                    f'[{_obj_id_str(first_child)}, ...] - backedge', indent)
            else:
                _print_indented(
                    f'{child_field}: {_obj_id_str(child)} - backedge', indent)
        else:
            _print_indented(f'{child_field}: None - backedge', indent)


def get_all_db_objs_from_trees(db_objs: Sequence[DatabaseEntity], result=None):
    if result is None:
        result = set()
    for root_obj in db_objs:
        for obj in get_all_db_objs_from_tree(root_obj, result):
            result.add(obj)
    return result


def get_all_db_objs_from_tree(db_obj: DatabaseEntity,
                              result=None) -> Set[DatabaseEntity]:
    if result is None:
        result = set()

    if db_obj in result:
        return result

    result.add(db_obj)

    set_fields = get_set_entity_field_names(
        db_obj, EntityFieldType.FORWARD_EDGE)
    for field in set_fields:
        child = db_obj.get_field_as_list(field)
        get_all_db_objs_from_trees(child, result)

    return result


def get_all_entities_from_tree(
        entity: Entity,
        result: Optional[List[Entity]] = None,
        seen_ids: Optional[Set[int]] = None) -> List[Entity]:
    """Returns a list of all entities in the tree below the entity,
    including the entity itself. Entities are deduplicated by Python object id.
    """

    if result is None:
        result = []
    if seen_ids is None:
        seen_ids = set()

    if id(entity) in seen_ids:
        return result

    result.append(entity)
    seen_ids.add(id(entity))

    fields = get_all_core_entity_field_names(entity,
                                             EntityFieldType.FORWARD_EDGE)

    for field in fields:
        child = entity.get_field(field)

        if child is None:
            continue

        if isinstance(child, list):
            for c in child:
                get_all_entities_from_tree(c, result, seen_ids)
        else:
            get_all_entities_from_tree(child, result, seen_ids)

    return result


def get_entities_by_type(
        all_entities: Sequence[DatabaseEntity],
        entities_of_type: Dict[Type, List[DatabaseEntity]] = None,
        seen_entities: Optional[Set[int]] = None) -> \
        Dict[Type, List[DatabaseEntity]]:
    """Creates a list of entities for each entity type present in the provided
    |all_entities| graph. Returns the types and the corresponding entity lists
    as a dictionary.
    - if |entities_of_type| is provided, this method will update this dictionary
      rather than creating a new one to return.
    - if |seen_entities| is provided, this method will skip over any entities
      found in |all_entities| that are also present in |seen_entities|.
    """
    if entities_of_type is None:
        entities_of_type = defaultdict(list)
    if seen_entities is None:
        seen_entities = set()

    for entity in all_entities:
        if id(entity) in seen_entities:
            continue
        seen_entities.add(id(entity))
        entity_cls = entity.__class__
        if entity_cls not in entities_of_type:
            entities_of_type[entity_cls] = []
        entities_of_type[entity_cls].append(entity)
        for child_name in get_set_entity_field_names(
                entity, EntityFieldType.FORWARD_EDGE):
            child_list = entity.get_field_as_list(child_name)
            get_entities_by_type(child_list, entities_of_type, seen_entities)

    return entities_of_type


def is_standalone_class(cls: Type[DatabaseEntity]) -> bool:
    """Returns True if the provided cls is a class that can exist separate
    from a StatePerson tree.
    """
    return 'person_id' not in cls.get_column_property_names()


def is_standalone_entity(entity: DatabaseEntity) -> bool:
    """Returns True if the provided entity is an instance of a class that
    can exist separate from a StatePerson tree.
    """
    return is_standalone_class(entity.__class__)


def log_entity_count(db_persons: List[schema.StatePerson]):
    """Counts and logs the total number of entities of each class included in
    the |db_persons| trees.
    """
    entities_by_type = get_entities_by_type(db_persons)
    debug_msg = 'Entity counter\n'
    for cls, entities_of_cls in entities_by_type.items():
        debug_msg += f'{str(cls.__name__)}: {str(len(entities_of_cls))}\n'
    logging.info(debug_msg)
