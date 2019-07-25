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
from enum import Enum
from types import ModuleType
from typing import Dict, List, Set, Type, Any
from functools import lru_cache

import attr

from recidiviz.persistence.entity.base_entity import Entity, ExternalIdEntity
from recidiviz.persistence.entity.county import entities as county_entities
from recidiviz.persistence.entity.state import entities as state_entities
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
    state_entities.StateSupervisionViolationResponse.__name__,
    state_entities.StateAssessment.__name__,
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

    def is_back_edge(self, from_obj, to_obj) -> bool:
        """Given two object types, returns whether traversing from the first to
        the second object would be traveling along a 'back edge' in the object
        graph. A back edge is an edge that might introduce a cycle in the graph.
        Without back edges, the object graph should have no cycles.

        Args:
            from_obj: An object that is the origin of this edge
            to_obj: An object that is the destination of this edge
        Returns:
            True if a graph edge travelling from from_src_obj to to_src_obj is
                a back edge, i.e. it travels in a direction opposite to the
                class hierarchy.
        """
        from_class_name = from_obj.__class__.__name__
        to_class_name = to_obj.__class__.__name__

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
    FLAT_FIELD = 1
    FORWARD_EDGE = 2
    BACK_EDGE = 3


def get_set_entity_field_names(
        entity: Entity,
        entity_field_type: EntityFieldType) -> Set[str]:
    """Returns a set of field_names that correspond to any set fields on the
    provided |entity| that match the provided |entity_field_type|.
    """
    if entity.get_entity_name().startswith('state_'):
        direction_checker = SchemaEdgeDirectionChecker.state_direction_checker()
    else:
        direction_checker = \
            SchemaEdgeDirectionChecker.county_direction_checker()

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
            is_back_edge = direction_checker.is_back_edge(entity, v)
            if is_back_edge:
                back_edges.add(field)
            else:
                forward_edges.add(field)
        elif isinstance(v, list):
            # Disregard empty lists
            if not v:
                continue
            is_back_edge = direction_checker.is_back_edge(entity, v[0])
            if is_back_edge:
                back_edges.add(field)
            else:
                forward_edges.add(field)
        else:
            flat_fields.add(field)

    if entity_field_type is EntityFieldType.FLAT_FIELD:
        return flat_fields
    if entity_field_type is EntityFieldType.FORWARD_EDGE:
        return forward_edges
    if entity_field_type is EntityFieldType.BACK_EDGE:
        return back_edges
    raise EntityMatchingError(
        f"Unrecognized EntityFieldType {entity_field_type}",
        'entity_field_type')


# TODO(2163): Use get/set_field_from_list when possible to clean up code.
def get_field_as_list(entity: Entity, child_field_name: str) -> List[Entity]:
    field = get_field(entity, child_field_name)
    if isinstance(field, List):
        return field
    return [field]


def get_field(entity: Entity, field_name: str):
    if not hasattr(entity, field_name):
        raise EntityMatchingError(
            f"Expected entity {entity} to have field {field_name}, but it did "
            f"not.", entity.get_entity_name())
    return getattr(entity, field_name)


def set_field(entity: Entity, field_name: str, value: Any):
    if not hasattr(entity, field_name):
        raise EntityMatchingError(
            f"Expected entity {entity} to have field {field_name}, but it did "
            f"not.", entity.get_entity_name())
    return setattr(entity, field_name, value)


def set_field_from_list(entity: Entity, field_name: str, value: List):
    """Given the provided |value|, sets the value onto the provided |entity|
    based on the given |field_name|.
    """
    field = get_field(entity, field_name)
    if isinstance(field, list):
        set_field(entity, field_name, value)
    else:
        if not value:
            set_field(entity, field_name, None)
        elif len(value) == 1:
            set_field(entity, field_name, value[0])
        else:
            raise EntityMatchingError(
                f"Attempting to set singular field: {field_name} on entity: "
                f"{entity.get_entity_name()}, but got multiple values: "
                f"{value}.", entity.get_entity_name())
