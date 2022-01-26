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
import json
import logging
from collections import defaultdict
from enum import Enum, auto
from functools import lru_cache
from types import ModuleType
from typing import Dict, Iterable, List, Optional, Sequence, Set, Type, Union, cast

import attr

from recidiviz.common.attr_utils import (
    get_non_flat_attribute_class_name,
    is_flat_field,
    is_forward_ref,
    is_list,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.ingest.aggregate.aggregate_ingest_utils import pairwise
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity.base_entity import (
    Entity,
    EnumEntity,
    ExternalIdEntity,
)
from recidiviz.persistence.entity.core_entity import CoreEntity
from recidiviz.persistence.entity.county import entities as county_entities
from recidiviz.persistence.entity.entity_deserialize import EntityFactory
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state.entities import StatePersonExternalId
from recidiviz.persistence.errors import PersistenceError

_STATE_CLASS_HIERARCHY = [
    state_entities.StatePerson.__name__,
    state_entities.StatePersonExternalId.__name__,
    state_entities.StatePersonAlias.__name__,
    state_entities.StatePersonRace.__name__,
    state_entities.StatePersonEthnicity.__name__,
    state_entities.StateIncarcerationSentence.__name__,
    state_entities.StateSupervisionSentence.__name__,
    state_entities.StateCharge.__name__,
    state_entities.StateCourtCase.__name__,
    state_entities.StateIncarcerationPeriod.__name__,
    state_entities.StateIncarcerationIncident.__name__,
    state_entities.StateIncarcerationIncidentOutcome.__name__,
    state_entities.StateSupervisionPeriod.__name__,
    state_entities.StateSupervisionContact.__name__,
    state_entities.StateSupervisionCaseTypeEntry.__name__,
    state_entities.StateSupervisionViolation.__name__,
    state_entities.StateSupervisionViolatedConditionEntry.__name__,
    state_entities.StateSupervisionViolationTypeEntry.__name__,
    state_entities.StateSupervisionViolationResponse.__name__,
    state_entities.StateSupervisionViolationResponseDecisionEntry.__name__,
    state_entities.StateAssessment.__name__,
    state_entities.StateProgramAssignment.__name__,
    state_entities.StateAgent.__name__,
    state_entities.StateEarlyDischarge.__name__,
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

_state_direction_checker = None
_county_direction_checker = None


class SchemaEdgeDirectionChecker:
    """A utility class to determine whether relationships between two objects
    are forward or back edges"""

    def __init__(self, class_hierarchy: List[str], module: ModuleType):
        self._class_hierarchy_map: Dict[str, int] = _build_class_hierarchy_map(
            class_hierarchy, module
        )

    @classmethod
    def state_direction_checker(cls):
        global _state_direction_checker
        if not _state_direction_checker:
            _state_direction_checker = cls(_STATE_CLASS_HIERARCHY, state_entities)
        return _state_direction_checker

    @classmethod
    def county_direction_checker(cls):
        global _county_direction_checker
        if not _county_direction_checker:
            _county_direction_checker = cls(_COUNTY_CLASS_HIERARCHY, county_entities)
        return _county_direction_checker

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
            to_class_name = from_obj.get_relationship_property_class_name(to_field_name)
        elif isinstance(from_obj, Entity):
            to_class_name = get_non_flat_property_class_name(from_obj, to_field_name)
        else:
            raise ValueError(f"Unexpected type [{type(from_obj)}]")

        if to_class_name is None:
            return False

        if from_class_name not in self._class_hierarchy_map:
            raise PersistenceError(
                f"Unable to convert: [{from_class_name}] not in the class "
                f"hierarchy map"
            )

        if to_class_name not in self._class_hierarchy_map:
            raise PersistenceError(
                f"Unable to convert: [{to_class_name}] not in the class "
                f"hierarchy map"
            )

        return (
            self._class_hierarchy_map[from_class_name]
            >= self._class_hierarchy_map[to_class_name]
        )

    def is_higher_ranked(self, cls_1: Type[CoreEntity], cls_2: Type[CoreEntity]):
        """Returns True if the provided |cls_1| has a higher rank than the
        provided |cls_2|.
        """
        type_1_name = cls_1.__name__
        type_2_name = cls_2.__name__

        return (
            self._class_hierarchy_map[type_1_name]
            < self._class_hierarchy_map[type_2_name]
        )

    def assert_sorted(self, entity_types: List[Type[CoreEntity]]):
        """Throws if the input |entity_types| list is not in descending order
        based on class hierarchy.
        """
        for type_1, type_2 in pairwise(entity_types):
            if not self.is_higher_ranked(type_1, type_2):
                raise ValueError(
                    f"Unexpected ordering, found {type_1.__name__} before "
                    f"{type_2.__name__}"
                )


def _build_class_hierarchy_map(
    class_hierarchy: List[str], entities_module: ModuleType
) -> Dict[str, int]:
    """Returns a map of class names with their associated rank in the schema
    graph ordering.

    Args:
        class_hierarchy: A list of class names, ordered by rank in the
            schema graph ordering.
    Returns:
        A map of class names with their associated rank in the schema graph
        ordering. Lower number means closer to the root of the graph.
    """
    _check_class_hierarchy_includes_all_expected_classes(
        class_hierarchy, entities_module
    )

    return {class_name: i for i, class_name in enumerate(class_hierarchy)}


def _check_class_hierarchy_includes_all_expected_classes(
    class_hierarchy: List[str], entities_module: ModuleType
) -> None:
    expected_class_names = get_all_entity_class_names_in_module(entities_module)

    given_minus_expected = set(class_hierarchy).difference(expected_class_names)
    expected_minus_given = expected_class_names.difference(class_hierarchy)

    if given_minus_expected or expected_minus_given:
        msg = ""
        if given_minus_expected:
            msg += (
                f"Found unexpected class in class hierarchy: "
                f"[{list(given_minus_expected)[0]}]. "
            )
        if expected_minus_given:
            msg += (
                f"Missing expected class in class hierarchy: "
                f"[{list(expected_minus_given)[0]}]. "
            )

        raise PersistenceError(msg)


def get_all_entity_classes_in_module(entities_module: ModuleType) -> Set[Type[Entity]]:
    """Returns a set of all subclasses of Entity/ExternalIdEntity that are
    defined in the given module."""
    expected_classes: Set[Type[Entity]] = set()
    for attribute_name in dir(entities_module):
        attribute = getattr(entities_module, attribute_name)
        if inspect.isclass(attribute):
            if (
                attribute is not Entity
                and attribute is not ExternalIdEntity
                and attribute is not EnumEntity
                and issubclass(attribute, Entity)
            ):
                expected_classes.add(attribute)

    return expected_classes


def get_all_entity_factory_classes_in_module(
    factories_module: ModuleType,
) -> Set[Type[EntityFactory]]:
    """Returns a set of all subclasses of EntityFactory that are defined in the
    given module."""
    expected_classes: Set[Type[EntityFactory]] = set()
    for attribute_name in dir(factories_module):
        attribute = getattr(factories_module, attribute_name)
        if inspect.isclass(attribute):
            if attribute is not EntityFactory and issubclass(attribute, EntityFactory):
                expected_classes.add(attribute)

    return expected_classes


@lru_cache(maxsize=None)
def get_entity_class_in_module_with_name(
    entities_module: ModuleType, class_name: str
) -> Type[Entity]:
    entity_classes = get_all_entity_classes_in_module(entities_module)

    for entity_class in entity_classes:
        if entity_class.__name__ == class_name:
            return entity_class

    raise LookupError(
        f"Entity class {class_name} does not exist in" f"{entities_module}."
    )


def get_all_entity_class_names_in_module(entities_module: ModuleType) -> Set[str]:
    """Returns a set of all names of subclasses of Entity/ExternalIdEntity that
    are defined in the given module."""
    return {cls_.__name__ for cls_ in get_all_entity_classes_in_module(entities_module)}


class EntityFieldType(Enum):
    FLAT_FIELD = auto()
    FOREIGN_KEYS = auto()
    FORWARD_EDGE = auto()
    BACK_EDGE = auto()
    ALL = auto()


class CoreEntityFieldIndex:
    """Class that caches the results of certain CoreEntity class introspection
    functionality.
    """

    def __init__(self) -> None:
        self.state_direction_checker = (
            SchemaEdgeDirectionChecker.state_direction_checker()
        )
        self.county_direction_checker = (
            SchemaEdgeDirectionChecker.county_direction_checker()
        )

        # Cache of fields by field type for DatabaseEntity classes
        self.database_entity_fields_by_field_type: Dict[
            str, Dict[EntityFieldType, Set[str]]
        ] = defaultdict(lambda: defaultdict(set))

        # Cache of fields by field type for Entity classes
        self.entity_fields_by_field_type: Dict[
            str, Dict[EntityFieldType, Set[str]]
        ] = defaultdict(lambda: defaultdict(set))

    def _direction_checker(self, entity_name: str) -> SchemaEdgeDirectionChecker:
        if entity_name.startswith("state_"):
            return self.state_direction_checker
        return self.county_direction_checker

    def get_fields_with_non_empty_values(
        self, entity: CoreEntity, entity_field_type: EntityFieldType
    ) -> Set[str]:
        """Returns a set of field_names that correspond to any non-empty (nonnull or
        non-empty list) fields on the provided |entity| class that match the provided
        |entity_field_type|.

        Note: This function is relatively slow for Entity type entities and should not
        be called in a tight loop.
        """
        result = set()
        for field_name in self.get_all_core_entity_fields(entity, entity_field_type):
            v = entity.get_field(field_name)
            if isinstance(v, list):
                if v:
                    result.add(field_name)
            elif v is not None:
                result.add(field_name)
        return result

    def get_all_core_entity_fields(
        self, entity: CoreEntity, entity_field_type: EntityFieldType
    ) -> Set[str]:
        """Returns a set of field_names that correspond to any fields (non-empty or
        otherwise) on the provided DatabaseEntity |entity| class that match the provided
        |entity_field_type|. Fields are included whether or not the values are non-empty
        on the provided object.

        This function is caches the results for subsequent calls.
        """
        entity_name = entity.get_entity_name()

        if isinstance(entity, DatabaseEntity):
            if not self.database_entity_fields_by_field_type[entity_name][
                entity_field_type
            ]:
                self.database_entity_fields_by_field_type[entity_name][
                    entity_field_type
                ] = self._get_all_database_entity_fields_slow(entity, entity_field_type)
            return self.database_entity_fields_by_field_type[entity_name][
                entity_field_type
            ]

        if isinstance(entity, Entity):
            if not self.entity_fields_by_field_type[entity_name][entity_field_type]:
                self.entity_fields_by_field_type[entity_name][
                    entity_field_type
                ] = self._get_entity_fields_with_type_slow(entity, entity_field_type)
            return self.entity_fields_by_field_type[entity_name][entity_field_type]

        raise ValueError(f"Unexpected entity type: {type(entity)}")

    def _get_all_database_entity_fields_slow(
        self,
        entity: DatabaseEntity,
        entity_field_type: EntityFieldType,
    ) -> Set[str]:
        """Returns a set of field_names that correspond to any fields (non-empty or
        otherwise) on the provided DatabaseEntity |entity| that match the provided
        |entity_field_type|.

        This function is relatively slow and the results should be cached across
        repeated calls.
        """
        entity_name = entity.get_entity_name()
        direction_checker = self._direction_checker(entity_name)

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
        raise ValueError(
            f"Unrecognized EntityFieldType [{entity_field_type}] on entity [{entity}]"
        )

    def _get_entity_fields_with_type_slow(
        self, entity: Entity, entity_field_type: EntityFieldType
    ) -> Set[str]:
        """Returns a set of field_names that correspond to any fields (non-empty or
        otherwise) on the provided Entity |entity| that match the provided
        |entity_field_type|.

        This function is relatively slow and the results should be cached across
        repeated calls.
        """
        entity_name = entity.get_entity_name()
        direction_checker = self._direction_checker(entity_name)

        back_edges = set()
        forward_edges = set()
        flat_fields = set()
        for field, attribute in attr.fields_dict(entity.__class__).items():
            # TODO(#1908): Update traversal logic if relationship fields can be
            # different types aside from Entity and List
            if is_forward_ref(attribute) or is_list(attribute):
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
        raise ValueError(
            f"Unrecognized EntityFieldType {entity_field_type} on entity [{entity}]"
        )


def prune_dangling_placeholders_from_tree(
    entity: CoreEntity,
    field_index: CoreEntityFieldIndex = CoreEntityFieldIndex(),
) -> Optional[CoreEntity]:
    """Recursively prunes dangling placeholders from the entity tree, returning
    the pruned entity tree, or None if the tree itself is a dangling
    placeholder. The returned tree will have no subtrees where all the nodes are
    placeholders.
    """

    has_non_placeholder_children = False
    for field_name in field_index.get_fields_with_non_empty_values(
        entity, EntityFieldType.FORWARD_EDGE
    ):

        children_to_keep = []
        for child in entity.get_field_as_list(field_name):
            pruned_tree = prune_dangling_placeholders_from_tree(child, field_index)
            if pruned_tree:
                children_to_keep.append(pruned_tree)
                has_non_placeholder_children = True

        entity.set_field_from_list(field_name, children_to_keep)

    if has_non_placeholder_children or not is_placeholder(entity, field_index):
        return entity

    return None


def is_placeholder(entity: CoreEntity, field_index: CoreEntityFieldIndex) -> bool:
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
        if any([entity.external_ids, entity.races, entity.aliases, entity.ethnicities]):
            return False

    set_flat_fields = get_explicilty_set_flat_fields(entity, field_index)
    return not bool(set_flat_fields)


def is_reference_only_entity(
    entity: CoreEntity, field_index: CoreEntityFieldIndex
) -> bool:
    """Returns true if this object does not contain any meaningful information
    describing the entity, but instead only identifies the entity for reference
    purposes. Concretely, this means the object has an external_id but no other set
    fields (aside from default values).
    """
    set_flat_fields = get_explicilty_set_flat_fields(entity, field_index)
    if isinstance(entity, (schema.StatePerson, entities.StatePerson)):
        if set_flat_fields or any([entity.races, entity.aliases, entity.ethnicities]):
            return False
        return bool(entity.external_ids)
    return set_flat_fields == {"external_id"}


def get_explicilty_set_flat_fields(
    entity: CoreEntity, field_index: CoreEntityFieldIndex
) -> Set[str]:
    """Returns the set of field names for fields on the entity that have been set with
    non-default values. The "state_code" field is also excluded, as it is set with the
    same value on every entity for a given ingest run.
    """
    set_flat_fields = field_index.get_fields_with_non_empty_values(
        entity, EntityFieldType.FLAT_FIELD
    )

    primary_key_name = entity.get_primary_key_column_name()
    if primary_key_name in set_flat_fields:
        set_flat_fields.remove(primary_key_name)

    # TODO(#2244): Change this to a general approach so we don't need to check
    # explicit columns
    if "state_code" in set_flat_fields:
        set_flat_fields.remove("state_code")

    default_enum_value_fields = {
        field_name
        for field_name in set_flat_fields
        if entity.is_default_enum(field_name)
    }

    set_flat_fields -= default_enum_value_fields

    if "incarceration_type" in set_flat_fields:
        if entity.is_default_enum(
            "incarceration_type", StateIncarcerationType.STATE_PRISON.value
        ):
            set_flat_fields.remove("incarceration_type")

    return set_flat_fields


def _sort_based_on_flat_fields(
    db_entities: Sequence[CoreEntity], field_index: CoreEntityFieldIndex
) -> None:
    """Helper function that sorts all entities in |db_entities| in place as
    well as all children of |db_entities|. Sorting is done by first by an
    external_id if that field exists, then present flat fields.
    """

    def _get_entity_sort_key(e: CoreEntity) -> str:
        """Generates a sort key for the given entity based on the flat field values
        in this entity."""
        return f"{e.get_external_id()}#{get_flat_fields_json_str(e, field_index)}"

    db_entities = cast(List, db_entities)
    db_entities.sort(key=_get_entity_sort_key)
    for entity in db_entities:
        for field_name in field_index.get_fields_with_non_empty_values(
            entity, EntityFieldType.FORWARD_EDGE
        ):
            field = entity.get_field_as_list(field_name)
            _sort_based_on_flat_fields(field, field_index)


def get_flat_fields_json_str(entity: CoreEntity, field_index: CoreEntityFieldIndex):
    flat_fields_dict: Dict[str, str] = {}
    for field_name in field_index.get_fields_with_non_empty_values(
        entity, EntityFieldType.FLAT_FIELD
    ):
        flat_fields_dict[field_name] = str(entity.get_field(field_name))
    return json.dumps(flat_fields_dict, sort_keys=True)


def _print_indented(s: str, indent: int):
    print(f'{" " * indent}{s}')


def _obj_id_str(entity: CoreEntity, id_mapping: Dict[int, int]):
    python_obj_id = id(entity)

    if python_obj_id not in id_mapping:
        fake_id = len(id_mapping)
        id_mapping[python_obj_id] = fake_id
    else:
        fake_id = id_mapping[python_obj_id]

    return f"{entity.get_entity_name()} ({fake_id})"


def print_entity_trees(
    entities_list: Sequence[CoreEntity],
    print_tree_structure_only: bool = False,
    python_id_to_fake_id: Dict[int, int] = None,
    # Default arg caches across calls to this function
    field_index: CoreEntityFieldIndex = CoreEntityFieldIndex(),
):
    """Recursively prints out all objects in the trees below the given list of
    entities. Each time we encounter a new object, we assign a new fake id (an
    auto-incrementing count) and print that with the object.

    This means that two lists with the exact same shape/flat fields will print
    out the exact same string, making it much easier to debug edge-related
    issues in Diffchecker, etc.

    Note: this function sorts any list fields in the provided entity IN PLACE
    (should not matter for any equality checks we generally do).
    """

    if python_id_to_fake_id is None:
        python_id_to_fake_id = {}
        _sort_based_on_flat_fields(entities_list, field_index)

    for entity in entities_list:
        print_entity_tree(
            entity,
            python_id_to_fake_id=python_id_to_fake_id,
            print_tree_structure_only=print_tree_structure_only,
            field_index=field_index,
        )


def print_entity_tree(
    entity: CoreEntity,
    print_tree_structure_only: bool = False,
    indent: int = 0,
    python_id_to_fake_id: Dict[int, int] = None,
    # Default arg caches across calls to this function
    field_index: CoreEntityFieldIndex = CoreEntityFieldIndex(),
):
    """Recursively prints out all objects in the tree below the given entity. Each time we encounter a new object, we
    assign a new fake id (an auto-incrementing count) and print that with the object.

    This means that two entity trees with the exact same shape/flat fields will print out the exact same string, making
    it much easier to debug edge-related issues in Diffchecker, etc.

    Note: this function sorts any list fields in the provided entity IN PLACE (should not matter for any equality checks
    we generally do).
    """
    if python_id_to_fake_id is None:
        python_id_to_fake_id = {}
        _sort_based_on_flat_fields([entity], field_index)

    _print_indented(_obj_id_str(entity, python_id_to_fake_id), indent)

    indent = indent + 2
    for field in field_index.get_fields_with_non_empty_values(
        entity, EntityFieldType.FLAT_FIELD
    ):
        if field == "external_id" or not print_tree_structure_only:
            val = entity.get_field(field)
            _print_indented(f"{field}: {str(val)}", indent)

    for child_field in field_index.get_fields_with_non_empty_values(
        entity, EntityFieldType.FORWARD_EDGE
    ):
        child = entity.get_field(child_field)

        if child is not None:
            if isinstance(child, list):
                if not child:
                    _print_indented(f"{child_field}: []", indent)
                else:
                    _print_indented(f"{child_field}: [", indent)
                    for c in child:
                        print_entity_tree(
                            c,
                            print_tree_structure_only,
                            indent + 2,
                            python_id_to_fake_id,
                            field_index=field_index,
                        )
                    _print_indented("]", indent)

            else:
                _print_indented(f"{child_field}:", indent)
                print_entity_tree(
                    child,
                    print_tree_structure_only,
                    indent + 2,
                    python_id_to_fake_id,
                    field_index=field_index,
                )
        else:
            _print_indented(f"{child_field}: None", indent)

    for child_field in field_index.get_fields_with_non_empty_values(
        entity, EntityFieldType.BACK_EDGE
    ):
        child = entity.get_field(child_field)
        if not child:
            raise ValueError(f"Expected non-empty child value for field {child_field}")
        if isinstance(child, list):
            first_child = next(iter(child))
            unique = {id(c) for c in child}
            len_str = (
                f"{len(child)}"
                if len(unique) == len(child)
                else f"{len(child)} - ONLY {len(unique)} UNIQUE!"
            )

            id_str = _obj_id_str(first_child, python_id_to_fake_id)
            ellipsis_str = ", ..." if len(child) > 1 else ""

            _print_indented(
                f"{child_field} ({len_str}): [{id_str}{ellipsis_str}] - backedge",
                indent,
            )
        else:
            id_str = _obj_id_str(child, python_id_to_fake_id)
            _print_indented(f"{child_field}: {id_str} - backedge", indent)


def get_all_db_objs_from_trees(
    db_objs: Sequence[DatabaseEntity], field_index: CoreEntityFieldIndex, result=None
):
    if result is None:
        result = set()
    for root_obj in db_objs:
        for obj in get_all_db_objs_from_tree(root_obj, field_index, result):
            result.add(obj)
    return result


def get_all_db_objs_from_tree(
    db_obj: DatabaseEntity, field_index: CoreEntityFieldIndex, result=None
) -> Set[DatabaseEntity]:
    if result is None:
        result = set()

    if db_obj in result:
        return result

    result.add(db_obj)

    set_fields = field_index.get_fields_with_non_empty_values(
        db_obj, EntityFieldType.FORWARD_EDGE
    )
    for field in set_fields:
        child = db_obj.get_field_as_list(field)
        get_all_db_objs_from_trees(child, field_index, result)

    return result


def get_all_entities_from_tree(
    entity: Entity,
    field_index: CoreEntityFieldIndex,
    result: Optional[List[Entity]] = None,
    seen_ids: Optional[Set[int]] = None,
) -> List[Entity]:
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

    fields = field_index.get_fields_with_non_empty_values(
        entity, EntityFieldType.FORWARD_EDGE
    )

    for field in fields:
        child = entity.get_field(field)

        if child is None:
            raise ValueError("Expected only nonnull values at this point")

        if isinstance(child, list):
            for c in child:
                get_all_entities_from_tree(c, field_index, result, seen_ids)
        else:
            get_all_entities_from_tree(child, field_index, result, seen_ids)

    return result


def get_entities_by_type(
    all_entities: Sequence[DatabaseEntity],
    field_index: CoreEntityFieldIndex,
    entities_of_type: Dict[Type, List[DatabaseEntity]] = None,
    seen_entities: Optional[Set[int]] = None,
) -> Dict[Type, List[DatabaseEntity]]:
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
        for child_name in field_index.get_fields_with_non_empty_values(
            entity, EntityFieldType.FORWARD_EDGE
        ):
            child_list = entity.get_field_as_list(child_name)
            get_entities_by_type(
                child_list, field_index, entities_of_type, seen_entities
            )

    return entities_of_type


def is_standalone_class(cls: Type[DatabaseEntity]) -> bool:
    """Returns True if the provided cls is a class that can exist separate
    from a StatePerson tree.
    """
    return "person_id" not in cls.get_column_property_names()


def is_standalone_entity(entity: DatabaseEntity) -> bool:
    """Returns True if the provided entity is an instance of a class that
    can exist separate from a StatePerson tree.
    """
    return is_standalone_class(entity.__class__)


def log_entity_count(
    db_persons: List[schema.StatePerson], field_index: CoreEntityFieldIndex
):
    """Counts and logs the total number of entities of each class included in
    the |db_persons| trees.
    """
    entities_by_type = get_entities_by_type(db_persons, field_index)
    debug_msg = "Entity counter\n"
    for cls, entities_of_cls in entities_by_type.items():
        debug_msg += f"{str(cls.__name__)}: {str(len(entities_of_cls))}\n"
    logging.info(debug_msg)


def person_has_id(
    person: Union[entities.StatePerson, schema.StatePerson], person_external_id: str
):
    """Returns true if the given |person_external_id| matches any of the
    external_ids for the given |person|.
    """
    external_ids_for_person = [eid.external_id for eid in person.external_ids]
    return person_external_id in external_ids_for_person


def get_ids(entities_list: Iterable[CoreEntity]) -> Set[int]:
    """Returns a set of all database ids in a list of entities"""
    return {e.get_id() for e in entities_list}


def get_single_state_code(
    external_id_entities: Iterable[Union[ExternalIdEntity, StatePersonExternalId]]
) -> str:
    """Returns the state code corresponding to the list of objects. Asserts if the list is empty or if there are
    multiple different state codes."""
    state_code = None
    for entity in external_id_entities:
        entity_state_code = entity.get_field("state_code")

        if not state_code:
            state_code = entity_state_code

        if state_code != entity_state_code:
            raise ValueError(
                f"Found two different state codes: {state_code} and {entity_state_code}"
            )

    if not state_code:
        raise ValueError("Expected at least one entity, found none.")

    return state_code


def get_non_flat_property_class_name(
    obj: Union[list, Entity], property_name: str
) -> Optional[str]:
    """Returns the class name of the property with |property_name| on obj, or
    None if the property is a flat field.
    """
    if not isinstance(obj, Entity) and not isinstance(obj, list):
        raise TypeError(f"Unexpected type [{type(obj)}]")

    if is_property_flat_field(obj, property_name):
        return None

    parent_cls = obj.__class__
    attribute = attr.fields_dict(parent_cls).get(property_name)
    if not attribute:
        return None

    property_class_name = get_non_flat_attribute_class_name(attribute)

    if not property_class_name:
        raise ValueError(
            f"Non-flat field [{property_name}] on class [{obj.__class__}] should "
            f"either correspond to list or union."
        )
    return property_class_name


def is_property_list(obj: Union[list, Entity], property_name: str) -> bool:
    """Returns true if the attribute corresponding to |property_name| on the
    given object is a List type."""

    if not isinstance(obj, Entity) and not isinstance(obj, list):
        raise TypeError(f"Unexpected type [{type(obj)}]")

    attribute = attr.fields_dict(obj.__class__).get(property_name)

    if not attribute:
        raise ValueError(
            f"Unexpected None attribute for property_name [{property_name}] on obj [{obj}]"
        )

    return is_list(attribute)


def is_property_forward_ref(obj: Union[list, Entity], property_name: str) -> bool:
    """Returns true if the attribute corresponding to |property_name| on the
    given object is a ForwardRef type."""

    if not isinstance(obj, Entity) and not isinstance(obj, list):
        raise TypeError(f"Unexpected type [{type(obj)}]")

    attribute = attr.fields_dict(obj.__class__).get(property_name)

    if not attribute:
        raise ValueError(
            f"Unexpected None attribute for property_name [{property_name}] on obj [{obj}]"
        )

    return is_forward_ref(attribute)


# TODO(#1886): We should not consider objects which are not ForwardRefs, but are properly typed to an entity cls
#  as a flat field
def is_property_flat_field(obj: Union[list, Entity], property_name: str) -> bool:
    """Returns true if the attribute corresponding to |property_name| on the
    given object is a flat field (not a List, attr class, or ForwardRef)."""

    if not isinstance(obj, Entity) and not isinstance(obj, list):
        raise TypeError(f"Unexpected type [{type(obj)}]")

    attribute = attr.fields_dict(obj.__class__).get(property_name)

    if not attribute:
        raise ValueError(
            f"Unexpected None attribute for property_name [{property_name}] on obj [{obj}]"
        )

    return is_flat_field(attribute)


# TODO(#10723): Delete this in favor of the existing utils in attr_mixins.py
def get_ref_fields_with_reference_class_names(
    entity_cls: Type[Entity], class_names_to_ignore: Optional[List[str]] = None
) -> Dict[str, str]:
    """Returns a dictionary mapping each field on the class that references another
    entity to the entity class name referenced in the attribute."""
    class_names_to_ignore = class_names_to_ignore or []

    fields_to_classes: Dict[str, str] = {}

    for field, attribute in attr.fields_dict(entity_cls).items():
        if is_flat_field(attribute):
            # This attribute is not a reference to another Entity
            continue

        ref_name = get_non_flat_attribute_class_name(attribute)
        if not ref_name:
            raise ValueError(
                "Expected class name of reference in non-flat attribute "
                f"{attribute}. Found none."
            )

        if ref_name in class_names_to_ignore:
            continue

        fields_to_classes[field] = ref_name

    return fields_to_classes
