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
# ============================================================================
"""Base class for all entity types"""
from typing import Optional, Dict, Type, Callable, List

import attr

# TODO(#1885): Enforce all ForwardRef attributes on an Entity are optional
from recidiviz.persistence.entity.core_entity import CoreEntity


@attr.s(eq=False)
class Entity(CoreEntity):
    """Base class for all entity types."""

    # Consider Entity abstract and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is Entity:
            raise Exception("Abstract class cannot be instantiated")
        return super().__new__(cls)

    def __eq__(self, other):
        return entity_graph_eq(self, other)


@attr.s(eq=False)
class ExternalIdEntity(Entity):
    external_id: Optional[str] = attr.ib()

    # Consider Entity abstract and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is ExternalIdEntity:
            raise Exception("Abstract class cannot be instantiated")
        return super().__new__(cls)


def _default_should_ignore_field_cb(_: Type, __: str) -> bool:
    return False


def entity_graph_eq(
    e1: Optional["Entity"],
    e2: Optional["Entity"],
    should_ignore_field_cb: Callable[
        [Type, str], bool
    ] = _default_should_ignore_field_cb,
) -> bool:
    """
    Checks for deep equality between two Entity objects, properly graph
    cycles.

    Args:
        e1: The first Entity to compare
        e2: The second Entity to compare
        should_ignore_field_cb: Callback that returns True if we should ignore
            equality checks between fields. Used for testing purposes.

    Returns:
        True if the two graphs are equal, False otherwise.
    """
    return _entity_graph_eq(e1, e2, should_ignore_field_cb, {id(e1): id(e2)})


def _entity_graph_eq(
    e1: Optional["Entity"],
    e2: Optional["Entity"],
    should_ignore_field_cb: Callable[[Type, str], bool],
    matching_objects_map: Dict[int, int],
) -> bool:
    """
    Recursive helper for checking deep equality of an Entity graph.

    Args:
        e1: The first Entity to compare
        e2: The second Entity to compare
        should_ignore_field_cb: Callback that returns True if we should ignore
            equality checks between fields. Used for testing purposes.
        matching_objects_map: Map of object id for an object in |e1| to the id
            of an equivalent object in |e2|.

    Returns:
        True if the two graphs are equal, False otherwise.
    """

    if (e1 is None) != (e2 is None):
        return False

    if e1 is None:
        return True

    type1 = type(e1)
    type2 = type(e2)
    if type1 != type2:
        return False

    # Add a 'hypothesis' that e1 and e2 are equivalent
    matching_objects_map[id(e1)] = id(e2)

    for field in attr.fields_dict(type1).keys():
        if should_ignore_field_cb(type1, field):
            continue

        value1 = getattr(e1, field)
        value2 = getattr(e2, field)

        if isinstance(value1, list):
            is_match = _entity_graph_list_eq(
                value1, value2, should_ignore_field_cb, matching_objects_map
            )
        elif isinstance(value1, Entity):
            is_match = _entity_graph_entity_eq_check_seen(
                value1, value2, should_ignore_field_cb, matching_objects_map
            )
        else:
            is_match = value1 == value2

        if not is_match:
            # We no longer believe that e1 and e2 are equal, remove this pairing
            # from the matching objects map.
            matching_objects_map.pop(id(e1))
            return False

    return True


def _entity_graph_list_eq(
    entity_list_1: List["Entity"],
    entity_list_2: List["Entity"],
    should_ignore_field_cb: Callable[[Type, str], bool],
    matching_objects_map: Dict[int, int],
):
    """Recursive helper for checking deep equality of two list fields in an
    entity graph. Ignores differences in list order.

    Args:
        entity_list_1: The first list to compare
        entity_list_2: The second list to compare
        should_ignore_field_cb: Callback that returns True if we should ignore
            equality checks between fields. Used for testing purposes.
        matching_objects_map: Map of object id for an objects in |e1| to the id
            of an equivalent object in |e2|.

    Returns:
        True if the two lists are equal, ignoring order, False otherwise.
    """

    if len(entity_list_1) != len(entity_list_2):
        return False

    entity_list_1_copy = entity_list_1.copy()
    entity_list_2_copy = entity_list_2.copy()

    while entity_list_1_copy:
        child1 = entity_list_1_copy.pop(0)

        if id(child1) in matching_objects_map:
            child2_list = [
                child2
                for child2 in entity_list_2_copy
                if id(child2) == matching_objects_map[id(child1)]
            ]
            child2 = child2_list[0] if child2_list else None
        else:
            child2 = _find_match_in_entity_list(
                child1, entity_list_2_copy, should_ignore_field_cb, matching_objects_map
            )

        if not child2:
            return False

        _remove_entity_from_list_with_id(child2, entity_list_2_copy)

    return True


def _remove_entity_from_list_with_id(
    e1: "Entity",
    entity_list: List["Entity"],
):
    matches = [i for i, e2 in enumerate(entity_list) if id(e1) == id(e2)]
    if matches:
        entity_list.pop(matches[0])


def _entity_graph_entity_eq_check_seen(
    e1: "Entity",
    e2: "Entity",
    should_ignore_field_cb: Callable[[Type, str], bool],
    matching_objects_map: Dict[int, int],
) -> bool:
    """Recursive helper for checking deep equality of two entity fields, which
    will avoid a recursive call if this object pairing has already been seen.

    Args:
        e1: The first Entity to compare
        e2: The second Entity to compare
        should_ignore_field_cb: Callback that returns True if we should ignore
            equality checks between fields. Used for testing purposes.
        matching_objects_map: Map of object id for an objects in |e1| to the id
            of an equivalent object in |e2|.

    Returns:
        True if the two entities are equal. False otherwise.
    """

    if id(e1) in matching_objects_map:
        # We have seen this object, we expect the second entity to be the same
        # object as we've already seen in association with this object.
        return id(e2) == matching_objects_map[id(e1)]

    # We haven't seen this object yet, check equality normally.
    return _entity_graph_eq(e1, e2, should_ignore_field_cb, matching_objects_map)


def _find_match_in_entity_list(
    e1: "Entity",
    entity_list: List["Entity"],
    should_ignore_field_cb: Callable[[Type, str], bool],
    matching_objects_map: Dict[int, int],
) -> Optional["Entity"]:
    """Returns the entity in |entity_list| that is equal to |e1|, or None if no
    match found.

    Args:
        e1: The Entity to search for.
        entity_list: The list to search in.
        should_ignore_field_cb: Callback that returns True if we should ignore
            equality checks between fields. Used for testing purposes.
        matching_objects_map: Map of object id for an objects in |e1| to the id
            of an equivalent object in |e2|.

    Returns:
        An Entity in |entity_list| equal to |e1| or None if no match found.
    """

    shallow_id_1 = _eq_shallow_id(e1, should_ignore_field_cb)
    for e2 in entity_list:
        shallow_id_2 = _eq_shallow_id(e2, should_ignore_field_cb)

        if shallow_id_1 != shallow_id_2:
            # If shallow ids don't match, we know these entities cannot possibly
            # be equal.
            continue

        if _entity_graph_eq(e1, e2, should_ignore_field_cb, matching_objects_map):
            return e2

    return None


def _eq_shallow_id(
    entity: "Entity", should_ignore_field_cb: Callable[[Type, str], bool]
) -> str:
    """Generates a string id for an entity that is used to optimize equality
    checks between entities - if the shallow ids don't match, then the entities
    are definitely not equal."""
    id_parts = [f"{type(entity)}"]
    for field in attr.fields_dict(type(entity)).keys():
        if should_ignore_field_cb(type(entity), field):
            continue
        v = getattr(entity, field)
        if isinstance(v, list) or issubclass(type(v), Entity):
            # For entity/list types, we can't use a string representation
            # because that would turn into a recursive graph search of its own.
            # However, the existence / non-existence of the object does still
            # give valuable signal on equality between graphs. We can't use
            # object ids for comparison here (id(entity)) because we're
            # comparing two different graphs that have different object
            # instances.
            id_parts.append(f"{field}:{v is None}")
        else:
            id_parts.append(f"{field}:{v}")

    return ", ".join(id_parts)
