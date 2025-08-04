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
import abc
from typing import Any, Callable, Dict, Generic, List, Optional, Type, TypeVar

import attr
from more_itertools import one

# TODO(#1885): Enforce all ForwardRef attributes on an Entity are optional
from recidiviz.common import attr_validators
from recidiviz.common.attr_mixins import attribute_field_type_reference_for_class
from recidiviz.common.constants.states import StateCode
from recidiviz.common.date import DateOrDateTime
from recidiviz.persistence.entity.core_entity import CoreEntity
from recidiviz.utils import environment


def _list_of_strings_to_tuple_converter(list_of_strings: list[str]) -> tuple[str, ...]:
    return tuple(list_of_strings)


def _set_of_state_code_to_tuple_converter(
    set_of_state_code: set[StateCode],
) -> tuple[StateCode, ...]:
    return tuple(set_of_state_code)


def _dict_to_tuple_of_tuples_converter(
    d: dict[str, Callable]
) -> tuple[tuple[str, Callable], ...]:
    return tuple((k, v) for k, v in d.items())


@attr.define(frozen=True, kw_only=True)
class UniqueConstraint:
    """Defines a uniqueness constraint that will be validated across all entities at the
    end of each ingest pipeline run.
    """

    name: str = attr.ib(validator=attr_validators.is_str)
    fields: tuple[str, ...] = attr.ib(
        validator=attr_validators.is_tuple,
        # Convert to an immutable tuple but expect a list as input (nicer, less
        # error-prone syntax).
        converter=_list_of_strings_to_tuple_converter,
    )

    # For fields that can be null, if this is True, we will ignore rows that overlap
    # because there is a null value in one of those fields.
    ignore_nulls: bool = attr.ib(default=False, validator=attr_validators.is_bool)

    # Mapping of field -> transform function that will be applied to that field before
    # we assess uniqueness. Allows us to, for example, lowercase a value so we are
    # checking for uniqueness in a case-insensitive way. This is stored in immutable
    # tuple form to keep this class hashable.
    transforms: tuple[tuple[str, Callable], ...] = attr.ib(
        validator=attr_validators.is_tuple,
        # Convert to an immutable tuple but expect a dict as input (nicer, less
        # error-prone syntax).
        converter=_dict_to_tuple_of_tuples_converter,
        factory=dict,  # type: ignore[arg-type]
    )

    # States that are known to violate this uniqueness constraint and for whom
    # constraint errors will not fail. THIS SHOULD ONLY BE USED IN CASES WHERE WE WANT
    # TO INTRODUCE NEW CONSTRAINTS MOVING FORWARD THAT ARE NOT YET TRUE FOR ALL LEGACY
    # STATES.
    exempt_states: tuple[StateCode, ...] = attr.ib(
        validator=attr_validators.is_tuple,
        # Convert to an immutable tuple but expect a set as input (nicer, less
        # error-prone syntax).
        converter=_set_of_state_code_to_tuple_converter,
        factory=set,  # type: ignore[arg-type]
    )

    @property
    def transforms_dict(self) -> dict[str, Callable]:
        return dict(self.transforms)


@attr.s(eq=False)
class Entity(CoreEntity):
    """Base class for all entity types."""

    # Consider Entity abstract and only allow instantiating subclasses
    def __new__(cls: Any, *_: Any, **__: Any) -> Any:
        if cls is Entity:
            raise NotImplementedError("Abstract class cannot be instantiated")
        return super().__new__(cls)

    def __eq__(self, other: Any) -> bool:
        return entity_graph_eq(self, other)

    @classmethod
    def global_unique_constraints(cls) -> List[UniqueConstraint]:
        """Returns unique constraints applied across all entities."""
        return []

    @classmethod
    def entity_tree_unique_constraints(cls) -> List[UniqueConstraint]:
        """Unique constraints applied only on a per root entity level
        (comparing for uniqueness only among a root entity and it's child entities)."""
        return []

    def assert_datetime_less_than_or_equal(
        self,
        before: DateOrDateTime | None,
        after: DateOrDateTime | None,
        before_description: str,
        after_description: str,
    ) -> None:
        """Raises a ValueError if the given "before" date/datetime is after the "after"
        one. Both field names must be datetime.datetime or datetime.date fields.

        Does not raise any error if the two values are equal or either value is null.
        """
        if before is None or after is None:
            return

        if before > after:
            raise ValueError(
                f"Found {self.limited_pii_repr()} with {before_description} datetime "
                f"{before} after {after_description} datetime {after}."
            )

    def assert_datetime_less_than(
        self,
        before: DateOrDateTime,
        after: DateOrDateTime | None,
        before_description: str,
        after_description: str,
    ) -> None:
        """Raises a ValueError if the given "before" date/datetime is equal to or after
        the "after" one. Both field names must be datetime.datetime or datetime.date
        fields.

        Does not raise any error if the "after" value is null.
        """
        if after is None:
            return

        if before == after:
            raise ValueError(
                f"Found {self.limited_pii_repr()} with {before_description} datetime "
                f"{before} equal to {after_description} datetime {after}. The "
                f"{before_description} date must come strictly before the "
                f"{after_description} date."
            )

        if before > after:
            raise ValueError(
                f"Found {self.limited_pii_repr()} with {before_description} datetime "
                f"{before} after {after_description} datetime {after}."
            )


EntityT = TypeVar("EntityT", bound=Entity)


@attr.s(eq=False, kw_only=True)
class ExternalIdEntity(Entity):
    """An entity that encodes ONLY external id information. This id entity will always
    be connected to a HasMultipleExternalIdsEntity.
    """

    external_id: str = attr.ib(validator=attr_validators.is_non_empty_str)
    id_type: str = attr.ib(validator=attr_validators.is_non_empty_str)

    # Consider ExternalIdEntity abstract and only allow instantiating subclasses
    def __new__(cls: Any, *_: Any, **__: Any) -> Any:
        if cls is ExternalIdEntity:
            raise NotImplementedError("Abstract class cannot be instantiated")
        return super().__new__(cls)


ExternalIdEntityT = TypeVar("ExternalIdEntityT", bound=ExternalIdEntity)


@attr.s(eq=False, kw_only=True)
class HasExternalIdEntity(Entity):
    """An entity that has a flat external_id field but also stores other meaningful
    information besides the id.
    """

    external_id: str = attr.ib(validator=attr_validators.is_non_empty_str)

    # Consider HasExternalIdEntity abstract and only allow instantiating subclasses
    def __new__(cls: Any, *_: Any, **__: Any) -> Any:
        if cls is HasExternalIdEntity:
            raise NotImplementedError("Abstract class cannot be instantiated")
        return super().__new__(cls)


HasExternalIdEntityT = TypeVar("HasExternalIdEntityT", bound=HasExternalIdEntity)


@attr.s(eq=False, kw_only=True)
class HasMultipleExternalIdsEntity(Generic[ExternalIdEntityT], Entity):
    """An entity that has multiple associated external ids."""

    @abc.abstractmethod
    def get_external_ids(self) -> List[ExternalIdEntityT]:
        """Returns the list of external id objects for this class."""

    @environment.test_only
    def has_external_id(self, external_id: str) -> bool:
        return external_id in {e.external_id for e in self.get_external_ids()}

    # Consider HasMultipleExternalIdsEntity abstract and only allow instantiating subclasses
    def __new__(cls: Any, *_: Any, **__: Any) -> Any:
        if cls is HasMultipleExternalIdsEntity:
            raise NotImplementedError("Abstract class cannot be instantiated")
        return super().__new__(cls)


class EnumEntity(Entity):
    """An entity that is a simple wrapper around an enum value and its associated raw
    text.
    """

    # Suffix to append to an enum field name to get the corresponding raw text field
    # name.
    RAW_TEXT_FIELD_SUFFIX = "_raw_text"

    # Consider EnumEntity abstract and only allow instantiating subclasses
    def __new__(cls: Any, *_: Any, **__: Any) -> Any:
        if cls is EnumEntity:
            raise NotImplementedError("Abstract class cannot be instantiated")
        return super().__new__(cls)

    @classmethod
    def get_enum_field_name(cls) -> str:
        """Returns the singular enum field associated with this EnumEntity."""
        class_reference = attribute_field_type_reference_for_class(cls)
        enum_fields = {
            field
            for field in class_reference.fields
            if class_reference.get_field_info(field).enum_cls
        }
        if len(enum_fields) != 1:
            raise ValueError(
                f"Expected exactly one enum field on EnumEntity "
                f"[{cls.__name__}]. Found: {enum_fields}."
            )
        return one(enum_fields)

    @classmethod
    def get_raw_text_field_name(cls) -> str:
        """Returns the singular enum raw text associated with this EnumEntity."""
        raw_text_field = f"{cls.get_enum_field_name()}{cls.RAW_TEXT_FIELD_SUFFIX}"
        class_fields = attribute_field_type_reference_for_class(cls).fields
        if raw_text_field not in class_fields:
            raise ValueError(
                f"Expected to find field [{raw_text_field}] on entity [{cls.__name__}]"
            )
        return raw_text_field


EnumEntityT = TypeVar("EnumEntityT", bound=EnumEntity)


class RootEntity:
    """Mixin interface for an entity that can be ingested as the root of an entity tree
    (e.g. StatePerson or StateStaff).
    """

    @classmethod
    @abc.abstractmethod
    def back_edge_field_name(cls) -> str:
        """The name of the field on all entities that are connected to this root
        entity, which connects that non-root entity back to the root entity (e.g.
        'person' or 'staff').
        """


# TODO(#1894): Write unit tests for entity graph equality that reference the
# schema defined in test_schema/test_entities.py.


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
) -> bool:
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
) -> None:
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
