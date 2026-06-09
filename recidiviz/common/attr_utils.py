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
"""Utils for working with Attr objects."""


import datetime
import inspect
from enum import Enum
from typing import Any, ForwardRef, Optional, Type, Union, get_args, get_origin

import attr
from more_itertools import one

from recidiviz.utils.types import non_optional


def convert_empty_string_to_none(value: str | None) -> str | None:
    if value is None or (not value):
        return None
    return None if not value.strip() else value


def is_forward_ref(attribute: attr.Attribute) -> bool:
    """Returns true if the attribute is a ForwardRef type."""

    if not isinstance(attribute, attr.Attribute):
        raise TypeError(f"Unexpected type [{type(attribute)}]")

    non_optional_type = get_non_optional_type(non_optional(attribute.type))
    return isinstance(non_optional_type, (ForwardRef, str))


def is_flat_field(attribute: attr.Attribute) -> bool:
    """Returns True if the attribute is a flat field (not a list, tuple, or
    reference)."""
    return (
        not is_list(attribute)
        and not is_tuple(attribute)
        and not is_forward_ref(attribute)
    )


def is_attr_decorated(obj_cls: Type[Any]) -> bool:
    """Returns True if the object type is attr decorated"""
    return attr.has(obj_cls)


def is_bool(attribute: attr.Attribute) -> bool:
    """Returns true if the attribute is a bool type."""

    if not isinstance(attribute, attr.Attribute):
        raise TypeError(f"Unexpected type [{type(attribute)}]")

    return get_non_optional_type(non_optional(attribute.type)) is bool


def is_enum(attribute: attr.Attribute) -> bool:
    """Returns true if the attribute is an Enum type."""

    if not isinstance(attribute, attr.Attribute):
        raise TypeError(f"Unexpected type [{type(attribute)}]")

    non_optional_type = get_non_optional_type(non_optional(attribute.type))
    return inspect.isclass(non_optional_type) and issubclass(non_optional_type, Enum)


def is_non_optional_enum(attribute: attr.Attribute) -> bool:
    """Returns true if the attribute is a non-optional Enum type (e.g. MyEnum but not
    Optional[MyEnum]).
    """
    if not isinstance(attribute, attr.Attribute):
        raise TypeError(f"Unexpected type [{type(attribute)}]")

    attr_type = non_optional(attribute.type)
    if is_optional_type(attr_type):
        return False
    return inspect.isclass(attr_type) and issubclass(
        get_non_optional_type(non_optional(attribute.type)), Enum
    )


def is_datetime(attribute: attr.Attribute) -> bool:
    """Returns true if the attribute is a datetime type."""

    if not isinstance(attribute, attr.Attribute):
        raise TypeError(f"Unexpected type [{type(attribute)}]")

    non_optional_type = get_non_optional_type(non_optional(attribute.type))
    return inspect.isclass(non_optional_type) and issubclass(
        non_optional_type, datetime.datetime
    )


def is_date(attribute: attr.Attribute) -> bool:
    """Returns true if the attribute is a date type. NOTE: this will return True for
    datetime types as well."""

    if not isinstance(attribute, attr.Attribute):
        raise TypeError(f"Unexpected type [{type(attribute)}]")

    non_optional_type = get_non_optional_type(non_optional(attribute.type))
    return inspect.isclass(non_optional_type) and issubclass(
        non_optional_type, datetime.date
    )


def is_float(attribute: attr.Attribute) -> bool:
    """Returns true if the attribute is a float type."""

    if not isinstance(attribute, attr.Attribute):
        raise TypeError(f"Unexpected type [{type(attribute)}]")

    return get_non_optional_type(non_optional(attribute.type)) is float


def is_list(attribute: attr.Attribute) -> bool:
    """Returns true if the attribute is a List type."""

    if not isinstance(attribute, attr.Attribute):
        raise TypeError(f"Unexpected type [{type(attribute)}]")

    non_optional_type = get_non_optional_type(non_optional(attribute.type))
    return is_list_type(non_optional_type)


def is_list_type(attr_type: Type) -> bool:
    return get_origin(attr_type) is list


def is_tuple(attribute: attr.Attribute) -> bool:
    """Returns true if the attribute is a tuple type."""

    if not isinstance(attribute, attr.Attribute):
        raise TypeError(f"Unexpected type [{type(attribute)}]")

    non_optional_type = get_non_optional_type(non_optional(attribute.type))
    return is_tuple_type(non_optional_type)


def is_tuple_type(attr_type: Type) -> bool:
    return get_origin(attr_type) is tuple


def is_str(attribute: attr.Attribute) -> bool:
    """Returns true if the attribute is a str type."""

    if not isinstance(attribute, attr.Attribute):
        raise TypeError(f"Unexpected type [{type(attribute)}]")

    return get_non_optional_type(non_optional(attribute.type)) is str


def is_int(attribute: attr.Attribute) -> bool:
    """Returns true if the attribute is an int type."""

    if not isinstance(attribute, attr.Attribute):
        raise TypeError(f"Unexpected type [{type(attribute)}]")
    return get_non_optional_type(non_optional(attribute.type)) is int


def get_enum_cls(attribute: attr.Attribute) -> Optional[Type[Enum]]:
    """Return the Enum cls from the provided type attribute,
    or None if the type isn't a Enum.
    """

    if not isinstance(attribute, attr.Attribute):
        raise TypeError(f"Unexpected type [{type(attribute)}]")

    non_optional_type = get_non_optional_type(non_optional(attribute.type))
    if inspect.isclass(non_optional_type) and issubclass(non_optional_type, Enum):
        return non_optional_type
    return None


def is_optional_type(t: Type | None) -> bool:
    type_args = get_args(t)
    if len(type_args) == 1:
        return False
    return type(None) in get_args(t)


def get_inner_type_from_optional_type(optional_type: Type) -> Type:
    if not is_optional_type(optional_type):
        raise ValueError(f"Expected list type, found [{optional_type}]")

    return one(
        t
        for t in get_args(optional_type)
        if t is not type(None)  # pylint: disable=unidiomatic-typecheck
    )


def get_non_optional_type(maybe_optional_type: Type) -> Type:
    if is_optional_type(maybe_optional_type):
        return get_inner_type_from_optional_type(maybe_optional_type)
    return maybe_optional_type


def get_inner_type_from_list_type(list_type: Type) -> Type:
    if not is_list_type(list_type):
        raise ValueError(f"Expected list type, found [{list_type}]")

    return one(t for t in get_args(list_type))


def get_inner_type_from_tuple_type(tuple_type: Type) -> Type:
    """Returns the element type from a `tuple[X, ...]` type, that is, a
    tuple of any number of elements that are all the same type.

    Only `tuple[X, ...]` is supported. Fixed-length tuples like `tuple[X]`
    or mixed-element tuples like `tuple[X, Y]` raise `ValueError`, since
    there is no single "inner type" to return for those shapes.
    """
    if not is_tuple_type(tuple_type):
        raise ValueError(f"Expected tuple type, found [{tuple_type}]")

    # tuple[Foo, ...] carries two type args: the element type and Ellipsis.
    args = get_args(tuple_type)
    if len(args) != 2 or args[1] is not Ellipsis:
        raise ValueError(
            f"Only tuples of the form `tuple[X, ...]` (any number of "
            f"elements of one type) are supported; found [{tuple_type}] "
            f"with args {args}."
        )
    return args[0]


def get_non_flat_attribute_class_name(attribute: attr.Attribute) -> Optional[str]:
    """Returns the the inner class name for an attribute's type that is one of
    `List[<type>]`, `tuple[<type>, ...]`, or `Optional[<type>]`, or None if the
    attribute type does not match any of those formats.

    If something is a nested List/tuple/Optional type, returns the innermost type
    if it is an object reference type.

    Examples:
        List[str] -> None
        List[List[str]] -> None
        List["StatePerson"] -> "StatePerson"
        tuple["IdentityClusterRace", ...] -> "IdentityClusterRace"
        Optional[List["StatePerson"]] -> "StatePerson"
        Optional["StatePerson"] -> "StatePerson"
    """

    if not attribute.type:
        raise ValueError(f"Unexpected None type for attribute [{attribute}]")

    return get_referenced_class_name_from_type(attribute.type)


def _extract_class_name_from_forward_ref(type_str: str) -> str:
    """Extracts a class name from a forward reference string that may use
    modern union syntax (e.g. "ClassName | None" -> "ClassName").
    """
    parts = [p.strip() for p in type_str.split("|")]
    non_none_parts = [p for p in parts if p != "None"]
    if len(non_none_parts) == 1:
        return non_none_parts[0]
    raise ValueError(f"Unexpected forward reference type: '{type_str}'")


def get_referenced_class_name_from_type(
    attr_type: Union[Type, ForwardRef, str],
) -> Optional[str]:
    """Returns the referenced class name from a type annotation, or None if the
    type does not reference an entity class.

    Handles ForwardRef, str, List, tuple, and Optional types, unwrapping nested
    containers to find the innermost class reference.

    Examples:
        List[str] -> None
        List[List[str]] -> None
        List["StatePerson"] -> "StatePerson"
        tuple["IdentityClusterRace", ...] -> "IdentityClusterRace"
        Optional[List["StatePerson"]] -> "StatePerson"
        Optional["StatePerson"] -> "StatePerson"
        ForwardRef("StatePerson | None") -> "StatePerson"
        "StatePerson | None" -> "StatePerson"
        "StatePerson" -> "StatePerson"
    """
    while True:
        if isinstance(attr_type, ForwardRef):
            return _extract_class_name_from_forward_ref(attr_type.__forward_arg__)
        if isinstance(attr_type, str):
            # For some forward references the inner type will just be the string
            # class name.
            return _extract_class_name_from_forward_ref(attr_type)
        if is_list_type(attr_type):
            attr_type = get_inner_type_from_list_type(attr_type)
            continue

        if is_tuple_type(attr_type):
            attr_type = get_inner_type_from_tuple_type(attr_type)
            continue

        if is_optional_type(attr_type):
            attr_type = get_inner_type_from_optional_type(attr_type)
            continue

        break
    return None
