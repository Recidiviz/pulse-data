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
from typing import Any, ForwardRef, Optional, Type, get_args, get_origin

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
    """Returns True if the attribute is a flat field (not a list or a reference)."""
    return not is_list(attribute) and not is_forward_ref(attribute)


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


def get_non_flat_attribute_class_name(attribute: attr.Attribute) -> Optional[str]:
    """Returns the the inner class name for an attribute's type that is either
    List[<type>] or Optional[<type>], or None if the attribute type does not match
    either format.

    If something is a nested List or Optional type, returns the innermost type if it
    is an object reference type.

    Examples:
        List[str] -> None
        List[List[str]] -> None
        List["StatePerson"] -> "StatePerson"
        Optional[List["StatePerson"]] -> "StatePerson"
        Optional["StatePerson"] -> "StatePerson"
    """

    if not attribute.type:
        raise ValueError(f"Unexpected None type for attribute [{attribute}]")

    return get_referenced_class_name_from_type(attribute.type)


def get_referenced_class_name_from_type(attr_type: Type) -> Optional[str]:
    """Returns the the inner class name for a type that is either List[<type>] or
    Optional[<type>], or None if the attribute type does not match either format.

    If something is a nested List or Optional type, returns the innermost type if it
    is an object reference type.

    Examples:
        List[str] -> None
        List[List[str]] -> None
        List["StatePerson"] -> "StatePerson"
        Optional[List["StatePerson"]] -> "StatePerson"
        Optional["StatePerson"] -> "StatePerson"
    """
    while True:
        if isinstance(attr_type, ForwardRef):
            return attr_type.__forward_arg__
        if isinstance(attr_type, str):
            # For some forward references the inner type will just be the string class
            # name.
            return attr_type
        if is_list_type(attr_type):
            attr_type = get_inner_type_from_list_type(attr_type)
            continue

        if is_optional_type(attr_type):
            attr_type = get_inner_type_from_optional_type(attr_type)
            continue

        break
    return None
