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
from typing import Any, Callable, Optional, Type, Union

import attr


def is_forward_ref(attribute: attr.Attribute) -> bool:
    """Returns true if the attribute is a ForwardRef type."""

    if not isinstance(attribute, attr.Attribute):
        raise TypeError(f"Unexpected type [{type(attribute)}]")

    attr_type = attribute.type
    if not attr_type:
        return False

    if _is_union(attr_type):
        return _is_forward_ref_in_union(attr_type)

    return _is_forward_ref(attr_type)


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

    attr_type = attribute.type
    if not attr_type:
        return False

    if _is_union(attr_type):
        return _is_type_is_union(attr_type, _is_bool)

    return _is_bool(attr_type)


def is_enum(attribute: attr.Attribute) -> bool:
    """Returns true if the attribute is an Enum type."""

    if not isinstance(attribute, attr.Attribute):
        raise TypeError(f"Unexpected type [{type(attribute)}]")

    attr_type = attribute.type
    if not attr_type:
        return False

    if _is_union(attr_type):
        return _extract_mappable_enum_from_union(attr_type) is not None

    return _is_enum_cls(attr_type)


def is_date(attribute: attr.Attribute) -> bool:
    """Returns true if the attribute is a date type."""

    if not isinstance(attribute, attr.Attribute):
        raise TypeError(f"Unexpected type [{type(attribute)}]")

    attr_type = attribute.type
    if not attr_type:
        return False

    if _is_union(attr_type):
        return _is_type_is_union(attr_type, _is_date_cls)

    return _is_date_cls(attr_type)


def is_float(attribute: attr.Attribute) -> bool:
    """Returns true if the attribute is a float type."""

    if not isinstance(attribute, attr.Attribute):
        raise TypeError(f"Unexpected type [{type(attribute)}]")

    attr_type = attribute.type
    if not attr_type:
        return False

    if _is_union(attr_type):
        return _is_type_is_union(attr_type, _is_float)

    return _is_float(attr_type)


def is_list(attribute: attr.Attribute) -> bool:
    """Returns true if the attribute is a List type."""

    if not isinstance(attribute, attr.Attribute):
        raise TypeError(f"Unexpected type [{type(attribute)}]")

    attr_type = attribute.type
    if not attr_type:
        return False

    if _is_union(attr_type):
        return _is_type_is_union(attr_type, _is_list)

    return _is_list(attr_type)


def is_str(attribute: attr.Attribute) -> bool:
    """Returns true if the attribute is a str type."""

    if not isinstance(attribute, attr.Attribute):
        raise TypeError(f"Unexpected type [{type(attribute)}]")

    attr_type = attribute.type
    if not attr_type:
        return False

    if _is_union(attr_type):
        return _is_type_is_union(attr_type, _is_str)

    return _is_str(attr_type)


def is_int(attribute: attr.Attribute) -> bool:
    """Returns true if the attribute is an int type."""

    if not isinstance(attribute, attr.Attribute):
        raise TypeError(f"Unexpected type [{type(attribute)}]")

    attr_type = attribute.type
    if not attr_type:
        return False

    if _is_union(attr_type):
        return _is_type_is_union(attr_type, _is_int)

    return _is_int(attr_type)


def _is_list(attr_type: Type) -> bool:
    return hasattr(attr_type, "__origin__") and attr_type.__origin__ is list


def _is_str(attr_type: Type) -> bool:
    return attr_type == str


def _is_int(attr_type: Type) -> bool:
    return attr_type == int


def _is_float(attr_type: Type) -> bool:
    return attr_type == float


def _is_bool(attr_type: Type) -> bool:
    return attr_type == bool


def get_enum_cls(attribute: attr.Attribute) -> Optional[Type[Enum]]:
    """Return the MappableEnum cls from the provided type attribute,
    or None if the type can't be a MappableEnum.
    """

    if not isinstance(attribute, attr.Attribute):
        raise TypeError(f"Unexpected type [{type(attribute)}]")

    attr_type = attribute.type
    if not attr_type:
        raise ValueError(f"Unexpected None type for attribute [{attribute}]")

    if _is_enum_cls(attr_type):
        return attribute.type

    if _is_union(attr_type):
        return _extract_mappable_enum_from_union(attr_type)

    return None


def _is_union(attr_type: Type) -> bool:
    return hasattr(attr_type, "__origin__") and attr_type.__origin__ is Union


def _is_enum_cls(attr_type: Type) -> bool:
    return inspect.isclass(attr_type) and issubclass(attr_type, Enum)


def _is_date_cls(attr_type: Type) -> bool:
    return inspect.isclass(attr_type) and issubclass(attr_type, datetime.date)


def _is_forward_ref_in_union(union: Type[Union[Any, None]]) -> bool:
    for type_in_union in union.__args__:  # type: ignore
        if _is_forward_ref(type_in_union):
            return True

    return False


def _is_forward_ref(attr_type: Type) -> bool:
    return hasattr(attr_type, "__forward_arg__")


def _extract_mappable_enum_from_union(
    union: Type[Union[Any, None]]
) -> Optional[Type[Enum]]:
    """Extracts a MappableEnum from a Union.

    This method throws an Error if multiple Enums exist and returns None if
    no Enums exist.
    """
    result = set()
    for type_in_union in union.__args__:  # type: ignore
        if _is_enum_cls(type_in_union):
            result.add(type_in_union)

    if not result:
        return None

    if len(result) == 1:
        return next(iter(result))

    raise TypeError(
        f"Can't extract Enum from a union containing multiple Enums: " f"{union}"
    )


def _is_type_is_union(
    union: Type[Union[Any, None]], type_check_func: Callable[[Type], bool]
) -> bool:
    """Looks for the presence of a single type in the Union. Looks for type where the type_check_func returns True.
    Returns whether exactly one is present."""

    # raise ValueError(f'{union}, {type(union)}')
    result = set()
    for type_in_union in union.__args__:  # type: ignore
        if type_check_func(type_in_union):
            result.add(type_in_union)

    if not result:
        return False

    if len(result) == 1:
        return True

    raise TypeError(f"Union contains multiple dates: {union}")


def _get_type_name_from_type(attr_type: Type) -> str:
    if _is_forward_ref(attr_type):
        return attr_type.__forward_arg__
    if hasattr(attr_type, "__name__"):
        return attr_type.__name__
    if hasattr(attr_type, "_name"):
        # This is the way to access the inner type on a List class
        return attr_type._name  # pylint: disable=protected-access
    raise ValueError(f"Cannot parse type name for type: {attr_type}")


def get_non_flat_attribute_class_name(attribute: attr.Attribute) -> Optional[str]:
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

    if not attribute.type:
        raise ValueError(f"Unexpected None type for attribute [{attribute}]")

    attr_type: Type[Any] = attribute.type

    while True:
        if _is_forward_ref(attr_type):
            return attr_type.__forward_arg__
        if _is_list(attr_type):
            attr_type = attr_type.__args__[0]  # type: ignore
            continue

        if _is_union(attr_type):
            inner_types = [
                t
                for t in attr_type.__args__
                if _get_type_name_from_type(t) not in {"NoneType"}
            ]
            if len(inner_types) > 1:
                raise ValueError(f"Multiple nonnull types found: {inner_types}")
            attr_type = inner_types[0]
            continue

        break
    return None
