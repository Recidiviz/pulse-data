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


import inspect
from typing import Optional, Type, Union

from enum import Enum


def is_forward_ref(attribute) -> bool:
    """Returns true if the attribute is a ForwardRef type."""
    if _is_union(attribute.type):
        return _is_forward_ref_in_union(attribute.type)

    return _is_forward_ref(attribute.type)


def is_enum(attribute) -> bool:
    """Returns true if the attribute is an Enum type."""
    if _is_union(attribute.type):
        return _extract_mappable_enum_from_union(attribute.type) is not None

    return _is_enum_cls(attribute.type)


def is_list(attribute) -> bool:
    """Returns true if the attribute is a List type."""
    return hasattr(attribute.type, '__origin__') \
        and attribute.type.__origin__ is list


def get_enum_cls(attribute) -> Optional[Type[Enum]]:
    """Return the MappableEnum cls from the provided type attribute,
    or None if the type can't be a MappableEnum.
    """
    if _is_enum_cls(attribute.type):
        return attribute.type

    if _is_union(attribute.type):
        return _extract_mappable_enum_from_union(attribute.type)

    return None


def _is_union(attr_type) -> bool:
    return hasattr(attr_type, '__origin__') \
           and attr_type.__origin__ is Union


def _is_enum_cls(attr_type) -> bool:
    return inspect.isclass(attr_type) and issubclass(attr_type, Enum)


def _is_forward_ref_in_union(union: Union) -> bool:
    for type_in_union in union.__args__:  # type: ignore
        if _is_forward_ref(type_in_union):
            return True

    return False


def _is_forward_ref(attr_type):
    return hasattr(attr_type, '__forward_arg__')


def _extract_mappable_enum_from_union(union: Union) \
        -> Optional[Type[Enum]]:
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
        f"Can't extract Enum from a union containing multiple Enums: "
        f"{union}")
