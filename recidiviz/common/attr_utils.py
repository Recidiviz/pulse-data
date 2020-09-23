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
import datetime

import attr


def get_non_flat_property_class_name(obj, property_name) -> Optional[str]:
    """Returns the class name of the property with |property_name| on obj, or
    None if the property is a flat field.
    """
    if is_property_flat_field(obj, property_name):
        return None

    attribute = attr.fields_dict(obj.__class__).get(property_name)
    if not attribute:
        return None

    attr_type = attribute.type

    if _is_list(attr_type):
        list_elem_type = attr_type.__args__[0]  # type: ignore
        return _get_type_name_from_type(list_elem_type)

    if _is_union(attr_type):
        type_names = [_get_type_name_from_type(t)
                      for t in attr_type.__args__]  # type: ignore

        type_names = [t for t in type_names if t != 'NoneType']
        if len(type_names) > 1:
            raise ValueError(f'Multiple nonnull types found: {type_names}')
        if not type_names:
            raise ValueError('Expected at least one nonnull type')
        return type_names[0]

    if _is_forward_ref(attr_type):
        return _get_type_name_from_type(attr_type)

    raise ValueError(
        f'Non-flat field [{property_name}] on class [{obj.__class__}] should '
        f'either correspond to list or union.')


def is_property_list(obj, property_name) -> bool:
    """Returns true if the attribute corresponding to |property_name| on the
     given object is a List type."""
    attribute = attr.fields_dict(obj.__class__).get(property_name)

    return is_list(attribute)


def is_property_forward_ref(obj, property_name) -> bool:
    """Returns true if the attribute corresponding to |property_name| on the
     given object is a ForwardRef type."""

    attribute = attr.fields_dict(obj.__class__).get(property_name)

    return is_forward_ref(attribute)


# TODO(#1886): We should not consider objects which are not ForwardRefs, but are properly typed to an entity cls
#  as a flat field
def is_property_flat_field(obj, property_name) -> bool:
    """Returns true if the attribute corresponding to |property_name| on the
     given object is a flat field (not a List, attr class, or ForwardRef)."""
    attribute = attr.fields_dict(obj.__class__).get(property_name)

    return not is_list(attribute) and not is_forward_ref(attribute)


def is_forward_ref(attribute) -> bool:
    """Returns true if the attribute is a ForwardRef type."""
    if _is_union(attribute.type):
        return _is_forward_ref_in_union(attribute.type)

    return _is_forward_ref(attribute.type)


def is_attr_decorated(obj) -> bool:
    """Returns True if the object type is attr decorated"""
    return attr.has(obj)


def is_bool(attribute) -> bool:
    """Returns true if the attribute is a bool type."""
    if _is_union(attribute.type):
        return _is_type_is_union(attribute.type, _is_bool)

    return _is_bool(attribute.type)


def is_enum(attribute) -> bool:
    """Returns true if the attribute is an Enum type."""
    if _is_union(attribute.type):
        return _extract_mappable_enum_from_union(attribute.type) is not None

    return _is_enum_cls(attribute.type)


def is_date(attribute) -> bool:
    """Returns true if the attribute is a date type."""

    if _is_union(attribute.type):
        return _is_type_is_union(attribute.type, _is_date_cls)

    return _is_date_cls(attribute.type)


def is_float(attribute) -> bool:
    """Returns true if the attribute is a float type."""
    if _is_union(attribute.type):
        return _is_type_is_union(attribute.type, _is_float)

    return _is_float(attribute.type)


def is_list(attribute) -> bool:
    """Returns true if the attribute is a List type."""
    if _is_union(attribute.type):
        return _is_type_is_union(attribute.type, _is_list)

    return _is_list(attribute.type)


def is_str(attribute) -> bool:
    """Returns true if the attribute is a str type."""
    if _is_union(attribute.type):
        return _is_type_is_union(attribute.type, _is_str)

    return _is_str(attribute.type)


def is_int(attribute) -> bool:
    """Returns true if the attribute is an int type."""
    if _is_union(attribute.type):
        return _is_type_is_union(attribute.type, _is_int)

    return _is_int(attribute.type)


def _is_list(attr_type) -> bool:
    return hasattr(attr_type, '__origin__') \
        and attr_type.__origin__ is list


def _is_str(attr_type) -> bool:
    return attr_type == str


def _is_int(attr_type) -> bool:
    return attr_type == int


def _is_float(attr_type) -> bool:
    return attr_type == float


def _is_bool(attr_type) -> bool:
    return attr_type == bool


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


def _is_date_cls(attr_type) -> bool:
    return inspect.isclass(attr_type) and issubclass(attr_type, datetime.date)


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


def _is_type_is_union(union: Union, type_check_func) -> bool:
    """Looks for the presence of a single type in the Union. Looks for type where the type_check_func returns True.
    Returns whether exactly one is present."""
    result = set()
    for type_in_union in union.__args__:  # type: ignore
        if type_check_func(type_in_union):
            result.add(type_in_union)

    if not result:
        return False

    if len(result) == 1:
        return True

    raise TypeError(f"Union contains multiple dates: {union}")


def _get_type_name_from_type(attr_type) -> str:
    if _is_forward_ref(attr_type):
        return attr_type.__forward_arg__
    return attr_type.__name__
