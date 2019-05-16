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
"""Contains helpers to interact with the database."""
import inspect
from enum import Enum
from typing import Optional, Union, Type

import attr

from recidiviz.common.constants.entity_enum import EntityEnum
from recidiviz.persistence.database.schema_entity_converter.\
    base_schema_entity_converter import DatabaseConversionError
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.county import entities as county_entities
from recidiviz.persistence.database.schema.county import schema as county_schema
from recidiviz.persistence.database.base_schema import Base
from recidiviz.persistence.database.schema.state import schema as state_schema
from recidiviz.persistence.entity.state import entities as state_entities


class _Direction(Enum):
    SCHEMA_TO_ENTITY = 1
    ENTITY_TO_SCHEMA = 2

    @staticmethod
    def for_cls(src_cls):
        if issubclass(src_cls, Entity):
            return _Direction.ENTITY_TO_SCHEMA

        if issubclass(src_cls, Base):
            return _Direction.SCHEMA_TO_ENTITY

        raise DatabaseConversionError(
            "Unable to convert class {0}".format(src_cls))


# TODO(1625): Convert all usages of this function to the new classes in
#   recidiviz.persistence.schema_entity_converter
def convert_all(src):
    """Converts the given list of objects into their entity/schema counterparts

    Args:
        src: list of schema objects or entity objects
    Returns:
        The converted list, a schema or entity list.
    """
    return [convert(s) for s in src]


# TODO(1625): Convert all usages of this function to the new classes in
#   recidiviz.persistence.schema_entity_converter
def convert(src):
    """Converts the given src object to its entity/schema counterpart."""
    if not src:
        return None

    direction = _Direction.for_cls(src.__class__)

    schema_cls = _get_schema_class(src)
    entity_cls = _get_entity_class(src)

    if entity_cls is None or schema_cls is None:
        raise DatabaseConversionError('Both |entity_cls| and |schema_cls| '
                                      'should be not None')

    if direction is _Direction.ENTITY_TO_SCHEMA:
        dst = schema_cls()
    else:
        dst = entity_cls.builder()

    for field, attribute in attr.fields_dict(entity_cls).items():
        if field == 'related_sentences':
            # TODO(1145): Correctly convert related_sentences once schema for
            # this field is finalized.
            continue
        v = getattr(src, field)
        if isinstance(v, list):
            value = [convert(i) for i in v]
        elif issubclass(type(v), Entity) or issubclass(type(v), Base):
            value = convert(v)
        elif v is None:
            value = None
        elif _is_enum(attribute.type):
            value = _convert_enum(src, field, attribute.type, direction)
        else:
            value = v

        setattr(dst, field, value)

    if direction is _Direction.SCHEMA_TO_ENTITY:
        dst = dst.build()
    return dst


_COUNTY_MODULE_NAMES = [county_schema.__name__, county_entities.__name__]
_STATE_MODULE_NAMES = [state_schema.__name__, state_entities.__name__]


def _get_schema_class(src):
    if src.__module__ in _COUNTY_MODULE_NAMES:
        return getattr(county_schema, src.__class__.__name__)
    if src.__module__ in _STATE_MODULE_NAMES:
        return getattr(state_schema, src.__class__.__name__)
    raise DatabaseConversionError(f'Attempting to convert class with unexpected'
                                  f' module: [{src.__module__}]')


def _get_entity_class(src):
    if src.__module__ in _COUNTY_MODULE_NAMES:
        return getattr(county_entities, src.__class__.__name__)
    if src.__module__ in _STATE_MODULE_NAMES:
        return getattr(state_entities, src.__class__.__name__)

    raise DatabaseConversionError(f'Attempting to convert class with unexpected'
                                  f' module: [{src.__module__}]')


def _is_enum(attr_type):
    return _get_enum_cls(attr_type) is not None


def _convert_enum(src, field, attr_type, direction):
    if direction is _Direction.SCHEMA_TO_ENTITY:
        enum_cls = _get_enum_cls(attr_type)
        return enum_cls(getattr(src, field))

    return getattr(src, field).value


def _get_enum_cls(attr_type) -> Optional[Type[EntityEnum]]:
    """Return the MappableEnum cls from the provided type attribute,
    or None if the type can't be a MappableEnum"""
    if inspect.isclass(attr_type) and issubclass(attr_type, EntityEnum):
        return attr_type

    if _is_union(attr_type):
        return _extract_mappable_enum_from_union(attr_type)

    return None


def _is_union(attr_type) -> bool:
    return hasattr(attr_type, '__origin__') and attr_type.__origin__ is Union


def _extract_mappable_enum_from_union(union: Union) \
        -> Optional[Type[EntityEnum]]:
    """Extracts a MappableEnum from a Union.

    This method throws an Error if multiple Enums exist and returns None if no
    Enums exist.
    """
    result = set()
    for type_in_union in union.__args__:  # type: ignore
        if issubclass(type_in_union, EntityEnum):
            result.add(type_in_union)

    if not result:
        return None

    if len(result) == 1:
        return next(iter(result))

    raise TypeError(
        "Can't extract Enum from a union containing multiple Enums: {}".format(
            union))
