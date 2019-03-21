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
from recidiviz.persistence import entities
from recidiviz.persistence.database import schema
from recidiviz.persistence.database.schema import Base
from recidiviz.persistence.entities import Entity


class DatabaseConversionError(Exception):
    """Raised if an error is encountered when converting between entity
    objects and schema objects (or vice versa)."""


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


def convert_all(src):
    """Converts the given list of objects into their entity/schema counterparts

    Args:
        src: list of schema objects or entity objects
    Returns:
        The converted list, a schema or entity list.
    """
    return [convert(s) for s in src]


def convert(src):
    """Converts the given src object to its entity/schema counterpart."""
    if not src:
        return None

    direction = _Direction.for_cls(src.__class__)

    schema_cls = getattr(schema, src.__class__.__name__)
    entity_cls = getattr(entities, src.__class__.__name__)

    if direction is _Direction.ENTITY_TO_SCHEMA:
        dst = schema_cls()
    else:
        dst = entity_cls.builder()

    for field, attribute in attr.fields_dict(entity_cls).items():
        if field == 'bookings':
            dst.bookings = [convert(b) for b in src.bookings]
        elif field == 'holds':
            dst.holds = [convert(h) for h in src.holds]
        elif field == 'arrest':
            dst.arrest = convert(src.arrest)
        elif field == 'charges':
            dst.charges = [convert(c) for c in src.charges]
        elif field == 'bond':
            dst.bond = convert(src.bond)
        elif field == 'sentence':
            dst.sentence = convert(src.sentence)
        elif field == 'related_sentences':
            # TODO(441): Correctly convert related_sentences once schema for
            # this field is finalized.
            continue
        else:
            if getattr(src, field) is None:
                value = None
            elif _is_enum(attribute.type):
                value = _convert_enum(src, field, attribute.type, direction)
            else:
                value = getattr(src, field)
            setattr(dst, field, value)

    if direction is _Direction.SCHEMA_TO_ENTITY:
        dst = dst.build()
    return dst


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
