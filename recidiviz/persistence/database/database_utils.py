# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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
from enum import Enum

import attr

from recidiviz import Base
from recidiviz.common.constants.mappable_enum import MappableEnum
from recidiviz.persistence import entities
from recidiviz.persistence.database import schema


class DatabaseConversionError(Exception):
    """Raised if an error is encountered when converting between entity
    objects and schema objects (or vice versa)."""


class _Direction(Enum):
    SCHEMA_TO_ENTITY = 1
    ENTITY_TO_SCHEMA = 2

    @staticmethod
    def for_cls(src_cls):
        # TODO(363): change this to check BuildableAttr class once all
        # entities extend BuildableAttr.
        if not issubclass(src_cls, Base):
            return _Direction.ENTITY_TO_SCHEMA

        if issubclass(src_cls, Base):
            return _Direction.SCHEMA_TO_ENTITY

        raise DatabaseConversionError(
            "Unable to convert class {0}".format(src_cls))


def convert_people(people_src):
    """Converts the given list of people to the correct objects

    Args:
        people_src: list of schema.Person or entities.Person
    Returns:
        The converted list, a schema.Person or entities.Person
    """
    return [convert_person(p) for p in people_src]


def convert_person(src):
    """Converts the given person to the correct object.

    Args:
        src: A schema Person or entity Person object
    Returns:
        The converted object, a schema or entity object.
    """
    return _convert(src)


def convert_bookings(bookings_src):
    """Converts the given list of bookings to the correct objects

    Args:
        bookings_src: list of schema.Booking or entities.Booking
    Returns:
        The converted list, a schema.Booking or entities.Booking
    """
    return [convert_booking(b) for b in bookings_src]


def convert_booking(src):
    """Converts the given booking to the correct object.

    Args:
        src: A schema Booking or entity Booking object
    Returns:
        The converted object, a schema or entity object
    """
    return _convert(src)


def convert_charge(src):
    """Converts the given charge to the correct object.

    Args:
        src: A schema Charge or entity Charge object
    Returns:
        The converted object, a schema or entity object
    """
    return _convert(src)


def convert_sentence(src):
    """Converts the given sentence to the correct object.

    Args:
        src: A schema Sentence or entity Sentence object
    Returns:
        The converted object, a schema or entity object
    """
    return _convert(src)


# TODO(363): Remove special casing for Person once all entities extend
# BuildableAttr
def _convert(src):
    """Converts the given src object to its entity/schema counterpart."""
    if not src:
        return None

    direction = _Direction.for_cls(src.__class__)

    schema_cls = getattr(schema, src.__class__.__name__)
    entity_cls = getattr(entities, src.__class__.__name__)

    if direction is _Direction.ENTITY_TO_SCHEMA:
        dst = schema_cls()
    else:
        if _should_use_builder(entity_cls):
            dst = entity_cls.builder()
        else:
            dst = entity_cls()

    for field, attribute in attr.fields_dict(entity_cls).items():
        if field == 'bookings':
            dst.bookings = [_convert(b) for b in src.bookings]
        elif field == 'holds':
            dst.holds = [_convert(h) for h in src.holds]
        elif field == 'arrest':
            dst.arrest = _convert(src.arrest)
        elif field == 'charges':
            dst.charges = [_convert(c) for c in src.charges]
        elif field == 'bond':
            dst.bond = _convert(src.bond)
        elif field == 'sentence':
            dst.sentence = _convert(src.sentence)
        elif field == 'related_sentences':
            # TODO(441): Correctly convert related_sentences once schema for
            # this field is finalized.
            continue
        else:
            value = _convert_field_or_enum(
                src, field, attribute.type, direction)
            setattr(dst, field, value)

    if direction is _Direction.SCHEMA_TO_ENTITY and \
            _should_use_builder(entity_cls):
        dst = dst.build()
    return dst


def _should_use_builder(entity_cls):
    return entity_cls in {entities.Person, entities.Booking, entities.Arrest}


def _convert_field_or_enum(src, field, attr_type, direction):
    if attr_type and _is_enum(attr_type) and getattr(src, field):
        if direction is _Direction.SCHEMA_TO_ENTITY:
            return attr_type(getattr(src, field))
        return getattr(src, field).value

    return getattr(src, field)


def _is_enum(attr_type):
    return attr_type and issubclass(attr_type, MappableEnum)
