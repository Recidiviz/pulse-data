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

"""Utils for the persistence layer."""
import datetime
import re
from typing import Union, Type, Optional

from recidiviz.common.constants.county.booking import CustodyStatus
from recidiviz.persistence.database.base_schema import Base
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.county import entities as county_entities


def remove_pii_for_person(person: county_entities.Person) -> None:
    """Removes all of the PII for a person

    Args:
        person: (entities.Person) The entity object to scrub.
    """
    person.full_name = None
    if person.birthdate:
        person.birthdate = datetime.date(
            year=person.birthdate.year,
            month=1,
            day=1,
        )


def is_booking_active(booking: county_entities.Booking) -> bool:
    """Determines whether or not a booking is active"""
    if booking.custody_status in CustodyStatus.get_released_statuses():
        return False
    return True


def has_active_booking(person: county_entities.Person) -> bool:
    """Determines if a person has an active booking"""
    return any(is_booking_active(booking) for booking in person.bookings)


first_cap_regex = re.compile('(.)([A-Z][a-z]+)')
all_cap_regex = re.compile('([a-z0-9])([A-Z])')


def _to_snake_case(capital_case_name: str) -> str:
    """Converts a capital case string (i.e. 'SupervisionViolationResponse'
    to a snake case string (i.e. 'supervision_violation_response'). See
    https://stackoverflow.com/questions/1175208/
    elegant-python-function-to-convert-camelcase-to-snake-case.
    """
    s1 = first_cap_regex.sub(r'\1_\2', capital_case_name)
    return all_cap_regex.sub(r'\1_\2', s1).lower()


def primary_key_name_from_cls(
        schema_cls: Union[Type[Base], Type[Entity]]) -> str:
    def _strip_prefix(s: str, prefix: str) -> str:
        return s[len(prefix):] if s.startswith(prefix) else s

    stripped_cls_name = _strip_prefix(schema_cls.__name__, 'State')
    return f'{_to_snake_case(stripped_cls_name)}_id'


def primary_key_name_from_obj(schema_object: Union[Base, Entity]) -> str:
    return primary_key_name_from_cls(schema_object.__class__)


def primary_key_value_from_obj(
        schema_object: Union[Base, Entity]) -> Optional[int]:
    primary_key_column_name = primary_key_name_from_obj(schema_object)
    return getattr(schema_object, primary_key_column_name, None)
