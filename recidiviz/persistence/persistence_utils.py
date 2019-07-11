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
from typing import Union, Type, Optional

from recidiviz.common.constants.county.booking import CustodyStatus
from recidiviz.common.str_field_utils import to_snake_case
from recidiviz.persistence.database.database_entity import DatabaseEntity
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


def primary_key_name_from_cls(
        schema_cls: Union[Type[DatabaseEntity], Type[Entity]]) -> str:
    def _strip_prefix(s: str, prefix: str) -> str:
        return s[len(prefix):] if s.startswith(prefix) else s

    snake_case_name = to_snake_case(schema_cls.__name__)
    return f"{_strip_prefix(snake_case_name, 'state_')}_id"


def primary_key_name_from_obj(
        schema_object: Union[DatabaseEntity, Entity]) -> str:
    return primary_key_name_from_cls(schema_object.__class__)


def primary_key_value_from_obj(
        schema_object: Union[DatabaseEntity, Entity]) -> Optional[int]:
    primary_key_column_name = primary_key_name_from_obj(schema_object)
    return getattr(schema_object, primary_key_column_name, None)
