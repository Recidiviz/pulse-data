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
import os

from recidiviz.common.constants.county.booking import CustodyStatus
from recidiviz.persistence.entity.county import entities as county_entities
from recidiviz.utils import environment
from recidiviz.utils.params import str_to_bool


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


def should_persist() -> bool:
    """
    Determines whether objects should be writed to the database in this context.
    """
    return environment.in_gcp() or str_to_bool(
        (os.environ.get("PERSIST_LOCALLY", "false"))
    )
