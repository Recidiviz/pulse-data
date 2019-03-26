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

"""Utils for persistence.py."""
import datetime

from recidiviz.common.constants.booking import CustodyStatus
from recidiviz.persistence import entities


def remove_pii_for_person(person: entities.Person) -> None:
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


def is_booking_active(booking: entities.Booking) -> bool:
    """Determines whether or not a booking is active"""
    if booking.custody_status in CustodyStatus.get_released_statuses():
        return False
    return True


def has_active_booking(person: entities.Person) -> bool:
    """Determines if a person has an active booking"""
    return any(is_booking_active(booking) for booking in person.bookings)
