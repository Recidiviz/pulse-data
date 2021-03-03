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

"""Validation logic for county-level entities."""

import collections
import datetime
import logging

import more_itertools

from recidiviz.persistence.entity.county import entities


def validate_county_person(person: entities.Person) -> bool:
    """Returns True if the person's bookings are valid, and False if an error was logged."""
    if _has_malformed_dates(person):
        logging.error(
            "Booking found with release date before admission date for person: [%s]",
            person,
        )
        return False
    if _has_overlapping_bookings(person):
        logging.error("Overlapping historical bookings found for person: [%s]", person)
        return False
    return True


def _has_malformed_dates(person: entities.Person) -> bool:
    """Returns True if any booking has a release date before its admission
    date."""
    return any(
        booking.admission_date
        and booking.release_date
        and booking.release_date < booking.admission_date
        for booking in person.bookings
    )


def _has_overlapping_bookings(person: entities.Person) -> bool:
    """Determines if a person has bookings with overlapping date ranges."""
    DateRange = collections.namedtuple("DateRange", ["start", "end"])
    booking_ranges = (
        DateRange(
            b.admission_date or datetime.date.min, b.release_date or datetime.date.max
        )
        for b in person.bookings
    )
    return any(
        range_2.start < range_1.end
        for range_1, range_2 in more_itertools.pairwise(sorted(booking_ranges))
    )
