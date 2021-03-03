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
"""Tests for entity_validator.py."""
import datetime
import unittest

from recidiviz.persistence.entity.county import entities as county_entities
from recidiviz.persistence.entity_validator.county.county_validator import (
    _has_malformed_dates,
    _has_overlapping_bookings,
)


class EntityValidatorTest(unittest.TestCase):
    """Tests entity validation."""

    def test_has_malformed_dates(self):
        person = county_entities.Person.new_with_defaults(
            bookings=[
                county_entities.Booking.new_with_defaults(
                    admission_date=datetime.date(2, 2, 2),
                    release_date=datetime.date(3, 3, 3),
                )
            ]
        )
        self.assertFalse(_has_malformed_dates(person))

        person.bookings[0].release_date = datetime.date(1, 1, 1)
        self.assertTrue(_has_malformed_dates(person))

    def test_has_overlapping_bookings(self):
        person = county_entities.Person.new_with_defaults()

        b1 = county_entities.Booking.new_with_defaults(
            admission_date=datetime.date(1, 1, 1), release_date=datetime.date(3, 3, 3)
        )
        b2 = county_entities.Booking.new_with_defaults(
            admission_date=datetime.date(3, 3, 3), release_date=datetime.date(4, 4, 4)
        )
        b_overlapping = county_entities.Booking.new_with_defaults(
            admission_date=datetime.date(2, 2, 2)
        )

        person.bookings = [b1, b2]
        self.assertFalse(_has_overlapping_bookings(person))

        person.bookings = [b_overlapping, b1, b2]
        self.assertTrue(_has_overlapping_bookings(person))
