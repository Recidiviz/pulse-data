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
"""Tests for persistence.py."""
import datetime
import unittest

from recidiviz.common.constants.county.booking import CustodyStatus
from recidiviz.persistence.entity.county import entities as county_entities
from recidiviz.persistence.persistence_utils import (
    remove_pii_for_person,
    is_booking_active,
    has_active_booking,
)


class PersistenceUtilsTest(unittest.TestCase):
    """Tests for common_utils.py."""

    def test_remove_pii_for_person(self):
        person = county_entities.Person.new_with_defaults(
            full_name="TEST", birthdate=datetime.date(1990, 3, 12)
        )

        remove_pii_for_person(person)
        expected_date = datetime.date(1990, 1, 1)

        self.assertEqual(person.birthdate, expected_date)
        self.assertIsNone(person.full_name)

    def test_has_active_booking(self):
        person_inactive_booking = county_entities.Person.new_with_defaults(
            bookings=[
                county_entities.Booking.new_with_defaults(
                    booking_id="1", custody_status=CustodyStatus.RELEASED
                )
            ]
        )
        person_active_booking = county_entities.Person.new_with_defaults(
            bookings=[county_entities.Booking.new_with_defaults(booking_id="2")]
        )

        self.assertFalse(has_active_booking(person_inactive_booking))
        self.assertTrue(has_active_booking(person_active_booking))

    def test_is_booking_active(self):
        inactive_booking = county_entities.Booking.new_with_defaults(
            booking_id="1", custody_status=CustodyStatus.RELEASED
        )
        active_booking = county_entities.Booking.new_with_defaults(booking_id="2")

        self.assertFalse(is_booking_active(inactive_booking))
        self.assertTrue((is_booking_active(active_booking)))
