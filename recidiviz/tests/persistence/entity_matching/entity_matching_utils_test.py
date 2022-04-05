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
"""Tests for entity_matching_utils.py."""
from datetime import datetime
from unittest import TestCase

import attr

from recidiviz.common.constants.booking import CustodyStatus
from recidiviz.persistence.entity.county import entities as county_entities
from recidiviz.persistence.entity_matching import entity_matching_utils

_DATE = datetime(2018, 12, 13)
_DATE_OTHER = datetime(2017, 12, 13)


class TestEntityMatchingUtils(TestCase):
    """Tests for entity matching logic"""

    def test_person_similarity(self):
        person = county_entities.Person.new_with_defaults(
            person_id=1, birthdate=_DATE,
            bookings=[county_entities.Booking.new_with_defaults(
                booking_id=2, custody_status=CustodyStatus.RELEASED)])

        person_another = attr.evolve(person, birthdate=_DATE_OTHER)
        self.assertEqual(
            entity_matching_utils.diff_count(person, person_another), 1)
