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
"""Tests for persistence.py."""
import datetime
import unittest

from recidiviz.persistence import entities
from recidiviz.persistence.persistence_utils import remove_pii_for_person


class PersistenceUtilsTest(unittest.TestCase):
    """Tests for common_utils.py."""

    def test_remove_pii_for_person(self):
        person = entities.Person.new_with_defaults(
            full_name='TEST', birthdate=datetime.date(1990, 3, 12))

        remove_pii_for_person(person)
        expected_date = datetime.date(1990, 1, 1)

        self.assertEqual(person.birthdate, expected_date)
        self.assertIsNone(person.full_name)
