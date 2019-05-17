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
"""Tests for state/schema_entity_converter.py."""
from unittest import TestCase

from recidiviz.tests.utils import fakes


class TestStateSchemaEntityConverter(TestCase):

    def setup_method(self, _test_method):
        fakes.use_in_memory_sqlite_database()

    def test_convert_person(self):
        # TODO(1625): Implement this test
        pass
