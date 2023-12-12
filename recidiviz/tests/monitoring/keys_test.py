# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
""" Tests for key enums"""
import unittest
from itertools import combinations

from recidiviz.monitoring.keys import InstrumentEnum


def member_attrs(enum: type[InstrumentEnum], attr: str) -> set[str]:
    return {getattr(member, attr) for member in list(enum)}


class MonitoringKeysTest(unittest.TestCase):
    """Tests for key enums"""

    def test_unique_enums(self) -> None:
        """Tests that no names / values overlap across our key enums"""
        instrument_combinations = combinations(InstrumentEnum.__subclasses__(), 2)

        for a, b in instrument_combinations:
            self.assertTrue(
                member_attrs(a, "name").isdisjoint(member_attrs(b, "name")),
                f"{a} and {b} contain overlapping member names",
            )
            self.assertTrue(
                member_attrs(a, "value").isdisjoint(member_attrs(b, "value")),
                f"{a} and {b} contain overlapping member values",
            )
