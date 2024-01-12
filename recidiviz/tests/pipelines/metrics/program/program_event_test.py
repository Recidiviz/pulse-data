# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Tests for program/program_event.py."""
import unittest
from datetime import date

from recidiviz.pipelines.metrics.program.events import ProgramEvent


class TestProgramEvent(unittest.TestCase):
    """Tests for ProgramEvent and ProgramReferralEvent."""

    def testProgramEvent(self) -> None:
        state_code = "CA"
        program_id = "PROGRAMX"
        event_date = date(2000, 11, 10)

        program_event = ProgramEvent(state_code, event_date, program_id)

        assert program_event.state_code == state_code
        assert program_event.program_id == program_id
        assert program_event.event_date == event_date

    def test_eq_different_field(self) -> None:
        state_code = "CA"
        program_id = "PROGRAMX"
        event_date = date(2000, 11, 10)

        first = ProgramEvent(state_code, event_date, program_id)

        second = ProgramEvent(state_code, event_date, "DIFFERENT")

        assert first != second
