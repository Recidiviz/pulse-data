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

from recidiviz.calculator.pipeline.program.program_event import (
    ProgramEvent,
    ProgramReferralEvent,
)
from recidiviz.common.constants.state.state_assessment import StateAssessmentType
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.common.constants.state.state_supervision import StateSupervisionType


class TestProgramEvent(unittest.TestCase):
    """ Tests for ProgramEvent and ProgramReferralEvent."""

    def testProgramEvent(self):
        state_code = "CA"
        program_id = "PROGRAMX"
        event_date = date(2000, 11, 10)

        program_event = ProgramEvent(state_code, event_date, program_id)

        assert program_event.state_code == state_code
        assert program_event.program_id == program_id
        assert program_event.event_date == event_date

    def test_program_referral_event(self):
        state_code = "CA"
        program_id = "PROGRAMX"
        event_date = date(2000, 11, 10)
        supervision_type = StateSupervisionType.PROBATION
        assessment_score = 5
        assessment_type = StateAssessmentType.ORAS
        assessment_level = None
        participation_status = StateProgramAssignmentParticipationStatus.IN_PROGRESS
        supervising_officer_external_id = "OFFICER211"
        supervising_district_external_id = "DISTRICT 100"

        program_event = ProgramReferralEvent(
            state_code,
            event_date,
            program_id,
            supervision_type,
            participation_status,
            assessment_score,
            assessment_type,
            assessment_level,
            supervising_officer_external_id,
            supervising_district_external_id,
        )

        assert program_event.state_code == state_code
        assert program_event.event_date == event_date
        assert program_event.program_id == program_id
        assert program_event.supervision_type == supervision_type
        assert program_event.assessment_score == assessment_score
        assert program_event.assessment_type == assessment_type
        assert program_event.participation_status == participation_status
        assert (
            program_event.supervising_officer_external_id
            == supervising_officer_external_id
        )
        assert (
            program_event.supervising_district_external_id
            == supervising_district_external_id
        )

    def test_eq_different_field(self):
        state_code = "CA"
        program_id = "PROGRAMX"
        event_date = date(2000, 11, 10)

        first = ProgramEvent(state_code, event_date, program_id)

        second = ProgramEvent(state_code, event_date, "DIFFERENT")

        assert first != second

    def test_eq_different_types(self):
        state_code = "CA"
        program_id = "PROGRAMX"
        event_date = date(2000, 11, 10)
        supervision_type = StateSupervisionType.PROBATION
        assessment_score = 9
        assessment_type = StateAssessmentType.ORAS
        assessment_level = None
        participation_status = StateProgramAssignmentParticipationStatus.IN_PROGRESS
        supervising_officer_external_id = "OFFICER211"
        supervising_district_external_id = "DISTRICT 100"

        program_event = ProgramReferralEvent(
            state_code,
            event_date,
            program_id,
            supervision_type,
            participation_status,
            assessment_score,
            assessment_type,
            assessment_level,
            supervising_officer_external_id,
            supervising_district_external_id,
        )

        different = "Everything you do is a banana"

        assert program_event != different
