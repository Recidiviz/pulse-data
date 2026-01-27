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
# pylint: disable=protected-access
"""Tests for program/identifier.py."""

import unittest
from datetime import date
from typing import Dict, List, Sequence, Union

from dateutil.relativedelta import relativedelta
from freezegun import freeze_time

from recidiviz.common.constants.state.state_assessment import StateAssessmentType
from recidiviz.common.constants.state.state_person import StateEthnicity
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateAssessment,
    NormalizedStatePerson,
    NormalizedStateProgramAssignment,
    NormalizedStateSupervisionPeriod,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.assessment_normalization_manager import (
    DEFAULT_ASSESSMENT_SCORE_BUCKET,
)
from recidiviz.pipelines.metrics.program import identifier
from recidiviz.pipelines.metrics.program.events import (
    ProgramEvent,
    ProgramParticipationEvent,
)
from recidiviz.pipelines.utils.execution_utils import TableRow

_STATE_CODE = "US_XX"


class TestFindProgramEvents(unittest.TestCase):
    """Tests the find_program_events function."""

    def setUp(self) -> None:
        self.identifier = identifier.ProgramIdentifier()
        self.person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=99000123,
            ethnicity=StateEthnicity.PRESENT_WITHOUT_INFO,
        )

    def _test_find_program_events(
        self,
        program_assignments: List[NormalizedStateProgramAssignment],
        assessments: List[NormalizedStateAssessment],
        supervision_periods: List[NormalizedStateSupervisionPeriod],
    ) -> List[ProgramEvent]:
        """Helper for testing the find_events function on the identifier."""
        all_kwargs: Dict[str, Union[Sequence[Entity], List[TableRow]]] = {
            NormalizedStateProgramAssignment.__name__: program_assignments,
            NormalizedStateSupervisionPeriod.__name__: supervision_periods,
            NormalizedStateAssessment.__name__: assessments,
        }

        return self.identifier.identify(
            self.person,
            identifier_context=all_kwargs,
            included_result_classes={ProgramParticipationEvent},
        )

    @freeze_time("2020-01-02 00:00:00-05:00")
    def test_find_program_events(self) -> None:
        program_assignment = NormalizedStateProgramAssignment(
            program_assignment_id=1,
            sequence_num=0,
            state_code="US_XX",
            external_id="pa1",
            program_id="PG3",
            referral_date=date(2020, 1, 3),
            participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
            program_location_id="LOCATION X",
            start_date=date(2020, 1, 1),
        )

        assessment = NormalizedStateAssessment(
            assessment_id=1,
            state_code="US_XX",
            external_id="a1",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_date=date(2019, 7, 10),
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=0,
        )

        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=999,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2019, 3, 5),
            termination_date=date(2020, 10, 1),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_site="OFFICE_1",
        )

        program_assignments = [program_assignment]
        assessments = [assessment]
        supervision_periods = [supervision_period]

        program_events = self._test_find_program_events(
            program_assignments,
            assessments,
            supervision_periods,
        )

        assert program_assignment.program_id is not None
        assert program_assignment.referral_date is not None
        assert program_assignment.start_date is not None
        expected_events = [
            ProgramParticipationEvent(
                state_code=program_assignment.state_code,
                event_date=program_assignment.start_date,
                is_first_day_in_program=True,
                program_id=program_assignment.program_id,
                program_location_id=program_assignment.program_location_id,
                supervision_type=supervision_period.supervision_type,
            ),
            ProgramParticipationEvent(
                state_code=program_assignment.state_code,
                event_date=program_assignment.start_date + relativedelta(days=1),
                is_first_day_in_program=False,
                program_id=program_assignment.program_id,
                program_location_id=program_assignment.program_location_id,
                supervision_type=supervision_period.supervision_type,
            ),
        ]

        self.assertListEqual(program_events, expected_events)

    def test_find_program_events_no_program_assignments(self) -> None:
        program_events = self._test_find_program_events(
            [],
            [],
            [],
        )

        self.assertEqual([], program_events)

    def test_find_program_events_wrong_result_class(self) -> None:
        with self.assertRaisesRegex(NotImplementedError, "Filtering of events"):
            self.identifier.identify(
                self.person,
                identifier_context={},
                included_result_classes=set(),
            )


class TestFindProgramParticipationEvents(unittest.TestCase):
    """Tests the find_program_participation_events function."""

    def setUp(self) -> None:
        self.identifier = identifier.ProgramIdentifier()

    @freeze_time("2000-01-01 00:00:00-05:00")
    def test_find_program_participation_events(self) -> None:
        program_assignment = NormalizedStateProgramAssignment(
            program_assignment_id=1,
            state_code="US_XX",
            external_id="pa1",
            sequence_num=0,
            program_id="PG3",
            referral_date=date(1999, 10, 3),
            participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
            program_location_id="LOCATION",
            start_date=date(1999, 12, 31),
        )

        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(1990, 3, 5),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        supervision_periods = [supervision_period]

        participation_events = self.identifier._find_program_participation_events(
            program_assignment, supervision_periods
        )

        assert program_assignment.program_id is not None
        assert program_assignment.start_date is not None
        expected_events = [
            ProgramParticipationEvent(
                state_code=program_assignment.state_code,
                program_id=program_assignment.program_id,
                event_date=program_assignment.start_date,
                is_first_day_in_program=True,
                program_location_id=program_assignment.program_location_id,
                supervision_type=supervision_period.supervision_type,
            ),
            ProgramParticipationEvent(
                state_code=program_assignment.state_code,
                program_id=program_assignment.program_id,
                event_date=program_assignment.start_date + relativedelta(days=1),
                is_first_day_in_program=False,
                program_location_id=program_assignment.program_location_id,
                supervision_type=supervision_period.supervision_type,
            ),
        ]
        self.assertListEqual(expected_events, participation_events)

    def test_find_program_participation_events_not_actively_participating(self) -> None:
        program_assignment = NormalizedStateProgramAssignment(
            program_assignment_id=1,
            state_code="US_XX",
            external_id="pa1",
            sequence_num=0,
            program_id="PG3",
            referral_date=date(2009, 10, 3),
            participation_status=StateProgramAssignmentParticipationStatus.DISCHARGED_UNKNOWN,
            program_location_id="LOCATION",
            start_date=date(2009, 11, 5),
            discharge_date=date(2009, 11, 8),
        )

        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(1990, 3, 5),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        supervision_periods = [supervision_period]

        participation_events = self.identifier._find_program_participation_events(
            program_assignment, supervision_periods
        )

        assert program_assignment.program_id is not None
        assert program_assignment.start_date is not None
        expected_events = [
            ProgramParticipationEvent(
                state_code=program_assignment.state_code,
                program_id=program_assignment.program_id,
                event_date=program_assignment.start_date,
                is_first_day_in_program=True,
                program_location_id=program_assignment.program_location_id,
                supervision_type=supervision_period.supervision_type,
            ),
            ProgramParticipationEvent(
                state_code=program_assignment.state_code,
                program_id=program_assignment.program_id,
                event_date=program_assignment.start_date + relativedelta(days=1),
                is_first_day_in_program=False,
                program_location_id=program_assignment.program_location_id,
                supervision_type=supervision_period.supervision_type,
            ),
            ProgramParticipationEvent(
                state_code=program_assignment.state_code,
                program_id=program_assignment.program_id,
                event_date=program_assignment.start_date + relativedelta(days=2),
                is_first_day_in_program=False,
                program_location_id=program_assignment.program_location_id,
                supervision_type=supervision_period.supervision_type,
            ),
        ]

        self.assertListEqual(expected_events, participation_events)

    def test_find_program_participation_events_no_start_date(self) -> None:
        program_assignment = NormalizedStateProgramAssignment(
            program_assignment_id=1,
            state_code="US_XX",
            external_id="pa1",
            sequence_num=0,
            program_id="PG3",
            referral_date=date(1999, 10, 3),
            # This program assignment is in progress, but it's missing a required start_date
            participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
            program_location_id="LOCATION",
        )

        supervision_periods: List[NormalizedStateSupervisionPeriod] = []

        participation_events = self.identifier._find_program_participation_events(
            program_assignment, supervision_periods
        )

        self.assertEqual([], participation_events)

    def test_find_program_participation_events_no_discharge_date(self) -> None:
        program_assignment = NormalizedStateProgramAssignment(
            program_assignment_id=1,
            state_code="US_XX",
            external_id="pa1",
            sequence_num=0,
            program_id="PG3",
            referral_date=date(1999, 10, 3),
            # This program assignment has a DISCHARGED status, but it's missing a required discharge_date
            participation_status=StateProgramAssignmentParticipationStatus.DISCHARGED_UNKNOWN,
            program_location_id="LOCATION",
            start_date=date(1999, 11, 2),
        )

        supervision_periods: List[NormalizedStateSupervisionPeriod] = []

        participation_events = self.identifier._find_program_participation_events(
            program_assignment, supervision_periods
        )

        self.assertEqual([], participation_events)
