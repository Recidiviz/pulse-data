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
# pylint: disable=unused-import,wrong-import-order,protected-access
"""Tests for program/identifier.py."""

import unittest
from datetime import date
from typing import List
from unittest import mock

from dateutil.relativedelta import relativedelta
from freezegun import freeze_time

from recidiviz.calculator.pipeline.program import identifier
from recidiviz.calculator.pipeline.program.events import (
    ProgramParticipationEvent,
    ProgramReferralEvent,
)
from recidiviz.common.constants.state.state_assessment import StateAssessmentType
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodStatus,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.entity.state.entities import (
    StateAssessment,
    StateProgramAssignment,
    StateSupervisionPeriod,
)
from recidiviz.tests.calculator.pipeline.utils.state_utils.us_xx.us_xx_incarceration_period_pre_processing_delegate import (
    UsXxIncarcerationPreProcessingDelegate,
)
from recidiviz.tests.calculator.pipeline.utils.state_utils.us_xx.us_xx_supervision_delegate import (
    UsXxSupervisionDelegate,
)

DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS = {
    999: {"agent_id": 000, "agent_external_id": "XXX", "supervision_period_id": 999}
}

DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST = list(
    DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS.values()
)


class TestFindProgramEvents(unittest.TestCase):
    """Tests the find_program_events function."""

    def setUp(self) -> None:
        self.assessment_types_patcher = mock.patch(
            "recidiviz.calculator.pipeline.program.identifier.assessment_utils."
            "_assessment_types_of_class_for_state"
        )
        self.mock_assessment_types = self.assessment_types_patcher.start()
        self.mock_assessment_types.return_value = [StateAssessmentType.ORAS]

        self.pre_processing_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.utils.entity_pre_processing_utils.get_state_specific_incarceration_period_pre_processing_delegate"
        )
        self.mock_pre_processing_delegate = self.pre_processing_delegate_patcher.start()
        self.mock_pre_processing_delegate.return_value = (
            UsXxIncarcerationPreProcessingDelegate()
        )
        self.supervision_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.program.identifier.get_state_specific_supervision_delegate"
        )
        self.mock_supervision_delegate = self.supervision_delegate_patcher.start()
        self.mock_supervision_delegate.return_value = UsXxSupervisionDelegate()
        self.identifier = identifier.ProgramIdentifier()

    def tearDown(self) -> None:
        self.assessment_types_patcher.stop()
        self.pre_processing_delegate_patcher.stop()
        self.supervision_delegate_patcher.stop()

    @freeze_time("2020-01-02")
    def test_find_program_events(self) -> None:
        program_assignment = StateProgramAssignment.new_with_defaults(
            state_code="US_XX",
            program_id="PG3",
            referral_date=date(2020, 1, 3),
            participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
            program_location_id="LOCATION X",
            start_date=date(2020, 1, 1),
        )

        assessment = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2019, 7, 10),
        )

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=999,
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2019, 3, 5),
            termination_date=date(2020, 10, 1),
            supervision_type=StateSupervisionType.PAROLE,
            supervision_site="OFFICE_1",
        )

        program_assignments = [program_assignment]
        assessments = [assessment]
        supervision_periods = [supervision_period]

        program_events = self.identifier._find_program_events(
            program_assignments,
            assessments,
            supervision_periods,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
        )

        assert program_assignment.program_id is not None
        assert program_assignment.referral_date is not None
        assert program_assignment.start_date is not None
        expected_events = [
            ProgramReferralEvent(
                state_code=program_assignment.state_code,
                event_date=program_assignment.referral_date,
                program_id=program_assignment.program_id,
                supervision_type=supervision_period.supervision_type,
                participation_status=program_assignment.participation_status,
                assessment_score=assessment.assessment_score,
                assessment_type=assessment.assessment_type,
                supervising_officer_external_id="XXX",
                supervising_district_external_id="OFFICE_1",
                level_1_supervision_location_external_id="OFFICE_1",
                level_2_supervision_location_external_id=None,
            ),
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
        program_events = self.identifier._find_program_events(
            [], [], [], DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST
        )

        self.assertEqual([], program_events)


class TestFindProgramReferrals(unittest.TestCase):
    """Tests the find_program_referrals function."""

    def setUp(self) -> None:
        self.assessment_types_patcher = mock.patch(
            "recidiviz.calculator.pipeline.program.identifier.assessment_utils."
            "_assessment_types_of_class_for_state"
        )
        self.mock_assessment_types = self.assessment_types_patcher.start()
        self.mock_assessment_types.return_value = [StateAssessmentType.ORAS]
        self.supervision_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.program.identifier.get_state_specific_supervision_delegate"
        )
        self.mock_supervision_delegate = self.supervision_delegate_patcher.start()
        self.mock_supervision_delegate.return_value = UsXxSupervisionDelegate()
        self.identifier = identifier.ProgramIdentifier()

    def tearDown(self) -> None:
        self.assessment_types_patcher.stop()
        self.supervision_delegate_patcher.stop()

    def test_find_program_referrals(self) -> None:
        program_assignment = StateProgramAssignment.new_with_defaults(
            state_code="US_XX",
            program_id="PG3",
            referral_date=date(2009, 10, 3),
            participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
        )

        assessment = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2009, 7, 10),
        )

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=999,
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2008, 3, 5),
            termination_date=date(2010, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PAROLE,
            supervision_site="OFFICE_1",
        )

        assessments = [assessment]
        supervision_periods = [supervision_period]

        program_referrals = self.identifier._find_program_referrals(
            program_assignment,
            assessments,
            supervision_periods,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
        )

        assert program_assignment.program_id is not None
        assert program_assignment.referral_date is not None
        self.assertListEqual(
            [
                ProgramReferralEvent(
                    state_code=program_assignment.state_code,
                    program_id=program_assignment.program_id,
                    event_date=program_assignment.referral_date,
                    participation_status=program_assignment.participation_status,
                    assessment_score=33,
                    assessment_type=StateAssessmentType.ORAS,
                    supervision_type=supervision_period.supervision_type,
                    supervising_officer_external_id="XXX",
                    supervising_district_external_id="OFFICE_1",
                    level_1_supervision_location_external_id="OFFICE_1",
                )
            ],
            program_referrals,
        )

    def test_find_program_referrals_no_referral(self) -> None:
        program_assignment = StateProgramAssignment.new_with_defaults(
            state_code="US_XX",
            program_id="PG3",
            participation_status=StateProgramAssignmentParticipationStatus.PRESENT_WITHOUT_INFO,
        )

        assessments: List[StateAssessment] = []
        supervision_periods: List[StateSupervisionPeriod] = []

        program_referrals = self.identifier._find_program_referrals(
            program_assignment,
            assessments,
            supervision_periods,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
        )

        self.assertListEqual([], program_referrals)

    def test_find_program_referrals_multiple_assessments(self) -> None:
        program_assignment = StateProgramAssignment.new_with_defaults(
            state_code="US_XX",
            program_id="PG3",
            referral_date=date(2009, 10, 3),
            participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
        )

        assessment_1 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2009, 3, 10),
        )

        assessment_2 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=29,
            assessment_date=date(2009, 9, 14),
        )

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2008, 3, 5),
            termination_date=date(2010, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PAROLE,
        )

        assessments = [assessment_1, assessment_2]
        supervision_periods = [supervision_period]

        program_referrals = self.identifier._find_program_referrals(
            program_assignment,
            assessments,
            supervision_periods,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
        )

        assert program_assignment.program_id is not None
        assert program_assignment.referral_date is not None
        self.assertListEqual(
            [
                ProgramReferralEvent(
                    state_code=program_assignment.state_code,
                    program_id=program_assignment.program_id,
                    event_date=program_assignment.referral_date,
                    participation_status=program_assignment.participation_status,
                    assessment_score=29,
                    assessment_type=StateAssessmentType.ORAS,
                    supervision_type=supervision_period.supervision_type,
                )
            ],
            program_referrals,
        )

    def test_find_program_referrals_assessment_after_referral(self) -> None:
        program_assignment = StateProgramAssignment.new_with_defaults(
            state_code="US_XX",
            program_id="PG3",
            referral_date=date(2009, 10, 3),
            participation_status=StateProgramAssignmentParticipationStatus.DISCHARGED,
        )

        assessment_1 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2009, 3, 10),
        )

        assessment_2 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=29,
            assessment_date=date(2009, 10, 4),
        )

        assessments = [assessment_1, assessment_2]
        supervision_periods: List[StateSupervisionPeriod] = []

        program_referrals = self.identifier._find_program_referrals(
            program_assignment,
            assessments,
            supervision_periods,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
        )

        assert program_assignment.program_id is not None
        assert program_assignment.referral_date is not None
        self.assertListEqual(
            [
                ProgramReferralEvent(
                    state_code=program_assignment.state_code,
                    program_id=program_assignment.program_id,
                    event_date=program_assignment.referral_date,
                    participation_status=program_assignment.participation_status,
                    assessment_score=33,
                    assessment_type=StateAssessmentType.ORAS,
                )
            ],
            program_referrals,
        )

    def test_find_program_referrals_multiple_supervisions(self) -> None:
        program_assignment = StateProgramAssignment.new_with_defaults(
            state_code="US_XX",
            program_id="PG3",
            referral_date=date(2009, 10, 3),
            participation_status=StateProgramAssignmentParticipationStatus.PENDING,
        )

        assessment = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2009, 7, 10),
        )

        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2008, 3, 5),
            termination_date=date(2010, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PAROLE,
        )

        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2006, 12, 1),
            termination_date=date(2013, 1, 4),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PROBATION,
        )

        assessments = [assessment]
        supervision_periods = [supervision_period_1, supervision_period_2]

        program_referrals = self.identifier._find_program_referrals(
            program_assignment,
            assessments,
            supervision_periods,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
        )

        assert program_assignment.program_id is not None
        assert program_assignment.referral_date is not None
        self.assertListEqual(
            [
                ProgramReferralEvent(
                    state_code=program_assignment.state_code,
                    program_id=program_assignment.program_id,
                    event_date=program_assignment.referral_date,
                    participation_status=program_assignment.participation_status,
                    assessment_score=33,
                    assessment_type=StateAssessmentType.ORAS,
                    supervision_type=supervision_period_1.supervision_type,
                ),
                ProgramReferralEvent(
                    state_code=program_assignment.state_code,
                    program_id=program_assignment.program_id,
                    event_date=program_assignment.referral_date,
                    participation_status=program_assignment.participation_status,
                    assessment_score=33,
                    assessment_type=StateAssessmentType.ORAS,
                    supervision_type=supervision_period_2.supervision_type,
                ),
            ],
            program_referrals,
        )

    def test_find_program_referrals_officer_info_us_nd(self) -> None:
        program_assignment = StateProgramAssignment.new_with_defaults(
            state_code="US_ND",
            program_id="PG3",
            referral_date=date(2009, 10, 3),
            participation_status=StateProgramAssignmentParticipationStatus.DISCHARGED,
        )

        assessment = StateAssessment.new_with_defaults(
            state_code="US_ND",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2009, 7, 10),
        )

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2008, 3, 5),
            termination_date=date(2010, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PAROLE,
            supervision_site="DISTRICT8",
        )

        assessments = [assessment]
        supervision_periods = [supervision_period]

        assert supervision_period.supervision_period_id is not None
        supervision_period_agent_associations = {
            supervision_period.supervision_period_id: {
                "agent_id": 000,
                "agent_external_id": "OFFICER10",
                "supervision_period_id": supervision_period.supervision_period_id,
            }
        }

        program_referrals = self.identifier._find_program_referrals(
            program_assignment,
            assessments,
            supervision_periods,
            supervision_period_agent_associations,
        )

        assert program_assignment.program_id is not None
        assert program_assignment.referral_date is not None
        self.assertListEqual(
            [
                ProgramReferralEvent(
                    state_code=program_assignment.state_code,
                    program_id=program_assignment.program_id,
                    event_date=program_assignment.referral_date,
                    participation_status=program_assignment.participation_status,
                    assessment_score=33,
                    assessment_type=StateAssessmentType.ORAS,
                    supervision_type=supervision_period.supervision_type,
                    supervising_officer_external_id="OFFICER10",
                    supervising_district_external_id="DISTRICT8",
                    level_1_supervision_location_external_id="DISTRICT8",
                    level_2_supervision_location_external_id=None,
                )
            ],
            program_referrals,
        )


class TestFindProgramParticipationEvents(unittest.TestCase):
    """Tests the find_program_participation_events function."""

    def setUp(self) -> None:
        self.identifier = identifier.ProgramIdentifier()

    @freeze_time("2000-01-01")
    def test_find_program_participation_events(self) -> None:
        program_assignment = StateProgramAssignment.new_with_defaults(
            state_code="US_XX",
            program_id="PG3",
            referral_date=date(1999, 10, 3),
            participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
            program_location_id="LOCATION",
            start_date=date(1999, 12, 31),
        )

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            status=StateSupervisionPeriodStatus.UNDER_SUPERVISION,
            state_code="US_XX",
            start_date=date(1990, 3, 5),
            supervision_type=StateSupervisionType.PAROLE,
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
        program_assignment = StateProgramAssignment.new_with_defaults(
            state_code="US_XX",
            program_id="PG3",
            referral_date=date(2009, 10, 3),
            participation_status=StateProgramAssignmentParticipationStatus.DISCHARGED,
            program_location_id="LOCATION",
            start_date=date(2009, 11, 5),
            discharge_date=date(2009, 11, 8),
        )

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            status=StateSupervisionPeriodStatus.UNDER_SUPERVISION,
            state_code="US_XX",
            start_date=date(1990, 3, 5),
            supervision_type=StateSupervisionType.PAROLE,
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
        program_assignment = StateProgramAssignment.new_with_defaults(
            state_code="US_XX",
            program_id="PG3",
            referral_date=date(1999, 10, 3),
            # This program assignment is in progress, but it's missing a required start_date
            participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
            program_location_id="LOCATION",
        )

        supervision_periods: List[StateSupervisionPeriod] = []

        participation_events = self.identifier._find_program_participation_events(
            program_assignment, supervision_periods
        )

        self.assertEqual([], participation_events)

    def test_find_program_participation_events_no_discharge_date(self) -> None:
        program_assignment = StateProgramAssignment.new_with_defaults(
            state_code="US_XX",
            program_id="PG3",
            referral_date=date(1999, 10, 3),
            # This program assignment has a DISCHARGED status, but it's missing a required discharge_date
            participation_status=StateProgramAssignmentParticipationStatus.DISCHARGED,
            program_location_id="LOCATION",
            start_date=date(1999, 11, 2),
        )

        supervision_periods: List[StateSupervisionPeriod] = []

        participation_events = self.identifier._find_program_participation_events(
            program_assignment, supervision_periods
        )

        self.assertEqual([], participation_events)


class TestFindSupervisionPeriodsOverlappingWithDate(unittest.TestCase):
    """Tests the find_supervision_periods_overlapping_with_date function."""

    def setUp(self) -> None:
        self.identifier = identifier.ProgramIdentifier()

    def test_find_supervision_periods_overlapping_with_date(self) -> None:
        referral_date = date(2013, 3, 1)

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2008, 3, 5),
            termination_date=date(2015, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PAROLE,
        )

        supervision_periods = [supervision_period]

        supervision_periods_during_referral = (
            self.identifier._find_supervision_periods_overlapping_with_date(
                referral_date, supervision_periods
            )
        )

        self.assertListEqual(supervision_periods, supervision_periods_during_referral)

    def test_find_supervision_periods_overlapping_with_date_no_termination(
        self,
    ) -> None:
        referral_date = date(2013, 3, 1)

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2002, 11, 5),
            supervision_type=StateSupervisionType.PAROLE,
        )

        supervision_periods = [supervision_period]

        supervision_periods_during_referral = (
            self.identifier._find_supervision_periods_overlapping_with_date(
                referral_date, supervision_periods
            )
        )

        self.assertListEqual(supervision_periods, supervision_periods_during_referral)

    def test_find_supervision_periods_overlapping_with_date_no_overlap(self) -> None:
        referral_date = date(2019, 3, 1)

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2008, 3, 5),
            termination_date=date(2015, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PAROLE,
        )

        supervision_periods = [supervision_period]

        supervision_periods_during_referral = (
            self.identifier._find_supervision_periods_overlapping_with_date(
                referral_date, supervision_periods
            )
        )

        self.assertListEqual([], supervision_periods_during_referral)


class TestReferralsForSupervisionPeriods(unittest.TestCase):
    """Tests the referrals_for_supervision_periods function."""

    def setUp(self) -> None:
        self.supervision_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.program.identifier.get_state_specific_supervision_delegate"
        )
        self.mock_supervision_delegate = self.supervision_delegate_patcher.start()
        self.mock_supervision_delegate.return_value = UsXxSupervisionDelegate()
        self.identifier = identifier.ProgramIdentifier()

    def tearDown(self) -> None:
        self.supervision_delegate_patcher.stop()

    def test_referrals_for_supervision_periods(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2008, 3, 5),
            termination_date=date(2010, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PAROLE,
        )

        program_referrals = self.identifier._referrals_for_supervision_periods(
            state_code="US_XX",
            program_id="XXX",
            referral_date=date(2009, 3, 12),
            participation_status=StateProgramAssignmentParticipationStatus.DISCHARGED,
            assessment_score=39,
            assessment_type=StateAssessmentType.LSIR,
            supervision_periods=[supervision_period],
            supervision_period_to_agent_associations=DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
        )

        self.assertListEqual(
            [
                ProgramReferralEvent(
                    state_code="US_XX",
                    program_id="XXX",
                    event_date=date(2009, 3, 12),
                    participation_status=StateProgramAssignmentParticipationStatus.DISCHARGED,
                    assessment_score=39,
                    assessment_type=StateAssessmentType.LSIR,
                    supervision_type=supervision_period.supervision_type,
                )
            ],
            program_referrals,
        )

    def test_referrals_for_supervision_periods_same_type(self) -> None:
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2008, 3, 5),
            termination_date=date(2010, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PROBATION,
        )

        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2008, 3, 5),
            termination_date=date(2010, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PROBATION,
        )

        supervision_periods = [supervision_period_1, supervision_period_2]

        program_referrals = self.identifier._referrals_for_supervision_periods(
            state_code="US_XX",
            program_id="XXX",
            referral_date=date(2009, 3, 19),
            participation_status=StateProgramAssignmentParticipationStatus.DISCHARGED,
            assessment_score=39,
            assessment_type=StateAssessmentType.LSIR,
            supervision_periods=supervision_periods,
            supervision_period_to_agent_associations=DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
        )

        self.assertListEqual(
            [
                ProgramReferralEvent(
                    state_code="US_XX",
                    program_id="XXX",
                    event_date=date(2009, 3, 19),
                    participation_status=StateProgramAssignmentParticipationStatus.DISCHARGED,
                    assessment_score=39,
                    assessment_type=StateAssessmentType.LSIR,
                    supervision_type=StateSupervisionType.PROBATION,
                ),
                ProgramReferralEvent(
                    state_code="US_XX",
                    program_id="XXX",
                    event_date=date(2009, 3, 19),
                    participation_status=StateProgramAssignmentParticipationStatus.DISCHARGED,
                    assessment_score=39,
                    assessment_type=StateAssessmentType.LSIR,
                    supervision_type=StateSupervisionType.PROBATION,
                ),
            ],
            program_referrals,
        )

    def test_referrals_for_supervision_periods_different_types(self) -> None:
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2008, 3, 5),
            termination_date=date(2010, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PAROLE,
        )

        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2008, 3, 5),
            termination_date=date(2010, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PROBATION,
        )

        supervision_periods = [supervision_period_1, supervision_period_2]

        program_referrals = self.identifier._referrals_for_supervision_periods(
            state_code="US_XX",
            program_id="XXX",
            referral_date=date(2009, 3, 19),
            participation_status=StateProgramAssignmentParticipationStatus.DENIED,
            assessment_score=39,
            assessment_type=StateAssessmentType.LSIR,
            supervision_periods=supervision_periods,
            supervision_period_to_agent_associations=DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
        )

        self.assertListEqual(
            [
                ProgramReferralEvent(
                    state_code="US_XX",
                    program_id="XXX",
                    event_date=date(2009, 3, 19),
                    participation_status=StateProgramAssignmentParticipationStatus.DENIED,
                    assessment_score=39,
                    assessment_type=StateAssessmentType.LSIR,
                    supervision_type=supervision_period_1.supervision_type,
                ),
                ProgramReferralEvent(
                    state_code="US_XX",
                    program_id="XXX",
                    event_date=date(2009, 3, 19),
                    participation_status=StateProgramAssignmentParticipationStatus.DENIED,
                    assessment_score=39,
                    assessment_type=StateAssessmentType.LSIR,
                    supervision_type=supervision_period_2.supervision_type,
                ),
            ],
            program_referrals,
        )
