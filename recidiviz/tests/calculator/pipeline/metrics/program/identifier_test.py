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
from typing import Dict, List, Optional, Sequence, Union
from unittest import mock

from dateutil.relativedelta import relativedelta
from freezegun import freeze_time

from recidiviz.calculator.pipeline.metrics.program import identifier
from recidiviz.calculator.pipeline.metrics.program.events import (
    ProgramEvent,
    ProgramParticipationEvent,
    ProgramReferralEvent,
)
from recidiviz.calculator.pipeline.metrics.program.pipeline import (
    ProgramMetricsPipelineRunDelegate,
)
from recidiviz.calculator.pipeline.normalization.utils.normalized_entities import (
    NormalizedStateProgramAssignment,
    NormalizedStateSupervisionPeriod,
)
from recidiviz.calculator.pipeline.utils.assessment_utils import (
    DEFAULT_ASSESSMENT_SCORE_BUCKET,
)
from recidiviz.calculator.pipeline.utils.execution_utils import TableRow
from recidiviz.calculator.pipeline.utils.state_utils.state_calculation_config_manager import (
    get_required_state_specific_delegates,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_delegate import (
    StateSpecificDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_supervision_delegate import (
    UsXxSupervisionDelegate,
)
from recidiviz.common.constants.state.state_assessment import StateAssessmentType
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state.entities import StateAssessment, StatePerson
from recidiviz.tests.calculator.pipeline.utils.state_utils.state_calculation_config_manager_test import (
    STATE_DELEGATES_FOR_TESTS,
)

_STATE_CODE = "US_XX"
DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS = {
    999: {"agent_id": 000, "agent_external_id": "XXX", "supervision_period_id": 999}
}

DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST = list(
    DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS.values()
)

DEFAULT_SUPERVISION_LOCATIONS_TO_NAMES_ASSOCIATIONS = {
    "level 1": {
        "state_code": _STATE_CODE,
        "level_1_supervision_location_external_id": "level 1",
        "level_2_supervision_location_external_id": "level 2",
    }
}
DEFAULT_SUPERVISION_LOCATIONS_TO_NAMES_ASSOCIATION_LIST = list(
    DEFAULT_SUPERVISION_LOCATIONS_TO_NAMES_ASSOCIATIONS.values()
)


class TestFindProgramEvents(unittest.TestCase):
    """Tests the find_program_events function."""

    def setUp(self) -> None:
        self.identifier = identifier.ProgramIdentifier()
        self.person = StatePerson.new_with_defaults(
            state_code="US_XX", person_id=99000123
        )

    def _test_find_program_events(
        self,
        program_assignments: List[NormalizedStateProgramAssignment],
        assessments: List[StateAssessment],
        supervision_periods: List[NormalizedStateSupervisionPeriod],
        state_code_override: Optional[str] = None,
    ) -> List[ProgramEvent]:
        """Helper for testing the find_events function on the identifier."""
        entity_kwargs: Dict[str, Union[Sequence[Entity], List[TableRow]]] = {
            NormalizedStateProgramAssignment.base_class_name(): program_assignments,
            NormalizedStateSupervisionPeriod.base_class_name(): supervision_periods,
            StateAssessment.__name__: assessments,
            "supervision_period_to_agent_association": DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
        }
        if not state_code_override:
            required_delegates = STATE_DELEGATES_FOR_TESTS
        else:
            required_delegates = get_required_state_specific_delegates(
                state_code=(state_code_override or _STATE_CODE),
                required_delegates=ProgramMetricsPipelineRunDelegate.pipeline_config().state_specific_required_delegates,
                entity_kwargs=entity_kwargs,
            )

        all_kwargs: Dict[
            str, Union[Sequence[Entity], List[TableRow], StateSpecificDelegate]
        ] = {
            **required_delegates,
            **entity_kwargs,
        }

        return self.identifier.identify(self.person, all_kwargs)

    @freeze_time("2020-01-02")
    def test_find_program_events(self) -> None:
        program_assignment = NormalizedStateProgramAssignment.new_with_defaults(
            state_code="US_XX",
            program_id="PG3",
            referral_date=date(2020, 1, 3),
            participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
            program_location_id="LOCATION X",
            start_date=date(2020, 1, 1),
        )

        assessment = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_date=date(2019, 7, 10),
        )

        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=999,
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
            ProgramReferralEvent(
                state_code=program_assignment.state_code,
                event_date=program_assignment.referral_date,
                program_id=program_assignment.program_id,
                supervision_type=supervision_period.supervision_type,
                participation_status=program_assignment.participation_status,
                assessment_score=assessment.assessment_score,
                assessment_type=assessment.assessment_type,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
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
        program_events = self._test_find_program_events(
            [],
            [],
            [],
        )

        self.assertEqual([], program_events)


class TestFindProgramReferrals(unittest.TestCase):
    """Tests the find_program_referrals function."""

    def setUp(self) -> None:
        self.identifier = identifier.ProgramIdentifier()

    def test_find_program_referrals(self) -> None:
        program_assignment = NormalizedStateProgramAssignment.new_with_defaults(
            state_code="US_XX",
            sequence_num=0,
            program_id="PG3",
            referral_date=date(2009, 10, 3),
            participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
        )

        assessment = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_date=date(2009, 7, 10),
        )

        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=999,
            state_code="US_XX",
            start_date=date(2008, 3, 5),
            termination_date=date(2010, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_site="OFFICE_1",
        )

        assessments = [assessment]
        supervision_periods = [supervision_period]

        program_referrals = self.identifier._find_program_referrals(
            program_assignment,
            assessments,
            supervision_periods,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            UsXxSupervisionDelegate(
                DEFAULT_SUPERVISION_LOCATIONS_TO_NAMES_ASSOCIATION_LIST
            ),
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
                    assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                    assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
                    supervision_type=supervision_period.supervision_type,
                    supervising_officer_external_id="XXX",
                    supervising_district_external_id="OFFICE_1",
                    level_1_supervision_location_external_id="OFFICE_1",
                )
            ],
            program_referrals,
        )

    def test_find_program_referrals_no_referral(self) -> None:
        program_assignment = NormalizedStateProgramAssignment.new_with_defaults(
            state_code="US_XX",
            sequence_num=0,
            program_id="PG3",
            participation_status=StateProgramAssignmentParticipationStatus.PRESENT_WITHOUT_INFO,
        )

        assessments: List[StateAssessment] = []
        supervision_periods: List[NormalizedStateSupervisionPeriod] = []

        program_referrals = self.identifier._find_program_referrals(
            program_assignment,
            assessments,
            supervision_periods,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            UsXxSupervisionDelegate(
                DEFAULT_SUPERVISION_LOCATIONS_TO_NAMES_ASSOCIATION_LIST
            ),
        )

        self.assertListEqual([], program_referrals)

    def test_find_program_referrals_multiple_assessments(self) -> None:
        program_assignment = NormalizedStateProgramAssignment.new_with_defaults(
            state_code="US_XX",
            sequence_num=0,
            program_id="PG3",
            referral_date=date(2009, 10, 3),
            participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
        )

        assessment_1 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_date=date(2009, 3, 10),
        )

        assessment_2 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=29,
            assessment_date=date(2009, 9, 14),
        )

        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            state_code="US_XX",
            start_date=date(2008, 3, 5),
            termination_date=date(2010, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        assessments = [assessment_1, assessment_2]
        supervision_periods = [supervision_period]

        program_referrals = self.identifier._find_program_referrals(
            program_assignment,
            assessments,
            supervision_periods,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            UsXxSupervisionDelegate([]),
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
                    assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                    assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
                    supervision_type=supervision_period.supervision_type,
                )
            ],
            program_referrals,
        )

    def test_find_program_referrals_assessment_after_referral(self) -> None:
        program_assignment = NormalizedStateProgramAssignment.new_with_defaults(
            state_code="US_XX",
            sequence_num=0,
            program_id="PG3",
            referral_date=date(2009, 10, 3),
            participation_status=StateProgramAssignmentParticipationStatus.DISCHARGED,
        )

        assessment_1 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_date=date(2009, 3, 10),
        )

        assessment_2 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=29,
            assessment_date=date(2009, 10, 4),
        )

        assessments = [assessment_1, assessment_2]
        supervision_periods: List[NormalizedStateSupervisionPeriod] = []

        program_referrals = self.identifier._find_program_referrals(
            program_assignment,
            assessments,
            supervision_periods,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            UsXxSupervisionDelegate(
                DEFAULT_SUPERVISION_LOCATIONS_TO_NAMES_ASSOCIATION_LIST
            ),
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
                    assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                    assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
                )
            ],
            program_referrals,
        )

    def test_find_program_referrals_multiple_supervisions(self) -> None:
        program_assignment = NormalizedStateProgramAssignment.new_with_defaults(
            state_code="US_XX",
            sequence_num=0,
            program_id="PG3",
            referral_date=date(2009, 10, 3),
            participation_status=StateProgramAssignmentParticipationStatus.PENDING,
        )

        assessment = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_date=date(2009, 7, 10),
        )

        supervision_period_1 = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            state_code="US_XX",
            start_date=date(2008, 3, 5),
            termination_date=date(2010, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        supervision_period_2 = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            state_code="US_XX",
            start_date=date(2006, 12, 1),
            termination_date=date(2013, 1, 4),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        assessments = [assessment]
        supervision_periods = [supervision_period_1, supervision_period_2]

        program_referrals = self.identifier._find_program_referrals(
            program_assignment,
            assessments,
            supervision_periods,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            UsXxSupervisionDelegate(
                DEFAULT_SUPERVISION_LOCATIONS_TO_NAMES_ASSOCIATION_LIST
            ),
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
                    assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                    assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
                    supervision_type=supervision_period_1.supervision_type,
                ),
                ProgramReferralEvent(
                    state_code=program_assignment.state_code,
                    program_id=program_assignment.program_id,
                    event_date=program_assignment.referral_date,
                    participation_status=program_assignment.participation_status,
                    assessment_score=33,
                    assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                    assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
                    supervision_type=supervision_period_2.supervision_type,
                ),
            ],
            program_referrals,
        )

    def test_find_program_referrals_officer_info_us_nd(self) -> None:
        program_assignment = NormalizedStateProgramAssignment.new_with_defaults(
            state_code="US_ND",
            program_id="PG3",
            referral_date=date(2009, 10, 3),
            participation_status=StateProgramAssignmentParticipationStatus.DISCHARGED,
        )

        assessment = StateAssessment.new_with_defaults(
            state_code="US_ND",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_date=date(2009, 7, 10),
        )

        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            state_code="US_ND",
            start_date=date(2008, 3, 5),
            termination_date=date(2010, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
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
            UsXxSupervisionDelegate([]),
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
                    assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                    assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
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
        program_assignment = NormalizedStateProgramAssignment.new_with_defaults(
            state_code="US_XX",
            sequence_num=0,
            program_id="PG3",
            referral_date=date(1999, 10, 3),
            participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
            program_location_id="LOCATION",
            start_date=date(1999, 12, 31),
        )

        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
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
        program_assignment = NormalizedStateProgramAssignment.new_with_defaults(
            state_code="US_XX",
            sequence_num=0,
            program_id="PG3",
            referral_date=date(2009, 10, 3),
            participation_status=StateProgramAssignmentParticipationStatus.DISCHARGED,
            program_location_id="LOCATION",
            start_date=date(2009, 11, 5),
            discharge_date=date(2009, 11, 8),
        )

        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
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
        program_assignment = NormalizedStateProgramAssignment.new_with_defaults(
            state_code="US_XX",
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
        program_assignment = NormalizedStateProgramAssignment.new_with_defaults(
            state_code="US_XX",
            sequence_num=0,
            program_id="PG3",
            referral_date=date(1999, 10, 3),
            # This program assignment has a DISCHARGED status, but it's missing a required discharge_date
            participation_status=StateProgramAssignmentParticipationStatus.DISCHARGED,
            program_location_id="LOCATION",
            start_date=date(1999, 11, 2),
        )

        supervision_periods: List[NormalizedStateSupervisionPeriod] = []

        participation_events = self.identifier._find_program_participation_events(
            program_assignment, supervision_periods
        )

        self.assertEqual([], participation_events)


class TestReferralsForSupervisionPeriods(unittest.TestCase):
    """Tests the referrals_for_supervision_periods function."""

    def setUp(self) -> None:
        self.identifier = identifier.ProgramIdentifier()

    def test_referrals_for_supervision_periods(self) -> None:
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            state_code="US_XX",
            start_date=date(2008, 3, 5),
            termination_date=date(2010, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
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
            supervision_delegate=UsXxSupervisionDelegate(
                DEFAULT_SUPERVISION_LOCATIONS_TO_NAMES_ASSOCIATION_LIST
            ),
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
                    assessment_score_bucket="39+",
                    supervision_type=supervision_period.supervision_type,
                )
            ],
            program_referrals,
        )

    def test_referrals_for_supervision_periods_same_type(self) -> None:
        supervision_period_1 = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            state_code="US_XX",
            start_date=date(2008, 3, 5),
            termination_date=date(2010, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        supervision_period_2 = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            state_code="US_XX",
            start_date=date(2008, 3, 5),
            termination_date=date(2010, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
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
            supervision_delegate=UsXxSupervisionDelegate(
                DEFAULT_SUPERVISION_LOCATIONS_TO_NAMES_ASSOCIATION_LIST
            ),
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
                    assessment_score_bucket="39+",
                    supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                ),
                ProgramReferralEvent(
                    state_code="US_XX",
                    program_id="XXX",
                    event_date=date(2009, 3, 19),
                    participation_status=StateProgramAssignmentParticipationStatus.DISCHARGED,
                    assessment_score=39,
                    assessment_type=StateAssessmentType.LSIR,
                    assessment_score_bucket="39+",
                    supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                ),
            ],
            program_referrals,
        )

    def test_referrals_for_supervision_periods_different_types(self) -> None:
        supervision_period_1 = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            state_code="US_XX",
            start_date=date(2008, 3, 5),
            termination_date=date(2010, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        supervision_period_2 = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            state_code="US_XX",
            start_date=date(2008, 3, 5),
            termination_date=date(2010, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
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
            supervision_delegate=UsXxSupervisionDelegate(
                DEFAULT_SUPERVISION_LOCATIONS_TO_NAMES_ASSOCIATION_LIST
            ),
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
                    assessment_score_bucket="39+",
                    supervision_type=supervision_period_1.supervision_type,
                ),
                ProgramReferralEvent(
                    state_code="US_XX",
                    program_id="XXX",
                    event_date=date(2009, 3, 19),
                    participation_status=StateProgramAssignmentParticipationStatus.DENIED,
                    assessment_score=39,
                    assessment_type=StateAssessmentType.LSIR,
                    assessment_score_bucket="39+",
                    supervision_type=supervision_period_2.supervision_type,
                ),
            ],
            program_referrals,
        )
