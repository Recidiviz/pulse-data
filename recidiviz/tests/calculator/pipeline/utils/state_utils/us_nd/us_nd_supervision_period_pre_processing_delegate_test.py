# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests for the us_nd_supervision_period_pre_processing_delegate.py file"""
import unittest
from datetime import date
from typing import Optional

from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_supervision_pre_processing_delegate import (
    UsNdSupervisionPreProcessingDelegate,
)
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod


class TestUsNdSupervisionPeriodPreProcessingDelegate(unittest.TestCase):
    """Unit tests for UsNdSupervisionPreProcessingDelegate"""

    def setUp(self) -> None:
        self.delegate = UsNdSupervisionPreProcessingDelegate()

    def test_supervision_admission_reason_override_conditional_release(self) -> None:
        current_supervision_period: StateSupervisionPeriod = (
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id="sp1",
                state_code="US_XX",
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionType.PAROLE,
            )
        )

        admission_reason: Optional[
            StateSupervisionPeriodAdmissionReason
        ] = self.delegate.supervision_admission_reason_override(
            supervision_period=current_supervision_period,
            supervision_periods=[current_supervision_period],
        )

        self.assertEqual(
            admission_reason, StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE
        )

    def test_supervision_admission_reason_override_return_from_absconsion(
        self,
    ) -> None:
        previous_supervision_period: StateSupervisionPeriod = (
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id="sp1",
                state_code="US_XX",
                start_date=date(2018, 2, 20),
                termination_date=date(2018, 2, 22),
                termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
                supervision_type=None,
            )
        )
        current_supervision_period: StateSupervisionPeriod = (
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id="sp1",
                state_code="US_XX",
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=None,
            )
        )

        admission_reason: Optional[
            StateSupervisionPeriodAdmissionReason
        ] = self.delegate.supervision_admission_reason_override(
            current_supervision_period,
            supervision_periods=[
                previous_supervision_period,
                current_supervision_period,
            ],
        )

        self.assertEqual(
            admission_reason,
            StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION,
        )

    def test_supervision_admission_reason_override_internal_unknown_after_probation(
        self,
    ) -> None:
        previous_supervision_period: StateSupervisionPeriod = (
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id="sp1",
                state_code="US_XX",
                start_date=date(2018, 2, 20),
                termination_date=date(2018, 2, 22),
                termination_reason=None,
                supervision_type=StateSupervisionType.PROBATION,
            )
        )
        current_supervision_period: StateSupervisionPeriod = (
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id="sp1",
                state_code="US_XX",
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionType.PAROLE,
            )
        )

        admission_reason: Optional[
            StateSupervisionPeriodAdmissionReason
        ] = self.delegate.supervision_admission_reason_override(
            current_supervision_period,
            supervision_periods=[
                previous_supervision_period,
                current_supervision_period,
            ],
        )

        self.assertEqual(
            admission_reason,
            StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
        )

    def test_supervision_admission_reason_override_court_sentence_after_parole(
        self,
    ) -> None:
        previous_supervision_period: StateSupervisionPeriod = (
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id="sp1",
                state_code="US_XX",
                start_date=date(2018, 2, 20),
                termination_date=date(2018, 2, 22),
                termination_reason=None,
                supervision_type=StateSupervisionType.PAROLE,
            )
        )
        current_supervision_period: StateSupervisionPeriod = (
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id="sp1",
                state_code="US_XX",
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionType.PROBATION,
            )
        )

        admission_reason: Optional[
            StateSupervisionPeriodAdmissionReason
        ] = self.delegate.supervision_admission_reason_override(
            current_supervision_period,
            supervision_periods=[
                previous_supervision_period,
                current_supervision_period,
            ],
        )

        self.assertEqual(
            admission_reason,
            StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
        )

    def test_supervision_admission_reason_override_no_previous_period_parole(
        self,
    ) -> None:
        current_supervision_period: StateSupervisionPeriod = (
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id="sp1",
                state_code="US_XX",
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionType.PAROLE,
            )
        )

        admission_reason: Optional[
            StateSupervisionPeriodAdmissionReason
        ] = self.delegate.supervision_admission_reason_override(
            current_supervision_period,
            supervision_periods=[
                current_supervision_period,
            ],
        )

        self.assertEqual(
            admission_reason,
            StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
        )

    def test_supervision_admission_reason_override_no_previous_period_probation(
        self,
    ) -> None:
        current_supervision_period: StateSupervisionPeriod = (
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id="sp1",
                state_code="US_XX",
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionType.PROBATION,
            )
        )

        admission_reason: Optional[
            StateSupervisionPeriodAdmissionReason
        ] = self.delegate.supervision_admission_reason_override(
            current_supervision_period,
            supervision_periods=[
                current_supervision_period,
            ],
        )

        self.assertEqual(
            admission_reason,
            StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
        )

    def test_supervision_admission_reason_override_change_supervising_officer(
        self,
    ) -> None:
        previous_supervision_period: StateSupervisionPeriod = (
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id="sp1",
                state_code="US_XX",
                supervising_officer="AGENTX",
                start_date=date(2018, 2, 20),
                termination_date=date(2018, 2, 22),
                termination_reason=None,
                supervision_type=StateSupervisionType.PROBATION,
            )
        )
        current_supervision_period: StateSupervisionPeriod = (
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id="sp1",
                state_code="US_XX",
                supervising_officer="AGENTY",
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionType.PROBATION,
            )
        )

        admission_reason: Optional[
            StateSupervisionPeriodAdmissionReason
        ] = self.delegate.supervision_admission_reason_override(
            current_supervision_period,
            supervision_periods=[
                previous_supervision_period,
                current_supervision_period,
            ],
        )

        self.assertEqual(
            admission_reason,
            StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
        )

    def test_supervision_admission_reason_override_halfway_house_to_parole(
        self,
    ) -> None:
        previous_supervision_period: StateSupervisionPeriod = (
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id="sp1",
                state_code="US_XX",
                supervising_officer="AGENTX",
                start_date=date(2018, 2, 20),
                termination_date=date(2018, 2, 22),
                termination_reason=None,
                supervision_type=StateSupervisionType.HALFWAY_HOUSE,
            )
        )
        current_supervision_period: StateSupervisionPeriod = (
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id="sp1",
                state_code="US_XX",
                supervising_officer="AGENTY",
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionType.PAROLE,
            )
        )

        admission_reason: Optional[
            StateSupervisionPeriodAdmissionReason
        ] = self.delegate.supervision_admission_reason_override(
            current_supervision_period,
            supervision_periods=[
                previous_supervision_period,
                current_supervision_period,
            ],
        )

        self.assertEqual(
            admission_reason,
            StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
        )

    def test_supervision_admission_reason_override_period_previous_period_termination_reason_revocation(
        self,
    ) -> None:
        previous_supervision_period: StateSupervisionPeriod = (
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id="sp1",
                state_code="US_XX",
                supervising_officer="AGENTX",
                start_date=date(2018, 2, 20),
                termination_date=date(2018, 2, 22),
                termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
                supervision_type=StateSupervisionType.PAROLE,
            )
        )
        current_supervision_period: StateSupervisionPeriod = (
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id="sp1",
                state_code="US_XX",
                supervising_officer="AGENTY",
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionType.PAROLE,
            )
        )

        admission_reason: Optional[
            StateSupervisionPeriodAdmissionReason
        ] = self.delegate.supervision_admission_reason_override(
            current_supervision_period,
            supervision_periods=[
                previous_supervision_period,
                current_supervision_period,
            ],
        )

        self.assertEqual(
            admission_reason,
            StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
        )
