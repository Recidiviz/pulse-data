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
"""Tests for us_nd_supervision_utils.py"""
import unittest
from datetime import date
from typing import Optional

import pytest

from recidiviz.calculator.pipeline.utils.pre_processed_incarceration_period_index import (
    PreProcessedIncarcerationPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.pre_processed_supervision_period_index import (
    PreProcessedSupervisionPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_supervision_utils import (
    us_nd_get_post_incarceration_supervision_type,
    us_nd_infer_supervision_period_admission,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateIncarcerationPeriodStatus,
)
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodStatus,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionPeriod,
)


class TestUsNdPostIncarcerationSupervisionTypeIdentification(unittest.TestCase):
    """Tests the us_nd_get_post_incarceration_supervision_type function."""

    def test_us_nd_get_post_incarceration_supervision_type_parole(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_ND",
            facility="PRISON",
            admission_date=date(2008, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            admission_reason_raw_text="Revocation",
            release_date=date(2008, 12, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        parole_raw_text_values = ["RPAR", "PARL", "PV"]

        for value in parole_raw_text_values:
            incarceration_period.release_reason_raw_text = value
            supervision_type_at_release = us_nd_get_post_incarceration_supervision_type(
                incarceration_period
            )

            self.assertEqual(
                StateSupervisionPeriodSupervisionType.PAROLE,
                supervision_type_at_release,
            )

    def test_us_nd_get_post_incarceration_supervision_type_probation(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_ND",
            facility="PRISON",
            admission_date=date(2008, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            admission_reason_raw_text="Revocation",
            release_date=date(2008, 12, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        parole_raw_text_values = ["RPRB", "NPROB", "NPRB", "PRB"]

        for value in parole_raw_text_values:
            incarceration_period.release_reason_raw_text = value
            supervision_type_at_release = us_nd_get_post_incarceration_supervision_type(
                incarceration_period
            )

            self.assertEqual(
                StateSupervisionPeriodSupervisionType.PROBATION,
                supervision_type_at_release,
            )

    def test_us_nd_get_post_incarceration_supervision_type_no_supervision(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_ND",
            facility="PRISON",
            admission_date=date(2008, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            admission_reason_raw_text="Revocation",
            release_date=date(2008, 12, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            release_reason_raw_text="X",
        )

        supervision_type_at_release = us_nd_get_post_incarceration_supervision_type(
            incarceration_period
        )

        self.assertIsNone(supervision_type_at_release)

    def test_us_nd_get_post_incarceration_supervision_type_unexpected_raw_text(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_ND",
            facility="PRISON",
            admission_date=date(2008, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            admission_reason_raw_text="Revocation",
            release_date=date(2008, 12, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text="NOT A VALID RAW TEXT VALUE",
        )

        with pytest.raises(ValueError):
            _ = us_nd_get_post_incarceration_supervision_type(incarceration_period)


class TestUsNdInferSupervisionPeriodAdmission(unittest.TestCase):
    """Tests the us_nd_supervision_period_admission function."""

    def test_us_nd_infer_supervision_period_admission_conditional_release(self) -> None:
        previous_incarceration_period: StateIncarcerationPeriod = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_ND",
            facility="PRISON",
            admission_date=date(2018, 2, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            admission_reason_raw_text="Revocation",
            release_date=date(2018, 2, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text="NOT A VALID RAW TEXT VALUE",
        )
        current_supervision_period: StateSupervisionPeriod = (
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id="sp1",
                status=StateSupervisionPeriodStatus.UNDER_SUPERVISION,
                state_code="US_XX",
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=None,
            )
        )

        supervision_period_index = PreProcessedSupervisionPeriodIndex(
            supervision_periods=[current_supervision_period]
        )

        incarceration_period_index = PreProcessedIncarcerationPeriodIndex(
            incarceration_periods=[previous_incarceration_period],
            ip_id_to_pfi_subtype={1112: None},
        )

        admission_reason: Optional[
            StateSupervisionPeriodAdmissionReason
        ] = us_nd_infer_supervision_period_admission(
            current_supervision_period,
            supervision_period_index,
            incarceration_period_index,
        )

        self.assertEqual(
            admission_reason, StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE
        )

    def test_us_nd_infer_supervision_period_admission_return_from_absconsion(
        self,
    ) -> None:
        previous_supervision_period: StateSupervisionPeriod = (
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id="sp1",
                status=StateSupervisionPeriodStatus.UNDER_SUPERVISION,
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
                status=StateSupervisionPeriodStatus.UNDER_SUPERVISION,
                state_code="US_XX",
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=None,
            )
        )

        supervision_period_index = PreProcessedSupervisionPeriodIndex(
            supervision_periods=[
                previous_supervision_period,
                current_supervision_period,
            ]
        )

        incarceration_period_index = PreProcessedIncarcerationPeriodIndex(
            incarceration_periods=[],
            ip_id_to_pfi_subtype={},
        )

        admission_reason: Optional[
            StateSupervisionPeriodAdmissionReason
        ] = us_nd_infer_supervision_period_admission(
            current_supervision_period,
            supervision_period_index,
            incarceration_period_index,
        )

        self.assertEqual(
            admission_reason,
            StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION,
        )

    def test_us_nd_infer_supervision_period_admission_internal_unknown_after_probation(
        self,
    ) -> None:
        previous_supervision_period: StateSupervisionPeriod = (
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id="sp1",
                status=StateSupervisionPeriodStatus.UNDER_SUPERVISION,
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
                status=StateSupervisionPeriodStatus.UNDER_SUPERVISION,
                state_code="US_XX",
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionType.PAROLE,
            )
        )

        supervision_period_index = PreProcessedSupervisionPeriodIndex(
            supervision_periods=[
                previous_supervision_period,
                current_supervision_period,
            ]
        )

        incarceration_period_index = PreProcessedIncarcerationPeriodIndex(
            incarceration_periods=[],
            ip_id_to_pfi_subtype={},
        )

        admission_reason: Optional[
            StateSupervisionPeriodAdmissionReason
        ] = us_nd_infer_supervision_period_admission(
            current_supervision_period,
            supervision_period_index,
            incarceration_period_index,
        )

        self.assertEqual(
            admission_reason,
            StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
        )

    def test_us_nd_infer_supervision_period_admission_court_sentence_after_parole(
        self,
    ) -> None:
        previous_supervision_period: StateSupervisionPeriod = (
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id="sp1",
                status=StateSupervisionPeriodStatus.UNDER_SUPERVISION,
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
                status=StateSupervisionPeriodStatus.UNDER_SUPERVISION,
                state_code="US_XX",
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionType.PROBATION,
            )
        )

        supervision_period_index = PreProcessedSupervisionPeriodIndex(
            supervision_periods=[
                previous_supervision_period,
                current_supervision_period,
            ]
        )

        incarceration_period_index = PreProcessedIncarcerationPeriodIndex(
            incarceration_periods=[],
            ip_id_to_pfi_subtype={},
        )

        admission_reason: Optional[
            StateSupervisionPeriodAdmissionReason
        ] = us_nd_infer_supervision_period_admission(
            current_supervision_period,
            supervision_period_index,
            incarceration_period_index,
        )

        self.assertEqual(
            admission_reason,
            StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
        )

    def test_us_nd_infer_supervision_period_admission_no_previous_preiod_parole(
        self,
    ) -> None:
        current_supervision_period: StateSupervisionPeriod = (
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id="sp1",
                status=StateSupervisionPeriodStatus.UNDER_SUPERVISION,
                state_code="US_XX",
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionType.PAROLE,
            )
        )

        supervision_period_index = PreProcessedSupervisionPeriodIndex(
            supervision_periods=[
                current_supervision_period,
            ]
        )

        incarceration_period_index = PreProcessedIncarcerationPeriodIndex(
            incarceration_periods=[],
            ip_id_to_pfi_subtype={},
        )

        admission_reason: Optional[
            StateSupervisionPeriodAdmissionReason
        ] = us_nd_infer_supervision_period_admission(
            current_supervision_period,
            supervision_period_index,
            incarceration_period_index,
        )

        self.assertEqual(
            admission_reason,
            StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
        )

    def test_us_nd_infer_supervision_period_admission_no_previous_preiod_probation(
        self,
    ) -> None:
        current_supervision_period: StateSupervisionPeriod = (
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id="sp1",
                status=StateSupervisionPeriodStatus.UNDER_SUPERVISION,
                state_code="US_XX",
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionType.PROBATION,
            )
        )

        supervision_period_index = PreProcessedSupervisionPeriodIndex(
            supervision_periods=[
                current_supervision_period,
            ]
        )

        incarceration_period_index = PreProcessedIncarcerationPeriodIndex(
            incarceration_periods=[],
            ip_id_to_pfi_subtype={},
        )

        admission_reason: Optional[
            StateSupervisionPeriodAdmissionReason
        ] = us_nd_infer_supervision_period_admission(
            current_supervision_period,
            supervision_period_index,
            incarceration_period_index,
        )

        self.assertEqual(
            admission_reason,
            StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
        )

    def test_us_nd_infer_supervision_period_admission_no_previous_period_parole(
        self,
    ) -> None:
        current_supervision_period: StateSupervisionPeriod = (
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id="sp1",
                status=StateSupervisionPeriodStatus.UNDER_SUPERVISION,
                state_code="US_XX",
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionType.PAROLE,
            )
        )

        supervision_period_index = PreProcessedSupervisionPeriodIndex(
            supervision_periods=[
                current_supervision_period,
            ]
        )

        incarceration_period_index = PreProcessedIncarcerationPeriodIndex(
            incarceration_periods=[],
            ip_id_to_pfi_subtype={},
        )

        admission_reason: Optional[
            StateSupervisionPeriodAdmissionReason
        ] = us_nd_infer_supervision_period_admission(
            current_supervision_period,
            supervision_period_index,
            incarceration_period_index,
        )

        self.assertEqual(
            admission_reason,
            StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
        )

    def test_us_nd_infer_supervision_period_admission_change_supervising_officer(
        self,
    ) -> None:
        previous_supervision_period: StateSupervisionPeriod = (
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id="sp1",
                status=StateSupervisionPeriodStatus.UNDER_SUPERVISION,
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
                status=StateSupervisionPeriodStatus.UNDER_SUPERVISION,
                state_code="US_XX",
                supervising_officer="AGENTY",
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionType.PROBATION,
            )
        )

        supervision_period_index = PreProcessedSupervisionPeriodIndex(
            supervision_periods=[
                previous_supervision_period,
                current_supervision_period,
            ]
        )

        incarceration_period_index = PreProcessedIncarcerationPeriodIndex(
            incarceration_periods=[],
            ip_id_to_pfi_subtype={},
        )

        admission_reason: Optional[
            StateSupervisionPeriodAdmissionReason
        ] = us_nd_infer_supervision_period_admission(
            current_supervision_period,
            supervision_period_index,
            incarceration_period_index,
        )

        self.assertEqual(
            admission_reason,
            StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
        )

    def test_us_nd_infer_supervision_period_admission_halfway_house_to_parole(
        self,
    ) -> None:
        previous_supervision_period: StateSupervisionPeriod = (
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id="sp1",
                status=StateSupervisionPeriodStatus.UNDER_SUPERVISION,
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
                status=StateSupervisionPeriodStatus.UNDER_SUPERVISION,
                state_code="US_XX",
                supervising_officer="AGENTY",
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionType.PAROLE,
            )
        )

        supervision_period_index = PreProcessedSupervisionPeriodIndex(
            supervision_periods=[
                previous_supervision_period,
                current_supervision_period,
            ]
        )

        incarceration_period_index = PreProcessedIncarcerationPeriodIndex(
            incarceration_periods=[],
            ip_id_to_pfi_subtype={},
        )

        admission_reason: Optional[
            StateSupervisionPeriodAdmissionReason
        ] = us_nd_infer_supervision_period_admission(
            current_supervision_period,
            supervision_period_index,
            incarceration_period_index,
        )

        self.assertEqual(
            admission_reason,
            StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
        )

    def test_us_nd_infer_supervision_period_previous_period_termination_reason_revocation(
        self,
    ) -> None:
        previous_supervision_period: StateSupervisionPeriod = (
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id="sp1",
                status=StateSupervisionPeriodStatus.UNDER_SUPERVISION,
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
                status=StateSupervisionPeriodStatus.UNDER_SUPERVISION,
                state_code="US_XX",
                supervising_officer="AGENTY",
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionType.PAROLE,
            )
        )

        supervision_period_index = PreProcessedSupervisionPeriodIndex(
            supervision_periods=[
                previous_supervision_period,
                current_supervision_period,
            ]
        )

        incarceration_period_index = PreProcessedIncarcerationPeriodIndex(
            incarceration_periods=[],
            ip_id_to_pfi_subtype={},
        )

        admission_reason: Optional[
            StateSupervisionPeriodAdmissionReason
        ] = us_nd_infer_supervision_period_admission(
            current_supervision_period,
            supervision_period_index,
            incarceration_period_index,
        )

        self.assertEqual(
            admission_reason,
            StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
        )
