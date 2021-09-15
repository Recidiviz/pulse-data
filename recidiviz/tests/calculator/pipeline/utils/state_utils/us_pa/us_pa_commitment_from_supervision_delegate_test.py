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
"""Tests the functions in us_pa_commitment_from_supervision_utils.py"""

import unittest
from datetime import date
from typing import Optional

from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_commitment_from_supervision_delegate import (
    UsPaCommitmentFromSupervisionDelegate,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateIncarcerationPeriodStatus,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionPeriod,
)

STATE_CODE = "US_PA"


class TestPreCommitmentSupervisionTypeIdentification(unittest.TestCase):
    """Tests the get_commitment_from_supervision_supervision_type function on the
    UsPaCommitmentFromSupervisionDelegate."""

    def setUp(self) -> None:
        self.delegate = UsPaCommitmentFromSupervisionDelegate()

    def _test_get_commitment_from_supervision_supervision_type(
        self,
        incarceration_period: StateIncarcerationPeriod,
        previous_supervision_period: Optional[StateSupervisionPeriod] = None,
    ) -> Optional[StateSupervisionPeriodSupervisionType]:
        return self.delegate.get_commitment_from_supervision_supervision_type(
            incarceration_sentences=[],
            supervision_sentences=[],
            incarceration_period=incarceration_period,
            previous_supervision_period=previous_supervision_period,
        )

    def test_us_pa_get_pre_commitment_supervision_type_default(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_PA",
            admission_date=date(2008, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            admission_reason_raw_text="ADMN",
            release_date=date(2010, 12, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        supervision_type_pre_commitment = (
            self._test_get_commitment_from_supervision_supervision_type(
                incarceration_period=incarceration_period
            )
        )

        self.assertEqual(
            StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_type_pre_commitment,
        )

    def test_us_pa_get_pre_commitment_supervision_type_probation_revocation(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            start_date=date(2008, 3, 5),
            termination_date=date(2008, 12, 16),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_PA",
            admission_date=date(2008, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            admission_reason_raw_text="ADMN",
            release_date=date(2010, 12, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        supervision_type_pre_commitment = (
            self._test_get_commitment_from_supervision_supervision_type(
                incarceration_period=incarceration_period,
                previous_supervision_period=supervision_period,
            )
        )

        self.assertEqual(
            StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_type_pre_commitment,
        )

    def test_us_pa_get_pre_commitment_supervision_type_sanction_admission_dual(
        self,
    ) -> None:
        """Right now, all sanction admissions are assumed to be from parole."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            start_date=date(2008, 3, 5),
            termination_date=date(2008, 12, 16),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_PA",
            admission_date=date(2008, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
            admission_reason_raw_text="ADMN",
            release_date=date(2010, 12, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        supervision_type_pre_commitment = (
            self._test_get_commitment_from_supervision_supervision_type(
                incarceration_period=incarceration_period,
                previous_supervision_period=supervision_period,
            )
        )

        self.assertEqual(
            StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_type_pre_commitment,
        )

    def test_us_pa_get_pre_commitment_supervision_type_erroneous(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_PA",
            admission_date=date(2008, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="ADMN",
            release_date=date(2010, 12, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        with self.assertRaises(ValueError):
            _ = self._test_get_commitment_from_supervision_supervision_type(
                incarceration_period=incarceration_period,
            )
