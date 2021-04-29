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
"""Tests the functions in the us_nd_commitment_from_supervision_utils file."""
from datetime import date

import unittest

from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_commitment_from_supervision_utils import (
    us_nd_pre_commitment_supervision_periods_if_commitment_from_supervision,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodStatus,
    StateIncarcerationPeriodAdmissionReason,
)
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodStatus,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.entity.state.entities import (
    StateSupervisionPeriod,
    StateIncarcerationPeriod,
)


class TestPreCommitmentSupervisionPeriodsIfCommitment(unittest.TestCase):
    """Tests the state-specific us_nd_pre_commitment_supervision_periods_if_commitment function."""

    def test_revoked_supervision_periods_if_revocation_occurred_US_ND_NewAdmissionNotAfterProbation(
        self,
    ) -> None:
        """Tests that when a NEW_ADMISSION incarceration follows a PAROLE+NOT REVOCATION
        supervision period directly, then this returns False, []."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 6, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PAROLE,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ND",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            admission_date=date(2019, 6, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        supervision_periods = [supervision_period]

        (
            admission_is_revocation,
            revoked_periods,
        ) = us_nd_pre_commitment_supervision_periods_if_commitment_from_supervision(
            incarceration_period,
            supervision_periods,
        )

        self.assertFalse(admission_is_revocation)
        self.assertEqual([], revoked_periods)

    def test_revoked_supervision_periods_if_revocation_occurred_US_ND_NewAdmissionNotAfterRevocation(
        self,
    ) -> None:
        """Tests that when a NEW_ADMISSION incarceration follows a PROBATION+NOT REVOCATION
        supervision period directly, then this returns False, []."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 6, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.EXPIRATION,
            supervision_type=StateSupervisionType.PROBATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ND",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            admission_date=date(2019, 6, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        supervision_periods = [supervision_period]

        (
            admission_is_revocation,
            revoked_periods,
        ) = us_nd_pre_commitment_supervision_periods_if_commitment_from_supervision(
            incarceration_period,
            supervision_periods,
        )

        self.assertFalse(admission_is_revocation)
        self.assertEqual([], revoked_periods)

    def test_revoked_supervision_periods_if_revocation_occurred_US_ND_NewAdmissionAfterProbationRevocation(
        self,
    ) -> None:
        """Tests that when a NEW_ADMISSION incarceration follows a PROBATION+REVOCATION
        supervision period directly, then this returns True, [supervision_period]."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 6, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PROBATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ND",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            admission_date=date(2019, 6, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        supervision_periods = [supervision_period]

        (
            admission_is_revocation,
            revoked_periods,
        ) = us_nd_pre_commitment_supervision_periods_if_commitment_from_supervision(
            incarceration_period,
            supervision_periods,
        )

        self.assertTrue(admission_is_revocation)
        self.assertEqual(supervision_periods, revoked_periods)

    def test_revoked_supervision_periods_if_revocation_occurred_US_ND_NewAdmissionNotDirectlyAfterProbationRevocation(
        self,
    ) -> None:
        """Tests that when a NEW_ADMISSION incarceration follows a PROBATION+REVOCATION
        supervision period, but not directly, as there is a PAROLE+REVOCATION supervision
        period in between them, then this returns False, []."""
        earlier_probation_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 5, 4),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PROBATION,
        )

        later_parole_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 6, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PAROLE,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ND",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            admission_date=date(2019, 6, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        supervision_periods = [
            earlier_probation_supervision_period,
            later_parole_supervision_period,
        ]

        (
            admission_is_revocation,
            revoked_periods,
        ) = us_nd_pre_commitment_supervision_periods_if_commitment_from_supervision(
            incarceration_period,
            supervision_periods,
        )

        self.assertFalse(admission_is_revocation)
        self.assertEqual([], revoked_periods)

    def test_revoked_supervision_periods_if_revocation_occurred_US_ND_RevocationAdmission(
        self,
    ) -> None:
        """Tests that when a REVOCATION admission incarceration follows a PAROLE+REVOCATION
        supervision period directly, then this returns True, [supervision_period]."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 6, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PAROLE,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ND",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            admission_date=date(2019, 6, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
        )

        supervision_periods = [supervision_period]

        (
            admission_is_revocation,
            revoked_periods,
        ) = us_nd_pre_commitment_supervision_periods_if_commitment_from_supervision(
            incarceration_period,
            supervision_periods,
        )

        self.assertTrue(admission_is_revocation)
        self.assertEqual(supervision_periods, revoked_periods)

    def test_revoked_supervision_periods_if_revocation_occurred_US_ND_RevocationAdmissionNoSupervisionPeriod(
        self,
    ) -> None:
        """Tests that when a REVOCATION admission incarceration follows no supervision period,
        then this returns True, []."""
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ND",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            admission_date=date(2019, 6, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
        )

        (
            admission_is_revocation,
            revoked_periods,
        ) = us_nd_pre_commitment_supervision_periods_if_commitment_from_supervision(
            incarceration_period,
            [],
        )

        self.assertTrue(admission_is_revocation)
        self.assertEqual([], revoked_periods)
