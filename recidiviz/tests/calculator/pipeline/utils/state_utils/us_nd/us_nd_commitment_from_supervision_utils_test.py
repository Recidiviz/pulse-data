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

from recidiviz.calculator.pipeline.utils.incarceration_period_index import (
    IncarcerationPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_commitment_from_supervision_utils import (
    us_nd_pre_commitment_supervision_period_if_commitment,
    us_nd_violation_history_window_pre_commitment_from_supervision,
    _us_nd_pre_commitment_supervision_period,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodStatus,
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
)
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodStatus,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.date import DateRange
from recidiviz.persistence.entity.state.entities import (
    StateSupervisionPeriod,
    StateIncarcerationPeriod,
)


class TestPreCommitmentSupervisionPeriodsIfCommitment(unittest.TestCase):
    """Tests the state-specific us_nd_pre_commitment_supervision_period_if_commitment function."""

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
        ip_index = IncarcerationPeriodIndex([incarceration_period])

        (
            admission_is_revocation,
            pre_commitment_period,
        ) = us_nd_pre_commitment_supervision_period_if_commitment(
            incarceration_period,
            supervision_periods,
            ip_index,
        )

        self.assertFalse(admission_is_revocation)
        self.assertEqual(None, pre_commitment_period)

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
        ip_index = IncarcerationPeriodIndex([incarceration_period])

        (
            admission_is_revocation,
            pre_commitment_period,
        ) = us_nd_pre_commitment_supervision_period_if_commitment(
            incarceration_period,
            supervision_periods,
            ip_index,
        )

        self.assertFalse(admission_is_revocation)
        self.assertEqual(None, pre_commitment_period)

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
        ip_index = IncarcerationPeriodIndex([incarceration_period])

        (
            admission_is_revocation,
            pre_commitment_period,
        ) = us_nd_pre_commitment_supervision_period_if_commitment(
            incarceration_period,
            supervision_periods,
            ip_index,
        )

        self.assertTrue(admission_is_revocation)
        self.assertEqual(supervision_period, pre_commitment_period)

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
        ip_index = IncarcerationPeriodIndex([incarceration_period])

        (
            admission_is_revocation,
            pre_commitment_period,
        ) = us_nd_pre_commitment_supervision_period_if_commitment(
            incarceration_period,
            supervision_periods,
            ip_index,
        )

        self.assertFalse(admission_is_revocation)
        self.assertEqual(None, pre_commitment_period)

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
        ip_index = IncarcerationPeriodIndex([incarceration_period])

        (
            admission_is_revocation,
            pre_commitment_period,
        ) = us_nd_pre_commitment_supervision_period_if_commitment(
            incarceration_period,
            supervision_periods,
            ip_index,
        )

        self.assertTrue(admission_is_revocation)
        self.assertEqual(supervision_period, pre_commitment_period)

    def test_revoked_supervision_periods_if_revocation_occurred_US_ND_RevocationAdmissionNoSupervisionPeriod(
        self,
    ) -> None:
        """Tests that when a REVOCATION admission incarceration follows no supervision period,
        then this returns True, None."""
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ND",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            admission_date=date(2019, 6, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
        )

        ip_index = IncarcerationPeriodIndex([incarceration_period])

        (
            admission_is_revocation,
            pre_commitment_period,
        ) = us_nd_pre_commitment_supervision_period_if_commitment(
            incarceration_period,
            [],
            ip_index,
        )

        self.assertTrue(admission_is_revocation)
        self.assertEqual(None, pre_commitment_period)

    def test_revoked_supervision_periods_if_revocation_occurred_US_ND_NewAdmissionAfterRevocationAndIntermediateAdmissionToStatePrison(
        self,
    ) -> None:
        """Tests that when a NEW_ADMISSION incarceration follows a PROBATION+REVOCATION
        supervision period, but not directly, as there is a separate incarceration
        admission in the interim, then this returns False, []."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(1996, 3, 5),
            termination_date=date(2000, 1, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PROBATION,
        )

        initial_commitment_incarceration_period = (
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=222,
                external_id="ip2",
                state_code="US_ND",
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                admission_date=date(2000, 2, 9),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=date(2000, 12, 24),
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            )
        )

        subsequent_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ND",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            admission_date=date(2001, 10, 14),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        supervision_periods = [supervision_period]
        ip_index = IncarcerationPeriodIndex(
            [initial_commitment_incarceration_period, subsequent_incarceration_period]
        )

        (
            admission_is_revocation,
            pre_commitment_period,
        ) = us_nd_pre_commitment_supervision_period_if_commitment(
            subsequent_incarceration_period, supervision_periods, ip_index
        )

        self.assertFalse(admission_is_revocation)
        self.assertIsNone(pre_commitment_period)

    def test_revoked_supervision_periods_if_revocation_occurred_US_ND_NewAdmissionAfterRevocationAndIntermediateAdmissionToCountyJail(
        self,
    ) -> None:
        """Tests that when a NEW_ADMISSION incarceration follows a PROBATION+REVOCATION
        supervision period, but not directly, as there is a separate incarceration
        admission in the interim, but that admission is to a county jail (i.e. is a
        temporary hold while revocation proceedings take place, then this returns
        True, [supervision_period]."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(1996, 3, 5),
            termination_date=date(2000, 1, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PROBATION,
        )

        initial_commitment_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ND",
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2000, 1, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2000, 4, 24),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        subsequent_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ND",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            admission_date=date(2000, 4, 24),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        supervision_periods = [supervision_period]
        ip_index = IncarcerationPeriodIndex(
            [initial_commitment_incarceration_period, subsequent_incarceration_period]
        )

        (
            admission_is_revocation,
            pre_commitment_period,
        ) = us_nd_pre_commitment_supervision_period_if_commitment(
            subsequent_incarceration_period, supervision_periods, ip_index
        )

        self.assertTrue(admission_is_revocation)
        self.assertEqual(supervision_period, pre_commitment_period)


class TestViolationHistoryWindowPreCommitment(unittest.TestCase):
    """Tests the us_nd_violation_history_window_pre_commitment_from_supervision
    function."""

    def test_us_nd_violation_history_window_pre_commitment_from_supervision(
        self,
    ):
        violation_window = (
            us_nd_violation_history_window_pre_commitment_from_supervision(
                admission_date=date(2000, 1, 1),
            )
        )

        expected_violation_window = DateRange(
            # 90 days before
            lower_bound_inclusive_date=date(1999, 10, 3),
            # 90 days, including admission_date
            upper_bound_exclusive_date=date(2000, 3, 31),
        )

        self.assertEqual(expected_violation_window, violation_window)


class TestPreCommitmentSupervisionPeriod(unittest.TestCase):
    """Tests the _us_nd_pre_commitment_supervision_period function."""

    def test_us_nd_pre_commitment_supervision_period_parole_revocation(self):
        """Tests that we prioritize the period with the supervision_type that matches
        the admission reason supervision type."""
        admission_date = date(2019, 5, 25)
        admission_reason = StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION

        # Overlapping parole period
        parole_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 6, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PAROLE,
        )

        # Overlapping probation period
        probation_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 6, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PROBATION,
        )

        pre_commitment_supervision_period = _us_nd_pre_commitment_supervision_period(
            admission_date,
            admission_reason,
            supervision_periods=[probation_period, parole_period],
        )

        self.assertEqual(parole_period, pre_commitment_supervision_period)

    def test_us_nd_pre_commitment_supervision_period_parole_revocation_overlap(self):
        """Tests that we prioritize the overlapping parole period over the one that
        was recently terminated because the admission is a PAROLE_REVOCATION."""
        admission_date = date(2019, 5, 25)
        admission_reason = StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION

        # Overlapping parole period
        overlapping_parole_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 6, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PAROLE,
        )

        # Terminated parole period
        terminated_parole_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 5, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PAROLE,
        )

        pre_commitment_supervision_period = _us_nd_pre_commitment_supervision_period(
            admission_date,
            admission_reason,
            supervision_periods=[terminated_parole_period, overlapping_parole_period],
        )

        self.assertEqual(overlapping_parole_period, pre_commitment_supervision_period)

    def test_us_nd_pre_commitment_supervision_period_parole_revocation_rev_term(self):
        """Tests that we prioritize the overlapping parole period with a termination
        reason of REVOCATION."""
        admission_date = date(2019, 5, 25)
        admission_reason = StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION

        # Overlapping revoked parole period
        revoked_parole_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 6, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PAROLE,
        )

        # Overlapping parole period
        expired_parole_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 6, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.EXPIRATION,
            supervision_type=StateSupervisionType.PAROLE,
        )

        pre_commitment_supervision_period = _us_nd_pre_commitment_supervision_period(
            admission_date,
            admission_reason,
            supervision_periods=[expired_parole_period, revoked_parole_period],
        )

        self.assertEqual(revoked_parole_period, pre_commitment_supervision_period)

    def test_us_nd_pre_commitment_supervision_period_parole_revocation_closer(self):
        """Tests that we prioritize the overlapping parole period with a termination
        reason of REVOCATION."""
        admission_date = date(2019, 5, 25)
        admission_reason = StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION

        # Overlapping revoked parole period, 5 days after admission
        revoked_parole_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 6, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PAROLE,
        )

        # Overlapping revoked parole period, 1 day after admission
        closer_revoked_parole_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 5, 26),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PAROLE,
        )

        pre_commitment_supervision_period = _us_nd_pre_commitment_supervision_period(
            admission_date,
            admission_reason,
            supervision_periods=[closer_revoked_parole_period, revoked_parole_period],
        )

        self.assertEqual(
            closer_revoked_parole_period, pre_commitment_supervision_period
        )

    def test_us_nd_pre_commitment_supervision_period_probation_revocation(self):
        """Tests that we prioritize the period with the supervision_type that matches
        the admission reason supervision type."""
        admission_date = date(2019, 5, 25)
        admission_reason = StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION

        # Overlapping parole period
        parole_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 6, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PAROLE,
        )

        # Overlapping probation period
        probation_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 6, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PROBATION,
        )

        pre_commitment_supervision_period = _us_nd_pre_commitment_supervision_period(
            admission_date,
            admission_reason,
            supervision_periods=[probation_period, parole_period],
        )

        self.assertEqual(probation_period, pre_commitment_supervision_period)

    def test_us_nd_pre_commitment_supervision_period_probation_revocation_overlap(self):
        """Tests that we prioritize the recently terminated probation period over the
        one that is overlapping because the admission is a PROBATION_REVOCATION."""
        admission_date = date(2019, 5, 25)
        admission_reason = StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION

        # Overlapping probation period
        overlapping_probation_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 6, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PROBATION,
        )

        # Terminated probation period
        terminated_probation_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 5, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PROBATION,
        )

        pre_commitment_supervision_period = _us_nd_pre_commitment_supervision_period(
            admission_date,
            admission_reason,
            supervision_periods=[
                terminated_probation_period,
                overlapping_probation_period,
            ],
        )

        self.assertEqual(terminated_probation_period, pre_commitment_supervision_period)

    def test_us_nd_pre_commitment_supervision_period_probation_revocation_rev_term(
        self,
    ):
        """Tests that we prioritize the probation period with a termination
        reason of REVOCATION."""
        admission_date = date(2019, 5, 25)
        admission_reason = StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION

        # Terminated revoked probation period
        revoked_probation_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 5, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PROBATION,
        )

        # Expired terminated probation period
        expired_probation_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 5, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.EXPIRATION,
            supervision_type=StateSupervisionType.PROBATION,
        )

        pre_commitment_supervision_period = _us_nd_pre_commitment_supervision_period(
            admission_date,
            admission_reason,
            supervision_periods=[expired_probation_period, revoked_probation_period],
        )

        self.assertEqual(revoked_probation_period, pre_commitment_supervision_period)

    def test_us_nd_pre_commitment_supervision_period_probation_revocation_closer(
        self,
    ):
        """Tests that we prioritize the overlapping probation period with a termination
        date that is closer to the admission_date."""
        admission_date = date(2019, 5, 25)
        admission_reason = StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION

        # Overlapping revoked probation period, 5 days after admission
        revoked_probation_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 6, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PROBATION,
        )

        # Overlapping revoked probation period, 1 day after admission
        closer_revoked_probation_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 5, 26),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PROBATION,
        )

        pre_commitment_supervision_period = _us_nd_pre_commitment_supervision_period(
            admission_date,
            admission_reason,
            supervision_periods=[
                closer_revoked_probation_period,
                revoked_probation_period,
            ],
        )

        self.assertEqual(
            closer_revoked_probation_period, pre_commitment_supervision_period
        )

    def test_us_nd_pre_commitment_supervision_period_no_periods(
        self,
    ):
        """Tests the situation where the person has no supervision periods."""
        admission_date = date(2019, 5, 25)
        admission_reason = StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION

        pre_commitment_supervision_period = _us_nd_pre_commitment_supervision_period(
            admission_date,
            admission_reason,
            supervision_periods=[],
        )

        self.assertIsNone(pre_commitment_supervision_period)
