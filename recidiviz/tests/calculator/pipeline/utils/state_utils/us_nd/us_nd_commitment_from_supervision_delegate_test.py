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
"""Tests the functions in the us_nd_commitment_from_supervision_delegate file."""
import unittest
from datetime import date
from typing import List, Optional

from recidiviz.calculator.pipeline.utils.commitment_from_supervision_utils import (
    _get_commitment_from_supervision_supervision_period,
)
from recidiviz.calculator.pipeline.utils.pre_processed_incarceration_period_index import (
    PreProcessedIncarcerationPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.pre_processed_supervision_period_index import (
    PreProcessedSupervisionPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_commitment_from_supervision_delegate import (
    UsNdCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_incarceration_delegate import (
    UsNdIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_incarceration_period_pre_processing_delegate import (
    PAROLE_REVOCATION_PREPROCESSING_PREFIX,
    PROBATION_REVOCATION_PREPROCESSING_PREFIX,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateIncarcerationPeriodStatus,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.date import DateRange
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionPeriod,
)


class TestViolationHistoryWindowPreCommitment(unittest.TestCase):
    """Tests the violation_history_window_pre_commitment_from_supervision
    function on the UsNdCommitmentFromSupervisionDelegate."""

    def test_us_nd_violation_history_window_pre_commitment_from_supervision(
        self,
    ) -> None:
        violation_window = UsNdCommitmentFromSupervisionDelegate().violation_history_window_pre_commitment_from_supervision(
            admission_date=date(2000, 1, 1),
            sorted_and_filtered_violation_responses=[],
            default_violation_history_window_months=0,
        )

        expected_violation_window = DateRange(
            # 90 days before
            lower_bound_inclusive_date=date(1999, 10, 3),
            # 90 days, including admission_date
            upper_bound_exclusive_date=date(2000, 3, 31),
        )

        self.assertEqual(expected_violation_window, violation_window)


class TestPreCommitmentSupervisionPeriod(unittest.TestCase):
    """Tests the _get_commitment_from_supervision_supervision_period function when
    the UsNdCommitmentFromSupervisionDelegate is provided."""

    @staticmethod
    def _test_us_nd_pre_commitment_supervision_period(
        admission_date: date,
        admission_reason: StateIncarcerationPeriodAdmissionReason,
        admission_reason_raw_text: str,
        supervision_periods: List[StateSupervisionPeriod],
    ) -> Optional[StateSupervisionPeriod]:
        ip = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_ND",
            incarceration_period_id=111,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            admission_date=admission_date,
            admission_reason=admission_reason,
            admission_reason_raw_text=admission_reason_raw_text,
        )

        incarceration_periods = [ip]

        return _get_commitment_from_supervision_supervision_period(
            incarceration_period=ip,
            commitment_from_supervision_delegate=UsNdCommitmentFromSupervisionDelegate(),
            supervision_period_index=PreProcessedSupervisionPeriodIndex(
                supervision_periods
            ),
            incarceration_period_index=PreProcessedIncarcerationPeriodIndex(
                incarceration_periods=incarceration_periods,
                ip_id_to_pfi_subtype={
                    ip.incarceration_period_id: None
                    for ip in incarceration_periods
                    if ip.incarceration_period_id
                },
                incarceration_delegate=UsNdIncarcerationDelegate(),
            ),
        )

    def test_us_nd_pre_commitment_supervision_period_parole_revocation(self) -> None:
        """Tests that we prioritize the period with the supervision_type that matches
        the admission reason supervision type."""
        admission_date = date(2019, 5, 25)
        admission_reason = StateIncarcerationPeriodAdmissionReason.REVOCATION
        admission_reason_raw_text = "PV"

        # Overlapping parole period
        parole_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 6, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        # Overlapping probation period
        probation_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 6, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        pre_commitment_supervision_period = (
            self._test_us_nd_pre_commitment_supervision_period(
                admission_date=admission_date,
                admission_reason=admission_reason,
                admission_reason_raw_text=admission_reason_raw_text,
                supervision_periods=[probation_period, parole_period],
            )
        )

        self.assertEqual(parole_period, pre_commitment_supervision_period)

    def test_us_nd_pre_commitment_supervision_period_parole_revocation_overlap(
        self,
    ) -> None:
        """Tests that we prioritize the overlapping parole period over the one that
        was recently terminated because the admission reason raw text is associated with a parole revocation."""
        admission_date = date(2019, 5, 25)
        admission_reason = StateIncarcerationPeriodAdmissionReason.REVOCATION
        admission_reason_raw_text = "PV"

        # Overlapping parole period
        overlapping_parole_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 6, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        # Terminated parole period.
        terminated_parole_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 5, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        pre_commitment_supervision_period = (
            self._test_us_nd_pre_commitment_supervision_period(
                admission_date=admission_date,
                admission_reason=admission_reason,
                admission_reason_raw_text=admission_reason_raw_text,
                supervision_periods=[
                    terminated_parole_period,
                    overlapping_parole_period,
                ],
            )
        )

        self.assertEqual(overlapping_parole_period, pre_commitment_supervision_period)

    def test_us_nd_pre_commitment_supervision_period_parole_revocation_rev_term(
        self,
    ) -> None:
        """Tests that we prioritize the overlapping parole period with a termination
        reason of REVOCATION."""
        admission_date = date(2019, 5, 25)
        admission_reason = StateIncarcerationPeriodAdmissionReason.REVOCATION
        admission_reason_raw_text = "PV"

        # Overlapping revoked parole period
        revoked_parole_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 6, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        # Overlapping parole period
        expired_parole_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 6, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.EXPIRATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        pre_commitment_supervision_period = (
            self._test_us_nd_pre_commitment_supervision_period(
                admission_date=admission_date,
                admission_reason=admission_reason,
                admission_reason_raw_text=admission_reason_raw_text,
                supervision_periods=[expired_parole_period, revoked_parole_period],
            )
        )

        self.assertEqual(revoked_parole_period, pre_commitment_supervision_period)

    def test_us_nd_pre_commitment_supervision_period_parole_revocation_closer(
        self,
    ) -> None:
        """Tests that we prioritize the overlapping parole period with a termination
        reason of REVOCATION."""
        admission_date = date(2019, 5, 25)
        admission_reason = StateIncarcerationPeriodAdmissionReason.REVOCATION
        admission_reason_raw_text = "PV"

        # Overlapping revoked parole period, 5 days after admission
        revoked_parole_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 6, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        # Overlapping revoked parole period, 1 day after admission
        closer_revoked_parole_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 5, 26),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        pre_commitment_supervision_period = (
            self._test_us_nd_pre_commitment_supervision_period(
                admission_date=admission_date,
                admission_reason=admission_reason,
                admission_reason_raw_text=admission_reason_raw_text,
                supervision_periods=[
                    closer_revoked_parole_period,
                    revoked_parole_period,
                ],
            )
        )

        self.assertEqual(
            closer_revoked_parole_period, pre_commitment_supervision_period
        )

    def test_us_nd_pre_commitment_supervision_period_probation_revocation(self) -> None:
        """Tests that we prioritize the period with the supervision_type that matches
        the admission reason supervision type."""
        admission_date = date(2019, 5, 25)
        admission_reason = StateIncarcerationPeriodAdmissionReason.REVOCATION
        admission_reason_raw_text = "NPROB"

        # Overlapping parole period
        parole_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 6, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        # Overlapping probation period
        probation_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 6, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        pre_commitment_supervision_period = (
            self._test_us_nd_pre_commitment_supervision_period(
                admission_date=admission_date,
                admission_reason=admission_reason,
                admission_reason_raw_text=admission_reason_raw_text,
                supervision_periods=[probation_period, parole_period],
            )
        )

        self.assertEqual(probation_period, pre_commitment_supervision_period)

    def test_us_nd_pre_commitment_supervision_period_probation_revocation_overlap(
        self,
    ) -> None:
        """Tests that we prioritize the recently terminated probation period over the
        one that is overlapping because the admission raw text is associated with a probation revocation."""
        admission_date = date(2019, 5, 25)
        admission_reason = StateIncarcerationPeriodAdmissionReason.REVOCATION
        admission_reason_raw_text = "NPROB"

        # Overlapping probation period
        overlapping_probation_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 6, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        # Terminated probation period
        terminated_probation_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 5, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        pre_commitment_supervision_period = (
            self._test_us_nd_pre_commitment_supervision_period(
                admission_date=admission_date,
                admission_reason=admission_reason,
                admission_reason_raw_text=admission_reason_raw_text,
                supervision_periods=[
                    terminated_probation_period,
                    overlapping_probation_period,
                ],
            )
        )

        self.assertEqual(terminated_probation_period, pre_commitment_supervision_period)

    def test_us_nd_pre_commitment_supervision_period_probation_revocation_rev_term(
        self,
    ) -> None:
        """Tests that we prioritize the probation period with a termination
        reason of REVOCATION."""
        admission_date = date(2019, 5, 25)
        admission_reason = StateIncarcerationPeriodAdmissionReason.REVOCATION
        admission_reason_raw_text = "NPROB"

        # Terminated revoked probation period
        revoked_probation_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 5, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        # Expired terminated probation period
        expired_probation_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 5, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.EXPIRATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        pre_commitment_supervision_period = (
            self._test_us_nd_pre_commitment_supervision_period(
                admission_date=admission_date,
                admission_reason=admission_reason,
                admission_reason_raw_text=admission_reason_raw_text,
                supervision_periods=[
                    expired_probation_period,
                    revoked_probation_period,
                ],
            )
        )

        self.assertEqual(revoked_probation_period, pre_commitment_supervision_period)

    def test_us_nd_pre_commitment_supervision_period_probation_revocation_closer(
        self,
    ) -> None:
        """Tests that we prioritize the overlapping probation period with a termination
        date that is closer to the admission_date."""
        admission_date = date(2019, 5, 25)
        admission_reason = StateIncarcerationPeriodAdmissionReason.REVOCATION
        admission_reason_raw_text = "NPROB"

        # Overlapping revoked probation period, 5 days after admission
        revoked_probation_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 6, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        # Overlapping revoked probation period, 1 day after admission
        closer_revoked_probation_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 5, 26),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        pre_commitment_supervision_period = (
            self._test_us_nd_pre_commitment_supervision_period(
                admission_date=admission_date,
                admission_reason=admission_reason,
                admission_reason_raw_text=admission_reason_raw_text,
                supervision_periods=[
                    closer_revoked_probation_period,
                    revoked_probation_period,
                ],
            )
        )

        self.assertEqual(
            closer_revoked_probation_period, pre_commitment_supervision_period
        )

    def test_us_nd_pre_commitment_supervision_period_no_periods(
        self,
    ) -> None:
        """Tests the situation where the person has no supervision periods."""
        admission_date = date(2019, 5, 25)
        admission_reason = StateIncarcerationPeriodAdmissionReason.REVOCATION
        admission_reason_raw_text = "NPROB"

        pre_commitment_supervision_period = (
            self._test_us_nd_pre_commitment_supervision_period(
                admission_date=admission_date,
                admission_reason=admission_reason,
                admission_reason_raw_text=admission_reason_raw_text,
                supervision_periods=[],
            )
        )

        self.assertIsNone(pre_commitment_supervision_period)


class TestGetPreIncarcerationSupervisionTypeFromIPAdmissionReason(unittest.TestCase):
    """Tests the get_pre_incarceration_supervision_type_from_ip_admission_reason function on the
    UsNdCommitmentFromSupervisionDelegate."""

    def setUp(self) -> None:
        self.delegate = UsNdCommitmentFromSupervisionDelegate()

    def _test_get_pre_incarceration_supervision_type_from_ip_admission_reason(
        self,
        admission_reason: StateIncarcerationPeriodAdmissionReason,
        admission_reason_raw_text: Optional[str],
    ) -> Optional[StateSupervisionPeriodSupervisionType]:
        return self.delegate.get_pre_incarceration_supervision_type_from_ip_admission_reason(
            admission_reason=admission_reason,
            admission_reason_raw_text=admission_reason_raw_text,
        )

    def test_us_nd_get_pre_incarceration_supervision_type_from_ip_admission_reason_not_commitment(
        self,
    ) -> None:
        admission_reason = StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION
        admission_reason_raw_text = None

        with self.assertRaises(ValueError):
            _: Optional[
                StateSupervisionPeriodSupervisionType
            ] = self._test_get_pre_incarceration_supervision_type_from_ip_admission_reason(
                admission_reason=admission_reason,
                admission_reason_raw_text=admission_reason_raw_text,
            )

    def test_us_nd_get_pre_incarceration_supervision_type_from_ip_admission_reason_parole_revocation(
        self,
    ) -> None:
        admission_reason = StateIncarcerationPeriodAdmissionReason.REVOCATION
        admission_reason_raw_text = "PARL"

        supervision_type: Optional[
            StateSupervisionPeriodSupervisionType
        ] = self._test_get_pre_incarceration_supervision_type_from_ip_admission_reason(
            admission_reason=admission_reason,
            admission_reason_raw_text=admission_reason_raw_text,
        )

        self.assertEqual(StateSupervisionPeriodSupervisionType.PAROLE, supervision_type)

    def test_us_nd_get_pre_incarceration_supervision_type_from_ip_admission_reason_parole_revocation_with_prefix(
        self,
    ) -> None:
        admission_reason = StateIncarcerationPeriodAdmissionReason.REVOCATION
        admission_reason_raw_text = f"{PAROLE_REVOCATION_PREPROCESSING_PREFIX}-ABC"

        supervision_type: Optional[
            StateSupervisionPeriodSupervisionType
        ] = self._test_get_pre_incarceration_supervision_type_from_ip_admission_reason(
            admission_reason=admission_reason,
            admission_reason_raw_text=admission_reason_raw_text,
        )

        self.assertEqual(StateSupervisionPeriodSupervisionType.PAROLE, supervision_type)

    def test_us_nd_get_pre_incarceration_supervision_type_from_ip_admission_reason_probation_revocation(
        self,
    ) -> None:
        admission_reason = StateIncarcerationPeriodAdmissionReason.REVOCATION
        admission_reason_raw_text = "NPRB"

        supervision_type: Optional[
            StateSupervisionPeriodSupervisionType
        ] = self._test_get_pre_incarceration_supervision_type_from_ip_admission_reason(
            admission_reason=admission_reason,
            admission_reason_raw_text=admission_reason_raw_text,
        )

        self.assertEqual(
            StateSupervisionPeriodSupervisionType.PROBATION, supervision_type
        )

    def test_us_nd_get_pre_incarceration_supervision_type_from_ip_admission_reason_probation_prb(
        self,
    ) -> None:
        admission_reason = StateIncarcerationPeriodAdmissionReason.REVOCATION
        admission_reason_raw_text = "PRB"

        supervision_type: Optional[
            StateSupervisionPeriodSupervisionType
        ] = self._test_get_pre_incarceration_supervision_type_from_ip_admission_reason(
            admission_reason=admission_reason,
            admission_reason_raw_text=admission_reason_raw_text,
        )

        self.assertEqual(
            StateSupervisionPeriodSupervisionType.PROBATION, supervision_type
        )

    def test_us_nd_get_pre_incarceration_supervision_type_from_ip_admission_reason_probation_revocation_with_prefix(
        self,
    ) -> None:
        admission_reason = StateIncarcerationPeriodAdmissionReason.REVOCATION
        admission_reason_raw_text = f"{PROBATION_REVOCATION_PREPROCESSING_PREFIX}-ABC"

        supervision_type: Optional[
            StateSupervisionPeriodSupervisionType
        ] = self._test_get_pre_incarceration_supervision_type_from_ip_admission_reason(
            admission_reason=admission_reason,
            admission_reason_raw_text=admission_reason_raw_text,
        )

        self.assertEqual(
            StateSupervisionPeriodSupervisionType.PROBATION, supervision_type
        )

    def test_us_nd_get_pre_incarceration_supervision_type_from_ip_admission_reason_not_present_in_list(
        self,
    ) -> None:
        admission_reason = StateIncarcerationPeriodAdmissionReason.REVOCATION
        admission_reason_raw_text = "ABC"

        with self.assertRaises(ValueError):
            _: Optional[
                StateSupervisionPeriodSupervisionType
            ] = self._test_get_pre_incarceration_supervision_type_from_ip_admission_reason(
                admission_reason=admission_reason,
                admission_reason_raw_text=admission_reason_raw_text,
            )


class TestPreCommitmentSupervisionTypeIdentification(unittest.TestCase):
    """Tests the _get_commitment_from_supervision_supervision_period function on the
    UsNdCommitmentFromSupervisionDelegate."""

    def setUp(self) -> None:
        self.delegate = UsNdCommitmentFromSupervisionDelegate()

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

    def test_us_nd_get_pre_commitment_supervision_type_default(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_ND",
            facility="NDSP",
            admission_date=date(2008, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_reason_raw_text="PARL",
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
