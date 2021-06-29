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
"""Tests the functions in us_id_commitment_from_supervision_utils.py"""
import unittest
from datetime import date
from typing import List, Optional

import attr

from recidiviz.calculator.pipeline.utils.commitment_from_supervision_utils import (
    get_commitment_from_supervision_supervision_period,
)
from recidiviz.calculator.pipeline.utils.pre_processed_incarceration_period_index import (
    PreProcessedIncarcerationPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.pre_processed_supervision_period_index import (
    PreProcessedSupervisionPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_commitment_from_supervision_utils import (
    UsIdCommitmentFromSupervisionDelegate,
    us_id_normalize_period_if_commitment_from_supervision,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateIncarcerationPeriodStatus,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodStatus,
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionPeriod,
)


class TestUsIdNormalizePeriodIfCommitmentFromSupervision(unittest.TestCase):
    """Tests the us_id_normalize_period_if_commitment_from_supervision function."""

    @staticmethod
    def _normalize_period_if_commitment_from_supervision(
        incarceration_period: StateIncarcerationPeriod,
        sorted_incarceration_periods: Optional[List[StateIncarcerationPeriod]] = None,
        supervision_periods: Optional[List[StateSupervisionPeriod]] = None,
    ) -> StateIncarcerationPeriod:
        sorted_incarceration_periods = sorted_incarceration_periods or [
            incarceration_period
        ]
        supervision_period_index = PreProcessedSupervisionPeriodIndex(
            supervision_periods or []
        )

        sorted_incarceration_periods.index(incarceration_period)

        return us_id_normalize_period_if_commitment_from_supervision(
            incarceration_period_list_index=sorted_incarceration_periods.index(
                incarceration_period
            ),
            sorted_incarceration_periods=sorted_incarceration_periods,
            supervision_period_index=supervision_period_index,
        )

    def test_us_id_normalize_period_if_commitment_from_supervision_probation_revocation(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=111,
            start_date=date(2017, 1, 1),
            termination_date=date(2017, 5, 17),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            release_date=date(2019, 3, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        expected_period = attr.evolve(
            incarceration_revocation_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
        )

        updated_period = self._normalize_period_if_commitment_from_supervision(
            incarceration_revocation_period, supervision_periods=[supervision_period]
        )

        self.assertEqual(expected_period, updated_period)

    def test_us_id_normalize_period_if_commitment_from_supervision_treatment(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=111,
            start_date=date(2017, 1, 1),
            termination_date=date(2017, 5, 17),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            release_date=date(2019, 3, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
        )

        expected_period = attr.evolve(
            incarceration_revocation_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
        )

        updated_period = self._normalize_period_if_commitment_from_supervision(
            incarceration_revocation_period, supervision_periods=[supervision_period]
        )

        self.assertEqual(expected_period, updated_period)

    def test_us_id_sanction_admission_shock_incarceration(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=111,
            start_date=date(2017, 1, 1),
            termination_date=date(2017, 5, 17),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        shock_incarceration_admission = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            release_date=date(2019, 3, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
        )

        with self.assertRaises(ValueError):
            # We don't expect to see SHOCK_INCARCERATION sanction admissions in US_ID
            _ = self._normalize_period_if_commitment_from_supervision(
                shock_incarceration_admission, supervision_periods=[supervision_period]
            )

    def test_us_id_normalize_period_if_commitment_from_supervision_parole_board_revocation(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=111,
            start_date=date(2017, 1, 1),
            termination_date=date(2017, 5, 17),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        board_hold_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            release_date=date(2017, 5, 29),
            release_reason=StateIncarcerationPeriodReleaseReason.STATUS_CHANGE,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
        )

        incarceration_revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 29),
            admission_reason=StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE,
            release_date=date(2018, 5, 29),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        expected_period = attr.evolve(
            incarceration_revocation_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
        )

        updated_period = self._normalize_period_if_commitment_from_supervision(
            incarceration_revocation_period,
            sorted_incarceration_periods=[
                board_hold_period,
                incarceration_revocation_period,
            ],
            supervision_periods=[supervision_period],
        )

        self.assertEqual(expected_period, updated_period)

    def test_us_id_normalize_period_if_commitment_from_supervision_parole_board_to_treatment_revocation(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=111,
            start_date=date(2017, 1, 1),
            termination_date=date(2017, 5, 17),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        board_hold_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            release_date=date(2017, 5, 29),
            release_reason=StateIncarcerationPeriodReleaseReason.STATUS_CHANGE,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
        )

        incarceration_revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 29),
            admission_reason=StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE,
            release_date=date(2018, 5, 29),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
        )

        expected_period = attr.evolve(
            incarceration_revocation_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
        )

        updated_period = self._normalize_period_if_commitment_from_supervision(
            incarceration_revocation_period,
            sorted_incarceration_periods=[
                board_hold_period,
                incarceration_revocation_period,
            ],
            supervision_periods=[supervision_period],
        )

        self.assertEqual(expected_period, updated_period)

    def test_us_id_normalize_period_if_commitment_from_supervision_treatment_transfer_not_revocation(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=111,
            start_date=date(2017, 1, 1),
            termination_date=date(2017, 5, 17),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        treatment_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            release_date=date(2017, 5, 29),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
        )

        transfer_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 29),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2018, 5, 29),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        period_copy = attr.evolve(transfer_incarceration_period)

        updated_period = self._normalize_period_if_commitment_from_supervision(
            transfer_incarceration_period,
            sorted_incarceration_periods=[
                treatment_period,
                transfer_incarceration_period,
            ],
            supervision_periods=[supervision_period],
        )

        self.assertEqual(period_copy, updated_period)

    def test_us_id_normalize_period_if_commitment_from_supervision_transfers_not_same_day(
        self,
    ) -> None:
        treatment_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            release_date=date(2017, 5, 29),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
        )

        general_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2018, 9, 29),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2019, 9, 29),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        period_copy = attr.evolve(general_period)

        updated_period = self._normalize_period_if_commitment_from_supervision(
            general_period,
            sorted_incarceration_periods=[
                treatment_period,
                general_period,
            ],
        )

        self.assertEqual(period_copy, updated_period)

    def test_us_id_normalize_period_if_commitment_from_supervision_no_revocation(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2019, 5, 29),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        period_copy = attr.evolve(incarceration_period)

        updated_period = self._normalize_period_if_commitment_from_supervision(
            incarceration_period,
        )

        self.assertEqual(period_copy, updated_period)

    def test_us_id_normalize_period_if_commitment_from_supervision_transfer_admission(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2019, 5, 29),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        period_copy = attr.evolve(
            incarceration_period,
        )

        updated_period = self._normalize_period_if_commitment_from_supervision(
            incarceration_period
        )

        self.assertEqual(period_copy, updated_period)

    def test_us_id_normalize_period_if_commitment_from_supervision_regular_transfer(
        self,
    ) -> None:
        first_general_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2017, 5, 29),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        second_general_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 29),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2018, 5, 29),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        period_copy = attr.evolve(second_general_period)

        updated_period = self._normalize_period_if_commitment_from_supervision(
            second_general_period,
            sorted_incarceration_periods=[
                first_general_period,
                second_general_period,
            ],
        )

        self.assertEqual(period_copy, updated_period)

    def test_us_id_normalize_period_if_commitment_from_supervision_investigation_not_revocation(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=111,
            start_date=date(2017, 1, 1),
            termination_date=date(2017, 5, 17),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            release_date=date(2019, 3, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        expected_period = attr.evolve(
            incarceration_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        updated_period = self._normalize_period_if_commitment_from_supervision(
            incarceration_period,
            supervision_periods=[supervision_period],
        )

        self.assertEqual(expected_period, updated_period)


class TestPreCommitmentSupervisionPeriod(unittest.TestCase):
    """Tests the get_commitment_from_supervision_supervision_period function when
    the UsIdCommitmentFromSupervisionDelegate is provided."""

    @staticmethod
    def _test_us_id_pre_commitment_supervision_period(
        admission_date: date,
        admission_reason: StateIncarcerationPeriodAdmissionReason,
        supervision_periods: List[StateSupervisionPeriod],
    ) -> Optional[StateSupervisionPeriod]:
        ip = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_ID",
            incarceration_period_id=111,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            admission_date=admission_date,
            admission_reason=admission_reason,
        )

        incarceration_periods = [ip]

        return get_commitment_from_supervision_supervision_period(
            incarceration_period=ip,
            supervision_periods=supervision_periods,
            commitment_from_supervision_delegate=UsIdCommitmentFromSupervisionDelegate(),
            incarceration_period_index=PreProcessedIncarcerationPeriodIndex(
                incarceration_periods
            ),
        )

    def test_us_id_pre_commitment_supervision_period(self) -> None:
        supervision_period_set = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ID",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        supervision_period_unset = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ID",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=None,
        )

        supervision_periods = [supervision_period_set, supervision_period_unset]

        self.assertEqual(
            supervision_period_set,
            self._test_us_id_pre_commitment_supervision_period(
                admission_date=date(2017, 5, 11),
                admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
                supervision_periods=supervision_periods,
            ),
        )

    def test_us_id_pre_commitment_supervision_period_internal_unknown(
        self,
    ) -> None:
        supervision_period_set = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ID",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        supervision_period_unset = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ID",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN,
        )

        supervision_periods = [supervision_period_set, supervision_period_unset]

        self.assertEqual(
            supervision_period_set,
            self._test_us_id_pre_commitment_supervision_period(
                admission_date=date(2017, 5, 11),
                admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
                supervision_periods=supervision_periods,
            ),
        )
