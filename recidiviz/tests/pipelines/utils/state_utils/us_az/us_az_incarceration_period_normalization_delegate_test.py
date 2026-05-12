#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2023 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Tests us_az_incarceration_period_normalization_delegate.py."""
import unittest
from datetime import date

from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateSupervisionPeriod,
)
from recidiviz.pipelines.utils.state_utils.us_az.us_az_incarceration_period_normalization_delegate import (
    UsAzIncarcerationNormalizationDelegate,
)
from recidiviz.tests.pipelines.utils.entity_normalization.normalization_testing_utils import (
    default_normalized_sp_index_for_tests,
)

_STATE_CODE = StateCode.US_AZ.value


class TestUsAzIncarcerationNormalizationDelegate(unittest.TestCase):
    """Tests functions in TestUsAzIncarcerationNormalizationDelegate."""

    def setUp(self) -> None:
        self.delegate = UsAzIncarcerationNormalizationDelegate()

    def _build_ip(
        self,
        admission_date: date,
        admission_reason: StateIncarcerationPeriodAdmissionReason,
        admission_reason_raw_text: str,
        external_id: str = "ip-1",
        release_date: date | None = None,
    ) -> StateIncarcerationPeriod:
        return StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=external_id,
            admission_date=admission_date,
            admission_reason=admission_reason,
            admission_reason_raw_text=admission_reason_raw_text,
            release_date=release_date,
        )

    def _build_sp(
        self,
        start_date: date,
        termination_date: date,
        sequence_num: int = 0,
    ) -> NormalizedStateSupervisionPeriod:
        return NormalizedStateSupervisionPeriod(
            supervision_period_id=sequence_num + 1,
            external_id=f"sp-{sequence_num}",
            state_code=_STATE_CODE,
            start_date=start_date,
            termination_date=termination_date,
            termination_reason=StateSupervisionPeriodTerminationReason.ADMITTED_TO_INCARCERATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            sequence_num=sequence_num,
        )

    def test_normalize_period_recommitment_with_preceding_sp_becomes_revocation(
        self,
    ) -> None:
        ip = self._build_ip(
            admission_date=date(2022, 6, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="RECOMMITMENT",
        )
        sp = self._build_sp(
            start_date=date(2022, 1, 1), termination_date=date(2022, 5, 15)
        )
        sp_index = default_normalized_sp_index_for_tests(supervision_periods=[sp])

        result = self.delegate.normalize_period_if_commitment_from_supervision(
            incarceration_period_list_index=0,
            sorted_incarceration_periods=[ip],
            original_sorted_incarceration_periods=[ip],
            supervision_period_index=sp_index,
        )

        self.assertEqual(
            result.admission_reason,
            StateIncarcerationPeriodAdmissionReason.REVOCATION,
        )

    def test_normalize_period_new_commitment_with_preceding_sp_becomes_revocation(
        self,
    ) -> None:
        ip = self._build_ip(
            admission_date=date(2022, 6, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="NEW COMMITMENT",
        )
        sp = self._build_sp(
            start_date=date(2022, 1, 1), termination_date=date(2022, 5, 15)
        )
        sp_index = default_normalized_sp_index_for_tests(supervision_periods=[sp])

        result = self.delegate.normalize_period_if_commitment_from_supervision(
            incarceration_period_list_index=0,
            sorted_incarceration_periods=[ip],
            original_sorted_incarceration_periods=[ip],
            supervision_period_index=sp_index,
        )

        self.assertEqual(
            result.admission_reason,
            StateIncarcerationPeriodAdmissionReason.REVOCATION,
        )

    def test_normalize_period_recommitment_no_preceding_sp_unchanged(self) -> None:
        ip = self._build_ip(
            admission_date=date(2022, 6, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="RECOMMITMENT",
        )
        sp_index = default_normalized_sp_index_for_tests(supervision_periods=[])

        result = self.delegate.normalize_period_if_commitment_from_supervision(
            incarceration_period_list_index=0,
            sorted_incarceration_periods=[ip],
            original_sorted_incarceration_periods=[ip],
            supervision_period_index=sp_index,
        )

        self.assertEqual(
            result.admission_reason,
            StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

    def test_normalize_period_other_raw_text_unchanged(self) -> None:
        ip = self._build_ip(
            admission_date=date(2022, 6, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_reason_raw_text="Community Supervision Revoked",
        )
        sp = self._build_sp(
            start_date=date(2022, 1, 1), termination_date=date(2022, 5, 15)
        )
        sp_index = default_normalized_sp_index_for_tests(supervision_periods=[sp])

        result = self.delegate.normalize_period_if_commitment_from_supervision(
            incarceration_period_list_index=0,
            sorted_incarceration_periods=[ip],
            original_sorted_incarceration_periods=[ip],
            supervision_period_index=sp_index,
        )

        self.assertEqual(
            result.admission_reason,
            StateIncarcerationPeriodAdmissionReason.REVOCATION,
        )

    def test_get_incarceration_admission_violation_type_recommitment_returns_felony(
        self,
    ) -> None:
        ip = self._build_ip(
            admission_date=date(2022, 6, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_reason_raw_text="RECOMMITMENT",
        )

        result = self.delegate.get_incarceration_admission_violation_type(ip)

        self.assertEqual(result, StateSupervisionViolationType.FELONY)

    def test_get_incarceration_admission_violation_type_new_commitment_returns_felony(
        self,
    ) -> None:
        ip = self._build_ip(
            admission_date=date(2022, 6, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_reason_raw_text="NEW COMMITMENT",
        )

        result = self.delegate.get_incarceration_admission_violation_type(ip)

        self.assertEqual(result, StateSupervisionViolationType.FELONY)

    def test_get_incarceration_admission_violation_type_other_raw_text_returns_none(
        self,
    ) -> None:
        ip = self._build_ip(
            admission_date=date(2022, 6, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_reason_raw_text="Community Supervision Revoked",
        )

        result = self.delegate.get_incarceration_admission_violation_type(ip)

        self.assertIsNone(result)
